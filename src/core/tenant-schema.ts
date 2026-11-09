/**
 * Schema-per-tenant isolation.
 *
 * Some customers keep each tenant in its own schema with identical tables,
 * e.g. `group_4048_indexed`, `group_4049_indexed`. Knowledge and generated SQL
 * use one template schema. At execution time we:
 *  1. rewrite every schema qualifier that matches the template pattern to the
 *     caller's tenant schema, and
 *  2. fail closed: refuse SQL that still mentions another tenant's schema, or
 *     reads a relation that is not in the tenant schema, a shared schema or a CTE.
 */

export interface TenantSchemaOptions {
	/** Schema name with one `{tenant}` placeholder, e.g. `group_{tenant}_indexed`. */
	template: string;
	tenantId: string;
	/** Extra schemas every tenant may read (lookup tables etc.). */
	sharedSchemas?: string[];
}

export class TenantSchemaError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TenantSchemaError";
	}
}

const PLACEHOLDER = "{tenant}";
const TEMPLATE_PART = /^[a-z0-9_]*$/;
const TENANT_ID = /^[A-Za-z0-9_]{1,64}$/;

interface ParsedTemplate {
	prefix: string;
	suffix: string;
}

export function parseTenantSchemaTemplate(template: string): ParsedTemplate {
	const value = template.trim().toLowerCase();
	const index = value.indexOf(PLACEHOLDER);
	if (index < 0 || value.indexOf(PLACEHOLDER, index + 1) >= 0) {
		throw new TenantSchemaError(
			`Tenant schema template must contain ${PLACEHOLDER} exactly once`,
		);
	}
	const prefix = value.slice(0, index);
	const suffix = value.slice(index + PLACEHOLDER.length);
	if (!TEMPLATE_PART.test(prefix) || !TEMPLATE_PART.test(suffix) || !(prefix || suffix)) {
		throw new TenantSchemaError(
			"Tenant schema template may only use a-z, 0-9 and _ around {tenant}",
		);
	}
	return { prefix, suffix };
}

/** The schema a tenant's queries run against. Throws for unsafe tenant ids. */
export function resolveTenantSchema(template: string, tenantId: string): string {
	const { prefix, suffix } = parseTenantSchemaTemplate(template);
	const id = tenantId.trim();
	if (!TENANT_ID.test(id)) {
		throw new TenantSchemaError(
			"tenantId must be 1-64 characters of letters, digits or _ for schema-per-tenant isolation",
		);
	}
	return `${prefix}${id.toLowerCase()}${suffix}`;
}

type Token =
	| { kind: "word"; text: string; quoted: boolean; start: number; end: number }
	| { kind: "punct"; text: string; start: number; end: number };

/** Words (bare or "quoted") and punctuation; strings, comments and numbers are skipped. */
function tokenize(sql: string): Token[] {
	const tokens: Token[] = [];
	let i = 0;
	while (i < sql.length) {
		const ch = sql[i] as string;
		const next = sql[i + 1];
		if (/\s/.test(ch)) {
			i++;
		} else if (ch === "-" && next === "-") {
			while (i < sql.length && sql[i] !== "\n") i++;
		} else if (ch === "/" && next === "*") {
			const end = sql.indexOf("*/", i + 2);
			i = end < 0 ? sql.length : end + 2;
		} else if (ch === "'") {
			i++;
			while (i < sql.length) {
				if (sql[i] === "\\") i += 2;
				else if (sql[i] === "'" && sql[i + 1] === "'") i += 2;
				else if (sql[i] === "'") break;
				else i++;
			}
			i++;
		} else if (ch === "$" && /[A-Za-z_$]/.test(next ?? "")) {
			const tag = /^\$[A-Za-z_]*\$/.exec(sql.slice(i))?.[0];
			if (tag) {
				const end = sql.indexOf(tag, i + tag.length);
				i = end < 0 ? sql.length : end + tag.length;
			} else {
				i++;
			}
		} else if (ch === '"') {
			const start = i;
			let text = "";
			i++;
			while (i < sql.length) {
				if (sql[i] === '"' && sql[i + 1] === '"') {
					text += '"';
					i += 2;
				} else if (sql[i] === '"') break;
				else text += sql[i++];
			}
			i++;
			tokens.push({ kind: "word", text, quoted: true, start, end: i });
		} else if (/[A-Za-z_]/.test(ch)) {
			const start = i;
			while (i < sql.length && /[A-Za-z0-9_$]/.test(sql[i] as string)) i++;
			tokens.push({ kind: "word", text: sql.slice(start, i), quoted: false, start, end: i });
		} else if (/[0-9]/.test(ch)) {
			while (i < sql.length && /[0-9.eE]/.test(sql[i] as string)) i++;
		} else {
			tokens.push({ kind: "punct", text: ch, start: i, end: i + 1 });
			i++;
		}
	}
	return tokens;
}

const name = (t: Token | undefined) =>
	t?.kind === "word" ? (t.quoted ? t.text : t.text.toLowerCase()) : undefined;
const isKeyword = (t: Token | undefined, word: string) =>
	t?.kind === "word" && !t.quoted && t.text.toLowerCase() === word;
const isPunct = (t: Token | undefined, p: string) => t?.kind === "punct" && t.text === p;

/** Words that end a FROM list item (so an alias is not mistaken for a table). */
const CLAUSE_WORDS = new Set([
	"where", "group", "order", "limit", "having", "union", "intersect", "except",
	"join", "inner", "left", "right", "full", "cross", "natural", "on", "using",
	"window", "qualify", "offset", "fetch", "for", "lateral", "as",
]);

/**
 * Rewrite template-schema qualifiers to the tenant's schema and verify the
 * result reads only tenant, shared or CTE relations. Throws TenantSchemaError.
 */
export function rewriteTenantSchema(sql: string, options: TenantSchemaOptions): string {
	const { prefix, suffix } = parseTenantSchemaTemplate(options.template);
	const tenantSchema = resolveTenantSchema(options.template, options.tenantId);
	const isTemplateSchema = (value: string) =>
		value.length > prefix.length + suffix.length &&
		value.startsWith(prefix) &&
		value.endsWith(suffix) &&
		/^[a-z0-9_]+$/.test(value.slice(prefix.length, value.length - suffix.length));

	// 1. Rewrite `<template schema>.` qualifiers.
	const tokens = tokenize(sql);
	let out = "";
	let cursor = 0;
	for (let k = 0; k < tokens.length; k++) {
		const token = tokens[k] as Token;
		const value = name(token);
		if (!value || !isPunct(tokens[k + 1], ".")) continue;
		if (!isTemplateSchema(value.toLowerCase()) || value.toLowerCase() === tenantSchema) continue;
		out += sql.slice(cursor, token.start) + (token.kind === "word" && token.quoted ? `"${tenantSchema}"` : tenantSchema);
		cursor = token.end;
	}
	const rewritten = out + sql.slice(cursor);

	// 2a. No other tenant's schema may remain anywhere (strings and comments included).
	const pattern = new RegExp(`${escapeRegex(prefix)}([a-z0-9_]+)${escapeRegex(suffix)}`, "gi");
	for (const match of rewritten.matchAll(pattern)) {
		if (match[0].toLowerCase() !== tenantSchema) {
			throw new TenantSchemaError("Query references another tenant's schema");
		}
	}

	// 2b. Every relation read must be tenant-schema, shared-schema or a CTE.
	assertRelationsAllowed(tokenize(rewritten), tenantSchema, options.sharedSchemas ?? []);
	return rewritten;
}

function assertRelationsAllowed(tokens: Token[], tenantSchema: string, sharedSchemas: string[]) {
	const allowedSchemas = new Set([tenantSchema, ...sharedSchemas.map((s) => s.trim().toLowerCase())]);
	const cteNames = new Set<string>();
	for (let k = 0; k + 2 < tokens.length; k++) {
		// `name AS (` and `name (cols) AS (` inside WITH
		const value = name(tokens[k]);
		if (!value) continue;
		if (isKeyword(tokens[k + 1], "as") && isPunct(tokens[k + 2], "(")) cteNames.add(value);
		if (isPunct(tokens[k + 1], "(")) {
			let j = k + 2;
			while (j < tokens.length && !isPunct(tokens[j], ")")) j++;
			if (isKeyword(tokens[j + 1], "as") && isPunct(tokens[j + 2], "(")) cteNames.add(value);
		}
	}

	const check = (start: number): number => {
		// Returns the index after the relation (and its alias).
		let k = start;
		if (isKeyword(tokens[k], "lateral")) k++;
		if (isPunct(tokens[k], "(")) return k; // subquery: checked as its own SELECT
		const parts: string[] = [];
		const first = name(tokens[k]);
		if (!first) return k;
		parts.push(first);
		k++;
		while (isPunct(tokens[k], ".") && name(tokens[k + 1])) {
			parts.push(name(tokens[k + 1]) as string);
			k += 2;
		}
		if (isPunct(tokens[k], "(")) return k; // table function, e.g. generate_series(...)
		const relation = parts.join(".");
		if (parts.length === 1) {
			if (!cteNames.has(parts[0] as string)) {
				throw new TenantSchemaError(
					`Table "${relation}" must be schema-qualified with the tenant schema`,
				);
			}
		} else {
			const schema = (parts[parts.length - 2] as string).toLowerCase();
			if (parts.length > 3 || !allowedSchemas.has(schema)) {
				throw new TenantSchemaError(`Table "${relation}" is outside the tenant schema`);
			}
		}
		if (isKeyword(tokens[k], "as")) k++;
		const alias = tokens[k];
		if (alias?.kind === "word" && !(alias.quoted === false && CLAUSE_WORDS.has(alias.text.toLowerCase()))) k++;
		return k;
	};

	// A FROM counts only at a depth where a SELECT was seen (skips EXTRACT(x FROM y) etc.).
	const selectAtDepth: boolean[] = [false];
	for (let k = 0; k < tokens.length; k++) {
		const token = tokens[k];
		if (isPunct(token, "(")) selectAtDepth.push(false);
		else if (isPunct(token, ")")) selectAtDepth.length > 1 && selectAtDepth.pop();
		else if (isKeyword(token, "select")) selectAtDepth[selectAtDepth.length - 1] = true;
		else if (isKeyword(token, "join")) k = check(k + 1) - 1;
		else if (
			isKeyword(token, "from") &&
			selectAtDepth[selectAtDepth.length - 1] &&
			!isKeyword(tokens[k - 1], "distinct") // IS [NOT] DISTINCT FROM
		) {
			let j = check(k + 1);
			while (isPunct(tokens[j], ",")) j = check(j + 1);
			k = j - 1;
		}
	}
}

function escapeRegex(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
