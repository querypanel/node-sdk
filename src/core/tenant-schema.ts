/**
 * Schema-per-tenant isolation.
 *
 * Some customers keep each tenant in its own schema with identical tables,
 * e.g. `group_4048_indexed`, `group_4049_indexed`. Knowledge and generated SQL
 * use one template schema. At execution time the SQL is rewritten to the
 * caller's tenant schema (`rewriteTenantSchema`); which relations it may read is
 * enforced by the adapter's allowlist (`tenantSchemaAllows`).
 */
import { identifier, isPunct, tokenize } from "./sql-relations";

/** Attach option for schema-per-tenant isolation. */
export interface TenantSchemaConfig {
	/** Schema name with one `{tenant}` placeholder, e.g. `group_{tenant}_indexed`. */
	template: string;
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

/** True for any schema the template can produce (`group_4048_indexed`, `group_x_indexed`, …). */
function matchesTemplate({ prefix, suffix }: ParsedTemplate, schema: string): boolean {
	const value = schema.toLowerCase();
	return (
		value.length > prefix.length + suffix.length &&
		value.startsWith(prefix) &&
		value.endsWith(suffix) &&
		/^[a-z0-9_]+$/.test(value.slice(prefix.length, value.length - suffix.length))
	);
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

/**
 * Rewrite every template-schema qualifier to the tenant's schema. Fails closed when
 * another tenant's schema is still named anywhere (strings and comments included).
 */
export function rewriteTenantSchema(sql: string, template: string, tenantId: string): string {
	const parsed = parseTenantSchemaTemplate(template);
	const tenantSchema = resolveTenantSchema(template, tenantId);

	const tokens = tokenize(sql);
	let rewritten = "";
	let cursor = 0;
	for (let k = 0; k < tokens.length; k++) {
		const token = tokens[k];
		const value = identifier(token);
		if (!token || !value || !isPunct(tokens[k + 1], ".")) continue;
		if (!matchesTemplate(parsed, value) || value.toLowerCase() === tenantSchema) continue;
		const replacement = token.kind === "word" && token.quoted ? `"${tenantSchema}"` : tenantSchema;
		rewritten += sql.slice(cursor, token.start) + replacement;
		cursor = token.end;
	}
	rewritten += sql.slice(cursor);

	const pattern = new RegExp(`${escapeRegex(parsed.prefix)}[a-z0-9_]+${escapeRegex(parsed.suffix)}`, "gi");
	for (const match of rewritten.matchAll(pattern)) {
		if (match[0].toLowerCase() !== tenantSchema) {
			throw new TenantSchemaError("Query references another tenant's schema");
		}
	}
	return rewritten;
}

/** Allowlist key that stands for "the tenant's own schema". */
export const TENANT_SCHEMA_KEY = PLACEHOLDER;

/**
 * Allowlist view of a schema name: every schema the template can produce maps to
 * TENANT_SCHEMA_KEY, so one allowlist (`group_4048_indexed.*`, or the default
 * `{tenant}.*`) covers every tenant. The rewrite has already pinned template
 * schemas to the caller's own schema before the allowlist is checked.
 */
export function tenantSchemaKey(template: string): (schema: string) => string {
	const parsed = parseTenantSchemaTemplate(template);
	return (schema) => (matchesTemplate(parsed, schema) ? TENANT_SCHEMA_KEY : schema.toLowerCase());
}

function escapeRegex(value: string): string {
	return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}
