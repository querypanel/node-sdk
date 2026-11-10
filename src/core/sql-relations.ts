/**
 * The relations (tables/views) a SQL statement reads, for allowlist checks.
 *
 * A small tokenizer, not a parser: strings, comments and dollar quotes are
 * skipped, quoted identifiers are kept as words. A relation is the name after
 * FROM (at a level where a SELECT was seen, so `EXTRACT(year FROM d)` does not
 * count) or JOIN, plus the comma-separated items of a FROM list. CTE names,
 * subqueries and table functions are not relations.
 */

export type Token =
	| { kind: "word"; text: string; quoted: boolean; start: number; end: number }
	| { kind: "punct"; text: string; start: number; end: number };

export interface Relation {
	/** Name parts as written: `[table]`, `[schema, table]` or `[database, schema, table]`. */
	parts: string[];
}

export function tokenize(sql: string): Token[] {
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

/** Identifier value: unquoted words fold to lower case, quoted words keep their case. */
export const identifier = (t: Token | undefined) =>
	t?.kind === "word" ? (t.quoted ? t.text : t.text.toLowerCase()) : undefined;
const isKeyword = (t: Token | undefined, word: string) =>
	t?.kind === "word" && !t.quoted && t.text.toLowerCase() === word;
export const isPunct = (t: Token | undefined, p: string) => t?.kind === "punct" && t.text === p;

/** Words that end a FROM list item (so an alias is not mistaken for a table). */
const CLAUSE_WORDS = new Set([
	"where", "group", "order", "limit", "having", "union", "intersect", "except",
	"join", "inner", "left", "right", "full", "cross", "natural", "on", "using",
	"window", "qualify", "offset", "fetch", "for", "lateral", "as",
]);

/** Keywords that end a FROM clause at the same depth. */
const FROM_CLAUSE_END = new Set([
	"where", "group", "having", "order", "limit", "offset", "fetch", "union", "intersect",
	"except", "window", "qualify", "for", "returning",
]);

function cteNames(tokens: Token[]): Set<string> {
	// `name AS (` and `name (cols) AS (` inside WITH
	const names = new Set<string>();
	for (let k = 0; k + 2 < tokens.length; k++) {
		const value = identifier(tokens[k]);
		if (!value) continue;
		if (isKeyword(tokens[k + 1], "as") && isPunct(tokens[k + 2], "(")) names.add(value);
		if (isPunct(tokens[k + 1], "(")) {
			let j = k + 2;
			while (j < tokens.length && !isPunct(tokens[j], ")")) j++;
			if (isKeyword(tokens[j + 1], "as") && isPunct(tokens[j + 2], "(")) names.add(value);
		}
	}
	return names;
}

export function findRelations(sql: string): Relation[] {
	const tokens = tokenize(sql);
	const ctes = cteNames(tokens);
	const relations: Relation[] = [];

	/** Reads one FROM/JOIN item at `start`; returns the index after it (and its alias). */
	const readItem = (start: number): number => {
		let k = start;
		if (isKeyword(tokens[k], "lateral")) k++;
		const first = identifier(tokens[k]);
		if (!first) return k; // subquery `(`: its own SELECT is scanned in turn
		const parts = [first];
		k++;
		while (isPunct(tokens[k], ".") && identifier(tokens[k + 1])) {
			parts.push(identifier(tokens[k + 1]) as string);
			k += 2;
		}
		if (isPunct(tokens[k], "(")) return k; // table function, e.g. generate_series(...)
		if (!(parts.length === 1 && ctes.has(first))) relations.push({ parts });
		if (isKeyword(tokens[k], "as")) k++;
		const alias = tokens[k];
		if (alias?.kind === "word" && (alias.quoted || !CLAUSE_WORDS.has(alias.text.toLowerCase()))) k++;
		return k;
	};

	// Per parenthesis depth: whether a SELECT was seen, and whether we are inside its FROM
	// clause (where every top-level comma, even after a JOIN ... ON condition, starts an item).
	const selectAtDepth: boolean[] = [false];
	const inFromAtDepth: boolean[] = [false];
	const top = () => selectAtDepth.length - 1;
	for (let k = 0; k < tokens.length; k++) {
		const token = tokens[k];
		if (isPunct(token, "(")) {
			selectAtDepth.push(false);
			inFromAtDepth.push(false);
		} else if (isPunct(token, ")")) {
			if (selectAtDepth.length > 1) {
				selectAtDepth.pop();
				inFromAtDepth.pop();
			}
		} else if (isKeyword(token, "select")) {
			selectAtDepth[top()] = true;
			inFromAtDepth[top()] = false;
		} else if (isKeyword(token, "join")) {
			k = readItem(k + 1) - 1;
		} else if (
			isKeyword(token, "from") &&
			selectAtDepth[top()] &&
			!isKeyword(tokens[k - 1], "distinct") // IS [NOT] DISTINCT FROM
		) {
			inFromAtDepth[top()] = true;
			k = readItem(k + 1) - 1;
		} else if (isPunct(token, ",") && inFromAtDepth[top()]) {
			k = readItem(k + 1) - 1;
		} else if (token?.kind === "word" && !token.quoted && FROM_CLAUSE_END.has(token.text.toLowerCase())) {
			inFromAtDepth[top()] = false;
		}
	}
	return relations;
}
