import type {
	ColumnSchema,
	IntrospectOptions,
	SchemaIntrospection,
	TableSchema,
} from "../schema/types";
import type { DatabaseAdapter, DatabaseExecutionResult } from "./types";

export interface PostgresQueryResult {
	rows: Array<Record<string, unknown>>;
	fields: Array<{ name: string }>;
}

export type PostgresClientFn = (
	sql: string,
	params?: unknown[],
) => Promise<PostgresQueryResult>;

export interface PostgresAdapterOptions {
	/** Logical database name used in introspection metadata. */
	database?: string;
	/** Schema to assume when a table is provided without qualification. */
	defaultSchema?: string;
	/** Optional database kind label. Defaults to "postgres". */
	kind?: SchemaIntrospection["db"]["kind"];
	/**
	 * Optional allow-list of table names (schema-qualified or bare).
	 * When specified, introspection and queries are restricted to these tables only.
	 */
	allowedTables?: string[];
}

type TableRow = {
	table_name: string;
	schema_name: string;
	table_type: string;
	comment: string | null;
};

type ColumnRow = {
	table_name: string;
	table_schema: string;
	column_name: string;
	data_type: string;
	udt_name: string | null;
	is_primary_key: boolean;
	description: string | null;
};

interface NormalizedTable {
	schema: string;
	table: string;
}

/**
 * Simplified PostgreSQL adapter following IngestRequest format
 * Removed: indexes, constraints, foreign keys, statistics
 * Kept only: tables, columns (name, type, isPrimaryKey, comment)
 */
export class PostgresAdapter implements DatabaseAdapter {
	private readonly databaseName: string;
	private readonly defaultSchema: string;
	private readonly kind: SchemaIntrospection["db"]["kind"];
	private readonly allowedTables?: NormalizedTable[];

	constructor(
		private readonly clientFn: PostgresClientFn,
		options: PostgresAdapterOptions = {},
	) {
		this.databaseName = options.database ?? "postgres";
		this.defaultSchema = options.defaultSchema ?? "public";
		this.kind = options.kind ?? "postgres";
		if (options.allowedTables) {
			this.allowedTables = normalizeTableFilter(
				options.allowedTables,
				this.defaultSchema,
			);
		}
	}

	async execute(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<DatabaseExecutionResult> {
		// Validate query against allowed tables if restrictions are in place
		if (this.allowedTables) {
			this.validateQueryTables(sql);
		}

		// Convert named params to positional array for PostgreSQL
		let paramArray: unknown[] | undefined;
		if (params) {
			paramArray = this.convertNamedToPositionalParams(params);
		}

		const result = await this.clientFn(sql, paramArray);
		const fields = result.fields.map((f) => f.name);
		return { fields, rows: result.rows };
	}

	private validateQueryTables(sql: string): void {
		if (!this.allowedTables || this.allowedTables.length === 0) {
			return;
		}

		const allowedSet = new Set(
			this.allowedTables.map((t) => tableKey(t.schema, t.table)),
		);

		// Extract potential table references from SQL
		const tablePattern =
			/(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?(["']?[a-zA-Z_][a-zA-Z0-9_]*["']?)/gi;
		const matches = sql.matchAll(tablePattern);

		for (const match of matches) {
			const schema = match[1] ?? this.defaultSchema;
			const table = match[2]?.replace(/['"]/g, "");
			if (table) {
				const key = tableKey(schema, table);
				if (!allowedSet.has(key)) {
					throw new Error(
						`Query references table "${schema}.${table}" which is not in the allowed tables list`,
					);
				}
			}
		}
	}

	/**
	 * Convert named params to positional array for PostgreSQL
	 * PostgreSQL expects $1, $2, $3 in SQL and an array of values [val1, val2, val3]
	 */
	private convertNamedToPositionalParams(
		params: Record<string, string | number | boolean | string[] | number[]>,
	): unknown[] {
		// Separate numeric and named keys
		const numericKeys = Object.keys(params)
			.filter((k) => /^\d+$/.test(k))
			.map((k) => Number.parseInt(k, 10))
			.sort((a, b) => a - b);

		const namedKeys = Object.keys(params)
			.filter((k) => !/^\d+$/.test(k))
			.sort(); // Alphabetical order for consistency

		// Build positional array
		const positionalParams: unknown[] = [];

		// First, add values from numeric keys (in sorted order)
		for (const key of numericKeys) {
			let val: unknown = params[String(key)];
			if (typeof val === "string") {
				// Resolve placeholder tokens like `<tenant_id>` to their named values
				const match = val.match(/^<([a-zA-Z0-9_]+)>$/);
				const namedKey = match?.[1];
				if (namedKey && namedKey in params) {
					val = params[namedKey as keyof typeof params];
				}
			}
			positionalParams.push(val);
		}

		// Then, add values from named keys (in alphabetical order)
		for (const key of namedKeys) {
			const val = params[key];
			positionalParams.push(val);
		}

		return positionalParams;
	}

	async validate(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<void> {
		let paramArray: unknown[] | undefined;
		if (params) {
			paramArray = this.convertNamedToPositionalParams(params);
		}

		await this.clientFn(`EXPLAIN ${sql}`, paramArray);
	}

	getDialect() {
		return "postgres" as const;
	}

	/**
	 * Simplified introspection: only collect table/column metadata for IngestRequest
	 * No indexes, constraints, or statistics
	 */
	async introspect(options?: IntrospectOptions): Promise<SchemaIntrospection> {
		// Use adapter-level allowedTables if no specific tables provided in options
		const tablesToIntrospect = options?.tables
			? normalizeTableFilter(options.tables, this.defaultSchema)
			: this.allowedTables;
		const normalizedTables = tablesToIntrospect ?? [];

		const tablesResult = await this.clientFn(
			buildTablesQuery(normalizedTables),
		);
		const tableRows = tablesResult.rows as TableRow[];

		const columnsResult = await this.clientFn(
			buildColumnsQuery(normalizedTables),
		);
		const columnRows = columnsResult.rows as ColumnRow[];

		const tablesByKey = new Map<string, TableSchema>();

		// Build tables
		for (const row of tableRows) {
			const key = tableKey(row.schema_name, row.table_name);
			const table: TableSchema = {
				name: row.table_name,
				schema: row.schema_name,
				type: asTableType(row.table_type),
				columns: [],
			};

			const comment = sanitize(row.comment);
			if (comment !== undefined) {
				table.comment = comment;
			}

			tablesByKey.set(key, table);
		}

		// Build columns
		for (const row of columnRows) {
			const key = tableKey(row.table_schema, row.table_name);
			const table = tablesByKey.get(key);
			if (!table) continue;

			const column: ColumnSchema = {
				name: row.column_name,
				type: row.data_type,
				isPrimaryKey: row.is_primary_key,
			};

			const rawType = row.udt_name ?? undefined;
			if (rawType !== undefined) column.rawType = rawType;

			const comment = sanitize(row.description);
			if (comment !== undefined) column.comment = comment;

			table.columns.push(column);
		}

		const tables = Array.from(tablesByKey.values()).sort((a, b) => {
			if (a.schema === b.schema) {
				return a.name.localeCompare(b.name);
			}
			return a.schema.localeCompare(b.schema);
		});

		return {
			db: {
				kind: this.kind,
				name: this.databaseName,
			},
			tables,
			introspectedAt: new Date().toISOString(),
		};
	}
}

function normalizeTableFilter(
	tables: string[] | undefined,
	defaultSchema: string,
): NormalizedTable[] {
	if (!tables?.length) return [];
	const normalized: NormalizedTable[] = [];
	const seen = new Set<string>();

	for (const raw of tables) {
		if (!raw) continue;
		const trimmed = raw.trim();
		if (!trimmed) continue;
		const parts = trimmed.split(".");
		const table = parts.pop() ?? "";
		const schema = parts.pop() ?? defaultSchema;
		if (!isSafeIdentifier(schema) || !isSafeIdentifier(table)) {
			continue;
		}
		const key = tableKey(schema, table);
		if (seen.has(key)) continue;
		seen.add(key);
		normalized.push({ schema, table });
	}

	return normalized;
}

function buildTablesQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(tables, "n.nspname", "c.relname");
	return `SELECT
    c.relname AS table_name,
    n.nspname AS schema_name,
    CASE c.relkind
      WHEN 'r' THEN 'table'
      WHEN 'v' THEN 'view'
      WHEN 'm' THEN 'materialized_view'
      ELSE c.relkind::text
    END AS table_type,
    obj_description(c.oid) AS comment
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    AND c.relkind IN ('r', 'v', 'm')
    ${filter}
  ORDER BY n.nspname, c.relname;`;
}

function buildColumnsQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(
		tables,
		"cols.table_schema",
		"cols.table_name",
	);
	return `SELECT
    cols.table_name,
    cols.table_schema,
    cols.column_name,
    cols.data_type,
    cols.udt_name,
    pgd.description,
    EXISTS(
      SELECT 1
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu
        ON tc.constraint_name = kcu.constraint_name
       AND tc.table_schema = kcu.table_schema
      WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_schema = cols.table_schema
        AND tc.table_name = cols.table_name
        AND kcu.column_name = cols.column_name
    ) AS is_primary_key
  FROM information_schema.columns cols
  LEFT JOIN pg_catalog.pg_class c
    ON c.relname = cols.table_name
   AND c.relkind IN ('r', 'v', 'm')
  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
  LEFT JOIN pg_catalog.pg_attribute attr
    ON attr.attrelid = c.oid
   AND attr.attname = cols.column_name
  LEFT JOIN pg_catalog.pg_description pgd
    ON pgd.objoid = attr.attrelid AND pgd.objsubid = attr.attnum
  WHERE cols.table_schema NOT IN ('pg_catalog', 'information_schema')
    ${filter}
  ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position;`;
}

function buildFilterClause(
	tables: NormalizedTable[],
	schemaExpr: string,
	tableExpr: string,
): string {
	if (!tables.length) return "";
	const clauses = tables.map(({ schema, table }) => {
		return `(${schemaExpr} = '${schema}' AND ${tableExpr} = '${table}')`;
	});
	return `AND (${clauses.join(" OR ")})`;
}

function tableKey(schema: string, table: string): string {
	return `${schema}.${table}`;
}

function isSafeIdentifier(value: string): boolean {
	return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}

function asTableType(value: string): TableSchema["type"] {
	const normalized = value.toLowerCase();
	if (normalized.includes("view")) {
		return normalized.includes("materialized") ? "materialized_view" : "view";
	}
	return "table";
}

function sanitize(value: unknown): string | undefined {
	if (value === null || value === undefined) return undefined;
	const trimmed = String(value).trim();
	return trimmed.length ? trimmed : undefined;
}
