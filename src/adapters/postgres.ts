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

/** Highest $N index referenced in SQL (0 when the query uses literal values only). */
export function maxPostgresPlaceholderIndex(sql: string): number {
	let max = 0;
	for (const match of sql.matchAll(/\$(\d+)\b/g)) {
		const index = Number.parseInt(match[1], 10);
		if (Number.isFinite(index) && index > max) {
			max = index;
		}
	}
	return max;
}

type PostgresParamRecord = Record<
	string,
	string | number | boolean | string[] | number[]
>;

/**
 * Drop bind parameters when SQL has no $1..$N placeholders (common after LLM inlines literals).
 */
export function filterParamsForPostgresSql(
	sql: string,
	params: PostgresParamRecord | undefined,
): PostgresParamRecord | undefined {
	if (!params || Object.keys(params).length === 0) {
		return undefined;
	}

	const placeholderCount = maxPostgresPlaceholderIndex(sql);
	if (placeholderCount === 0) {
		return undefined;
	}

	const hasNumericKeys = Object.keys(params).some((key) => /^\d+$/.test(key));
	if (hasNumericKeys) {
		const filtered: PostgresParamRecord = {};
		for (let index = 1; index <= placeholderCount; index += 1) {
			const key = String(index);
			if (key in params) {
				filtered[key] = params[key];
			}
		}
		return Object.keys(filtered).length > 0 ? filtered : undefined;
	}

	return params;
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

		const boundParams = filterParamsForPostgresSql(sql, params);

		// Convert named params to positional array for PostgreSQL
		let paramArray: unknown[] | undefined;
		if (boundParams) {
			paramArray = this.convertNamedToPositionalParams(sql, boundParams);
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

		// First, neutralize function calls that use FROM keyword (EXTRACT, SUBSTRING, TRIM, etc.)
		// Replace their FROM with a placeholder to avoid false positives
		const neutralizedSql = sql
			.replace(/EXTRACT\s*\([^)]*FROM\s+[^)]+\)/gi, "EXTRACT(/*neutralized*/)")
			.replace(/SUBSTRING\s*\([^)]*FROM\s+[^)]+\)/gi, "SUBSTRING(/*neutralized*/)")
			.replace(/TRIM\s*\([^)]*FROM\s+[^)]+\)/gi, "TRIM(/*neutralized*/)")
			.replace(/POSITION\s*\([^)]*FROM\s+[^)]+\)/gi, "POSITION(/*neutralized*/)");

		// Extract potential table references from SQL
		const tablePattern =
			/(?:FROM|JOIN)\s+(?:ONLY\s+)?(?:([a-zA-Z_][a-zA-Z0-9_]*)\.)?(["']?[a-zA-Z_][a-zA-Z0-9_]*["']?)/gi;
		const matches = neutralizedSql.matchAll(tablePattern);

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
	 * Convert params to positional array for PostgreSQL ($1, $2, …).
	 * Values must align with v2 generation order via numeric keys from mapGeneratedParams.
	 */
	private convertNamedToPositionalParams(
		sql: string,
		params: Record<string, string | number | boolean | string[] | number[]>,
	): unknown[] {
		const placeholderCount = maxPostgresPlaceholderIndex(sql);
		if (placeholderCount === 0) {
			return [];
		}

		const positionalParams: unknown[] = [];

		for (let index = 1; index <= placeholderCount; index += 1) {
			const key = String(index);
			let val: unknown = params[key];

			if (val === undefined && placeholderCount === 1) {
				const namedKeys = Object.keys(params).filter((k) => !/^\d+$/.test(k));
				if (namedKeys.length === 1) {
					val = params[namedKeys[0] as keyof typeof params];
				}
			}

			if (val === undefined) {
				throw new Error(`Missing value for PostgreSQL parameter $${index}`);
			}

			if (typeof val === "string") {
				const match = val.match(/^<([a-zA-Z0-9_]+)>$/);
				const namedKey = match?.[1];
				if (namedKey && namedKey in params) {
					val = params[namedKey as keyof typeof params];
				}
			}

			positionalParams.push(val);
		}

		return positionalParams;
	}

	async validate(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<void> {
		const boundParams = filterParamsForPostgresSql(sql, params);
		let paramArray: unknown[] | undefined;
		if (boundParams) {
			paramArray = this.convertNamedToPositionalParams(sql, boundParams);
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
