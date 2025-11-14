import type {
	ColumnSchema,
	ConstraintSchema,
	IndexSchema,
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
	total_rows: number | null;
	total_bytes: number | null;
};

type ColumnRow = {
	table_name: string;
	table_schema: string;
	column_name: string;
	data_type: string;
	udt_name: string | null;
	is_nullable: string;
	column_default: string | null;
	character_maximum_length: number | null;
	numeric_precision: number | null;
	numeric_scale: number | null;
	ordinal_position: number;
	description: string | null;
};

type ConstraintRow = {
	table_schema: string;
	table_name: string;
	constraint_name: string;
	constraint_type: string;
	column_name: string | null;
	foreign_table_schema: string | null;
	foreign_table_name: string | null;
	foreign_column_name: string | null;
};

type IndexRow = {
	schema_name: string;
	table_name: string;
	index_name: string;
	indisunique: boolean;
	column_names: string[] | null;
	definition: string | null;
};

interface NormalizedTable {
	schema: string;
	table: string;
}

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

	/**
	 * Validate that the SQL query only references allowed tables.
	 * This is a basic validation that extracts table-like patterns from the query.
	 */
	private validateQueryTables(sql: string): void {
		if (!this.allowedTables || this.allowedTables.length === 0) {
			return;
		}

		const allowedSet = new Set(
			this.allowedTables.map((t) => tableKey(t.schema, t.table)),
		);

		// Extract potential table references from SQL
		// This regex looks for identifiers after FROM/JOIN keywords
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
	 *
	 * Supports two formats:
	 * 1. Numeric keys: { '1': 'value1', '2': 'value2' } - maps directly to $1, $2
	 * 2. Named keys: { 'tenant_id': 'value' } - values extracted in alphabetical order
	 * 3. Mixed: { '1': 'value1', 'tenant_id': 'value' } - numeric keys first, then named keys
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
		// This handles cases where tenant isolation adds named params like {'tenant_id': 'value'}
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
		// Convert named params to positional array for PostgreSQL
		let paramArray: unknown[] | undefined;
		if (params) {
			paramArray = this.convertNamedToPositionalParams(params);
		}

		await this.clientFn(`EXPLAIN ${sql}`, paramArray);
	}

	getDialect() {
		return "postgres" as const;
	}

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

		const constraintsResult = await this.clientFn(
			buildConstraintsQuery(normalizedTables),
		);
		const constraintRows = constraintsResult.rows as ConstraintRow[];

		const indexesResult = await this.clientFn(
			buildIndexesQuery(normalizedTables),
		);
		const indexRows = indexesResult.rows as IndexRow[];

		const tablesByKey = new Map<string, TableSchema>();
		const columnsByKey = new Map<string, Map<string, ColumnSchema>>();

		for (const row of tableRows) {
			const key = tableKey(row.schema_name, row.table_name);
			const statistics = buildTableStatistics(
				toNumber(row.total_rows),
				toNumber(row.total_bytes),
			);

			const table: TableSchema = {
				name: row.table_name,
				schema: row.schema_name,
				type: asTableType(row.table_type),
				columns: [],
				indexes: [],
				constraints: [],
			};

			const comment = sanitize(row.comment);
			if (comment !== undefined) {
				table.comment = comment;
			}
			if (statistics) {
				table.statistics = statistics;
			}

			tablesByKey.set(key, table);
			columnsByKey.set(key, new Map());
		}

		for (const row of columnRows) {
			const key = tableKey(row.table_schema, row.table_name);
			const table = tablesByKey.get(key);
			if (!table) continue;

			const column: ColumnSchema = {
				name: row.column_name,
				type: row.data_type,
				nullable: row.is_nullable.toUpperCase() === "YES",
				isPrimaryKey: false,
				isForeignKey: false,
			};

			const rawType = row.udt_name ?? undefined;
			if (rawType !== undefined) column.rawType = rawType;

			const defaultExpression = sanitize(row.column_default);
			if (defaultExpression !== undefined)
				column.defaultExpression = defaultExpression;

			const comment = sanitize(row.description);
			if (comment !== undefined) column.comment = comment;

			const maxLength = row.character_maximum_length ?? undefined;
			if (maxLength !== undefined) column.maxLength = maxLength;

			const precision = row.numeric_precision ?? undefined;
			if (precision !== undefined) column.precision = precision;

			const scale = row.numeric_scale ?? undefined;
			if (scale !== undefined) column.scale = scale;

			table.columns.push(column);
			columnsByKey.get(key)?.set(row.column_name, column);
		}

		const constraintGroups = groupConstraints(constraintRows);
		for (const group of constraintGroups) {
			const key = tableKey(group.table_schema, group.table_name);
			const table = tablesByKey.get(key);
			if (!table) continue;

			const constraint: ConstraintSchema = {
				name: group.constraint_name,
				type: group.constraint_type,
				columns: [...group.columns],
			};

			if (group.type === "FOREIGN KEY") {
				if (group.foreign_table_name) {
					const referencedTable = group.foreign_table_schema
						? `${group.foreign_table_schema}.${group.foreign_table_name}`
						: group.foreign_table_name;
					constraint.referencedTable = referencedTable;
				}
				if (group.foreign_columns.length) {
					constraint.referencedColumns = [...group.foreign_columns];
				}
			}

			table.constraints.push(constraint);

			for (let index = 0; index < group.columns.length; index += 1) {
				const columnName = group.columns[index];
				if (!columnName) continue;
				const column = columnsByKey.get(key)?.get(columnName);
				if (!column) continue;
				if (group.type === "PRIMARY KEY") {
					column.isPrimaryKey = true;
				}
				if (group.type === "FOREIGN KEY") {
					column.isForeignKey = true;
					if (group.foreign_table_name) {
						column.foreignKeyTable = group.foreign_table_schema
							? `${group.foreign_table_schema}.${group.foreign_table_name}`
							: group.foreign_table_name;
					}
					const referencedColumn = group.foreign_columns[index];
					if (referencedColumn) {
						column.foreignKeyColumn = referencedColumn;
					}
				}
			}
		}

		for (const row of indexRows) {
			const key = tableKey(row.schema_name, row.table_name);
			const table = tablesByKey.get(key);
			if (!table) continue;
			const columns = coerceStringArray(row.column_names)
				.map((c) => c.trim())
				.filter(Boolean);
			const index: IndexSchema = {
				name: row.index_name,
				columns,
				unique: Boolean(row.indisunique),
				type: columns.length === 1 ? "INDEX" : "COMPOSITE INDEX",
			};
			const definition = sanitize(row.definition);
			if (definition !== undefined) index.definition = definition;
			table.indexes.push(index);
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

interface ConstraintGroup {
	table_schema: string;
	table_name: string;
	constraint_name: string;
	constraint_type: ConstraintSchema["type"];
	columns: string[];
	foreign_table_schema?: string | null;
	foreign_table_name?: string | null;
	foreign_columns: string[];
	type: ConstraintSchema["type"];
}

function groupConstraints(rows: ConstraintRow[]): ConstraintGroup[] {
	const groups = new Map<string, ConstraintGroup>();

	for (const row of rows) {
		const key = `${row.table_schema}.${row.table_name}.${row.constraint_name}`;
		let group = groups.get(key);
		if (!group) {
			group = {
				table_schema: row.table_schema,
				table_name: row.table_name,
				constraint_name: row.constraint_name,
				constraint_type: row.constraint_type,
				columns: [],
				foreign_columns: [],
				type: row.constraint_type,
			};
			groups.set(key, group);
		}

		if (row.column_name) {
			group.columns.push(row.column_name);
		}

		if (row.constraint_type === "FOREIGN KEY") {
			group.foreign_table_schema = row.foreign_table_schema;
			group.foreign_table_name = row.foreign_table_name;
			if (row.foreign_column_name) {
				group.foreign_columns.push(row.foreign_column_name);
			}
		}
	}

	return Array.from(groups.values());
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
    obj_description(c.oid) AS comment,
    c.reltuples AS total_rows,
    pg_total_relation_size(c.oid) AS total_bytes
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
    cols.is_nullable,
    cols.column_default,
    cols.character_maximum_length,
    cols.numeric_precision,
    cols.numeric_scale,
    cols.ordinal_position,
    pgd.description
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

function buildConstraintsQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(tables, "tc.table_schema", "tc.table_name");
	return `SELECT
    tc.table_schema,
    tc.table_name,
    tc.constraint_name,
    tc.constraint_type,
    kcu.column_name,
    ccu.table_schema AS foreign_table_schema,
    ccu.table_name AS foreign_table_name,
    ccu.column_name AS foreign_column_name
  FROM information_schema.table_constraints tc
  LEFT JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
   AND tc.table_schema = kcu.table_schema
  LEFT JOIN information_schema.constraint_column_usage ccu
    ON ccu.constraint_name = tc.constraint_name
   AND ccu.table_schema = tc.table_schema
  WHERE tc.constraint_type IN ('PRIMARY KEY', 'UNIQUE', 'FOREIGN KEY')
    AND tc.table_schema NOT IN ('pg_catalog', 'information_schema')
    ${filter}
  ORDER BY tc.table_schema, tc.table_name, tc.constraint_name, kcu.ordinal_position;`;
}

function buildIndexesQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(tables, "n.nspname", "c.relname");
	return `SELECT
    n.nspname AS schema_name,
    c.relname AS table_name,
    ci.relname AS index_name,
    idx.indisunique,
    array_remove(
      array_agg(pg_get_indexdef(idx.indexrelid, g.k, true) ORDER BY g.k),
      NULL
    ) AS column_names,
    pg_get_indexdef(idx.indexrelid) AS definition
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  JOIN pg_index idx ON idx.indrelid = c.oid
  JOIN pg_class ci ON ci.oid = idx.indexrelid
  JOIN LATERAL generate_subscripts(idx.indkey, 1) AS g(k) ON true
  WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
    ${filter}
  GROUP BY n.nspname, c.relname, ci.relname, idx.indisunique, idx.indexrelid;`;
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

function buildTableStatistics(
	totalRows?: number,
	totalBytes?: number,
): TableSchema["statistics"] | undefined {
	if (totalRows === undefined && totalBytes === undefined) return undefined;
	const stats: NonNullable<TableSchema["statistics"]> = {};
	if (totalRows !== undefined) stats.totalRows = totalRows;
	if (totalBytes !== undefined) stats.totalBytes = totalBytes;
	return stats;
}

function sanitize(value: unknown): string | undefined {
	if (value === null || value === undefined) return undefined;
	const trimmed = String(value).trim();
	return trimmed.length ? trimmed : undefined;
}

function toNumber(value: unknown): number | undefined {
	if (value === null || value === undefined) return undefined;
	if (typeof value === "number") return value;
	const parsed = Number.parseFloat(String(value));
	return Number.isNaN(parsed) ? undefined : parsed;
}

function coerceStringArray(value: unknown): string[] {
	if (!value) return [];
	if (Array.isArray(value)) {
		return value.map((entry) => String(entry));
	}
	const text = String(value).trim();
	if (!text) return [];
	const withoutBraces =
		text.startsWith("{") && text.endsWith("}") ? text.slice(1, -1) : text;
	if (!withoutBraces) return [];
	return withoutBraces
		.split(",")
		.map((part) => part.trim().replace(/^"(.+)"$/, "$1"))
		.filter(Boolean);
}
