import type {
	ColumnSchema,
	IntrospectOptions,
	SchemaIntrospection,
	TableSchema,
} from "../schema/types";
import {
	type CatalogSchema,
	type CatalogTable,
	type DatabaseAdapter,
	type DatabaseExecutionResult,
	type ListTablesOptions,
	resolveCatalogLimit,
} from "./types";
import { findRelations, type Relation } from "../core/sql-relations";
import {
	TENANT_SCHEMA_KEY,
	type TenantSchemaConfig,
	tenantSchemaKey,
} from "../core/tenant-schema";

export interface PostgresQueryResult {
	rows: Array<Record<string, unknown>>;
	fields: Array<{ name: string }>;
}

export type PostgresClientFn = (
	sql: string,
	params?: unknown[],
) => Promise<PostgresQueryResult>;

export type PostgresWireDialect = "postgres" | "redshift";

export interface PostgresAdapterOptions {
	/** Logical database name used in introspection metadata. */
	database?: string;
	/** Schema to assume when a table is provided without qualification. */
	defaultSchema?: string;
	/** Optional database kind label. Defaults to the dialect. */
	kind?: SchemaIntrospection["db"]["kind"];
	/**
	 * Wire-compatible engine behind the client. "redshift" uses Redshift's
	 * svv_* catalog views for introspection. Defaults to "postgres".
	 */
	dialect?: PostgresWireDialect;
	/**
	 * Optional allow-list of table names (schema-qualified or bare).
	 * Use `schema.*` to allow every table in a schema (a bare `*` means the
	 * default schema). When specified, introspection and queries are restricted
	 * to these tables only.
	 */
	allowedTables?: string[];
	/**
	 * Schema-per-tenant connection: SQL has been rewritten to the caller's tenant
	 * schema, so queries may read any schema the template produces (or the tables
	 * `allowedTables` names in it) plus `sharedSchemas`, and nothing else.
	 */
	tenantSchema?: TenantSchemaConfig;
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

/** `table` is `"*"` when every table in `schema` is allowed. */
export interface NormalizedTable {
	schema: string;
	table: string;
}

const WILDCARD = "*";

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
	private readonly dialect: PostgresWireDialect;
	private readonly allowedTables?: NormalizedTable[];
	/** Undefined when queries are unrestricted. */
	private readonly allowsRelation?: (relation: Relation) => boolean;

	constructor(
		private readonly clientFn: PostgresClientFn,
		options: PostgresAdapterOptions = {},
	) {
		this.databaseName = options.database ?? "postgres";
		this.defaultSchema = options.defaultSchema ?? "public";
		this.dialect = options.dialect ?? "postgres";
		this.kind = options.kind ?? this.dialect;
		if (options.allowedTables) {
			this.allowedTables = normalizeTableFilter(
				options.allowedTables,
				this.defaultSchema,
			);
		}
		this.allowsRelation = buildRelationAllowlist(
			this.allowedTables,
			options.tenantSchema,
			this.defaultSchema,
		);
	}

	async execute(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<DatabaseExecutionResult> {
		// Validate query against allowed tables if restrictions are in place
		if (this.allowsRelation) {
			this.validateQueryTables(sql, this.allowsRelation);
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

	private validateQueryTables(sql: string, allows: (relation: Relation) => boolean): void {
		for (const relation of findRelations(sql)) {
			if (!allows(relation)) {
				const name = relation.parts.length === 1
					? `${this.defaultSchema}.${relation.parts[0]}`
					: relation.parts.join(".");
				throw new Error(`Query references table "${name}" which is not in the allowed tables list`);
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
		// Check the allowlist before EXPLAIN, so a refused query never reaches the database.
		if (this.allowsRelation) {
			this.validateQueryTables(sql, this.allowsRelation);
		}
		const boundParams = filterParamsForPostgresSql(sql, params);
		let paramArray: unknown[] | undefined;
		if (boundParams) {
			paramArray = this.convertNamedToPositionalParams(sql, boundParams);
		}

		await this.clientFn(`EXPLAIN ${sql}`, paramArray);
	}

	getDialect(): PostgresWireDialect {
		return this.dialect;
	}

	/**
	 * List schemas with their table/view counts. Reads the full catalog (not
	 * restricted by `allowedTables`) so it can back an allowed-tables picker.
	 */
	async listSchemas(): Promise<CatalogSchema[]> {
		const result = await this.clientFn(
			this.dialect === "redshift"
				? buildRedshiftListSchemasQuery()
				: buildListSchemasQuery(),
		);
		return result.rows.map((row) => ({
			schema: String(row.schema),
			tableCount: Number(row.table_count ?? 0),
		}));
	}

	/**
	 * List tables/views, optionally filtered by schema and a case-insensitive
	 * substring of the table name. Reads the full catalog (not restricted by
	 * `allowedTables`). Default limit 500, max 5000.
	 */
	async listTables(options: ListTablesOptions = {}): Promise<CatalogTable[]> {
		const { sql, params } = buildListTablesQuery(this.dialect, options);
		const result = await this.clientFn(sql, params);
		return result.rows.map((row) => ({
			schema: String(row.schema),
			name: String(row.name),
			type: asTableType(String(row.table_type ?? "")),
		}));
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

		const isRedshift = this.dialect === "redshift";
		const tablesResult = await this.clientFn(
			isRedshift
				? buildRedshiftTablesQuery(normalizedTables)
				: buildTablesQuery(normalizedTables),
		);
		const tableRows = tablesResult.rows as TableRow[];

		const columnsResult = await this.clientFn(
			isRedshift
				? buildRedshiftColumnsQuery(normalizedTables)
				: buildColumnsQuery(normalizedTables),
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
		if (
			!isSafeIdentifier(schema) ||
			(table !== WILDCARD && !isSafeIdentifier(table))
		) {
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
  LEFT JOIN pg_catalog.pg_namespace n ON n.nspname = cols.table_schema
  LEFT JOIN pg_catalog.pg_class c
    ON c.relname = cols.table_name
   AND c.relnamespace = n.oid
   AND c.relkind IN ('r', 'v', 'm')
  LEFT JOIN pg_catalog.pg_attribute attr
    ON attr.attrelid = c.oid
   AND attr.attname = cols.column_name
  LEFT JOIN pg_catalog.pg_description pgd
    ON pgd.objoid = attr.attrelid AND pgd.objsubid = attr.attnum
  WHERE cols.table_schema NOT IN ('pg_catalog', 'information_schema')
    ${filter}
  ORDER BY cols.table_schema, cols.table_name, cols.ordinal_position;`;
}

// Redshift's information_schema constraint views are very slow on large
// clusters, so read the svv_* catalog views instead. Primary keys are
// informational-only in Redshift and are not reported.
const REDSHIFT_EXCLUDED_SCHEMAS =
	"('pg_catalog', 'information_schema', 'pg_internal', 'pg_automv')";

function buildRedshiftTablesQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(tables, "table_schema", "table_name");
	return `SELECT
    table_name,
    table_schema AS schema_name,
    table_type,
    remarks AS comment
  FROM svv_tables
  WHERE table_catalog = current_database()
    AND table_schema NOT IN ${REDSHIFT_EXCLUDED_SCHEMAS}
    AND table_schema NOT LIKE 'pg_temp%'
    ${filter}
  ORDER BY table_schema, table_name;`;
}

function buildRedshiftColumnsQuery(tables: NormalizedTable[]): string {
	const filter = buildFilterClause(tables, "table_schema", "table_name");
	return `SELECT
    table_name,
    table_schema,
    column_name,
    data_type,
    NULL AS udt_name,
    remarks AS description,
    FALSE AS is_primary_key
  FROM svv_columns
  WHERE table_catalog = current_database()
    AND table_schema NOT IN ${REDSHIFT_EXCLUDED_SCHEMAS}
    AND table_schema NOT LIKE 'pg_temp%'
    ${filter}
  ORDER BY table_schema, table_name, ordinal_position;`;
}

/**
 * WHERE fragment restricting introspection to the normalized allow-list.
 * Identifiers are validated by normalizeTableFilter (isSafeIdentifier), so
 * interpolating them is safe. `schema.*` entries match the whole schema.
 */
export function buildFilterClause(
	tables: NormalizedTable[],
	schemaExpr: string,
	tableExpr: string,
): string {
	if (!tables.length) return "";
	const wildcardSchemas = new Set(
		tables.filter((t) => t.table === WILDCARD).map((t) => t.schema),
	);
	const clauses: string[] = [];
	for (const schema of wildcardSchemas) {
		clauses.push(`(${schemaExpr} = '${schema}')`);
	}
	for (const { schema, table } of tables) {
		if (table === WILDCARD || wildcardSchemas.has(schema)) continue;
		clauses.push(`(${schemaExpr} = '${schema}' AND ${tableExpr} = '${table}')`);
	}
	return `AND (${clauses.join(" OR ")})`;
}

function buildListSchemasQuery(): string {
	return `SELECT n.nspname AS schema, COUNT(*) AS table_count
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'v', 'm')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
    AND n.nspname NOT LIKE 'pg_toast_temp%'
  GROUP BY n.nspname
  ORDER BY n.nspname;`;
}

function buildRedshiftListSchemasQuery(): string {
	return `SELECT table_schema AS schema, COUNT(*) AS table_count
  FROM svv_tables
  WHERE table_catalog = current_database()
    AND table_schema NOT IN ${REDSHIFT_EXCLUDED_SCHEMAS}
    AND table_schema NOT LIKE 'pg_temp%'
  GROUP BY table_schema
  ORDER BY table_schema;`;
}

/** Escape LIKE/ILIKE metacharacters so search is a literal substring match. */
export function toLikeSubstringPattern(search: string): string {
	return `%${search.replace(/[\\%_]/g, (ch) => `\\${ch}`)}%`;
}

export function buildListTablesQuery(
	dialect: PostgresWireDialect,
	options: ListTablesOptions,
): { sql: string; params: unknown[] } {
	const isRedshift = dialect === "redshift";
	const schemaExpr = isRedshift ? "table_schema" : "n.nspname";
	const tableExpr = isRedshift ? "table_name" : "c.relname";
	const params: unknown[] = [];
	const filters: string[] = [];
	if (options.schema) {
		params.push(options.schema);
		filters.push(`AND ${schemaExpr} = $${params.length}`);
	}
	const search = options.search?.trim();
	if (search) {
		params.push(toLikeSubstringPattern(search));
		filters.push(`AND ${tableExpr} ILIKE $${params.length}`);
	}
	// Integer produced by resolveCatalogLimit, never raw input.
	const limit = resolveCatalogLimit(options.limit);
	const filterSql = filters.join("\n    ");

	const sql = isRedshift
		? `SELECT table_schema AS schema, table_name AS name, table_type
  FROM svv_tables
  WHERE table_catalog = current_database()
    AND table_schema NOT IN ${REDSHIFT_EXCLUDED_SCHEMAS}
    AND table_schema NOT LIKE 'pg_temp%'
    ${filterSql}
  ORDER BY table_schema, table_name
  LIMIT ${limit};`
		: `SELECT n.nspname AS schema, c.relname AS name,
    CASE c.relkind
      WHEN 'v' THEN 'view'
      WHEN 'm' THEN 'materialized_view'
      ELSE 'table'
    END AS table_type
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE c.relkind IN ('r', 'v', 'm')
    AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
    AND n.nspname NOT LIKE 'pg_temp%'
    AND n.nspname NOT LIKE 'pg_toast_temp%'
    ${filterSql}
  ORDER BY n.nspname, c.relname
  LIMIT ${limit};`;

	return { sql, params };
}

/**
 * Which relations a query may read: the allowlist entries, and for a schema-per-tenant
 * connection every template schema (or the allowlisted tables in it) plus the shared
 * schemas. Undefined when nothing restricts queries.
 */
function buildRelationAllowlist(
	allowedTables: NormalizedTable[] | undefined,
	tenantSchema: TenantSchemaConfig | undefined,
	defaultSchema: string,
): ((relation: Relation) => boolean) | undefined {
	if (!allowedTables?.length && !tenantSchema) return undefined;
	const schemaKey = tenantSchema
		? tenantSchemaKey(tenantSchema.template)
		: (schema: string) => schema.toLowerCase();
	const entries: NormalizedTable[] = [
		...(allowedTables?.length
			? allowedTables
			: [{ schema: TENANT_SCHEMA_KEY, table: WILDCARD }]),
		...(tenantSchema?.sharedSchemas ?? []).map((schema) => ({ schema, table: WILDCARD })),
	];
	const tables = new Set<string>();
	const wildcardSchemas = new Set<string>();
	for (const entry of entries) {
		const schema = entry.schema === TENANT_SCHEMA_KEY ? entry.schema : schemaKey(entry.schema);
		if (entry.table === WILDCARD) wildcardSchemas.add(schema);
		else tables.add(tableKey(schema, entry.table.toLowerCase()));
	}
	return ({ parts }) => {
		if (parts.length > 2) return false; // cross-database reference
		const table = (parts[parts.length - 1] as string).toLowerCase();
		const schema = schemaKey(parts.length === 2 ? (parts[0] as string) : defaultSchema);
		return wildcardSchemas.has(schema) || tables.has(tableKey(schema, table));
	};
}

function tableKey(schema: string, table: string): string {
	return `${schema}.${table}`;
}

function isSafeIdentifier(value: string): boolean {
	return /^[A-Za-z_][A-Za-z0-9_]*$/.test(value);
}

function asTableType(value: string): CatalogTable["type"] {
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
