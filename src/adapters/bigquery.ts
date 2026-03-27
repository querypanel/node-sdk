import type {
	ColumnSchema,
	IntrospectOptions,
	SchemaIntrospection,
	TableSchema,
} from "../schema/types";
import type { DatabaseAdapter, DatabaseExecutionResult } from "./types";

/**
 * Request passed to BigQueryClientFn.
 * dryRun: when true, the client should run the query as a dry run (validation only, no execution).
 */
export interface BigQueryQueryRequest {
	query: string;
	params?: Record<string, string | number | boolean | string[] | number[]>;
	/** When true, validate the query without executing (e.g. BigQuery dry run). */
	dryRun?: boolean;
}

export interface BigQueryQueryResult {
	rows: Array<Record<string, unknown>>;
	fields: string[];
}

/**
 * Client function for the BigQuery adapter.
 * Implement using @google-cloud/bigquery: createQueryJob with dryRun for validation, getQueryResults for execute.
 */
export type BigQueryClientFn = (
	request: BigQueryQueryRequest,
) => Promise<BigQueryQueryResult>;

export interface BigQueryAdapterOptions {
	/** GCP project ID. Used for INFORMATION_SCHEMA queries. */
	projectId: string;
	/** Optional project that owns the dataset. Defaults to projectId. */
	datasetProjectId?: string;
	/** Default dataset (schema) name. Used for introspection and unqualified table names. */
	dataset: string;
	/** Optional location for the dataset (e.g. "US", "EU"). */
	location?: string;
	/** Logical database name used in introspection metadata. */
	database?: string;
	/** Optional database kind label. Defaults to "bigquery". */
	kind?: SchemaIntrospection["db"]["kind"];
	/**
	 * Optional allow-list of table names (dataset.table or bare table name).
	 * When specified, introspection and queries are restricted to these tables only.
	 */
	allowedTables?: string[];
}

type TableRow = {
	table_name: string;
	table_schema: string;
	table_type: string;
};

type ColumnRow = {
	table_name: string;
	table_schema: string;
	column_name: string;
	data_type: string;
	ordinal_position: number;
	is_nullable: string | null;
};

interface NormalizedTable {
	dataset: string;
	table: string;
}

/**
 * BigQuery adapter following the same pattern as PostgresAdapter and ClickHouseAdapter.
 * - execute: runs the query via the client function.
 * - validate: uses dry run (no EXPLAIN in BigQuery) to validate SQL without executing.
 * - introspect: uses INFORMATION_SCHEMA.TABLES and INFORMATION_SCHEMA.COLUMNS.
 */
export class BigQueryAdapter implements DatabaseAdapter {
	private readonly projectId: string;
	private readonly datasetProjectId: string;
	private readonly defaultDataset: string;
	private readonly databaseName: string;
	private readonly kind: SchemaIntrospection["db"]["kind"];
	private readonly allowedTables?: NormalizedTable[];

	constructor(
		private readonly clientFn: BigQueryClientFn,
		options: BigQueryAdapterOptions,
	) {
		this.projectId = options.projectId;
		this.datasetProjectId = options.datasetProjectId ?? options.projectId;
		this.defaultDataset = options.dataset;
		this.databaseName = options.database ?? options.dataset;
		this.kind = options.kind ?? "bigquery";
		if (options.allowedTables) {
			this.allowedTables = normalizeTableFilter(
				options.allowedTables,
				this.defaultDataset,
			);
		}
	}

	async execute(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<DatabaseExecutionResult> {
		if (this.allowedTables) {
			this.validateQueryTables(sql);
		}

		const result = await this.clientFn({
			query: sql,
			params,
			dryRun: false,
		});
		return { fields: result.fields, rows: result.rows };
	}

	async validate(
		sql: string,
		params?: Record<string, string | number | boolean | string[] | number[]>,
	): Promise<void> {
		await this.clientFn({
			query: sql,
			params,
			dryRun: true,
		});
	}

	getDialect() {
		return "bigquery" as const;
	}

	/**
	 * Introspect using BigQuery INFORMATION_SCHEMA (TABLES and COLUMNS).
	 * Restricts to the default dataset (or allowedTables) and returns tables/columns only.
	 */
	async introspect(options?: IntrospectOptions): Promise<SchemaIntrospection> {
		const tablesToIntrospect = options?.tables
			? normalizeTableFilter(options.tables, this.defaultDataset)
			: this.allowedTables;
		const normalizedTables = tablesToIntrospect ?? [];

		const tablesSql = buildTablesQuery(
			this.datasetProjectId,
			this.defaultDataset,
			normalizedTables,
		);
		const tablesResult = await this.clientFn({
			query: tablesSql,
			dryRun: false,
		});
		const tableRows = tablesResult.rows as unknown as TableRow[];

		const columnsSql = buildColumnsQuery(
			this.datasetProjectId,
			this.defaultDataset,
			normalizedTables,
		);
		const columnsResult = await this.clientFn({
			query: columnsSql,
			dryRun: false,
		});
		const columnRows = columnsResult.rows as unknown as ColumnRow[];

		const tablesByKey = new Map<string, TableSchema>();

		for (const row of tableRows) {
			const key = tableKey(row.table_schema, row.table_name);
			const table: TableSchema = {
				name: row.table_name,
				schema: row.table_schema,
				type: asTableType(row.table_type),
				columns: [],
			};
			tablesByKey.set(key, table);
		}

		// columnRows is ordered by ordinal_position, so columns are added in correct order
		for (const row of columnRows) {
			const key = tableKey(row.table_schema, row.table_name);
			const table = tablesByKey.get(key);
			if (!table) continue;

			const column: ColumnSchema = {
				name: row.column_name,
				type: row.data_type,
				isPrimaryKey: false,
			};
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

	private validateQueryTables(sql: string): void {
		if (!this.allowedTables || this.allowedTables.length === 0) {
			return;
		}

		const allowedSet = new Set(
			this.allowedTables.map((t) => tableKey(t.dataset, t.table)),
		);

		// Match FROM `project.dataset.table` or FROM dataset.table or FROM table
		const tablePattern =
			/(?:FROM|JOIN)\s+`?(?:[\w-]+\.)?([\w-]+)\.([\w-]+)`?|(?:FROM|JOIN)\s+`?([\w-]+)`?/gi;
		const matches = sql.matchAll(tablePattern);

		for (const match of matches) {
			const dataset = match[1] ?? match[3] ?? this.defaultDataset;
			const table = match[2] ?? match[3];
			if (table) {
				const key = tableKey(dataset, table);
				if (!allowedSet.has(key)) {
					throw new Error(
						`Query references table "${dataset}.${table}" which is not in the allowed tables list`,
					);
				}
			}
		}
	}
}

function normalizeTableFilter(
	tables: string[] | undefined,
	defaultDataset: string,
): NormalizedTable[] {
	if (!tables?.length) return [];
	const normalized: NormalizedTable[] = [];
	const seen = new Set<string>();

	for (const raw of tables) {
		if (!raw?.trim()) continue;
		const trimmed = raw.trim();
		const parts = trimmed.split(".");
		const table = parts.pop() ?? "";
		const dataset = parts.pop() ?? defaultDataset;
		const key = tableKey(dataset, table);
		if (seen.has(key)) continue;
		seen.add(key);
		normalized.push({ dataset, table });
	}

	return normalized;
}

function tableKey(dataset: string, table: string): string {
	return `${dataset}.${table}`;
}

function buildTablesQuery(
	datasetProjectId: string,
	dataset: string,
	tables: NormalizedTable[],
): string {
	const qualifier = `\`${datasetProjectId}.${dataset}.INFORMATION_SCHEMA.TABLES\``;
	const filter =
		tables.length > 0
			? ` AND table_name IN (${tables.map((t) => `'${escapeSql(t.table)}'`).join(", ")})`
			: "";
	return `SELECT table_name, table_schema, table_type
  FROM ${qualifier}
  WHERE table_schema = '${escapeSql(dataset)}'${filter}
  ORDER BY table_schema, table_name`;
}

function buildColumnsQuery(
	datasetProjectId: string,
	dataset: string,
	tables: NormalizedTable[],
): string {
	const qualifier = `\`${datasetProjectId}.${dataset}.INFORMATION_SCHEMA.COLUMNS\``;
	const filter =
		tables.length > 0
			? ` AND table_name IN (${tables.map((t) => `'${escapeSql(t.table)}'`).join(", ")})`
			: "";
	return `SELECT table_name, table_schema, column_name, data_type, ordinal_position, is_nullable
  FROM ${qualifier}
  WHERE table_schema = '${escapeSql(dataset)}'${filter}
  ORDER BY table_schema, table_name, ordinal_position`;
}

function escapeSql(value: string): string {
	return value.replace(/\\/g, "\\\\").replace(/'/g, "''");
}

function asTableType(tableType: string): TableSchema["type"] {
	const normalized = tableType?.toLowerCase() ?? "";
	if (normalized.includes("view")) {
		return normalized.includes("materialized") ? "materialized_view" : "view";
	}
	if (normalized.includes("external") || normalized.includes("snapshot") || normalized.includes("clone")) {
		return "table";
	}
	return "table";
}
