import type {
  IntrospectOptions,
  SchemaIntrospection,
} from "../schema/types";

export type DatabaseDialect =
	| "bigquery"
	| "clickhouse"
	| "postgres"
	| "redshift"
	| "mysql";

/** A schema (or ClickHouse database / BigQuery dataset) in the catalog. */
export interface CatalogSchema {
  schema: string;
  tableCount: number;
}

export type CatalogTableType = "table" | "view" | "materialized_view";

/** A table or view in the catalog (no columns; cheap to list). */
export interface CatalogTable {
  schema: string;
  name: string;
  type: CatalogTableType;
}

export interface ListTablesOptions {
  /** Only list tables in this schema. */
  schema?: string;
  /** Case-insensitive substring match on the table name. */
  search?: string;
  /** Max rows to return. Default 500, capped at 5000. */
  limit?: number;
}

export const DEFAULT_CATALOG_LIMIT = 500;
export const MAX_CATALOG_LIMIT = 5000;

/** Clamp a caller-provided limit to a safe positive integer. */
export function resolveCatalogLimit(limit?: number): number {
  if (limit === undefined || !Number.isFinite(limit)) {
    return DEFAULT_CATALOG_LIMIT;
  }
  return Math.min(Math.max(Math.floor(limit), 1), MAX_CATALOG_LIMIT);
}

export interface DatabaseExecutionResult {
  fields: string[];
  rows: Array<Record<string, unknown>>;
}

/**
 * Database adapter interface for abstracting database-specific operations.
 * Allows the SDK to work with multiple database types.
 */
export interface DatabaseAdapter {
  /**
   * Execute a SQL query and return results
   * @param sql - The SQL query to execute
   * @param params - Optional query parameters for parameterized queries
   */
  execute(
    sql: string,
    params?: Record<string, string | number | boolean | string[] | number[]>,
  ): Promise<DatabaseExecutionResult>;

  /**
   * Validate SQL query (e.g., using EXPLAIN)
   * Throws an error if the SQL is invalid
   * @param sql - The SQL query to validate
   * @param params - Optional query parameters for parameterized queries
   */
  validate(
    sql: string,
    params?: Record<string, string | number | boolean | string[] | number[]>,
  ): Promise<void>;

  /**
   * Introspect database schema metadata
   */
  introspect(options?: IntrospectOptions): Promise<SchemaIntrospection>;

  /**
   * Get the database dialect/type
   */
  getDialect(): DatabaseDialect;

  /**
   * Optional: List schemas with table counts (cheap catalog read for pickers).
   */
  listSchemas?(): Promise<CatalogSchema[]>;

  /**
   * Optional: List tables/views without columns (cheap catalog read for pickers).
   */
  listTables?(options?: ListTablesOptions): Promise<CatalogTable[]>;

  /**
   * Optional: Close/cleanup database connection
   */
  close?(): Promise<void>;
}
