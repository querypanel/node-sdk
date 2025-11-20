import type {
  IntrospectOptions,
  SchemaIntrospection,
} from "../schema/types";

export type DatabaseDialect = "clickhouse" | "postgres" | "mysql";

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
   * Optional: Close/cleanup database connection
   */
  close?(): Promise<void>;
}
