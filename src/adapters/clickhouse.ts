import type {
  ColumnSchema,
  IntrospectOptions,
  SchemaIntrospection,
  TableSchema,
} from "../schema/types";
import {
  extractFixedStringLength,
  extractPrecisionScale,
  isNullableType,
  parseKeyExpression,
  unwrapTypeModifiers,
} from "../utils/clickhouse";
import type { DatabaseAdapter, DatabaseExecutionResult } from "./types";

type QueryParams = {
  query: string;
  format?: string;
};

type ClickHouseSettings = Record<string, unknown>;
type Row = Record<string, unknown>;

export interface ClickHouseAdapterOptions {
  /** Optional logical database name used in introspection metadata. */
  database?: string;
  /** Override the default response format used for query execution. */
  defaultFormat?: QueryParams["format"];
  /**
   * Optional database kind label. Defaults to "clickhouse" but allows
   * sub-classing/custom branding if needed.
   */
  kind?: SchemaIntrospection["db"]["kind"];
  /**
   * Optional allow-list of table names.
   * When specified, introspection and queries are restricted to these tables only.
   * ClickHouse tables are not schema-qualified, so just provide table names.
   */
  allowedTables?: string[];
}

export type ClickHouseQueryResult = { json: () => Promise<unknown> };

export type ClickHouseClientFn = (
  params: QueryParams & {
    query_params?: Record<string, unknown>;
    clickhouse_settings?: ClickHouseSettings;
  },
) => Promise<ClickHouseQueryResult | Array<Record<string, unknown>> | Row[]>;

interface QueryOptions {
  params?: Record<string, unknown>;
  format?: QueryParams["format"];
  settings?: Record<string, unknown>;
}

type TableRow = {
  name: string;
  engine: string;
  comment: string | null;
  total_rows: string | number | null;
  total_bytes: string | number | null;
  primary_key: string | null;
  sorting_key?: string | null;
};

type ColumnRow = {
  table: string;
  name: string;
  type: string;
  position: number;
  default_kind: string | null;
  default_expression: string | null;
  comment: string | null;
  is_in_primary_key: string | number | null;
};

export class ClickHouseAdapter implements DatabaseAdapter {
  private readonly databaseName: string;
  private readonly defaultFormat: QueryParams["format"];
  private readonly kind: SchemaIntrospection["db"]["kind"];
  private readonly allowedTables?: string[];

  constructor(
    private readonly clientFn: ClickHouseClientFn,
    options: ClickHouseAdapterOptions = {},
  ) {
    this.databaseName = options.database ?? "default";
    this.defaultFormat = options.defaultFormat ?? "JSONEachRow";
    this.kind = options.kind ?? "clickhouse";
    if (options.allowedTables) {
      this.allowedTables = normalizeTableFilter(options.allowedTables);
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

    const queryOptions: QueryOptions = {
      format: this.defaultFormat,
    };
    if (params) {
      queryOptions.params = params;
    }

    const rows = await this.query<Record<string, unknown>>(sql, queryOptions);
    const fields = rows.length > 0 ? Object.keys(rows[0] ?? {}) : [];
    return { fields, rows };
  }

  async validate(
    sql: string,
    params?: Record<string, string | number | boolean | string[] | number[]>,
  ): Promise<void> {
    const queryOptions: QueryOptions = {
      format: this.defaultFormat,
    };
    if (params) {
      queryOptions.params = params;
    }

    await this.query(`EXPLAIN ${sql}`, queryOptions);
  }

  getDialect() {
    return "clickhouse" as const;
  }

  async introspect(options?: IntrospectOptions): Promise<SchemaIntrospection> {
    // Use adapter-level allowedTables if no specific tables provided in options
    const tablesToIntrospect = options?.tables
      ? normalizeTableFilter(options.tables)
      : this.allowedTables;
    const allowTables = tablesToIntrospect ?? [];
    const hasFilter = allowTables.length > 0;
    const queryParams: Record<string, unknown> = {
      db: this.databaseName,
    };
    if (hasFilter) {
      queryParams.tables = allowTables;
    }

    const filterClause = hasFilter ? " AND name IN {tables:Array(String)}" : "";
    const tables = await this.query<TableRow>(
      `SELECT name, engine, comment, total_rows, total_bytes, primary_key, sorting_key
       FROM system.tables
       WHERE database = {db:String}${filterClause}
       ORDER BY name`,
      { params: queryParams },
    );

    const columnFilterClause = hasFilter
      ? " AND table IN {tables:Array(String)}"
      : "";
    const columns = await this.query<ColumnRow>(
      `SELECT table, name, type, position, default_kind, default_expression, comment, is_in_primary_key
       FROM system.columns
       WHERE database = {db:String}${columnFilterClause}
       ORDER BY table, position`,
      { params: queryParams },
    );

    const columnsByTable = new Map<string, ColumnSchema[]>();
    for (const rawColumn of columns) {
      const list = columnsByTable.get(rawColumn.table) ?? [];
      list.push(transformColumnRow(rawColumn));
      columnsByTable.set(rawColumn.table, list);
    }

    const tableSchemas: TableSchema[] = tables.map((table) => {
      const tableColumns = columnsByTable.get(table.name) ?? [];
      const primaryKeyColumns = parseKeyExpression(table.primary_key);
      const totalRows = toNumber(table.total_rows);
      const totalBytes = toNumber(table.total_bytes);

      for (const column of tableColumns) {
        column.isPrimaryKey =
          column.isPrimaryKey || primaryKeyColumns.includes(column.name);
      }

      const indexes = primaryKeyColumns.length
        ? [
            {
              name: "primary_key",
              columns: primaryKeyColumns,
              unique: true,
              type: "PRIMARY KEY",
              ...(table.primary_key ? { definition: table.primary_key } : {}),
            },
          ]
        : [];

      const constraints = primaryKeyColumns.length
        ? [
            {
              name: "primary_key",
              type: "PRIMARY KEY",
              columns: primaryKeyColumns,
            },
          ]
        : [];

      const base: TableSchema = {
        name: table.name,
        schema: this.databaseName,
        type: asTableType(table.engine),
        engine: table.engine,
        columns: tableColumns,
        indexes,
        constraints,
      };

      const comment = sanitize(table.comment);
      if (comment !== undefined) {
        base.comment = comment;
      }

      const statistics = buildTableStatistics(totalRows, totalBytes);
      if (statistics) {
        base.statistics = statistics;
      }

      return base;
    });

    return {
      db: {
        kind: this.kind,
        name: this.databaseName,
      },
      tables: tableSchemas,
      introspectedAt: new Date().toISOString(),
    };
  }

  /**
   * Validate that the SQL query only references allowed tables.
   * This is a basic validation that extracts table-like patterns from the query.
   */
  private validateQueryTables(sql: string): void {
    if (!this.allowedTables || this.allowedTables.length === 0) {
      return;
    }

    const allowedSet = new Set(this.allowedTables);

    // Extract potential table references from SQL
    // This regex looks for identifiers after FROM/JOIN keywords
    // ClickHouse may use database.table notation
    const tablePattern =
      /(?:FROM|JOIN)\s+(?:FINAL\s+)?(?:(?:[a-zA-Z_][a-zA-Z0-9_]*)\.)?(["'`]?[a-zA-Z_][a-zA-Z0-9_]*["'`]?)/gi;
    const matches = sql.matchAll(tablePattern);

    for (const match of matches) {
      const table = match[1]?.replace(/["'`]/g, "");
      if (table) {
        if (!allowedSet.has(table)) {
          throw new Error(
            `Query references table "${table}" which is not in the allowed tables list`,
          );
        }
      }
    }
  }

  async close(): Promise<void> {
    // No-op: lifecycle of the underlying client is controlled by the caller.
  }

  private async query<T>(sql: string, options?: QueryOptions): Promise<T[]> {
    const params: QueryParams & {
      query_params?: Record<string, unknown>;
      clickhouse_settings?: ClickHouseSettings;
    } = {
      query: sql,
    };

    const format = options?.format ?? this.defaultFormat;
    if (format !== undefined) {
      params.format = format;
    }

    if (options?.params) {
      params.query_params = options.params;
    }

    if (options?.settings) {
      params.clickhouse_settings = options.settings as ClickHouseSettings;
    }

    const result = await this.clientFn(params);
    return this.extractRows<T>(result);
  }

  private async extractRows<T>(
    result: ClickHouseQueryResult | Array<Record<string, unknown>> | Row[],
  ): Promise<T[]> {
    if (Array.isArray(result)) {
      return result as T[];
    }

    if (
      result &&
      typeof (result as ClickHouseQueryResult).json === "function"
    ) {
      const payload = await (result as ClickHouseQueryResult).json();
      return normalizePayload<T>(payload);
    }

    return [];
  }
}

function normalizePayload<T>(payload: unknown): T[] {
  if (Array.isArray(payload)) {
    return payload as T[];
  }
  if (payload && typeof payload === "object") {
    const maybeData = (payload as { data?: unknown }).data;
    if (Array.isArray(maybeData)) {
      return maybeData as T[];
    }
  }
  return [];
}

function normalizeTableFilter(tables?: string[] | null): string[] {
  if (!tables?.length) return [];
  const seen = new Set<string>();
  const normalized: string[] = [];
  for (const table of tables) {
    if (!table) continue;
    const trimmed = table.trim();
    if (!trimmed) continue;
    const parts = trimmed.split(".");
    const tableName = parts[parts.length - 1];
    if (!tableName || seen.has(tableName)) continue;
    seen.add(tableName);
    normalized.push(tableName);
  }
  return normalized;
}

function transformColumnRow(row: ColumnRow): ColumnSchema {
  const nullable = isNullableType(row.type);
  const unwrappedType = unwrapTypeModifiers(row.type);
  const { precision, scale } = extractPrecisionScale(row.type);
  const maxLength = extractFixedStringLength(row.type);

  const column: ColumnSchema = {
    name: row.name,
    type: unwrappedType,
    rawType: row.type,
    nullable,
    isPrimaryKey: Boolean(toNumber(row.is_in_primary_key)),
    isForeignKey: false,
  };

  const defaultKind = sanitize(row.default_kind);
  if (defaultKind !== undefined) column.defaultKind = defaultKind;

  const defaultExpression = sanitize(row.default_expression);
  if (defaultExpression !== undefined) {
    column.defaultExpression = defaultExpression;
  }

  const comment = sanitize(row.comment);
  if (comment !== undefined) column.comment = comment;

  if (maxLength !== undefined) column.maxLength = maxLength;
  if (precision !== undefined) column.precision = precision;
  if (scale !== undefined) column.scale = scale;

  return column;
}

function asTableType(engine: unknown): TableSchema["type"] {
  if (typeof engine === "string") {
    const normalized = engine.toLowerCase();
    // ClickHouse view engines: View, MaterializedView, LiveView
    if (normalized.includes("view")) {
      return "view";
    }
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
