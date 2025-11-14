export type DatabaseKind = "clickhouse" | "postgres" | string;

export interface DatabaseIdentifier {
  kind: DatabaseKind;
  name: string;
  schema?: string;
  version?: string;
}

export interface ColumnStatistics {
  compressedBytes?: number;
  uncompressedBytes?: number;
  distinctValues?: number;
}

export interface ColumnSchema {
  name: string;
  type: string;
  rawType?: string;
  nullable: boolean;
  defaultKind?: string;
  defaultExpression?: string;
  comment?: string;
  isPrimaryKey: boolean;
  isForeignKey: boolean;
  foreignKeyTable?: string;
  foreignKeyColumn?: string;
  maxLength?: number;
  precision?: number;
  scale?: number;
  codec?: string;
  ttlExpression?: string;
  statistics?: ColumnStatistics;
}

export interface IndexSchema {
  name: string;
  columns: string[];
  unique: boolean;
  type: string;
  definition?: string;
}

export interface ConstraintSchema {
  name: string;
  type: "PRIMARY KEY" | "FOREIGN KEY" | "UNIQUE" | "CHECK" | string;
  columns: string[];
  referencedTable?: string;
  referencedColumns?: string[];
  definition?: string;
}

export interface TableStatistics {
  totalRows?: number;
  totalBytes?: number;
  uncompressedBytes?: number;
}

export interface TableSchema {
  name: string;
  schema: string;
  type: "table" | "view" | string;
  engine?: string;
  comment?: string;
  statistics?: TableStatistics;
  columns: ColumnSchema[];
  indexes: IndexSchema[];
  constraints: ConstraintSchema[];
}

export interface SchemaIntrospection {
  db: DatabaseIdentifier;
  tables: TableSchema[];
  introspectedAt: string;
  metadata?: Record<string, unknown>;
}

export interface IntrospectOptions {
  /** Optional allow-list of table names to introspect (schema-qualified or bare). */
  tables?: string[];
}
