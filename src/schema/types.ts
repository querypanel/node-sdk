/**
 * Simplified schema types aligned with backend IngestRequest format
 * Following Ousterhout's principle: "Define errors out of existence"
 * - Removed indexes, constraints, foreign keys, statistics
 * - Only collect what the backend needs for vectorization
 */

export type DatabaseKind = "clickhouse" | "postgres" | string;

export interface DatabaseIdentifier {
	kind: DatabaseKind;
	name: string;
	schema?: string;
	version?: string;
}

export interface ColumnSchema {
	name: string;
	type: string;
	rawType?: string;
	isPrimaryKey: boolean;
	comment?: string;
}

export interface TableSchema {
	name: string;
	schema: string;
	type: "table" | "view" | string;
	comment?: string;
	columns: ColumnSchema[];
}

export interface SchemaIntrospection {
	db: DatabaseIdentifier;
	tables: TableSchema[];
	introspectedAt: string;
}

export interface IntrospectOptions {
	/** Optional allow-list of table names to introspect (schema-qualified or bare). */
	tables?: string[];
}
