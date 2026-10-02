import type { DatabaseAdapter, DatabaseDialect } from "../adapters/types";
import { mapGeneratedParams as mapGeneratedParamsFromMetadata } from "./map-generated-params";
import type { ParamRecord } from "./param-types";

export type { ParamRecord, ParamValue } from "./param-types";

export interface DatabaseMetadata {
	name: string;
	dialect: DatabaseDialect;
	description?: string;
	tags?: string[];
	tenantFieldName?: string;
	tenantFieldType?: string;
	enforceTenantIsolation?: boolean;
}

export interface DatabaseExecutionResult {
	rows: Array<Record<string, unknown>>;
	fields: string[];
}

/**
 * Deep module: Hides SQL execution complexity and tenant isolation logic
 * Following Ousterhout's principle: "Information hiding"
 */
export class QueryEngine {
	private databases = new Map<string, DatabaseAdapter>();
	private databaseMetadata = new Map<string, DatabaseMetadata>();
	private defaultDatabase?: string;

	attachDatabase(
		name: string,
		adapter: DatabaseAdapter,
		metadata: DatabaseMetadata,
	): void {
		this.databases.set(name, adapter);
		this.databaseMetadata.set(name, metadata);
		if (!this.defaultDatabase) {
			this.defaultDatabase = name;
		}
	}

	getDatabase(name?: string): DatabaseAdapter {
		const dbName = name ?? this.defaultDatabase;
		if (!dbName) {
			throw new Error("No database attached.");
		}
		const adapter = this.databases.get(dbName);
		if (!adapter) {
			throw new Error(
				`Database '${dbName}' not found. Attached: ${Array.from(
					this.databases.keys(),
				).join(", ")}`,
			);
		}
		return adapter;
	}

	getDatabaseMetadata(name?: string): DatabaseMetadata | undefined {
		const dbName = name ?? this.defaultDatabase;
		if (!dbName) return undefined;
		return this.databaseMetadata.get(dbName);
	}

	getDefaultDatabase(): string | undefined {
		return this.defaultDatabase;
	}

	async validateAndExecute(
		sql: string,
		params: ParamRecord,
		databaseName: string,
		tenantId: string,
	): Promise<DatabaseExecutionResult> {
		const adapter = this.getDatabase(databaseName);
		const metadata = this.getDatabaseMetadata(databaseName);

		// Apply tenant isolation if configured
		let finalSql = sql;
		if (metadata) {
			finalSql = this.ensureTenantIsolation(sql, params, metadata, tenantId);
		}

		// Validate SQL
		await adapter.validate(finalSql, params);

		// Execute
		const result = await adapter.execute(finalSql, params);
		return {
			rows: result.rows,
			fields: result.fields,
		};
	}

	async execute(
		sql: string,
		params: ParamRecord | undefined,
		databaseName?: string,
	): Promise<Array<Record<string, unknown>>> {
		try {
			const adapter = this.getDatabase(databaseName);
			const result = await adapter.execute(sql, params);
			return result.rows;
		} catch (error) {
			console.warn(
				`Failed to execute SQL locally for database '${databaseName}':`,
				error,
			);
			return [];
		}
	}

	/**
	 * Maps API params (array in SQL placeholder order $1, $2, $3) to ParamRecord.
	 * Sets both numeric keys ("1", "2", "3") and named keys so that:
	 * - Postgres adapter uses numeric order for positional params (matches SQL).
	 * - Named keys remain for tenant isolation / other lookups.
	 */
	mapGeneratedParams(params: Array<Record<string, unknown>>): ParamRecord {
		return mapGeneratedParamsFromMetadata(params);
	}

	private ensureTenantIsolation(
		sql: string,
		params: ParamRecord,
		metadata: DatabaseMetadata,
		tenantId: string,
	): string {
		if (
			!metadata.tenantFieldName ||
			metadata.enforceTenantIsolation === false
		) {
			return sql;
		}

		const tenantField = metadata.tenantFieldName;
		const normalizedSql = sql.toLowerCase();
		if (normalizedSql.includes(tenantField.toLowerCase())) {
			return sql;
		}

		let tenantPredicate: string;

		if (metadata.dialect === "clickhouse") {
			// ClickHouse supports named parameters natively
			const paramKey = tenantField;
			params[paramKey] = tenantId;
			tenantPredicate = `${tenantField} = {${tenantField}:${metadata.tenantFieldType ?? "String"}}`;
		} else if (metadata.dialect === "bigquery") {
			// BigQuery uses @paramName
			params[tenantField] = tenantId;
			tenantPredicate = `${tenantField} = @${tenantField}`;
		} else {
			const paramIndex = this.nextPostgresPlaceholderIndex(sql, params);
			params[String(paramIndex)] = tenantId;
			params[tenantField] = tenantId;
			tenantPredicate = `${tenantField} = $${paramIndex}`;
		}

		if (/\bwhere\b/i.test(sql)) {
			return sql.replace(
				/\bwhere\b/i,
				(match) => `${match} ${tenantPredicate} AND `,
			);
		}

		return `${sql} WHERE ${tenantPredicate}`;
	}

	private nextPostgresPlaceholderIndex(
		sql: string,
		params: ParamRecord,
	): number {
		let max = 0;
		for (const match of sql.matchAll(/\$(\d+)\b/g)) {
			const index = Number.parseInt(match[1], 10);
			if (Number.isFinite(index) && index > max) {
				max = index;
			}
		}

		if (max > 0) {
			return max + 1;
		}

		const numericKeys = Object.keys(params)
			.filter((key) => /^\d+$/.test(key))
			.map((key) => Number.parseInt(key, 10))
			.filter((index) => Number.isFinite(index));

		if (numericKeys.length > 0) {
			return Math.max(...numericKeys) + 1;
		}

		return 1;
	}
}
