import type { DatabaseAdapter, DatabaseDialect } from "../adapters/types";

export type ParamValue = string | number | boolean | string[] | number[];
export type ParamRecord = Record<string, ParamValue>;

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

	attachDatabase(name: string, adapter: DatabaseAdapter, metadata: DatabaseMetadata): void {
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

	mapGeneratedParams(params: Array<Record<string, unknown>>): ParamRecord {
		const record: ParamRecord = {};

		params.forEach((param, index) => {
			const value = param.value as ParamValue | undefined;
			if (value === undefined) {
				return;
			}
			const nameCandidate =
				(typeof param.name === "string" && param.name.trim()) ||
				(typeof param.placeholder === "string" && param.placeholder.trim()) ||
				(typeof param.position === "number" && String(param.position)) ||
				String(index + 1);
			const key = nameCandidate.replace(/[{}:$]/g, "").trim();
			record[key] = value;
		});

		return record;
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
		const paramKey = tenantField;
		params[paramKey] = tenantId;

		const normalizedSql = sql.toLowerCase();
		if (normalizedSql.includes(tenantField.toLowerCase())) {
			return sql;
		}

		const tenantPredicate =
			metadata.dialect === "clickhouse"
				? `${tenantField} = {${tenantField}:${metadata.tenantFieldType ?? "String"}}`
				: `${tenantField} = '${tenantId}'`;

		if (/\bwhere\b/i.test(sql)) {
			return sql.replace(
				/\bwhere\b/i,
				(match) => `${match} ${tenantPredicate} AND `,
			);
		}

		return `${sql} WHERE ${tenantPredicate}`;
	}
}
