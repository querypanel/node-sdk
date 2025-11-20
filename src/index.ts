import {
	ClickHouseAdapter,
	type ClickHouseAdapterOptions,
	type ClickHouseClientFn,
} from "./adapters/clickhouse";
import {
	PostgresAdapter,
	type PostgresAdapterOptions,
	type PostgresClientFn,
} from "./adapters/postgres";
import type { DatabaseAdapter, DatabaseDialect } from "./adapters/types";
import { ApiClient } from "./core/client";
import { type DatabaseMetadata, QueryEngine } from "./core/query-engine";
import * as activeChartsRoute from "./routes/active-charts";
import * as chartsRoute from "./routes/charts";
import * as ingestRoute from "./routes/ingest";
import * as queryRoute from "./routes/query";
import type { SchemaIntrospection } from "./schema/types";

// Re-export all public types
export { ClickHouseAdapter, PostgresAdapter };

export type {
	ClickHouseAdapterOptions,
	ClickHouseClientFn,
	DatabaseAdapter,
	DatabaseDialect,
	PostgresAdapterOptions,
	PostgresClientFn,
	SchemaIntrospection,
};

// Re-export from query-engine
export type { ParamRecord, ParamValue } from "./core/query-engine";
export type {
	ActiveChartCreateInput,
	ActiveChartListOptions,
	ActiveChartUpdateInput,
	SdkActiveChart,
} from "./routes/active-charts";

export type {
	ChartCreateInput,
	ChartListOptions,
	ChartUpdateInput,
	PaginatedResponse,
	PaginationInfo,
	PaginationQuery,
	SdkChart,
} from "./routes/charts";
// Re-export route types
export type {
	IngestResponse,
	SchemaSyncOptions,
} from "./routes/ingest";
export type {
	AskOptions,
	AskResponse,
	ChartEnvelope,
	ContextDocument,
} from "./routes/query";

// Re-export anonymizeResults utility
export { anonymizeResults } from "./routes/query";

/**
 * Main SDK class - Thin orchestrator
 * Delegates to deep modules (ApiClient, QueryEngine, route modules)
 * Following Ousterhout's principle: "Simple interface hiding complexity"
 */
export class QueryPanelSdkAPI {
	private readonly client: ApiClient;
	private readonly queryEngine: QueryEngine;

	constructor(
		baseUrl: string,
		privateKey: string,
		organizationId: string,
		options?: {
			defaultTenantId?: string;
			additionalHeaders?: Record<string, string>;
			fetch?: typeof fetch;
		},
	) {
		this.client = new ApiClient(baseUrl, privateKey, organizationId, options);
		this.queryEngine = new QueryEngine();
	}

	// Database attachment methods

	attachClickhouse(
		name: string,
		clientFn: ClickHouseClientFn,
		options?: ClickHouseAdapterOptions & {
			description?: string;
			tags?: string[];
			tenantFieldName?: string;
			tenantFieldType?: string;
			enforceTenantIsolation?: boolean;
		},
	): void {
		const adapter = new ClickHouseAdapter(clientFn, options);

		const metadata: DatabaseMetadata = {
			name,
			dialect: "clickhouse",
			description: options?.description,
			tags: options?.tags,
			tenantFieldName: options?.tenantFieldName,
			tenantFieldType: options?.tenantFieldType ?? "String",
			enforceTenantIsolation: options?.tenantFieldName
				? (options?.enforceTenantIsolation ?? true)
				: undefined,
		};

		this.queryEngine.attachDatabase(name, adapter, metadata);
	}

	attachPostgres(
		name: string,
		clientFn: PostgresClientFn,
		options?: PostgresAdapterOptions & {
			description?: string;
			tags?: string[];
			tenantFieldName?: string;
			enforceTenantIsolation?: boolean;
		},
	): void {
		const adapter = new PostgresAdapter(clientFn, options);

		const metadata: DatabaseMetadata = {
			name,
			dialect: "postgres",
			description: options?.description,
			tags: options?.tags,
			tenantFieldName: options?.tenantFieldName,
			enforceTenantIsolation: options?.tenantFieldName
				? (options?.enforceTenantIsolation ?? true)
				: undefined,
		};

		this.queryEngine.attachDatabase(name, adapter, metadata);
	}

	attachDatabase(name: string, adapter: DatabaseAdapter): void {
		const metadata: DatabaseMetadata = {
			name,
			dialect: adapter.getDialect(),
		};
		this.queryEngine.attachDatabase(name, adapter, metadata);
	}

	// Schema introspection and sync

	async introspect(
		databaseName: string,
		tables?: string[],
	): Promise<SchemaIntrospection> {
		const adapter = this.queryEngine.getDatabase(databaseName);
		return await adapter.introspect(tables ? { tables } : undefined);
	}

	async syncSchema(
		databaseName: string,
		options: ingestRoute.SchemaSyncOptions,
		signal?: AbortSignal,
	): Promise<ingestRoute.IngestResponse> {
		return await ingestRoute.syncSchema(
			this.client,
			this.queryEngine,
			databaseName,
			options,
			signal,
		);
	}

	// Natural language query

	async ask(
		question: string,
		options: queryRoute.AskOptions,
		signal?: AbortSignal,
	): Promise<queryRoute.AskResponse> {
		return await queryRoute.ask(
			this.client,
			this.queryEngine,
			question,
			options,
			signal,
		);
	}

	// Chart CRUD operations

	async createChart(
		body: chartsRoute.ChartCreateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<chartsRoute.SdkChart> {
		return await chartsRoute.createChart(this.client, body, options, signal);
	}

	async listCharts(
		options?: chartsRoute.ChartListOptions,
		signal?: AbortSignal,
	): Promise<chartsRoute.PaginatedResponse<chartsRoute.SdkChart>> {
		return await chartsRoute.listCharts(
			this.client,
			this.queryEngine,
			options,
			signal,
		);
	}

	async getChart(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<chartsRoute.SdkChart> {
		return await chartsRoute.getChart(
			this.client,
			this.queryEngine,
			id,
			options,
			signal,
		);
	}

	async updateChart(
		id: string,
		body: chartsRoute.ChartUpdateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<chartsRoute.SdkChart> {
		return await chartsRoute.updateChart(
			this.client,
			id,
			body,
			options,
			signal,
		);
	}

	async deleteChart(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await chartsRoute.deleteChart(this.client, id, options, signal);
	}

	// Active Chart CRUD operations

	async createActiveChart(
		body: activeChartsRoute.ActiveChartCreateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<activeChartsRoute.SdkActiveChart> {
		return await activeChartsRoute.createActiveChart(
			this.client,
			body,
			options,
			signal,
		);
	}

	async listActiveCharts(
		options?: activeChartsRoute.ActiveChartListOptions,
		signal?: AbortSignal,
	): Promise<chartsRoute.PaginatedResponse<activeChartsRoute.SdkActiveChart>> {
		return await activeChartsRoute.listActiveCharts(
			this.client,
			this.queryEngine,
			options,
			signal,
		);
	}

	async getActiveChart(
		id: string,
		options?: activeChartsRoute.ActiveChartListOptions,
		signal?: AbortSignal,
	): Promise<activeChartsRoute.SdkActiveChart> {
		return await activeChartsRoute.getActiveChart(
			this.client,
			this.queryEngine,
			id,
			options,
			signal,
		);
	}

	async updateActiveChart(
		id: string,
		body: activeChartsRoute.ActiveChartUpdateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<activeChartsRoute.SdkActiveChart> {
		return await activeChartsRoute.updateActiveChart(
			this.client,
			id,
			body,
			options,
			signal,
		);
	}

	async deleteActiveChart(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await activeChartsRoute.deleteActiveChart(this.client, id, options, signal);
	}
}
