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
import * as modifyRoute from "./routes/modify";
import * as queryRoute from "./routes/query";
import * as vizspecRoute from "./routes/vizspec";
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
	AxisFieldInput,
	ChartModifyInput,
	ChartModifyOptions,
	ChartModifyResponse,
	DateRangeInput,
	FieldRefInput,
	SqlModifications,
	VizModifications,
} from "./routes/modify";
export type {
	AskOptions,
	AskResponse,
	ChartEnvelope,
	ContextDocument,
} from "./routes/query";
// Re-export anonymizeResults utility
export { anonymizeResults } from "./routes/query";
export type {
	VizSpecGenerateInput,
	VizSpecGenerateOptions,
	VizSpecResponse,
} from "./routes/vizspec";
// Re-export VizSpec types
export type {
	AggregateOp,
	AxisField,
	ChartEncoding,
	ChartSpec,
	ChartType,
	FieldRef,
	FieldType,
	MetricEncoding,
	MetricField,
	MetricSpec,
	StackingMode,
	TableColumn,
	TableEncoding,
	TableSpec,
	TimeUnit,
	VizSpec,
} from "./types/vizspec";

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
			tenantFieldType?: string;
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
			tenantFieldType: options?.tenantFieldType ?? "String",
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

	/**
	 * Syncs the database schema to QueryPanel for natural language query generation.
	 *
	 * This method introspects your database schema and uploads it to QueryPanel's
	 * vector store. The schema is used by the LLM to generate accurate SQL queries.
	 * Schema embedding is skipped if no changes are detected (drift detection).
	 *
	 * @param databaseName - Name of the attached database to sync
	 * @param options - Sync options including tenantId and forceReindex
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Response with sync status and chunk counts
	 *
	 * @example
	 * ```typescript
	 * // Basic schema sync (skips if no changes)
	 * await qp.syncSchema("analytics", { tenantId: "tenant_123" });
	 *
	 * // Force re-embedding even if schema hasn't changed
	 * await qp.syncSchema("analytics", {
	 *   tenantId: "tenant_123",
	 *   forceReindex: true,
	 * });
	 * ```
	 */
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

	/**
	 * Generates SQL from a natural language question and executes it.
	 *
	 * This is the primary method for converting user questions into data.
	 * It handles the complete flow: SQL generation → validation → execution → chart generation.
	 *
	 * @param question - Natural language question (e.g., "Show revenue by country")
	 * @param options - Query options including tenantId, database, and retry settings
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Response with SQL, executed data rows, and generated chart
	 * @throws {Error} When SQL generation or execution fails after all retries
	 *
	 * @example
	 * ```typescript
	 * // Basic query
	 * const result = await qp.ask("Top 10 customers by revenue", {
	 *   tenantId: "tenant_123",
	 *   database: "analytics",
	 * });
	 * console.log(result.sql);      // Generated SQL
	 * console.log(result.rows);     // Query results
	 * console.log(result.chart);    // Vega-Lite chart spec
	 *
	 * // With automatic SQL repair on failure
	 * const result = await qp.ask("Show monthly trends", {
	 *   tenantId: "tenant_123",
	 *   maxRetry: 3,  // Retry up to 3 times if SQL fails
	 * });
	 * ```
	 */
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

	// VizSpec generation

	/**
	 * Generates a VizSpec visualization specification from query results.
	 *
	 * Use this when you have raw SQL results and want to generate a chart
	 * specification without going through the full ask() flow. Useful for
	 * re-generating charts with different settings.
	 *
	 * @param input - VizSpec generation input with question, SQL, and result data
	 * @param options - Optional settings for tenant and retries
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns VizSpec specification for chart, table, or metric visualization
	 *
	 * @example
	 * ```typescript
	 * const vizspec = await qp.generateVizSpec({
	 *   question: "Revenue by country",
	 *   sql: "SELECT country, SUM(revenue) FROM orders GROUP BY country",
	 *   fields: ["country", "revenue"],
	 *   rows: queryResults,
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
	async generateVizSpec(
		input: vizspecRoute.VizSpecGenerateInput,
		options?: vizspecRoute.VizSpecGenerateOptions,
		signal?: AbortSignal,
	): Promise<vizspecRoute.VizSpecResponse> {
		return await vizspecRoute.generateVizSpec(
			this.client,
			input,
			options,
			signal,
		);
	}

	// Chart modification

	/**
	 * Modifies a chart by regenerating SQL and/or applying visualization changes.
	 *
	 * This method supports three modes of operation:
	 *
	 * 1. **SQL Modifications**: When `sqlModifications` is provided, the SQL is
	 *    regenerated using the query endpoint with modification hints. If `customSql`
	 *    is set, it's used directly without regeneration.
	 *
	 * 2. **Visualization Modifications**: When only `vizModifications` is provided,
	 *    the existing SQL is re-executed and a new chart is generated with the
	 *    specified encoding preferences.
	 *
	 * 3. **Combined**: Both SQL and visualization modifications can be applied
	 *    together. SQL is regenerated first, then viz modifications are applied.
	 *
	 * @param input - Chart modification input with source data and modifications
	 * @param options - Optional settings for tenant, user, and chart generation
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Modified chart response with SQL, data, and chart specification
	 *
	 * @example
	 * ```typescript
	 * // Change chart type and axis from an ask() response
	 * const modified = await qp.modifyChart({
	 *   sql: response.sql,
	 *   question: "revenue by country",
	 *   database: "analytics",
	 *   vizModifications: {
	 *     chartType: "bar",
	 *     xAxis: { field: "country" },
	 *     yAxis: { field: "revenue", aggregate: "sum" },
	 *   },
	 * }, { tenantId: "tenant_123" });
	 *
	 * // Change time granularity (triggers SQL regeneration)
	 * const monthly = await qp.modifyChart({
	 *   sql: response.sql,
	 *   question: "revenue over time",
	 *   database: "analytics",
	 *   sqlModifications: {
	 *     timeGranularity: "month",
	 *     dateRange: { from: "2024-01-01", to: "2024-12-31" },
	 *   },
	 * }, { tenantId: "tenant_123" });
	 *
	 * // Direct SQL edit with chart regeneration
	 * const customized = await qp.modifyChart({
	 *   sql: response.sql,
	 *   question: "revenue by country",
	 *   database: "analytics",
	 *   sqlModifications: {
	 *     customSql: "SELECT country, SUM(revenue) FROM orders GROUP BY country",
	 *   },
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
	async modifyChart(
		input: modifyRoute.ChartModifyInput,
		options?: modifyRoute.ChartModifyOptions,
		signal?: AbortSignal,
	): Promise<modifyRoute.ChartModifyResponse> {
		return await modifyRoute.modifyChart(
			this.client,
			this.queryEngine,
			input,
			options,
			signal,
		);
	}

	// Chart CRUD operations

	/**
	 * Saves a chart to the QueryPanel system for later retrieval.
	 *
	 * Charts store the SQL query, parameters, and visualization spec - never the actual data.
	 * Data is fetched live when the chart is rendered or refreshed.
	 *
	 * @param body - Chart data including title, SQL, and Vega-Lite spec
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns The saved chart with its generated ID
	 *
	 * @example
	 * ```typescript
	 * const savedChart = await qp.createChart({
	 *   title: "Revenue by Country",
	 *   sql: response.sql,
	 *   sql_params: response.params,
	 *   vega_lite_spec: response.chart.vegaLiteSpec,
	 *   target_db: "analytics",
	 * }, { tenantId: "tenant_123", userId: "user_456" });
	 * ```
	 */
	async createChart(
		body: chartsRoute.ChartCreateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<chartsRoute.SdkChart> {
		return await chartsRoute.createChart(this.client, body, options, signal);
	}

	/**
	 * Lists saved charts with optional filtering and pagination.
	 *
	 * Use `includeData: true` to execute each chart's SQL and include live data.
	 *
	 * @param options - Filtering, pagination, and data options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Paginated list of charts
	 *
	 * @example
	 * ```typescript
	 * // List charts with pagination
	 * const charts = await qp.listCharts({
	 *   tenantId: "tenant_123",
	 *   pagination: { page: 1, limit: 10 },
	 *   sortBy: "created_at",
	 *   sortDir: "desc",
	 * });
	 *
	 * // List with live data
	 * const chartsWithData = await qp.listCharts({
	 *   tenantId: "tenant_123",
	 *   includeData: true,
	 * });
	 * ```
	 */
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

	/**
	 * Retrieves a single chart by ID with live data.
	 *
	 * The chart's SQL is automatically executed and data is included in the response.
	 *
	 * @param id - Chart ID
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Chart with live data populated
	 *
	 * @example
	 * ```typescript
	 * const chart = await qp.getChart("chart_123", {
	 *   tenantId: "tenant_123",
	 * });
	 * console.log(chart.vega_lite_spec.data.values); // Live data
	 * ```
	 */
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

	/**
	 * Updates an existing chart's metadata or configuration.
	 *
	 * @param id - Chart ID to update
	 * @param body - Fields to update (partial update supported)
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated chart
	 *
	 * @example
	 * ```typescript
	 * const updated = await qp.updateChart("chart_123", {
	 *   title: "Updated Chart Title",
	 *   description: "New description",
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
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

	/**
	 * Deletes a chart permanently.
	 *
	 * @param id - Chart ID to delete
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 *
	 * @example
	 * ```typescript
	 * await qp.deleteChart("chart_123", { tenantId: "tenant_123" });
	 * ```
	 */
	async deleteChart(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await chartsRoute.deleteChart(this.client, id, options, signal);
	}

	// Active Chart CRUD operations (Dashboard)

	/**
	 * Pins a saved chart to the dashboard (Active Charts).
	 *
	 * Active Charts are used for building dashboards. Unlike the chart history,
	 * active charts are meant to be displayed together with layout metadata.
	 *
	 * @param body - Active chart config with chart_id, order, and optional meta
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Created active chart entry
	 *
	 * @example
	 * ```typescript
	 * const pinned = await qp.createActiveChart({
	 *   chart_id: savedChart.id,
	 *   order: 1,
	 *   meta: { width: "full", variant: "dark" },
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
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

	/**
	 * Lists all active charts (dashboard items) with optional live data.
	 *
	 * Use `withData: true` to execute each chart's SQL and include results.
	 * This is the primary method for loading a complete dashboard.
	 *
	 * @param options - Filtering and data options including withData
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Paginated list of active charts with optional live data
	 *
	 * @example
	 * ```typescript
	 * // Load dashboard with live data
	 * const dashboard = await qp.listActiveCharts({
	 *   tenantId: "tenant_123",
	 *   withData: true,
	 * });
	 *
	 * dashboard.data.forEach(item => {
	 *   console.log(item.chart?.title);
	 *   console.log(item.chart?.vega_lite_spec.data.values);
	 * });
	 * ```
	 */
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

	/**
	 * Retrieves a single active chart by ID.
	 *
	 * @param id - Active chart ID
	 * @param options - Options including withData for live data
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Active chart with associated chart data
	 *
	 * @example
	 * ```typescript
	 * const activeChart = await qp.getActiveChart("active_123", {
	 *   tenantId: "tenant_123",
	 *   withData: true,
	 * });
	 * ```
	 */
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

	/**
	 * Updates an active chart's order or metadata.
	 *
	 * Use this to reorder dashboard items or update layout hints.
	 *
	 * @param id - Active chart ID to update
	 * @param body - Fields to update (order, meta)
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated active chart
	 *
	 * @example
	 * ```typescript
	 * const updated = await qp.updateActiveChart("active_123", {
	 *   order: 5,
	 *   meta: { width: "half" },
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
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

	/**
	 * Removes a chart from the dashboard (unpins it).
	 *
	 * This only removes the active chart entry, not the underlying saved chart.
	 *
	 * @param id - Active chart ID to delete
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 *
	 * @example
	 * ```typescript
	 * await qp.deleteActiveChart("active_123", { tenantId: "tenant_123" });
	 * ```
	 */
	async deleteActiveChart(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await activeChartsRoute.deleteActiveChart(this.client, id, options, signal);
	}
}
