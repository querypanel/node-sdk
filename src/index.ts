import {
	BigQueryAdapter,
	type BigQueryAdapterOptions,
	type BigQueryClientFn,
	type BigQueryQueryRequest,
	type BigQueryQueryResult,
} from "./adapters/bigquery";
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
import type { IQueryPanelApi, RequestHandler } from "./core/api-types";
import { ApiClient } from "./core/client";
import { CallbackApiClient } from "./core/callback-client";
import {
	type DatabaseMetadata,
	type ParamRecord,
	QueryEngine,
} from "./core/query-engine";
import { QueryErrorCode, QueryPipelineError } from "./errors";
import * as activeChartsRoute from "./routes/active-charts";
import * as chartsRoute from "./routes/charts";
import * as dashboardsRoute from "./routes/dashboards";
import * as ingestRoute from "./routes/ingest";
import * as modifyRoute from "./routes/modify";
import * as queryRoute from "./routes/query";
import * as sessionsRoute from "./routes/sessions";
import * as vizspecRoute from "./routes/vizspec";
import type { SchemaIntrospection } from "./schema/types";
import type { ChartType } from "./types/vizspec";

// Re-export all public types
export { BigQueryAdapter, ClickHouseAdapter, PostgresAdapter };

// Re-export error types
export type { QueryErrorCode as QueryErrorCodeType } from "./errors";
export { QueryErrorCode, QueryPipelineError };

export type {
	BigQueryAdapterOptions,
	BigQueryClientFn,
	BigQueryQueryRequest,
	BigQueryQueryResult,
	ClickHouseAdapterOptions,
	ClickHouseClientFn,
	DatabaseAdapter,
	DatabaseDialect,
	PostgresAdapterOptions,
	PostgresClientFn,
	SchemaIntrospection,
};

// Re-export callback API types for in-process usage
export type {
	IQueryPanelApi,
	RequestHandler,
	RequestHandlerOptions,
	RequestHandlerResult,
} from "./core/api-types";
export { CallbackApiClient } from "./core/callback-client";

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

export type {
	DashboardCreateInput,
	DashboardForkInput,
	DashboardForkUpdateInput,
	DashboardListOptions,
	DashboardUpdateInput,
	SdkDashboard,
} from "./routes/dashboards";
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
	IntentResult,
	PipelineTrace,
} from "./routes/query";
// Re-export anonymizeResults utility
export { anonymizeResults } from "./routes/query";
export type {
	SdkSession,
	SdkSessionTurn,
	SessionGetOptions,
	SessionListOptions,
	SessionUpdateInput,
} from "./routes/sessions";
export type {
	VizSpecGenerateInput,
	VizSpecGenerateOptions,
	VizSpecResponse,
} from "./routes/vizspec";
// Re-export VizSpec types
export {
	ALL_VIZ_CHART_TYPES,
} from "./types/vizspec";
export type {
	AggregateOp,
	AxisField,
	ChartEncoding,
	ChartSpec,
	ChartType,
	EncodingHints,
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
	ValueFormat,
	VizSpec,
	VizSpecGeneratorInput,
	VizSpecKind,
	VizSpecResult,
} from "./types/vizspec";

/** Options when constructing the SDK with HTTP (default) or with a custom API implementation. */
export interface QueryPanelSdkAPIOptions {
	defaultTenantId?: string;
	additionalHeaders?: Record<string, string>;
	fetch?: typeof fetch;
	/**
	 * Use a custom API implementation (e.g. CallbackApiClient) instead of HTTP.
	 * When set, baseUrl and privateKey are ignored and no requests are sent to querypanel-sdk;
	 * use this when calling the SDK from within your own API to avoid recursion.
	 */
	api?: IQueryPanelApi;
	/**
	 * Restrict VizSpec chart kinds (when using `chartType: "vizspec"`).
	 * Omitted means all types: line, bar, column, area, scatter, pie.
	 * Per-request `supportedChartTypes` on `ask` / `modifyChart` overrides this default.
	 */
	supportedChartTypes?: ChartType[];
}

/**
 * Main SDK class - Thin orchestrator
 * Delegates to deep modules (ApiClient or custom API, QueryEngine, route modules)
 * Following Ousterhout's principle: "Simple interface hiding complexity"
 */
export class QueryPanelSdkAPI {
	private readonly client: IQueryPanelApi;
	private readonly queryEngine: QueryEngine;
	private readonly supportedChartTypes?: ChartType[];

	/**
	 * @param workspaceId - Workspace UUID (same value as your org in the dashboard). Passed to the API as JWT claim `organizationId`.
	 */
	constructor(
		baseUrl: string,
		privateKey: string,
		workspaceId: string,
		options?: QueryPanelSdkAPIOptions,
	) {
		if (options?.api) {
			this.client = options.api;
		} else {
			if (!baseUrl) throw new Error("Base URL is required");
			if (!privateKey) throw new Error("Private key is required");
			if (!workspaceId) throw new Error("Workspace ID is required");
			this.client = new ApiClient(baseUrl, privateKey, workspaceId, {
				defaultTenantId: options?.defaultTenantId,
				additionalHeaders: options?.additionalHeaders,
				fetch: options?.fetch,
			});
		}
		this.queryEngine = new QueryEngine();
		this.supportedChartTypes = options?.supportedChartTypes;
	}

	/**
	 * Create an SDK instance that uses a request callback instead of HTTP.
	 * Use this when calling the SDK from within your own API so that "API" calls
	 * are handled in-process (e.g. by invoking querypanel-sdk services directly)
	 * and do not cause recursion.
	 *
	 * @param workspaceId - Workspace identifier (sent as `organizationId` in JWTs and API auth)
	 * @param requestHandler - Callback invoked for each logical API request (method, path, body, tenantId, etc.)
	 * @param options - Optional defaultTenantId
	 * @returns QueryPanelSdkAPI instance; attach databases and call ask(), syncSchema(), etc. as usual
	 *
	 * @example
	 * ```ts
	 * const qp = QueryPanelSdkAPI.withCallbacks(
	 *   process.env.QUERYPANEL_WORKSPACE_ID!,
	 *   async (opts) => {
	 *     // Call your in-process services (e.g. querypanel-sdk logic) instead of HTTP
	 *     if (opts.path === '/query' || opts.path === '/v2/query') {
	 *       const result = await yourSqlGenerator.generate(opts.body, opts.tenantId);
	 *       return { data: result, headers: new Headers({ 'x-querypanel-session-id': result.sessionId }) };
	 *     }
	 *     if (opts.path === '/chart') return { data: await yourChartService.generate(opts.body) };
	 *     // ... other paths
	 *     return { data: await yourProxy(opts) };
	 *   },
	 *   { defaultTenantId: process.env.DEFAULT_TENANT_ID }
	 * );
	 * qp.attachPostgres('db', createClient, { tenantFieldName: 'tenant_id' });
	 * const result = await qp.ask('Top 10 orders', { tenantId: 't1', database: 'db' });
	 * ```
	 */
	static withCallbacks(
		workspaceId: string,
		requestHandler: RequestHandler,
		options?: { defaultTenantId?: string; supportedChartTypes?: ChartType[] },
	): QueryPanelSdkAPI {
		const api = new CallbackApiClient(requestHandler, {
			defaultTenantId: options?.defaultTenantId,
		});
		return new QueryPanelSdkAPI("", "", workspaceId, {
			api,
			supportedChartTypes: options?.supportedChartTypes,
		});
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

	attachBigQuery(
		name: string,
		clientFn: BigQueryClientFn,
		options: BigQueryAdapterOptions & {
			description?: string;
			tags?: string[];
			tenantFieldName?: string;
			tenantFieldType?: string;
			enforceTenantIsolation?: boolean;
		},
	): void {
		const adapter = new BigQueryAdapter(clientFn, options);

		const metadata: DatabaseMetadata = {
			name,
			dialect: "bigquery",
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
	 * console.log(result.querypanelSessionId); // Use for follow-ups
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
			{
				...options,
				supportedChartTypes:
					options.supportedChartTypes ?? this.supportedChartTypes,
			},
			signal,
		);
	}

	// Embedded dashboard / JWT

	/**
	 * Creates a JWT for the given tenant (and optional userId, scopes).
	 * Use this when you need to pass a token to the embed (e.g. frontend or demo).
	 * Only available when using the HTTP client (ApiClient); not available when using withCallbacks.
	 */
	async createJwt(options: {
		tenantId: string;
		userId?: string;
		scopes?: string[];
	}): Promise<string> {
		const tenantId = options.tenantId?.trim();
		if (!tenantId) {
			throw new Error("tenantId is required");
		}
		if (!(this.client instanceof ApiClient)) {
			throw new Error(
				"createJwt is not available when using withCallbacks. Use the SDK with baseUrl + privateKey to create JWTs.",
			);
		}
		return await this.client.createJwt(
			tenantId,
			options.userId,
			options.scopes,
		);
	}

	/**
	 * Runs SQL against an attached database with tenant isolation applied.
	 * Used by embed run-sql (e.g. dashboard chart execution). Requires a database
	 * to be attached (e.g. via attachPostgres) and options.tenantId.
	 */
	async runSqlForDashboard(
		input: {
			sql: string;
			params?: Record<string, unknown>;
			database?: string;
		},
		options: { tenantId: string },
	): Promise<{ rows: unknown[]; fields: string[] }> {
		const tenantId = options.tenantId?.trim();
		if (!tenantId) {
			throw new Error("tenantId is required");
		}
		const databaseName = input.database ?? "db";
		const params = (input.params ?? {}) as ParamRecord;
		const result = await this.queryEngine.validateAndExecute(
			input.sql,
			params,
			databaseName,
			tenantId,
		);
		return { rows: result.rows, fields: result.fields };
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
			{
				...input,
				supported_chart_types:
					input.supported_chart_types ?? this.supportedChartTypes,
			},
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
			options
				? {
						...options,
						supportedChartTypes:
							options.supportedChartTypes ?? this.supportedChartTypes,
					}
				: this.supportedChartTypes
					? { supportedChartTypes: this.supportedChartTypes }
					: undefined,
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

	// Session history CRUD operations

	/**
	 * Lists query sessions with pagination and filtering.
	 *
	 * @param options - Filtering, pagination, and sort options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Paginated list of sessions
	 *
	 * @example
	 * ```typescript
	 * const sessions = await qp.listSessions({
	 *   tenantId: "tenant_123",
	 *   pagination: { page: 1, limit: 20 },
	 *   sortBy: "updated_at",
	 * });
	 * ```
	 */
	async listSessions(
		options?: sessionsRoute.SessionListOptions,
		signal?: AbortSignal,
	): Promise<sessionsRoute.PaginatedResponse<sessionsRoute.SdkSession>> {
		return await sessionsRoute.listSessions(this.client, options, signal);
	}

	/**
	 * Retrieves a session by session_id with optional turn history.
	 *
	 * @param sessionId - QueryPanel session identifier used in ask()
	 * @param options - Tenant, user, scopes, and includeTurns flag
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Session metadata with optional turns
	 *
	 * @example
	 * ```typescript
	 * const session = await qp.getSession("session_123", {
	 *   tenantId: "tenant_123",
	 *   includeTurns: true,
	 * });
	 * ```
	 */
	async getSession(
		sessionId: string,
		options?: sessionsRoute.SessionGetOptions,
		signal?: AbortSignal,
	): Promise<sessionsRoute.SdkSession> {
		return await sessionsRoute.getSession(
			this.client,
			sessionId,
			options,
			signal,
		);
	}

	/**
	 * Updates session metadata (title).
	 *
	 * @param sessionId - QueryPanel session identifier to update
	 * @param body - Fields to update
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated session
	 *
	 * @example
	 * ```typescript
	 * const updated = await qp.updateSession(
	 *   "session_123",
	 *   { title: "Q4 Revenue Analysis" },
	 *   { tenantId: "tenant_123" },
	 * );
	 * ```
	 */
	async updateSession(
		sessionId: string,
		body: sessionsRoute.SessionUpdateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<sessionsRoute.SdkSession> {
		return await sessionsRoute.updateSession(
			this.client,
			sessionId,
			body,
			options,
			signal,
		);
	}

	/**
	 * Deletes a session and its turn history.
	 *
	 * @param sessionId - QueryPanel session identifier to delete
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 *
	 * @example
	 * ```typescript
	 * await qp.deleteSession("session_123", { tenantId: "tenant_123" });
	 * ```
	 */
	async deleteSession(
		sessionId: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await sessionsRoute.deleteSession(this.client, sessionId, options, signal);
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

	// Dashboard CRUD operations

	/**
	 * Creates a new dashboard with BlockNote content.
	 *
	 * @param body - Dashboard configuration including name and BlockNote content
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Created dashboard
	 *
	 * @example
	 * ```typescript
	 * const dashboard = await qp.createDashboard({
	 *   name: "Sales Dashboard",
	 *   description: "Monthly sales metrics",
	 *   content_json: blockNoteContent,
	 *   editor_type: "blocknote",
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
	async createDashboard(
		body: dashboardsRoute.DashboardCreateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.createDashboard(
			this.client,
			body,
			options,
			signal,
		);
	}

	/**
	 * Lists dashboards with pagination and filtering.
	 *
	 * @param options - Filtering and pagination options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Paginated list of dashboards
	 *
	 * @example
	 * ```typescript
	 * const dashboards = await qp.listDashboards({
	 *   tenantId: "tenant_123",
	 *   status: "deployed",
	 *   pagination: { page: 1, limit: 10 },
	 * });
	 * ```
	 */
	async listDashboards(
		options?: dashboardsRoute.DashboardListOptions,
		signal?: AbortSignal,
	): Promise<chartsRoute.PaginatedResponse<dashboardsRoute.SdkDashboard>> {
		return await dashboardsRoute.listDashboards(this.client, options, signal);
	}

	/**
	 * Gets a dashboard by ID.
	 *
	 * @param id - Dashboard ID
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Dashboard details
	 *
	 * @example
	 * ```typescript
	 * const dashboard = await qp.getDashboard("dash_123", {
	 *   tenantId: "tenant_123",
	 * });
	 * ```
	 */
	async getDashboard(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.getDashboard(
			this.client,
			id,
			options,
			signal,
		);
	}

	/**
	 * Gets a dashboard for a specific tenant.
	 * Returns customer fork if exists, otherwise returns the original dashboard.
	 *
	 * @param id - Dashboard ID
	 * @param tenantId - Tenant ID to check for fork
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Dashboard (fork if exists, original otherwise)
	 *
	 * @example
	 * ```typescript
	 * const dashboard = await qp.getDashboardForTenant(
	 *   "dash_123",
	 *   "tenant_456",
	 * );
	 * ```
	 */
	async getDashboardForTenant(
		id: string,
		tenantId: string,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.getDashboardForTenant(
			this.client,
			id,
			tenantId,
			options,
			signal,
		);
	}

	/**
	 * Updates a dashboard.
	 *
	 * @param id - Dashboard ID to update
	 * @param body - Fields to update
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated dashboard
	 *
	 * @example
	 * ```typescript
	 * const updated = await qp.updateDashboard("dash_123", {
	 *   name: "Updated Dashboard",
	 *   content_json: newBlockNoteContent,
	 * }, { tenantId: "tenant_123" });
	 * ```
	 */
	async updateDashboard(
		id: string,
		body: dashboardsRoute.DashboardUpdateInput,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.updateDashboard(
			this.client,
			id,
			body,
			options,
			signal,
		);
	}

	/**
	 * Updates dashboard status (deploy/undeploy).
	 *
	 * @param id - Dashboard ID
	 * @param status - New status
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated dashboard
	 *
	 * @example
	 * ```typescript
	 * const deployed = await qp.updateDashboardStatus(
	 *   "dash_123",
	 *   "deployed",
	 *   { tenantId: "tenant_123" },
	 * );
	 * ```
	 */
	async updateDashboardStatus(
		id: string,
		status: "draft" | "deployed",
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.updateDashboardStatus(
			this.client,
			id,
			status,
			options,
			signal,
		);
	}

	/**
	 * Deletes a dashboard.
	 *
	 * @param id - Dashboard ID to delete
	 * @param options - Tenant, user, and scope options
	 * @param signal - Optional AbortSignal for cancellation
	 *
	 * @example
	 * ```typescript
	 * await qp.deleteDashboard("dash_123", { tenantId: "tenant_123" });
	 * ```
	 */
	async deleteDashboard(
		id: string,
		options?: { tenantId?: string; userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await dashboardsRoute.deleteDashboard(this.client, id, options, signal);
	}

	// Dashboard Fork operations (Customer Customization)

	/**
	 * Forks a dashboard for customer customization (copy-on-write).
	 *
	 * Creates a full copy of the dashboard that the customer can edit independently.
	 * The original dashboard remains unchanged.
	 *
	 * @param id - Dashboard ID to fork
	 * @param input - Fork configuration with tenant_id
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Forked dashboard
	 *
	 * @example
	 * ```typescript
	 * const fork = await qp.forkDashboard("dash_123", {
	 *   tenant_id: "tenant_456",
	 *   name: "Customer's Custom Dashboard",
	 * });
	 * ```
	 */
	async forkDashboard(
		id: string,
		input: dashboardsRoute.DashboardForkInput,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.forkDashboard(
			this.client,
			id,
			input,
			options,
			signal,
		);
	}

	/**
	 * Updates a customer fork.
	 *
	 * @param forkId - Fork ID to update
	 * @param input - Update data including tenant_id
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Updated fork
	 *
	 * @example
	 * ```typescript
	 * const updated = await qp.updateFork("fork_123", {
	 *   tenant_id: "tenant_456",
	 *   content_json: newBlockNoteContent,
	 * });
	 * ```
	 */
	async updateFork(
		forkId: string,
		input: dashboardsRoute.DashboardForkUpdateInput,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.updateFork(
			this.client,
			forkId,
			input,
			options,
			signal,
		);
	}

	/**
	 * Rollbacks a fork to the original dashboard.
	 * This deletes the fork and the customer will see the original again.
	 *
	 * @param forkId - Fork ID to rollback
	 * @param tenantId - Tenant ID
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Original dashboard
	 *
	 * @example
	 * ```typescript
	 * const original = await qp.rollbackFork("fork_123", "tenant_456");
	 * ```
	 */
	async rollbackFork(
		forkId: string,
		tenantId: string,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard> {
		return await dashboardsRoute.rollbackFork(
			this.client,
			forkId,
			tenantId,
			options,
			signal,
		);
	}

	/**
	 * Deletes a customer fork.
	 *
	 * @param forkId - Fork ID to delete
	 * @param tenantId - Tenant ID
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 *
	 * @example
	 * ```typescript
	 * await qp.deleteFork("fork_123", "tenant_456");
	 * ```
	 */
	async deleteFork(
		forkId: string,
		tenantId: string,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<void> {
		await dashboardsRoute.deleteFork(
			this.client,
			forkId,
			tenantId,
			options,
			signal,
		);
	}

	/**
	 * Lists all customer forks for a tenant.
	 *
	 * @param tenantId - Tenant ID
	 * @param options - Additional auth options
	 * @param signal - Optional AbortSignal for cancellation
	 * @returns Array of forks
	 *
	 * @example
	 * ```typescript
	 * const forks = await qp.listForksForTenant("tenant_456");
	 * ```
	 */
	async listForksForTenant(
		tenantId: string,
		options?: { userId?: string; scopes?: string[] },
		signal?: AbortSignal,
	): Promise<dashboardsRoute.SdkDashboard[]> {
		return await dashboardsRoute.listForksForTenant(
			this.client,
			tenantId,
			options,
			signal,
		);
	}
}
