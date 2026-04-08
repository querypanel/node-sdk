import crypto from "node:crypto";
import type { IQueryPanelApi } from "../core/api-types";
import type { ParamRecord, QueryEngine } from "../core/query-engine";
import { type QueryErrorCode, QueryPipelineError } from "../errors";
import type { ChartType, VizSpec } from "../types/vizspec";

/**
 * Context document returned by the query pipeline.
 */
export interface ContextDocument {
	/** Optional source identifier for the document. */
	source?: string;
	/** Raw document content or excerpt. */
	pageContent: string;
	/** Additional metadata attached to the document. */
	metadata?: Record<string, unknown>;
	/** Optional relevance score from retrieval. */
	score?: number;
}

/**
 * Chart payload returned with query results.
 */
export interface ChartEnvelope {
	/** Vega-Lite spec when specType is "vega-lite". */
	vegaLiteSpec?: Record<string, unknown> | null;
	/** VizSpec payload when specType is "vizspec". */
	vizSpec?: VizSpec | null;
	/** Chart specification type. */
	specType: "vega-lite" | "vizspec";
	/** Optional chart generation notes or errors. */
	notes: string | null;
}

/**
 * Configuration options for query generation.
 */
export interface AskOptions {
	/** Tenant identifier for scoped access. */
	tenantId?: string;
	/** Optional user identifier for audit/telemetry. */
	userId?: string;
	/** Optional scopes to include in the auth token. */
	scopes?: string[];
	/** Override the default database name. */
	database?: string;
	/** Previous error message for retry context. */
	lastError?: string;
	/** Previous SQL statement for retry context. */
	previousSql?: string;
	/** Maximum number of retry attempts on execution failure. */
	maxRetry?: number;
	/** Maximum number of retries for chart generation. */
	chartMaxRetries?: number;
	/** Choose chart generation method. */
	chartType?: "vega-lite" | "vizspec";
	/**
	 * QueryPanel session ID for context-aware follow-ups.
	 * Use this to reuse a previously returned session for follow-up prompts.
	 */
	querypanelSessionId?: string;
	/**
	 * Pipeline version to use for query generation.
	 * - "v1" (default): Original query pipeline
	 * - "v2": Improved pipeline with intent planning, hybrid retrieval, schema linking, and SQL reflection
	 */
	pipeline?: "v1" | "v2";
	/**
	 * Optional OpenAI model id for v2 SQL generation. Omit to use server default.
	 */
	model?: string;
	/**
	 * Optional additional system prompt text for the v2 pipeline.
	 *
	 * This is appended to the backend's SQL generator/reflection system prompts
	 * (it does not change tenant isolation mechanics; it only guides the LLM).
	 *
	 * Use this for tenant-specific policies (e.g. retention windows) that must
	 * be enforced on every query.
	 *
	 * Only sent when `pipeline: "v2"`.
	 */
	systemPrompt?: string;
	/**
	 * Restrict VizSpec `chartType` values when `chartType` is `"vizspec"`.
	 * Sent to `POST /vizspec` as `supported_chart_types`. Omitted allows all standard types.
	 */
	supportedChartTypes?: ChartType[];
}

/**
 * Intent analysis result from the v2 pipeline.
 */
export interface IntentResult {
	intent: string;
	confidence: number;
	plan: {
		tables: string[];
		operations: string[];
		filters: string[];
		orderBy?: string;
		limit?: number;
	};
	ambiguities: Array<{ issue: string; suggestion: string }>;
}

/**
 * Pipeline execution trace with step-level timing.
 */
export interface PipelineTrace {
	totalDurationMs: number;
	steps: Array<{ step: string; durationMs: number }>;
}

/**
 * Response returned after executing a query.
 */
export interface AskResponse {
	/** Generated SQL statement. */
	sql: string;
	/** Parameter values for the generated SQL. */
	params: ParamRecord;
	/** Raw parameter metadata from the backend. */
	paramMetadata: Array<Record<string, unknown>>;
	/** Optional reasoning for SQL generation. */
	rationale?: string;
	/** SQL dialect selected by the backend. */
	dialect: string;
	/** Optional query identifier for traceability. */
	queryId?: string;
	/** Result rows returned by the query execution. */
	rows: Array<Record<string, unknown>>;
	/** Column names for returned rows. */
	fields: string[];
	/** Generated chart payload. */
	chart: ChartEnvelope;
	/** Optional context documents used for query generation. */
	context?: ContextDocument[];
	/** Number of attempts used for execution. */
	attempts?: number;
	/** Target database name resolved by the backend or engine. */
	target_db?: string;
	/** QueryPanel session ID for follow-up queries. */
	querypanelSessionId?: string;
	/** Intent analysis from v2 pipeline. */
	intent?: IntentResult;
	/** Pipeline execution trace from v2 pipeline. */
	trace?: PipelineTrace;
}

interface ServerQueryResponse {
	success: boolean;
	sql?: string;
	params?: Array<Record<string, unknown>>;
	dialect?: string;
	database?: string;
	table?: string;
	rationale?: string;
	queryId?: string;
	context?: ContextDocument[];
	// v2-specific fields
	intent?: IntentResult;
	trace?: PipelineTrace;
	// Error fields
	error?: string;
	code?: QueryErrorCode;
	details?: Record<string, unknown>;
}

interface ServerChartResponse {
	chart: Record<string, unknown> | null;
	notes: string | null;
}

interface ServerVizSpecResponse {
	spec: VizSpec;
	notes: string | null;
}

/**
 * Route module for natural language query generation
 * Simple orchestration following Ousterhout's principle
 */
export async function ask(
	client: IQueryPanelApi,
	queryEngine: QueryEngine,
	question: string,
	options: AskOptions,
	signal?: AbortSignal,
): Promise<AskResponse> {
	const tenantId = resolveTenantId(client, options.tenantId);
	const sessionId = crypto.randomUUID();
	const querypanelSessionId = options.querypanelSessionId ?? sessionId;
	const maxRetry = options.maxRetry ?? 0;
	let attempt = 0;
	let lastError: string | undefined = options.lastError;
	let previousSql: string | undefined = options.previousSql;

	const queryEndpoint =
		options.pipeline === "v2" ? "/v2/query" : "/query";

	while (attempt <= maxRetry) {
		// Step 1: Get SQL from backend
		console.log({ lastError, previousSql });

		const databaseName = options.database ?? queryEngine.getDefaultDatabase();
		const metadata = databaseName
			? queryEngine.getDatabaseMetadata(databaseName)
			: undefined;

		// Include tenant settings if available in metadata
		let tenantSettings: Record<string, unknown> | undefined;
		if (metadata?.tenantFieldName) {
			tenantSettings = {
				tenantFieldName: metadata.tenantFieldName,
				tenantFieldType: metadata.tenantFieldType,
				enforceTenantIsolation: metadata.enforceTenantIsolation,
			};
		}

		const queryResponse = await client.postWithHeaders<ServerQueryResponse>(
			queryEndpoint,
			{
				question,
				...(querypanelSessionId ? { session_id: querypanelSessionId } : {}),
				...(lastError ? { last_error: lastError } : {}),
				...(previousSql ? { previous_sql: previousSql } : {}),
				...(options.maxRetry ? { max_retry: options.maxRetry } : {}),
				...(tenantSettings ? { tenant_settings: tenantSettings } : {}),
				...(databaseName ? { database: databaseName } : {}),
				...(metadata?.dialect ? { dialect: metadata.dialect } : {}),
				...(options.model?.trim() ? { model: options.model.trim() } : {}),
				...(options.pipeline === "v2" && options.systemPrompt?.trim()
					? { system_prompt: options.systemPrompt.trim() }
					: {}),
			},
			tenantId,
			options.userId,
			options.scopes,
			signal,
			sessionId,
		);
		const responseSessionId =
			queryResponse.headers.get("x-querypanel-session-id") ??
			querypanelSessionId;

		// Handle pipeline errors from server
		if (!queryResponse.data.success) {
			throw new QueryPipelineError(
				queryResponse.data.error || "Query generation failed",
				queryResponse.data.code || "INTERNAL_ERROR",
				queryResponse.data.details,
			);
		}

		const sql = queryResponse.data.sql;
		const dialect = queryResponse.data.dialect;
		if (!sql || !dialect) {
			throw new Error("Query response missing required SQL or dialect");
		}

		const dbName =
			queryResponse.data.database ??
			options.database ??
			queryEngine.getDefaultDatabase();
		if (!dbName) {
			throw new Error(
				"No database attached. Call attachPostgres, attachClickhouse, or attachBigQuery first.",
			);
		}

		// Step 2: Map and validate parameters
		const paramMetadata = Array.isArray(queryResponse.data.params)
			? queryResponse.data.params
			: [];
		const paramValues = queryEngine.mapGeneratedParams(paramMetadata);

		// Step 3: Execute SQL with tenant isolation
		try {
			const execution = await queryEngine.validateAndExecute(
				sql,
				paramValues,
				dbName,
				tenantId,
			);
			const rows = execution.rows ?? [];

			// Step 4: Generate chart if we have data
			const chartType = options.chartType ?? "vega-lite"; // Default to vega-lite for backward compatibility
			let chart: ChartEnvelope = {
				specType: chartType,
				notes: rows.length === 0 ? "Query returned no rows." : null,
			};

			if (rows.length > 0) {
				if (chartType === "vizspec") {
					// Use new VizSpec generation
					const vizspecResponse = await client.post<ServerVizSpecResponse>(
						"/vizspec",
						{
							question,
							sql,
							rationale: queryResponse.data.rationale,
							fields: execution.fields,
							rows: anonymizeResults(rows),
							max_retries: options.chartMaxRetries ?? 3,
							query_id: queryResponse.data.queryId,
							...(options.supportedChartTypes?.length
								? {
										supported_chart_types: options.supportedChartTypes,
									}
								: {}),
						},
						tenantId,
						options.userId,
						options.scopes,
						signal,
						sessionId,
					);

					chart = {
						vizSpec: vizspecResponse.spec,
						specType: "vizspec",
						notes: vizspecResponse.notes,
					};
				} else {
					// Use traditional Vega-Lite chart generation
					const chartResponse = await client.post<ServerChartResponse>(
						"/chart",
						{
							question,
							sql,
							rationale: queryResponse.data.rationale,
							fields: execution.fields,
							rows: anonymizeResults(rows),
							max_retries: options.chartMaxRetries ?? 3,
							query_id: queryResponse.data.queryId,
						},
						tenantId,
						options.userId,
						options.scopes,
						signal,
						sessionId,
					);

					chart = {
						vegaLiteSpec: chartResponse.chart
							? {
									...chartResponse.chart,
									data: { values: rows },
								}
							: null,
						specType: "vega-lite",
						notes: chartResponse.notes,
					};
				}
			}

			return {
				sql,
				params: paramValues,
				paramMetadata,
				rationale: queryResponse.data.rationale,
				dialect,
				queryId: queryResponse.data.queryId,
				rows,
				fields: execution.fields,
				chart,
				context: queryResponse.data.context,
				attempts: attempt + 1,
				target_db: dbName,
				querypanelSessionId: responseSessionId ?? undefined,
				intent: queryResponse.data.intent,
				trace: queryResponse.data.trace,
			};
		} catch (error) {
			attempt++;

			// If we've exhausted all retries, throw the error
			if (attempt > maxRetry) {
				throw error;
			}

			// Save error and SQL for next retry
			lastError = error instanceof Error ? error.message : String(error);
			previousSql = queryResponse.data.sql ?? previousSql;

			// Log retry attempt
			console.warn(
				`SQL execution failed (attempt ${attempt}/${maxRetry + 1}): ${lastError}. Retrying...`,
			);
		}
	}

	// This should never be reached, but TypeScript needs it
	throw new Error("Unexpected error in ask retry loop");
}

function resolveTenantId(client: IQueryPanelApi, tenantId?: string): string {
	const resolved = tenantId ?? client.getDefaultTenantId();
	if (!resolved) {
		throw new Error(
			"tenantId is required. Provide it per request or via defaultTenantId option.",
		);
	}
	return resolved;
}

export function anonymizeResults(
	rows: Array<Record<string, unknown>>,
): Array<Record<string, string>> {
	if (!rows?.length) return [];
	return rows.map((row) => {
		const masked: Record<string, string> = {};
		Object.entries(row).forEach(([key, value]) => {
			if (value === null) masked[key] = "null";
			else if (Array.isArray(value)) masked[key] = "array";
			else masked[key] = typeof value;
		});
		return masked;
	});
}
