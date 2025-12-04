import crypto from "node:crypto";
import type { ApiClient } from "../core/client";
import type { ParamRecord, QueryEngine } from "../core/query-engine";
import type {
	AggregateOp,
	ChartType,
	FieldType,
	StackingMode,
	TimeUnit,
	ValueFormat,
	VizSpec,
} from "../types/vizspec";
import {
	type AskResponse,
	anonymizeResults,
	type ChartEnvelope,
} from "./query";

// ============================================================================
// Input Types for Modifications
// ============================================================================

/**
 * Simplified field reference for modification inputs.
 * More ergonomic than the full AxisField type.
 */
export interface AxisFieldInput {
	/** Column name from the SQL result */
	field: string;
	/** Human-friendly label for the axis */
	label?: string;
	/** Field data type */
	type?: FieldType;
	/** Aggregation operation (e.g., 'sum', 'avg') */
	aggregate?: AggregateOp;
	/** Time unit for temporal fields */
	timeUnit?: TimeUnit;
	/** Value formatting options */
	format?: ValueFormat;
}

/**
 * Simplified field reference for series/grouping fields.
 */
export interface FieldRefInput {
	/** Column name from the SQL result */
	field: string;
	/** Human-friendly label */
	label?: string;
	/** Field data type */
	type?: FieldType;
}

/**
 * Date range specification for SQL modifications.
 */
export interface DateRangeInput {
	/** Start date in ISO format (e.g., '2024-01-01') */
	from?: string;
	/** End date in ISO format (e.g., '2024-12-31') */
	to?: string;
}

/**
 * SQL modification options that trigger query regeneration.
 * When any of these are provided, a new ask() call is made.
 */
export interface SqlModifications {
	/**
	 * Direct SQL override. When provided, this SQL is executed directly
	 * without calling the query generation endpoint.
	 */
	customSql?: string;

	/**
	 * Change the time granularity of the query.
	 * Triggers SQL regeneration with hints about the desired grouping.
	 */
	timeGranularity?: TimeUnit;

	/**
	 * Filter the query to a specific date range.
	 * Triggers SQL regeneration with date filter hints.
	 */
	dateRange?: DateRangeInput;

	/**
	 * Additional natural language instructions to modify the query.
	 * These are appended to the original question as hints.
	 * Example: "exclude cancelled orders" or "only show top 10"
	 */
	additionalInstructions?: string;
}

/**
 * Visualization modification options that don't affect the SQL.
 * These changes only affect how the chart is rendered.
 */
export interface VizModifications {
	/** Change the chart type (line, bar, area, scatter, pie) */
	chartType?: ChartType;

	/** Configure the X axis field and settings */
	xAxis?: AxisFieldInput;

	/** Configure the Y axis field(s) and settings */
	yAxis?: AxisFieldInput | AxisFieldInput[];

	/** Configure the series/grouping field for multi-series charts */
	series?: FieldRefInput;

	/** Stacking mode for multi-series charts */
	stacking?: StackingMode;

	/** Maximum number of rows to display in the chart */
	limit?: number;
}

/**
 * Input for the modifyChart() method.
 * Accepts chart data from either ask() responses or saved charts.
 */
export interface ChartModifyInput {
	/**
	 * The SQL query to modify or re-execute.
	 * From ask() response: response.sql
	 * From saved chart: chart.sql
	 */
	sql: string;

	/**
	 * The original natural language question.
	 * Used when regenerating SQL with modifications.
	 */
	question: string;

	/**
	 * The database to execute the query against.
	 * From ask() response: response.target_db
	 * From saved chart: chart.target_db
	 */
	database: string;

	/**
	 * Query parameters (optional).
	 * From ask() response: response.params
	 * From saved chart: chart.sql_params
	 */
	params?: ParamRecord;

	/**
	 * SQL modifications that trigger query regeneration.
	 * When provided, a new ask() call is made with modification hints.
	 */
	sqlModifications?: SqlModifications;

	/**
	 * Visualization modifications that don't affect the SQL.
	 * Applied during chart generation.
	 */
	vizModifications?: VizModifications;
}

/**
 * Options for the modifyChart() method.
 */
export interface ChartModifyOptions {
	/** Tenant ID for multi-tenant isolation */
	tenantId?: string;
	/** User ID for audit/tracking */
	userId?: string;
	/** Permission scopes */
	scopes?: string[];
	/** Maximum retry attempts for SQL generation */
	maxRetry?: number;
	/** Maximum retry attempts for chart generation */
	chartMaxRetries?: number;
	/** Chart generation method: 'vega-lite' or 'vizspec' */
	chartType?: "vega-lite" | "vizspec";
}

/**
 * Response from modifyChart(), extending AskResponse with modification metadata.
 */
export interface ChartModifyResponse extends AskResponse {
	/** Metadata about what was modified */
	modified: {
		/** Whether the SQL was changed (regenerated or custom) */
		sqlChanged: boolean;
		/** Whether visualization settings were applied */
		vizChanged: boolean;
	};
}

// ============================================================================
// Server Response Types
// ============================================================================

interface ServerQueryResponse {
	success: boolean;
	sql: string;
	params?: Array<Record<string, unknown>>;
	dialect: string;
	database?: string;
	table?: string;
	rationale?: string;
	queryId?: string;
}

interface ServerChartResponse {
	chart: Record<string, unknown> | null;
	notes: string | null;
}

interface ServerVizSpecResponse {
	spec: VizSpec;
	notes: string | null;
}

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Builds a modified question string with SQL modification hints.
 * These hints guide the LLM to generate appropriate SQL.
 */
function buildModifiedQuestion(
	originalQuestion: string,
	modifications: SqlModifications,
): string {
	const hints: string[] = [];

	if (modifications.timeGranularity) {
		hints.push(`group results by ${modifications.timeGranularity}`);
	}

	if (modifications.dateRange) {
		const parts: string[] = [];
		if (modifications.dateRange.from) {
			parts.push(`from ${modifications.dateRange.from}`);
		}
		if (modifications.dateRange.to) {
			parts.push(`to ${modifications.dateRange.to}`);
		}
		if (parts.length > 0) {
			hints.push(`filter date range ${parts.join(" ")}`);
		}
	}

	if (modifications.additionalInstructions) {
		hints.push(modifications.additionalInstructions);
	}

	if (hints.length === 0) {
		return originalQuestion;
	}

	return `${originalQuestion} (${hints.join(", ")})`;
}

/**
 * Builds viz modification hints for the chart generation endpoint.
 */
function buildVizHints(
	modifications: VizModifications,
): Record<string, unknown> {
	const hints: Record<string, unknown> = {};

	if (modifications.chartType) {
		hints.chartType = modifications.chartType;
	}

	if (modifications.xAxis) {
		hints.xAxis = modifications.xAxis;
	}

	if (modifications.yAxis) {
		hints.yAxis = modifications.yAxis;
	}

	if (modifications.series) {
		hints.series = modifications.series;
	}

	if (modifications.stacking) {
		hints.stacking = modifications.stacking;
	}

	if (modifications.limit !== undefined) {
		hints.limit = modifications.limit;
	}

	return hints;
}

/**
 * Resolves tenant ID from options or client default.
 */
function resolveTenantId(client: ApiClient, tenantId?: string): string {
	const resolved = tenantId ?? client.getDefaultTenantId();
	if (!resolved) {
		throw new Error(
			"tenantId is required. Provide it per request or via defaultTenantId option.",
		);
	}
	return resolved;
}

// ============================================================================
// Main Function
// ============================================================================

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
 * @param client - The API client for making requests
 * @param queryEngine - The query engine for executing SQL
 * @param input - Chart modification input with source data and modifications
 * @param options - Optional settings for tenant, user, and chart generation
 * @param signal - Optional AbortSignal for cancellation
 * @returns Modified chart response with SQL, data, and chart specification
 *
 * @example
 * ```typescript
 * // Change chart type and axis
 * const modified = await modifyChart(client, engine, {
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
 * const monthly = await modifyChart(client, engine, {
 *   sql: response.sql,
 *   question: "revenue over time",
 *   database: "analytics",
 *   sqlModifications: {
 *     timeGranularity: "month",
 *     dateRange: { from: "2024-01-01", to: "2024-12-31" },
 *   },
 * }, { tenantId: "tenant_123" });
 * ```
 */
export async function modifyChart(
	client: ApiClient,
	queryEngine: QueryEngine,
	input: ChartModifyInput,
	options?: ChartModifyOptions,
	signal?: AbortSignal,
): Promise<ChartModifyResponse> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const sessionId = crypto.randomUUID();
	const chartType = options?.chartType ?? "vega-lite";

	const hasSqlMods = !!input.sqlModifications;
	const hasVizMods = !!input.vizModifications;
	const hasCustomSql = !!input.sqlModifications?.customSql;

	// Determine which SQL to use
	let finalSql = input.sql;
	let finalParams = input.params ?? {};
	let paramMetadata: Array<Record<string, unknown>> = [];
	let rationale: string | undefined;
	let queryId: string | undefined;
	let sqlChanged = false;

	// Get database metadata for tenant settings
	const databaseName = input.database ?? queryEngine.getDefaultDatabase();
	if (!databaseName) {
		throw new Error(
			"No database specified. Provide database in input or attach a default database.",
		);
	}

	const metadata = queryEngine.getDatabaseMetadata(databaseName);
	let tenantSettings: Record<string, unknown> | undefined;
	if (metadata?.tenantFieldName) {
		tenantSettings = {
			tenantFieldName: metadata.tenantFieldName,
			tenantFieldType: metadata.tenantFieldType,
			enforceTenantIsolation: metadata.enforceTenantIsolation,
		};
	}

	// Path 1: Custom SQL provided - use it directly
	if (hasCustomSql) {
		finalSql = input.sqlModifications!.customSql!;
		finalParams = {};
		paramMetadata = [];
		sqlChanged = true;
	}
	// Path 2: SQL modifications (non-custom) - regenerate via query endpoint
	else if (hasSqlMods && !hasCustomSql) {
		const modifiedQuestion = buildModifiedQuestion(
			input.question,
			input.sqlModifications!,
		);

		const queryResponse = await client.post<ServerQueryResponse>(
			"/query",
			{
				question: modifiedQuestion,
				previous_sql: input.sql,
				...(options?.maxRetry ? { max_retry: options.maxRetry } : {}),
				...(tenantSettings ? { tenant_settings: tenantSettings } : {}),
				...(databaseName ? { database: databaseName } : {}),
				...(metadata?.dialect ? { dialect: metadata.dialect } : {}),
			},
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
			sessionId,
		);

		finalSql = queryResponse.sql;
		paramMetadata = Array.isArray(queryResponse.params)
			? queryResponse.params
			: [];
		finalParams = queryEngine.mapGeneratedParams(paramMetadata);
		rationale = queryResponse.rationale;
		queryId = queryResponse.queryId;
		sqlChanged = finalSql !== input.sql;
	}

	// Execute the SQL
	const execution = await queryEngine.validateAndExecute(
		finalSql,
		finalParams,
		databaseName,
		tenantId,
	);
	const rows = execution.rows ?? [];

	// Generate chart
	let chart: ChartEnvelope = {
		specType: chartType,
		notes: rows.length === 0 ? "Query returned no rows." : null,
	};

	if (rows.length > 0) {
		// Build viz hints if modifications provided
		const vizHints = hasVizMods ? buildVizHints(input.vizModifications!) : {};

		if (chartType === "vizspec") {
			const vizspecResponse = await client.post<ServerVizSpecResponse>(
				"/vizspec",
				{
					question: input.question,
					sql: finalSql,
					rationale,
					fields: execution.fields,
					rows: anonymizeResults(rows),
					max_retries: options?.chartMaxRetries ?? 3,
					query_id: queryId,
					// Include viz hints for the chart generator
					...(hasVizMods ? { encoding_hints: vizHints } : {}),
				},
				tenantId,
				options?.userId,
				options?.scopes,
				signal,
				sessionId,
			);

			chart = {
				vizSpec: vizspecResponse.spec,
				specType: "vizspec",
				notes: vizspecResponse.notes,
			};
		} else {
			const chartResponse = await client.post<ServerChartResponse>(
				"/chart",
				{
					question: input.question,
					sql: finalSql,
					rationale,
					fields: execution.fields,
					rows: anonymizeResults(rows),
					max_retries: options?.chartMaxRetries ?? 3,
					query_id: queryId,
					// Include viz hints for the chart generator
					...(hasVizMods ? { encoding_hints: vizHints } : {}),
				},
				tenantId,
				options?.userId,
				options?.scopes,
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
		sql: finalSql,
		params: finalParams,
		paramMetadata,
		rationale,
		dialect: metadata?.dialect ?? "unknown",
		queryId,
		rows,
		fields: execution.fields,
		chart,
		attempts: 1,
		target_db: databaseName,
		modified: {
			sqlChanged,
			vizChanged: hasVizMods,
		},
	};
}
