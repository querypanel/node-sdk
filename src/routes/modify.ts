import crypto from "node:crypto";
import type { IQueryPanelApi } from "../core/api-types";
import type { ParamRecord, QueryEngine } from "../core/query-engine";
import type {
	AggregateOp,
	ChartType,
	FieldType,
	StackingMode,
	TimeUnit,
	ValueFormat,
	VizSpec,
	VizSpecKind,
} from "../types/vizspec";
import {
	type AskResponse,
	anonymizeResults,
	type ChartEnvelope,
	resolveQueryPipeline,
} from "./query";
import { throwIfGenerationErrorResponse } from "../errors";
import { resolveQuery } from "./queries";

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
	/** Change the VizSpec kind (chart, table, metric, map). Applies to vizspec only. */
	kind?: VizSpecKind;
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
	 * Server-issued id of the query to modify or re-execute.
	 * From ask() / modifyChart() response: response.queryId
	 * From saved chart: chart.query_id
	 *
	 * Preferred over `sql`: when present the SQL and params are loaded from the server
	 * (scoped to the authenticated tenant) and `sql` / `params` are ignored.
	 */
	queryId?: string;

	/**
	 * The SQL query to modify or re-execute. Required when `queryId` is not provided.
	 * From ask() response: response.sql
	 * From saved chart: chart.sql
	 *
	 * Kept for backward compatibility; prefer `queryId`.
	 */
	sql?: string;

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
	/**
	 * Query pipeline. Only `"v2"` is accepted (`POST /v2/query`).
	 * `"v1"` was removed: passing it fails typecheck and throws at runtime.
	 * Omit this field; chart modifications always use v2.
	 */
	pipeline?: "v2";
	/**
	 * Fast path for structured controls (pinned charts): date-only changes
	 * substitute params or call `/v2/rewrite-datefilter` instead of `/v2/query`,
	 * and chart generation is skipped when the caller keeps its own encoding
	 * (`chart.vizSpec` / `vegaLiteSpec` are then omitted). Default: false.
	 */
	lightweight?: boolean;
	/**
	 * QueryPanel session ID for context-aware follow-ups.
	 * Pass the querypanelSessionId from a previous ask() response
	 * to preserve conversation history when modifying charts.
	 * If omitted, modifyChart starts a new QueryPanel session and sends
	 * the original question with modification hints.
	 */
	querypanelSessionId?: string;
	/**
	 * Restrict VizSpec chart types for this call (merged with SDK default from constructor when omitted).
	 */
	supportedChartTypes?: ChartType[];
	/**
	 * When true, rationale includes engineer-style schema detail (tables, columns).
	 * When false (default), rationale is client-safe for end users.
	 */
	debug?: boolean;
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
	hasSessionContext = false,
): string {
	const hints: string[] = [];

	if (modifications.timeGranularity) {
		hints.push(`group results by ${modifications.timeGranularity}`);
	}

	if (modifications.dateRange) {
		const from = normalizeDateInput(modifications.dateRange.from);
		const to = normalizeDateInput(modifications.dateRange.to);

		if (from && to) {
			hints.push(`change date range to ${from} through ${to}`);
		} else if (from) {
			hints.push(`change start date to ${from}`);
		} else if (to) {
			hints.push(`change end date to ${to}`);
		}
	}

	if (modifications.additionalInstructions) {
		hints.push(modifications.additionalInstructions);
	}

	if (hints.length === 0) {
		return originalQuestion;
	}

	// Hints-only when continuing a QueryPanel session that already has the question.
	if (hasSessionContext) {
		return hints.join(", ");
	}

	return `${originalQuestion} (${hints.join(", ")})`;
}

const REWRITE_DATEFILTER_ENDPOINT = "/v2/rewrite-datefilter";
const START_PARAM_KEY_REGEX = /(^|_)(start|from)(_|$)/i;
const END_PARAM_KEY_REGEX = /(^|_)(end|to)(_|$)/i;
const ISO_DATETIME_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z?$/;
const SQL_DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const SQL_DATETIME_RE = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

function normalizeDateInput(value?: string): string | undefined {
	if (!value) return undefined;
	const trimmed = value.trim();
	if (!trimmed) return undefined;

	if (ISO_DATETIME_RE.test(trimmed)) {
		return trimmed
			.replace("T", " ")
			.replace(/\.\d+Z?$/, "")
			.replace(/Z$/, "");
	}

	return trimmed;
}

function keyLooksLikeDateBoundary(
	key: string,
	boundary: "start" | "end",
): boolean {
	return boundary === "start"
		? START_PARAM_KEY_REGEX.test(key)
		: END_PARAM_KEY_REGEX.test(key);
}

function hasTimeComponent(value: unknown): boolean {
	return typeof value === "string" && /\d{2}:\d{2}:\d{2}/.test(value);
}

function formatDateOverride(
	dateValue: string,
	boundary: "start" | "end",
	existingValue: unknown,
): string {
	// Preserve date-only params as date-only values.
	if (SQL_DATE_RE.test(dateValue) && !hasTimeComponent(existingValue)) {
		return dateValue;
	}

	if (SQL_DATE_RE.test(dateValue)) {
		return `${dateValue} ${boundary === "start" ? "00:00:00" : "23:59:59"}`;
	}

	if (SQL_DATETIME_RE.test(dateValue)) {
		return dateValue;
	}

	// Best effort fallback for already-normalized non-standard values.
	return dateValue;
}

function normalizeGeneratedParamKey(
	param: Record<string, unknown>,
	index: number,
): string {
	const nameCandidate =
		(typeof param.name === "string" && param.name.trim()) ||
		(typeof param.placeholder === "string" && param.placeholder.trim()) ||
		(typeof param.position === "number" && String(param.position)) ||
		String(index + 1);

	return nameCandidate
		.replace(/[{}]/g, "")
		.replace(/(.+):.*$/, "$1")
		.replace(/^[:$]/, "")
		.trim();
}

function collectParamKeys(
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>>,
): string[] {
	const keys = new Set<string>(Object.keys(params));
	for (let i = 0; i < paramMetadata.length; i++) {
		const param = paramMetadata[i];
		if (!param) continue;
		keys.add(normalizeGeneratedParamKey(param, i));
	}
	return [...keys];
}

/** `$1` → `1`, `{start_date:Date}` → `start_date`, `@start_date` → `start_date`. */
function placeholderKey(token: string): string {
	if (token.startsWith("$") || token.startsWith("@")) return token.slice(1);
	return token.slice(1, -1).replace(/:.+$/, "").trim();
}

function isDateLike(value: unknown): boolean {
	return typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value.trim());
}

/**
 * Param keys the SQL uses as a date bound (`created_at >= $1`, `{start_date:Date}`).
 * Postgres stores both the `$n` key it binds and a named alias; only the `$n` key
 * changes the query.
 */
function dateBoundKeysFromSql(sql: string): {
	start: Set<string>;
	end: Set<string>;
} {
	const start = new Set<string>();
	const end = new Set<string>();
	const comparison =
		/(>=|<=|>|<)\s*(\$\d+|\{[^}]+\}|@[A-Za-z_][\w]*)/g;
	const between =
		/BETWEEN\s+(\$\d+|\{[^}]+\}|@[A-Za-z_][\w]*)\s+AND\s+(\$\d+|\{[^}]+\}|@[A-Za-z_][\w]*)/gi;

	for (const match of sql.matchAll(comparison)) {
		const key = placeholderKey(match[2] ?? "");
		if (!key) continue;
		if (match[1] === ">=" || match[1] === ">") start.add(key);
		else end.add(key);
	}

	for (const match of sql.matchAll(between)) {
		const startKey = placeholderKey(match[1] ?? "");
		const endKey = placeholderKey(match[2] ?? "");
		if (startKey) start.add(startKey);
		if (endKey) end.add(endKey);
	}

	return { start, end };
}

function paramValueForKey(
	key: string,
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>>,
): unknown {
	if (key in params) return params[key];
	for (let i = 0; i < paramMetadata.length; i++) {
		const param = paramMetadata[i];
		if (!param) continue;
		if (normalizeGeneratedParamKey(param, i) === key) return param.value;
	}
	return undefined;
}

/** Date-like params this range can overwrite: SQL placeholders plus named aliases. */
function dateBoundKeys(
	sql: string,
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>>,
	boundary: "start" | "end",
): string[] {
	const fromSql = dateBoundKeysFromSql(sql)[boundary];
	const keys: string[] = [];
	for (const key of collectParamKeys(params, paramMetadata)) {
		const named = keyLooksLikeDateBoundary(key, boundary);
		if (!named && !fromSql.has(key)) continue;
		if (!isDateLike(paramValueForKey(key, params, paramMetadata))) continue;
		keys.push(key);
	}
	return keys;
}

/**
 * True when date-range-only SQL mods can be applied by substituting existing
 * start/end params (no NL→SQL regeneration).
 */
function canSubstituteDateRangeParams(
	dateRange: DateRangeInput | undefined,
	sql: string,
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>> = [],
): boolean {
	if (!dateRange) return false;
	const from = normalizeDateInput(dateRange.from);
	const to = normalizeDateInput(dateRange.to);
	if (!from && !to) return false;

	if (from && dateBoundKeys(sql, params, paramMetadata, "start").length === 0) {
		return false;
	}
	if (to && dateBoundKeys(sql, params, paramMetadata, "end").length === 0) {
		return false;
	}
	return true;
}

/** Date-range only (no custom SQL, granularity, or extra NL instructions). */
function isDateRangeOnlySqlModification(
	modifications: SqlModifications | undefined,
): boolean {
	if (!modifications?.dateRange) return false;
	if (modifications.customSql) return false;
	if (modifications.timeGranularity) return false;
	if (
		typeof modifications.additionalInstructions === "string" &&
		modifications.additionalInstructions.trim().length > 0
	) {
		return false;
	}
	return true;
}

function applyDateRangeOverrides(
	dateRange: DateRangeInput | undefined,
	sql: string,
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>>,
): void {
	if (!dateRange) return;

	const from = normalizeDateInput(dateRange.from);
	const to = normalizeDateInput(dateRange.to);
	if (!from && !to) return;

	const startKeys = new Set(
		from ? dateBoundKeys(sql, params, paramMetadata, "start") : [],
	);
	const endKeys = new Set(
		to ? dateBoundKeys(sql, params, paramMetadata, "end") : [],
	);

	for (const [key, value] of Object.entries(params)) {
		if (startKeys.has(key)) {
			params[key] = formatDateOverride(from ?? "", "start", value);
		}
		if (endKeys.has(key)) {
			params[key] = formatDateOverride(to ?? "", "end", value);
		}
	}

	for (let i = 0; i < paramMetadata.length; i++) {
		const param = paramMetadata[i];
		if (!param) continue;
		const key = normalizeGeneratedParamKey(param, i);

		if (startKeys.has(key)) {
			param.value = formatDateOverride(from ?? "", "start", param.value);
		}
		if (endKeys.has(key)) {
			param.value = formatDateOverride(to ?? "", "end", param.value);
		}
	}
}

/** Previous params for rewrite-datefilter: prefer metadata, else `{name,value}` entries. */
function buildPreviousParams(
	params: ParamRecord,
	paramMetadata: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> {
	if (paramMetadata.length > 0) return paramMetadata;
	return Object.entries(params).map(([name, value]) => ({ name, value }));
}

/**
 * Builds viz modification hints for the chart generation endpoint.
 */
function buildVizHints(
	modifications: VizModifications,
): Record<string, unknown> {
	const hints: Record<string, unknown> = {};

	if (modifications.kind) {
		hints.kind = modifications.kind;
	}

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
 * Remove VizSpec-only hints for Vega-Lite chart generation.
 */
function stripVizSpecOnlyHints(
	hints: Record<string, unknown>,
): Record<string, unknown> {
	if (!("kind" in hints)) {
		return hints;
	}

	const { kind: _kind, ...rest } = hints as { kind?: unknown };
	return rest;
}

/**
 * Resolves tenant ID from options or client default.
 */
function resolveTenantId(client: IQueryPanelApi, tenantId?: string): string {
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
	client: IQueryPanelApi,
	queryEngine: QueryEngine,
	input: ChartModifyInput,
	options?: ChartModifyOptions,
	signal?: AbortSignal,
): Promise<ChartModifyResponse> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const sessionId = crypto.randomUUID();
	const querypanelSessionId = options?.querypanelSessionId ?? sessionId;
	const hasQuerypanelSessionContext = !!options?.querypanelSessionId;
	const chartType = options?.chartType ?? "vega-lite";

	const hasSqlMods = !!input.sqlModifications;
	const hasVizMods = !!input.vizModifications;
	const hasCustomSql = !!input.sqlModifications?.customSql;
	// `lightweight` (pinned chart controls): the caller reapplies the previous
	// chart encoding, so date-only changes avoid NL→SQL and no chart LLM runs.
	const lightweight = options?.lightweight === true;
	const dateOnly =
		lightweight && isDateRangeOnlySqlModification(input.sqlModifications);
	const skipChartLlm =
		lightweight && !hasCustomSql && (dateOnly || (hasVizMods && !hasSqlMods));

	resolveQueryPipeline(options?.pipeline);
	const queryEndpoint = "/v2/query";

	// Determine which SQL to use. A queryId takes priority: the server-stored SQL and
	// params are used and any client-supplied sql / params are ignored.
	let currentSql = input.sql;
	let finalParams = input.params ?? {};
	let paramMetadata: Array<Record<string, unknown>> = [];
	let rationale: string | undefined;
	let queryId: string | undefined;
	if (input.queryId) {
		const resolved = await resolveQuery(
			client,
			queryEngine,
			input.queryId,
			{ tenantId, userId: options?.userId, scopes: options?.scopes },
			signal,
		);
		currentSql = resolved.sql;
		finalParams = resolved.params;
		paramMetadata = resolved.paramMetadata;
		queryId = resolved.queryId;
	}
	if (!currentSql) {
		throw new Error("modifyChart requires either queryId or sql.");
	}
	let finalSql = currentSql;
	let sqlChanged = false;
	let finalQuestion = input.question;

	// Get database metadata for tenant settings
	const databaseName = input.database ?? queryEngine.getDefaultDatabase();
	if (!databaseName) {
		throw new Error(
			"No database specified. Provide database in input or attach a default database.",
		);
	}

	const metadata = queryEngine.getDatabaseMetadata(databaseName);
	let tenantSettings: Record<string, unknown> | undefined;
	if (metadata?.enforceTenantIsolation === false) {
		tenantSettings = {
			tenantFieldName: metadata.tenantFieldName ?? "",
			tenantFieldType: metadata.tenantFieldType ?? "String",
			enforceTenantIsolation: false,
		};
	} else if (metadata?.tenantFieldName) {
		tenantSettings = {
			tenantFieldName: metadata.tenantFieldName,
			tenantFieldType: metadata.tenantFieldType,
			enforceTenantIsolation: true,
		};
	}

	// Path 1: Custom SQL provided - use it directly
	if (hasCustomSql) {
		finalSql = input.sqlModifications!.customSql!;
		finalParams = {};
		paramMetadata = [];
		// Client-supplied SQL is not a server-generated query, so it has no id.
		queryId = undefined;
		sqlChanged = true;
	}
	// Path 2a: Date-range only + existing start/end params → substitute, keep SQL
	else if (
		dateOnly &&
		canSubstituteDateRangeParams(
			input.sqlModifications?.dateRange,
			finalSql,
			finalParams,
			paramMetadata,
		)
	) {
		finalParams = { ...finalParams };
		applyDateRangeOverrides(
			input.sqlModifications?.dateRange,
			finalSql,
			finalParams,
			paramMetadata,
		);
		sqlChanged = false;
	}
	// Path 2b: SQL modifications (non-custom) - regenerate via query endpoint,
	// or via the cheaper date-filter rewrite when only the date range changed.
	else if (hasSqlMods && !hasCustomSql) {
		const modifiedQuestion = buildModifiedQuestion(
			input.question,
			input.sqlModifications!,
			hasQuerypanelSessionContext,
		);
		finalQuestion = modifiedQuestion;

		const sharedBody = {
			session_id: querypanelSessionId,
			previous_sql: currentSql,
			...(tenantSettings ? { tenant_settings: tenantSettings } : {}),
			...(databaseName ? { database: databaseName } : {}),
			...(metadata?.dialect ? { dialect: metadata.dialect } : {}),
			debug: options?.debug ?? false,
		};
		const [endpoint, body] = dateOnly
			? [
					REWRITE_DATEFILTER_ENDPOINT,
					{
						...sharedBody,
						question: input.question,
						previous_params: buildPreviousParams(finalParams, paramMetadata),
						date_range: {
							from: normalizeDateInput(input.sqlModifications?.dateRange?.from),
							to: normalizeDateInput(input.sqlModifications?.dateRange?.to),
						},
					},
				]
			: [
					queryEndpoint,
					{
						...sharedBody,
						question: modifiedQuestion,
						...(options?.maxRetry ? { max_retry: options.maxRetry } : {}),
					},
				];

		const queryResponse = await client.post<ServerQueryResponse>(
			endpoint,
			body,
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
		applyDateRangeOverrides(
			input.sqlModifications?.dateRange,
			finalSql,
			finalParams,
			paramMetadata,
		);
		rationale = queryResponse.rationale;
		queryId = queryResponse.queryId;
		sqlChanged = finalSql !== currentSql;
	}

	// Execute the SQL
	const execution = await queryEngine.validateAndExecute(
		finalSql,
		finalParams,
		databaseName,
		tenantId,
	);
	const rows = execution.rows ?? [];

	// Generate chart encoding even when the query returns no rows,
	// so headless clients can show "No Data" with the correct chart type.
	let chart: ChartEnvelope = {
		specType: chartType,
		notes: rows.length === 0 ? "Query returned no rows." : null,
	};

	// Build viz hints if modifications provided
	const vizHints = hasVizMods ? buildVizHints(input.vizModifications!) : {};
	const vizHintsForChart =
		chartType === "vizspec" ? vizHints : stripVizSpecOnlyHints(vizHints);

	if (!skipChartLlm && chartType === "vizspec") {
		const vizspecResponse = await client.post<ServerVizSpecResponse>(
			"/vizspec",
			{
				question: finalQuestion,
				sql: finalSql,
				rationale,
				fields: execution.fields,
				rows: anonymizeResults(rows),
				max_retries: options?.chartMaxRetries ?? 3,
				query_id: queryId,
				// Include viz hints for the chart generator
				...(hasVizMods ? { encoding_hints: vizHintsForChart } : {}),
				...(options?.supportedChartTypes?.length
					? { supported_chart_types: options.supportedChartTypes }
					: {}),
			},
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
			sessionId,
		);
		throwIfGenerationErrorResponse(vizspecResponse);

		chart = {
			vizSpec: vizspecResponse.spec,
			specType: "vizspec",
			notes:
				rows.length === 0
					? (vizspecResponse.notes ?? "Query returned no rows.")
					: vizspecResponse.notes,
		};
	} else if (!skipChartLlm) {
		const chartResponse = await client.post<ServerChartResponse>(
			"/chart",
			{
				question: finalQuestion,
				sql: finalSql,
				rationale,
				fields: execution.fields,
				rows: anonymizeResults(rows),
				max_retries: options?.chartMaxRetries ?? 3,
				query_id: queryId,
				// Include viz hints for the chart generator
				...(hasVizMods ? { encoding_hints: vizHintsForChart } : {}),
			},
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
			sessionId,
		);
		throwIfGenerationErrorResponse(chartResponse);

		chart = {
			vegaLiteSpec: chartResponse.chart
				? {
						...chartResponse.chart,
						data: { values: rows },
					}
				: null,
			specType: "vega-lite",
			notes:
				rows.length === 0
					? (chartResponse.notes ?? "Query returned no rows.")
					: chartResponse.notes,
		};
	}

	return {
		sql: finalSql,
		params: finalParams,
		paramMetadata,
		rationale,
		dialect: metadata?.dialect ?? "unknown",
		queryId,
		querypanelSessionId,
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
