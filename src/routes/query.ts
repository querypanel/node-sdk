// randomUUID is available via global crypto.randomUUID() in Node.js 18+ and Deno
import type { ApiClient } from "../core/client";
import type { ParamRecord, QueryEngine } from "../core/query-engine";

export interface ContextDocument {
	source?: string;
	pageContent: string;
	metadata?: Record<string, unknown>;
	score?: number;
}

export interface ChartEnvelope {
	vegaLiteSpec: Record<string, unknown> | null;
	notes: string | null;
}

export interface AskOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	database?: string;
	lastError?: string;
	previousSql?: string;
	maxRetry?: number;
	chartMaxRetries?: number;
}

export interface AskResponse {
	sql: string;
	params: ParamRecord;
	paramMetadata: Array<Record<string, unknown>>;
	rationale?: string;
	dialect: string;
	queryId?: string;
	rows: Array<Record<string, unknown>>;
	fields: string[];
	chart: ChartEnvelope;
	context?: ContextDocument[];
	attempts?: number;
	target_db?: string;
}

interface ServerQueryResponse {
	success: boolean;
	sql: string;
	params?: Array<Record<string, unknown>>;
	dialect: string;
	database?: string;
	table?: string;
	rationale?: string;
	queryId?: string;
	context?: ContextDocument[];
}

interface ServerChartResponse {
	chart: Record<string, unknown> | null;
	notes: string | null;
}

/**
 * Route module for natural language query generation
 * Simple orchestration following Ousterhout's principle
 */
export async function ask(
	client: ApiClient,
	queryEngine: QueryEngine,
	question: string,
	options: AskOptions,
	signal?: AbortSignal,
): Promise<AskResponse> {
	const tenantId = resolveTenantId(client, options.tenantId);
	const sessionId = crypto.randomUUID();
	const maxRetry = options.maxRetry ?? 0;
	let attempt = 0;
	let lastError: string | undefined = options.lastError;
	let previousSql: string | undefined = options.previousSql;

	while (attempt <= maxRetry) {
		// Step 1: Get SQL from backend
		console.log({ lastError, previousSql });
		const queryResponse = await client.post<ServerQueryResponse>(
			"/query",
			{
				question,
				...(lastError ? { last_error: lastError } : {}),
				...(previousSql ? { previous_sql: previousSql } : {}),
				...(options.maxRetry ? { max_retry: options.maxRetry } : {}),
			},
			tenantId,
			options.userId,
			options.scopes,
			signal,
			sessionId,
		);

		const databaseName =
			queryResponse.database ??
			options.database ??
			queryEngine.getDefaultDatabase();
		if (!databaseName) {
			throw new Error(
				"No database attached. Call attachPostgres/attachClickhouse first.",
			);
		}

		// Step 2: Map and validate parameters
		const paramMetadata = Array.isArray(queryResponse.params)
			? queryResponse.params
			: [];
		const paramValues = queryEngine.mapGeneratedParams(paramMetadata);

		// Step 3: Execute SQL with tenant isolation
		try {
			const execution = await queryEngine.validateAndExecute(
				queryResponse.sql,
				paramValues,
				databaseName,
				tenantId,
			);
			const rows = execution.rows ?? [];

			// Step 4: Generate chart if we have data
			let chart: ChartEnvelope = {
				vegaLiteSpec: null,
				notes: rows.length === 0 ? "Query returned no rows." : null,
			};

			if (rows.length > 0) {
				const chartResponse = await client.post<ServerChartResponse>(
					"/chart",
					{
						question,
						sql: queryResponse.sql,
						rationale: queryResponse.rationale,
						fields: execution.fields,
						rows: anonymizeResults(rows),
						max_retries: options.chartMaxRetries ?? 3,
						query_id: queryResponse.queryId,
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
					notes: chartResponse.notes,
				};
			}

			return {
				sql: queryResponse.sql,
				params: paramValues,
				paramMetadata,
				rationale: queryResponse.rationale,
				dialect: queryResponse.dialect,
				queryId: queryResponse.queryId,
				rows,
				fields: execution.fields,
				chart,
				context: queryResponse.context,
				attempts: attempt + 1,
				target_db: databaseName,
			};
		} catch (error) {
			attempt++;

			// If we've exhausted all retries, throw the error
			if (attempt > maxRetry) {
				throw error;
			}

			// Save error and SQL for next retry
			lastError = error instanceof Error ? error.message : String(error);
			previousSql = queryResponse.sql;

			// Log retry attempt
			console.warn(
				`SQL execution failed (attempt ${attempt}/${maxRetry + 1}): ${lastError}. Retrying...`,
			);
		}
	}

	// This should never be reached, but TypeScript needs it
	throw new Error("Unexpected error in ask retry loop");
}

function resolveTenantId(client: ApiClient, tenantId?: string): string {
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
