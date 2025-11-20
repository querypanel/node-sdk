import type { ApiClient } from "../core/client";
import type { ParamRecord, QueryEngine } from "../core/query-engine";

export interface SdkChart {
	id: string;
	title: string;
	description: string | null;
	sql: string;
	sql_params: Record<string, unknown> | null;
	vega_lite_spec: Record<string, unknown>;
	query_id: string | null;
	organization_id: string | null;
	tenant_id: string | null;
	user_id: string | null;
	created_at: string | null;
	updated_at: string | null;
	active?: boolean;
	target_db?: string | null;
}

export interface ChartCreateInput {
	title: string;
	description?: string;
	sql: string;
	sql_params?: Record<string, unknown>;
	vega_lite_spec: Record<string, unknown>;
	query_id?: string;
	target_db?: string;
}

export interface ChartUpdateInput {
	title?: string;
	description?: string;
	sql?: string;
	sql_params?: Record<string, unknown>;
	vega_lite_spec?: Record<string, unknown>;
	target_db?: string;
}

export interface PaginationQuery {
	page?: number;
	limit?: number;
}

export interface PaginationInfo {
	page: number;
	limit: number;
	total: number;
	totalPages: number;
	hasNext: boolean;
	hasPrev: boolean;
}

export interface PaginatedResponse<T> {
	data: T[];
	pagination: PaginationInfo;
}

export interface ChartListOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	pagination?: PaginationQuery;
	sortBy?: "title" | "user_id" | "created_at" | "updated_at";
	sortDir?: "asc" | "desc";
	title?: string;
	userFilter?: string;
	createdFrom?: string;
	createdTo?: string;
	updatedFrom?: string;
	updatedTo?: string;
	includeData?: boolean;
}

interface RequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

/**
 * Route module for Chart CRUD operations
 * Simple pass-through to backend with optional data hydration
 */
export async function createChart(
	client: ApiClient,
	body: ChartCreateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.post<SdkChart>(
		"/charts",
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

export async function listCharts(
	client: ApiClient,
	queryEngine: QueryEngine,
	options?: ChartListOptions,
	signal?: AbortSignal,
): Promise<PaginatedResponse<SdkChart>> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const params = new URLSearchParams();
	if (options?.pagination?.page)
		params.set("page", `${options.pagination.page}`);
	if (options?.pagination?.limit)
		params.set("limit", `${options.pagination.limit}`);
	if (options?.sortBy) params.set("sort_by", options.sortBy);
	if (options?.sortDir) params.set("sort_dir", options.sortDir);
	if (options?.title) params.set("title", options.title);
	if (options?.userFilter) params.set("user_id", options.userFilter);
	if (options?.createdFrom) params.set("created_from", options.createdFrom);
	if (options?.createdTo) params.set("created_to", options.createdTo);
	if (options?.updatedFrom) params.set("updated_from", options.updatedFrom);
	if (options?.updatedTo) params.set("updated_to", options.updatedTo);

	const response = await client.get<PaginatedResponse<SdkChart>>(
		`/charts${params.toString() ? `?${params.toString()}` : ""}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);

	if (options?.includeData) {
		response.data = await Promise.all(
			response.data.map(async (chart) => ({
				...chart,
				vega_lite_spec: {
					...chart.vega_lite_spec,
					data: {
						values: await executeChartQuery(queryEngine, chart, tenantId),
					},
				},
			})),
		);
	}

	return response;
}

export async function getChart(
	client: ApiClient,
	queryEngine: QueryEngine,
	id: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const chart = await client.get<SdkChart>(
		`/charts/${encodeURIComponent(id)}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);

	return {
		...chart,
		vega_lite_spec: {
			...chart.vega_lite_spec,
			data: {
				values: await executeChartQuery(queryEngine, chart, tenantId),
			},
		},
	};
}

export async function updateChart(
	client: ApiClient,
	id: string,
	body: ChartUpdateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.put<SdkChart>(
		`/charts/${encodeURIComponent(id)}`,
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

export async function deleteChart(
	client: ApiClient,
	id: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<void> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	await client.delete(
		`/charts/${encodeURIComponent(id)}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
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

async function executeChartQuery(
	queryEngine: QueryEngine,
	chart: SdkChart,
	tenantId: string,
): Promise<Record<string, unknown>[]> {
	const databaseName = chart.target_db ?? queryEngine.getDefaultDatabase();
	if (!databaseName) {
		console.warn("No database available to execute chart query");
		return [];
	}
	try {
		const result = await queryEngine.validateAndExecute(
			chart.sql,
			(chart.sql_params as ParamRecord | null) ?? {},
			databaseName,
			tenantId,
		);
		return result.rows;
	} catch (error) {
		console.warn(`Failed to execute chart query: ${error}`);
		return [];
	}
}
