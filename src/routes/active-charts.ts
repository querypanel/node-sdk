import type { ApiClient } from "../core/client";
import type { QueryEngine } from "../core/query-engine";
import * as charts from "./charts";

export interface SdkActiveChart {
	id: string;
	chart_id: string;
	order: number | null;
	meta: Record<string, unknown> | null;
	organization_id: string | null;
	tenant_id: string | null;
	user_id: string | null;
	created_at: string | null;
	updated_at: string | null;
	chart?: charts.SdkChart | null;
}

export interface ActiveChartCreateInput {
	chart_id: string;
	order?: number;
	meta?: Record<string, unknown>;
}

export interface ActiveChartUpdateInput {
	chart_id?: string;
	order?: number;
	meta?: Record<string, unknown>;
}

export interface ActiveChartListOptions extends charts.ChartListOptions {
	withData?: boolean;
}

interface RequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

/**
 * Route module for Active Chart CRUD operations
 * Simple pass-through to backend with optional chart data hydration
 */
export async function createActiveChart(
	client: ApiClient,
	body: ActiveChartCreateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkActiveChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.post<SdkActiveChart>(
		"/active-charts",
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

export async function listActiveCharts(
	client: ApiClient,
	queryEngine: QueryEngine,
	options?: ActiveChartListOptions,
	signal?: AbortSignal,
): Promise<charts.PaginatedResponse<SdkActiveChart>> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const params = new URLSearchParams();
	if (options?.pagination?.page)
		params.set("page", `${options.pagination.page}`);
	if (options?.pagination?.limit)
		params.set("limit", `${options.pagination.limit}`);
	if (options?.sortBy) params.set("sort_by", options.sortBy);
	if (options?.sortDir) params.set("sort_dir", options.sortDir);
	if (options?.title) params.set("name", options.title);
	if (options?.userFilter) params.set("user_id", options.userFilter);
	if (options?.createdFrom) params.set("created_from", options.createdFrom);
	if (options?.createdTo) params.set("created_to", options.createdTo);
	if (options?.updatedFrom) params.set("updated_from", options.updatedFrom);
	if (options?.updatedTo) params.set("updated_to", options.updatedTo);

	const response = await client.get<
		charts.PaginatedResponse<SdkActiveChart>
	>(
		`/active-charts${params.toString() ? `?${params.toString()}` : ""}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);

	if (options?.withData) {
		response.data = await Promise.all(
			response.data.map(async (active) => ({
				...active,
				chart: active.chart
					? await charts.getChart(
							client,
							queryEngine,
							active.chart_id,
							options,
							signal,
						)
					: null,
			})),
		);
	}

	return response;
}

export async function getActiveChart(
	client: ApiClient,
	queryEngine: QueryEngine,
	id: string,
	options?: ActiveChartListOptions,
	signal?: AbortSignal,
): Promise<SdkActiveChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const active = await client.get<SdkActiveChart>(
		`/active-charts/${encodeURIComponent(id)}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);

	if (options?.withData && active.chart_id) {
		return {
			...active,
			chart: await charts.getChart(
				client,
				queryEngine,
				active.chart_id,
				options,
				signal,
			),
		};
	}

	return active;
}

export async function updateActiveChart(
	client: ApiClient,
	id: string,
	body: ActiveChartUpdateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkActiveChart> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.put<SdkActiveChart>(
		`/active-charts/${encodeURIComponent(id)}`,
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

export async function deleteActiveChart(
	client: ApiClient,
	id: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<void> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	await client.delete(
		`/active-charts/${encodeURIComponent(id)}`,
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
