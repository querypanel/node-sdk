import type { IQueryPanelApi } from "../core/api-types";
import type { PaginatedResponse, PaginationQuery } from "./charts";

/**
 * Dashboard interface matching querypanel-sdk schema
 */
export interface SdkDashboard {
	id: string;
	organization_id: string;
	name: string;
	description: string | null;
	status: "draft" | "deployed";
	content_json: string | null;
	widget_config: Record<string, unknown> | null;
	editor_type: "blocknote" | "custom";
	is_customer_fork: boolean;
	forked_from_dashboard_id: string | null;
	tenant_id: string | null;
	datasource_id: string | null;
	version: number;
	deployed_at: string | null;
	created_at: string;
	updated_at: string;
	created_by: string | null;
}

/**
 * Dashboard creation input
 */
export interface DashboardCreateInput {
	name: string;
	description?: string;
	content_json?: string;
	widget_config?: Record<string, unknown>;
	editor_type?: "blocknote" | "custom";
	datasource_id?: string;
}

/**
 * Dashboard update input
 */
export interface DashboardUpdateInput {
	name?: string;
	description?: string;
	content_json?: string;
	widget_config?: Record<string, unknown>;
	editor_type?: "blocknote" | "custom";
	datasource_id?: string | null;
}

/**
 * Dashboard list options
 */
export interface DashboardListOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	pagination?: PaginationQuery;
	status?: "draft" | "deployed";
	sortBy?: "name" | "created_at" | "updated_at" | "deployed_at";
	sortDir?: "asc" | "desc";
}

/**
 * Fork dashboard input
 */
export interface DashboardForkInput {
	tenant_id: string;
	name?: string;
}

/**
 * Fork update input
 */
export interface DashboardForkUpdateInput {
	tenant_id: string;
	content_json?: string;
	widget_config?: Record<string, unknown>;
}

interface RequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

function resolveTenantId(client: IQueryPanelApi, tenantId?: string): string {
	const resolved = tenantId ?? client.getDefaultTenantId();
	if (resolved === undefined) {
		throw new Error("tenantId is required (option or client default)");
	}
	return resolved;
}

/**
 * Creates a new dashboard
 */
export async function createDashboard(
	client: IQueryPanelApi,
	body: DashboardCreateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.post<SdkDashboard>(
		"/dashboards",
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Lists dashboards with pagination and filtering
 */
export async function listDashboards(
	client: IQueryPanelApi,
	options?: DashboardListOptions,
	signal?: AbortSignal,
): Promise<PaginatedResponse<SdkDashboard>> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const params = new URLSearchParams();

	if (options?.pagination?.page) {
		params.set("page", options.pagination.page.toString());
	}
	if (options?.pagination?.limit) {
		params.set("limit", options.pagination.limit.toString());
	}
	if (options?.status) {
		params.set("status", options.status);
	}
	if (options?.sortBy) {
		params.set("sort_by", options.sortBy);
	}
	if (options?.sortDir) {
		params.set("sort_dir", options.sortDir);
	}

	return await client.get<PaginatedResponse<SdkDashboard>>(
		`/dashboards?${params.toString()}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Gets a dashboard by ID
 */
export async function getDashboard(
	client: IQueryPanelApi,
	id: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.get<SdkDashboard>(
		`/dashboards/${id}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Gets a dashboard for a specific tenant (returns fork if exists, otherwise original)
 */
export async function getDashboardForTenant(
	client: IQueryPanelApi,
	id: string,
	tenantId: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const params = new URLSearchParams();
	params.set("tenant_id", tenantId);

	return await client.get<SdkDashboard>(
		`/dashboards/${id}/for-tenant?${params.toString()}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Updates a dashboard
 */
export async function updateDashboard(
	client: IQueryPanelApi,
	id: string,
	body: DashboardUpdateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.put<SdkDashboard>(
		`/dashboards/${id}`,
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Updates dashboard status (deploy/undeploy)
 */
export async function updateDashboardStatus(
	client: IQueryPanelApi,
	id: string,
	status: "draft" | "deployed",
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.patch<SdkDashboard>(
		`/dashboards/${id}/status`,
		{ status },
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Deletes a dashboard
 */
export async function deleteDashboard(
	client: IQueryPanelApi,
	id: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<void> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	await client.delete(
		`/dashboards/${id}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

// ============================================================================
// Customer Fork Operations
// ============================================================================

/**
 * Forks a dashboard for customer customization
 */
export async function forkDashboard(
	client: IQueryPanelApi,
	id: string,
	input: DashboardForkInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.post<SdkDashboard>(
		`/dashboards/${id}/fork`,
		input,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Updates a customer fork
 */
export async function updateFork(
	client: IQueryPanelApi,
	forkId: string,
	input: DashboardForkUpdateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.put<SdkDashboard>(
		`/dashboards/forks/${forkId}`,
		input,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Rollbacks a fork to the original dashboard
 */
export async function rollbackFork(
	client: IQueryPanelApi,
	forkId: string,
	tenantId: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard> {
	return await client.post<SdkDashboard>(
		`/dashboards/forks/${forkId}/rollback`,
		{ tenant_id: tenantId },
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Deletes a customer fork
 */
export async function deleteFork(
	client: IQueryPanelApi,
	forkId: string,
	tenantId: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<void> {
	const params = new URLSearchParams();
	params.set("tenant_id", tenantId);

	await client.delete(
		`/dashboards/forks/${forkId}?${params.toString()}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Lists customer forks for a tenant
 */
export async function listForksForTenant(
	client: IQueryPanelApi,
	tenantId: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkDashboard[]> {
	return await client.get<SdkDashboard[]>(
		`/dashboards/customer/${tenantId}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}
