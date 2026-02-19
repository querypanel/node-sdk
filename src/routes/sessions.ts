import type { IQueryPanelApi } from "../core/api-types";

/**
 * A single session turn containing the question and optional SQL output.
 */
export interface SdkSessionTurn {
	id: string;
	session_id: string;
	turn_index: number;
	question: string;
	sql: string | null;
	rationale: string | null;
	row_count: number | null;
	fields: string[] | null;
	error: string | null;
	created_at: string;
}

/**
 * Session metadata with optional turn history.
 */
export interface SdkSession {
	id: string;
	session_id: string;
	organization_id: string;
	tenant_id: string | null;
	user_id: string | null;
	title: string | null;
	created_at: string;
	updated_at: string;
	turns?: SdkSessionTurn[];
}

/**
 * Fields allowed when updating a session.
 */
export interface SessionUpdateInput {
	title?: string;
}

/**
 * Pagination settings for list endpoints.
 */
export interface PaginationQuery {
	page?: number;
	limit?: number;
}

/**
 * Pagination metadata returned by list endpoints.
 */
export interface PaginationInfo {
	page: number;
	limit: number;
	total: number;
	totalPages: number;
	hasNext: boolean;
	hasPrev: boolean;
}

/**
 * Generic paginated response wrapper.
 */
export interface PaginatedResponse<T> {
	data: T[];
	pagination: PaginationInfo;
}

/**
 * Options for listing sessions with filters and pagination.
 */
export interface SessionListOptions {
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
}

/**
 * Options for retrieving a session.
 */
export interface SessionGetOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	includeTurns?: boolean;
}

interface RequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

/**
 * Route module for Session history CRUD operations.
 */
/**
 * Lists sessions with optional filtering and pagination.
 */
export async function listSessions(
	client: IQueryPanelApi,
	options?: SessionListOptions,
	signal?: AbortSignal,
): Promise<PaginatedResponse<SdkSession>> {
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

	return await client.get<PaginatedResponse<SdkSession>>(
		`/sessions${params.toString() ? `?${params.toString()}` : ""}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Retrieves a session by session_id, optionally including turns.
 */
export async function getSession(
	client: IQueryPanelApi,
	sessionId: string,
	options?: SessionGetOptions,
	signal?: AbortSignal,
): Promise<SdkSession> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	const params = new URLSearchParams();
	if (options?.includeTurns !== undefined) {
		params.set("include_turns", `${options.includeTurns}`);
	}

	return await client.get<SdkSession>(
		`/sessions/${encodeURIComponent(sessionId)}${params.toString() ? `?${params.toString()}` : ""}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Updates a session's metadata.
 */
export async function updateSession(
	client: IQueryPanelApi,
	sessionId: string,
	body: SessionUpdateInput,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<SdkSession> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	return await client.patch<SdkSession>(
		`/sessions/${encodeURIComponent(sessionId)}`,
		body,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
}

/**
 * Deletes a session and its turn history.
 */
export async function deleteSession(
	client: IQueryPanelApi,
	sessionId: string,
	options?: RequestOptions,
	signal?: AbortSignal,
): Promise<void> {
	const tenantId = resolveTenantId(client, options?.tenantId);
	await client.delete<void>(
		`/sessions/${encodeURIComponent(sessionId)}`,
		tenantId,
		options?.userId,
		options?.scopes,
		signal,
	);
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
