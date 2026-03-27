/**
 * Interface for the API layer used by route modules.
 * Implemented by ApiClient (HTTP to querypanel-sdk) and CallbackApiClient (in-process callbacks).
 * Allows the SDK to be used inside your own API without HTTP recursion by providing a callback-based implementation.
 */
export interface IQueryPanelApi {
	getDefaultTenantId(): string | undefined;

	get<T>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T>;

	post<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T>;

	postWithHeaders<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<{ data: T; headers: Headers }>;

	put<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T>;

	patch<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T>;

	delete<T = void>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T>;
}

/**
 * Options passed to the request handler when using the callback-based API.
 * Use this when you run the SDK inside your own API and want to handle "API" calls in-process (e.g. call querypanel-sdk services directly) to avoid recursion.
 */
export interface RequestHandlerOptions {
	method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE";
	path: string;
	body?: unknown;
	tenantId: string;
	userId?: string;
	scopes?: string[];
	signal?: AbortSignal;
	sessionId?: string;
}

/**
 * Result from the request handler. Return `headers` for POST requests to paths like `/query` or `/v2/query` when the SDK needs to read response headers (e.g. x-querypanel-session-id).
 */
export interface RequestHandlerResult {
	data: unknown;
	headers?: Headers;
}

export type RequestHandler = (
	opts: RequestHandlerOptions,
) => Promise<RequestHandlerResult>;
