import type {
	IQueryPanelApi,
	RequestHandler,
	RequestHandlerOptions,
} from "./api-types";

/**
 * API client that delegates every request to a callback instead of HTTP.
 * Use this when running the SDK inside your own API so that "API" calls are handled in-process (e.g. by calling querypanel-sdk services directly) and do not cause recursion.
 */
export class CallbackApiClient implements IQueryPanelApi {
	private readonly requestHandler: RequestHandler;
	private readonly defaultTenantId?: string;

	constructor(
		requestHandler: RequestHandler,
		options?: { defaultTenantId?: string },
	) {
		this.requestHandler = requestHandler;
		this.defaultTenantId = options?.defaultTenantId;
	}

	getDefaultTenantId(): string | undefined {
		return this.defaultTenantId;
	}

	private async request<T>(
		opts: Omit<RequestHandlerOptions, "method" | "body"> & {
			method: RequestHandlerOptions["method"];
			body?: unknown;
		},
	): Promise<{ data: T; headers?: Headers }> {
		const { data, headers } = await this.requestHandler({
			method: opts.method,
			path: opts.path,
			body: opts.body,
			tenantId: opts.tenantId,
			userId: opts.userId,
			scopes: opts.scopes,
			signal: opts.signal,
			sessionId: opts.sessionId,
		});
		return { data: data as T, headers };
	}

	async get<T>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		const { data } = await this.request<T>({
			method: "GET",
			path,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return data;
	}

	async post<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		const { data } = await this.request<T>({
			method: "POST",
			path,
			body,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return data;
	}

	async postWithHeaders<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<{ data: T; headers: Headers }> {
		const { data, headers } = await this.request<T>({
			method: "POST",
			path,
			body,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return {
			data,
			headers: headers ?? new Headers(),
		};
	}

	async put<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		const { data } = await this.request<T>({
			method: "PUT",
			path,
			body,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return data;
	}

	async patch<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		const { data } = await this.request<T>({
			method: "PATCH",
			path,
			body,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return data;
	}

	async delete<T = void>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		const { data } = await this.request<T>({
			method: "DELETE",
			path,
			tenantId,
			userId,
			scopes,
			signal,
			sessionId,
		});
		return data;
	}
}
