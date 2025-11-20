import { createSign } from "node:crypto";

/**
 * Deep module: Hides JWT signing and HTTP complexity behind simple interface
 * Following Ousterhout's principle: "Pull complexity downward"
 */
export class ApiClient {
	private readonly baseUrl: string;
	private readonly privateKey: string;
	private readonly organizationId: string;
	private readonly defaultTenantId?: string;
	private readonly additionalHeaders?: Record<string, string>;
	private readonly fetchImpl: typeof fetch;

	constructor(
		baseUrl: string,
		privateKey: string,
		organizationId: string,
		options?: {
			defaultTenantId?: string;
			additionalHeaders?: Record<string, string>;
			fetch?: typeof fetch;
		},
	) {
		if (!baseUrl) {
			throw new Error("Base URL is required");
		}
		if (!privateKey) {
			throw new Error("Private key is required");
		}
		if (!organizationId) {
			throw new Error("Organization ID is required");
		}

		this.baseUrl = baseUrl.replace(/\/+$/, "");
		this.privateKey = privateKey;
		this.organizationId = organizationId;
		this.defaultTenantId = options?.defaultTenantId;
		this.additionalHeaders = options?.additionalHeaders;
		this.fetchImpl = options?.fetch ?? globalThis.fetch;

		if (!this.fetchImpl) {
			throw new Error(
				"Fetch implementation not found. Provide options.fetch or use Node 18+.",
			);
		}
	}

	getDefaultTenantId(): string | undefined {
		return this.defaultTenantId;
	}

	async get<T>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "GET",
			headers: await this.buildHeaders(
				tenantId,
				userId,
				scopes,
				false,
				sessionId,
			),
			signal,
		});
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
		return await this.request<T>(path, {
			method: "POST",
			headers: await this.buildHeaders(
				tenantId,
				userId,
				scopes,
				true,
				sessionId,
			),
			body: JSON.stringify(body ?? {}),
			signal,
		});
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
		return await this.request<T>(path, {
			method: "PUT",
			headers: await this.buildHeaders(
				tenantId,
				userId,
				scopes,
				true,
				sessionId,
			),
			body: JSON.stringify(body ?? {}),
			signal,
		});
	}

	async delete<T = void>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "DELETE",
			headers: await this.buildHeaders(
				tenantId,
				userId,
				scopes,
				false,
				sessionId,
			),
			signal,
		});
	}

	private async request<T>(path: string, init: RequestInit): Promise<T> {
		const response = await this.fetchImpl(`${this.baseUrl}${path}`, init);
		const text = await response.text();
		let json: any;
		try {
			json = text ? JSON.parse(text) : undefined;
		} catch {
			json = undefined;
		}

		if (!response.ok) {
			const error = new Error(
				json?.error || response.statusText || "Request failed",
			);
			(error as any).status = response.status;
			if (json?.details) (error as any).details = json.details;
			throw error;
		}

		return json as T;
	}

	private async buildHeaders(
		tenantId: string,
		userId?: string,
		scopes?: string[],
		includeJson: boolean = true,
		sessionId?: string,
	): Promise<Record<string, string>> {
		const token = await this.generateJWT(tenantId, userId, scopes);
		const headers: Record<string, string> = {
			Authorization: `Bearer ${token}`,
			Accept: "application/json",
		};
		if (includeJson) {
			headers["Content-Type"] = "application/json";
		}
		if (sessionId) {
			headers["x-session-id"] = sessionId;
		}
		if (this.additionalHeaders) {
			Object.assign(headers, this.additionalHeaders);
		}
		return headers;
	}

	private async generateJWT(
		tenantId: string,
		userId?: string,
		scopes?: string[],
	): Promise<string> {
		const header = {
			alg: "RS256",
			typ: "JWT",
		};

		const payload: Record<string, unknown> = {
			organizationId: this.organizationId,
			tenantId,
		};

		if (userId) payload.userId = userId;
		if (scopes?.length) payload.scopes = scopes;

		const encodeJson = (obj: unknown): string => {
			const json = JSON.stringify(obj);
			const base64 = Buffer.from(json).toString("base64");
			// base64url encoding: replace non-url chars and strip padding
			return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
		};

		const encodedHeader = encodeJson(header);
		const encodedPayload = encodeJson(payload);
		const data = `${encodedHeader}.${encodedPayload}`;

		const signer = createSign("RSA-SHA256");
		signer.update(data);
		signer.end();

		const signature = signer.sign(this.privateKey);
		const encodedSignature = signature
			.toString("base64")
			.replace(/\+/g, "-")
			.replace(/\//g, "_")
			.replace(/=+$/g, "");

		return `${data}.${encodedSignature}`;
	}
}
