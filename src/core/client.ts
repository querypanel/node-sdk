/**
 * Deep module: Hides JWT signing and HTTP complexity behind simple interface
 * Following Ousterhout's principle: "Pull complexity downward"
 */

// Web Crypto API type declarations (available in Node.js 18+, Deno, and Bun)
// Minimal type declaration for server-side use without DOM types
// This matches the Web Crypto API CryptoKey interface
interface CryptoKey {
	readonly type: "public" | "private" | "secret";
	readonly extractable: boolean;
	readonly algorithm: { name: string };
	readonly usages: Array<
		| "encrypt"
		| "decrypt"
		| "sign"
		| "verify"
		| "deriveKey"
		| "deriveBits"
		| "wrapKey"
		| "unwrapKey"
	>;
}

export class ApiClient {
	private readonly baseUrl: string;
	private readonly privateKey: string;
	private readonly organizationId: string;
	private readonly defaultTenantId?: string;
	private readonly additionalHeaders?: Record<string, string>;
	private readonly fetchImpl: typeof fetch;
	private cryptoKey: CryptoKey | null = null;

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

	/**
	 * Base64URL encode a string (works in both Node.js 18+ and Deno)
	 */
	private base64UrlEncode(str: string): string {
		// Convert string to bytes
		const bytes = new TextEncoder().encode(str);

		// btoa is available in both Node.js 18+ and Deno
		// Convert bytes to binary string efficiently (handle large arrays)
		let binary = "";
		const chunkSize = 8192; // Process in chunks to avoid stack overflow
		for (let i = 0; i < bytes.length; i += chunkSize) {
			const chunk = bytes.slice(i, i + chunkSize);
			binary += String.fromCharCode(...chunk);
		}

		const base64 = btoa(binary);

		// Convert to base64url: replace non-url chars and strip padding
		return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
	}

	/**
	 * Base64URL encode from Uint8Array (for binary data like signatures)
	 */
	private base64UrlEncodeBytes(bytes: Uint8Array): string {
		// Convert bytes to binary string efficiently (handle large arrays)
		let binary = "";
		const chunkSize = 8192; // Process in chunks to avoid stack overflow
		for (let i = 0; i < bytes.length; i += chunkSize) {
			const chunk = bytes.slice(i, i + chunkSize);
			binary += String.fromCharCode(...chunk);
		}

		const base64 = btoa(binary);

		// Convert to base64url: replace non-url chars and strip padding
		return base64.replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/g, "");
	}

	/**
	 * Import the private key into Web Crypto API format (cached after first import)
	 */
	private async getCryptoKey(): Promise<CryptoKey> {
		if (this.cryptoKey) {
			return this.cryptoKey;
		}

		// Import the private key for Web Crypto API
		// Works in both Node.js 18+ and Deno
		this.cryptoKey = await crypto.subtle.importKey(
			"pkcs8",
			this.privateKeyToArrayBuffer(this.privateKey),
			{
				name: "RSASSA-PKCS1-v1_5",
				hash: "SHA-256",
			},
			false,
			["sign"],
		);

		return this.cryptoKey;
	}

	/**
	 * Convert PEM private key to ArrayBuffer for Web Crypto API
	 */
	private privateKeyToArrayBuffer(pem: string): ArrayBuffer {
		// Remove PEM headers and whitespace
		const pemHeader = "-----BEGIN PRIVATE KEY-----";
		const pemFooter = "-----END PRIVATE KEY-----";
		const pemContents = pem
			.replace(pemHeader, "")
			.replace(pemFooter, "")
			.replace(/\s/g, "");

		// Decode base64 to binary string, then to ArrayBuffer
		const binaryString = atob(pemContents);
		const bytes = new Uint8Array(binaryString.length);
		for (let i = 0; i < binaryString.length; i++) {
			bytes[i] = binaryString.charCodeAt(i);
		}
		return bytes.buffer;
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

		const encodedHeader = this.base64UrlEncode(JSON.stringify(header));
		const encodedPayload = this.base64UrlEncode(JSON.stringify(payload));
		const data = `${encodedHeader}.${encodedPayload}`;

		// Sign using Web Crypto API (works in both Node.js 18+ and Deno)
		const key = await this.getCryptoKey();
		const dataBytes = new TextEncoder().encode(data);
		const signature = await crypto.subtle.sign(
			{
				name: "RSASSA-PKCS1-v1_5",
			},
			key,
			dataBytes,
		);

		// Convert signature ArrayBuffer to base64url
		const signatureBytes = new Uint8Array(signature);
		const encodedSignature = this.base64UrlEncodeBytes(signatureBytes);

		return `${data}.${encodedSignature}`;
	}
}
