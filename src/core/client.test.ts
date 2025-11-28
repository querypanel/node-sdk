import { describe, it, expect, vi, beforeEach } from "vitest";
import { ApiClient } from "./client";
import { TEST_PRIVATE_KEY, TEST_ORG_ID, TEST_BASE_URL } from "../test-utils";

describe("ApiClient", () => {
	const mockBaseUrl = TEST_BASE_URL;
	const mockPrivateKey = TEST_PRIVATE_KEY;
	const mockOrgId = TEST_ORG_ID;
	let mockFetch: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		mockFetch = vi.fn();
	});

	describe("constructor", () => {
		it("should create instance with valid params", () => {
			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
			});
			expect(client).toBeInstanceOf(ApiClient);
		});

		it("should throw error if baseUrl is missing", () => {
			expect(
				() =>
					new ApiClient("", mockPrivateKey, mockOrgId, { fetch: mockFetch }),
			).toThrow("Base URL is required");
		});

		it("should throw error if privateKey is missing", () => {
			expect(
				() => new ApiClient(mockBaseUrl, "", mockOrgId, { fetch: mockFetch }),
			).toThrow("Private key is required");
		});

		it("should throw error if organizationId is missing", () => {
			expect(
				() =>
					new ApiClient(mockBaseUrl, mockPrivateKey, "", { fetch: mockFetch }),
			).toThrow("Organization ID is required");
		});

		it("should strip trailing slashes from baseUrl", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({ success: true }),
			});

			const client = new ApiClient(
				"https://api.example.com///",
				mockPrivateKey,
				mockOrgId,
				{ fetch: mockFetch },
			);

			await client.get("/test", "tenant-1");

			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/test",
				expect.any(Object),
			);
		});

		it("should use defaultTenantId from options", () => {
			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				defaultTenantId: "tenant-default",
				fetch: mockFetch,
			});

			expect(client.getDefaultTenantId()).toBe("tenant-default");
		});
	});

	describe("HTTP methods", () => {
		let client: ApiClient;

		beforeEach(() => {
			client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
				defaultTenantId: "tenant-1",
			});
		});

		describe("get", () => {
			it("should make GET request", async () => {
				mockFetch.mockResolvedValue({
					ok: true,
					text: async () => JSON.stringify({ data: "test" }),
				});

				const result = await client.get("/test", "tenant-1");

				expect(mockFetch).toHaveBeenCalledWith(
					"https://api.example.com/test",
					expect.objectContaining({
						method: "GET",
						headers: expect.objectContaining({
							Accept: "application/json",
							Authorization: expect.stringMatching(/^Bearer /),
						}),
					}),
				);
				expect(result).toEqual({ data: "test" });
			});

			it("should include session ID when provided", async () => {
				mockFetch.mockResolvedValue({
					ok: true,
					text: async () => JSON.stringify({ data: "test" }),
				});

				await client.get("/test", "tenant-1", "user-1", [], undefined, "session-123");

				expect(mockFetch).toHaveBeenCalledWith(
					expect.any(String),
					expect.objectContaining({
						headers: expect.objectContaining({
							"x-session-id": "session-123",
						}),
					}),
				);
			});
		});

		describe("post", () => {
			it("should make POST request with body", async () => {
				mockFetch.mockResolvedValue({
					ok: true,
					text: async () => JSON.stringify({ id: "123" }),
				});

				const body = { name: "test" };
				const result = await client.post("/test", body, "tenant-1");

				expect(mockFetch).toHaveBeenCalledWith(
					"https://api.example.com/test",
					expect.objectContaining({
						method: "POST",
						headers: expect.objectContaining({
							"Content-Type": "application/json",
						}),
						body: JSON.stringify(body),
					}),
				);
				expect(result).toEqual({ id: "123" });
			});
		});

		describe("put", () => {
			it("should make PUT request with body", async () => {
				mockFetch.mockResolvedValue({
					ok: true,
					text: async () => JSON.stringify({ updated: true }),
				});

				const body = { name: "updated" };
				const result = await client.put("/test/123", body, "tenant-1");

				expect(mockFetch).toHaveBeenCalledWith(
					"https://api.example.com/test/123",
					expect.objectContaining({
						method: "PUT",
						body: JSON.stringify(body),
					}),
				);
				expect(result).toEqual({ updated: true });
			});
		});

		describe("delete", () => {
			it("should make DELETE request", async () => {
				mockFetch.mockResolvedValue({
					ok: true,
					text: async () => "",
				});

				await client.delete("/test/123", "tenant-1");

				expect(mockFetch).toHaveBeenCalledWith(
					"https://api.example.com/test/123",
					expect.objectContaining({
						method: "DELETE",
					}),
				);
			});
		});
	});

	describe("error handling", () => {
		let client: ApiClient;

		beforeEach(() => {
			client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
			});
		});

		it("should throw error on failed request", async () => {
			mockFetch.mockResolvedValue({
				ok: false,
				status: 404,
				statusText: "Not Found",
				text: async () =>
					JSON.stringify({ error: "Resource not found", details: "extra info" }),
			});

			await expect(client.get("/test", "tenant-1")).rejects.toThrow(
				"Resource not found",
			);
		});

		it("should include error details in thrown error", async () => {
			mockFetch.mockResolvedValue({
				ok: false,
				status: 400,
				statusText: "Bad Request",
				text: async () =>
					JSON.stringify({ error: "Validation failed", details: { field: "name" } }),
			});

			try {
				await client.get("/test", "tenant-1");
				expect.fail("Should have thrown");
			} catch (error: any) {
				expect(error.message).toBe("Validation failed");
				expect(error.details).toEqual({ field: "name" });
				expect(error.status).toBe(400);
			}
		});

		it("should handle non-JSON error responses", async () => {
			mockFetch.mockResolvedValue({
				ok: false,
				status: 500,
				statusText: "Internal Server Error",
				text: async () => "Plain text error",
			});

			await expect(client.get("/test", "tenant-1")).rejects.toThrow(
				"Internal Server Error",
			);
		});
	});

	describe("JWT generation", () => {
		it("should include organizationId and tenantId in JWT", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});

			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
			});

			await client.get("/test", "tenant-123");

			const call = mockFetch.mock.calls[0];
			const headers = call[1].headers;
			const authHeader = headers.Authorization;

			expect(authHeader).toMatch(/^Bearer /);

			// Decode JWT payload (without verification for testing)
			const token = authHeader.replace("Bearer ", "");
			const parts = token.split(".");
			const payload = JSON.parse(
				Buffer.from(parts[1], "base64url").toString(),
			);

			expect(payload.organizationId).toBe(TEST_ORG_ID);
			expect(payload.tenantId).toBe("tenant-123");
		});

		it("should include userId in JWT when provided", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});

			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
			});

			await client.get("/test", "tenant-1", "user-456");

			const call = mockFetch.mock.calls[0];
			const token = call[1].headers.Authorization.replace("Bearer ", "");
			const parts = token.split(".");
			const payload = JSON.parse(
				Buffer.from(parts[1], "base64url").toString(),
			);

			expect(payload.userId).toBe("user-456");
		});

		it("should include scopes in JWT when provided", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});

			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
			});

			await client.get("/test", "tenant-1", undefined, ["read", "write"]);

			const call = mockFetch.mock.calls[0];
			const token = call[1].headers.Authorization.replace("Bearer ", "");
			const parts = token.split(".");
			const payload = JSON.parse(
				Buffer.from(parts[1], "base64url").toString(),
			);

			expect(payload.scopes).toEqual(["read", "write"]);
		});
	});

	describe("additional headers", () => {
		it("should include additional headers when provided", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});

			const client = new ApiClient(mockBaseUrl, mockPrivateKey, mockOrgId, {
				fetch: mockFetch,
				additionalHeaders: {
					"X-Custom-Header": "custom-value",
					"X-Another": "value",
				},
			});

			await client.get("/test", "tenant-1");

			expect(mockFetch).toHaveBeenCalledWith(
				expect.any(String),
				expect.objectContaining({
					headers: expect.objectContaining({
						"X-Custom-Header": "custom-value",
						"X-Another": "value",
					}),
				}),
			);
		});
	});
});
