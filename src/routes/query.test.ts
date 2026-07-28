import { beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryEngine } from "../core/query-engine";
import { QueryErrorCode, QueryPipelineError } from "../errors";
import { createMockQueryPanelApi } from "../test-utils";
import { anonymizeResults, ask } from "./query";

describe("routes/query", () => {
	let mockClient: ReturnType<typeof createMockQueryPanelApi>;
	let mockQueryEngine: QueryEngine;
	let mockQueryEngineSetup: {
		getDefaultDatabase: ReturnType<typeof vi.fn>;
		getDatabaseMetadata: ReturnType<typeof vi.fn>;
		mapGeneratedParams: ReturnType<typeof vi.fn>;
		validateAndExecute: ReturnType<typeof vi.fn>;
	};

	beforeEach(() => {
		mockClient = createMockQueryPanelApi({
			post: vi.fn(),
			postWithHeaders: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		});

		mockQueryEngineSetup = {
			getDefaultDatabase: vi.fn(() => "default-db"),
			getDatabaseMetadata: vi.fn((name) =>
				name === "default-db" || name === "custom-db" || name === "test-db"
					? { name, dialect: "postgres" }
					: undefined,
			),
			mapGeneratedParams: vi.fn((params) => {
				const record: Record<string, any> = {};
				params.forEach((p: any) => {
					record[p.name] = p.value;
				});
				return record;
			}),
			validateAndExecute: vi.fn(),
		};
		mockQueryEngine = mockQueryEngineSetup as unknown as QueryEngine;
	});

	function mockHeaders(): Headers {
		return new Headers();
	}

	describe("ask", () => {
		it("should generate SQL and execute query", async () => {
			const queryResponse = {
				success: true,
				sql: "SELECT * FROM users LIMIT 10",
				params: [{ name: "limit", value: 10 }],
				dialect: "postgres",
				database: "test-db",
				rationale: "Fetching first 10 users",
				queryId: "query-123",
			};

			const executionResult = {
				rows: [
					{ id: 1, name: "Alice" },
					{ id: 2, name: "Bob" },
				],
				fields: ["id", "name"],
			};

			const chartResponse = {
				chart: {
					mark: "bar",
					encoding: {},
				},
				notes: null,
			};

			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: queryResponse,
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce(chartResponse);

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue(executionResult);

			const result = await ask(
				mockClient,
				mockQueryEngine,
				"Show me the first 10 users",
				{ tenantId: "tenant-1" },
			);

			expect(result.sql).toBe("SELECT * FROM users LIMIT 10");
			expect(result.rows).toEqual(executionResult.rows);
			expect(result.fields).toEqual(["id", "name"]);
			expect(result.chart.vegaLiteSpec).toMatchObject({
				mark: "bar",
				data: { values: executionResult.rows },
			});
		});

		it("should use default tenant ID if not provided", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: [],
			});

			await ask(mockClient, mockQueryEngine, "test", {});

			expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			const call = mockClient.postWithHeaders.mock.calls[0];
			expect(call[0]).toBe("/query");
			expect(call[2]).toBe("default-tenant"); // tenantId
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(
				ask(mockClient, mockQueryEngine, "test", {}),
			).rejects.toThrow("tenantId is required");
		});

		it("should use database from options if provided", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: [],
			});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				database: "custom-db",
			});

			expect(mockQueryEngine.validateAndExecute).toHaveBeenCalledWith(
				expect.any(String),
				expect.any(Object),
				"custom-db",
				"tenant-1",
			);
		});

		it("should retry on SQL execution failure", async () => {
			const queryResponse1 = {
				success: true,
				sql: "SELECT * FROM invalid_table",
				params: [],
				dialect: "postgres",
			};

			const queryResponse2 = {
				success: true,
				sql: "SELECT * FROM users",
				params: [],
				dialect: "postgres",
			};

			const chartResponse = {
				chart: { mark: "table" },
				notes: null,
			};

			mockClient.postWithHeaders
				.mockResolvedValueOnce({
					data: queryResponse1,
					headers: mockHeaders(),
				})
				.mockResolvedValueOnce({
					data: queryResponse2,
					headers: mockHeaders(),
				});
			mockClient.post.mockResolvedValueOnce(chartResponse);

			mockQueryEngine.validateAndExecute
				.mockRejectedValueOnce(new Error("Table does not exist"))
				.mockResolvedValueOnce({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

			const result = await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				maxRetry: 1,
			});

			expect(mockClient.postWithHeaders).toHaveBeenCalledTimes(2);
			expect(mockClient.post).toHaveBeenCalledTimes(1);
			expect(result.sql).toBe("SELECT * FROM users");
			expect(result.attempts).toBe(2);
		});

		it("should include error context in retry request", async () => {
			mockClient.postWithHeaders
				.mockResolvedValueOnce({
					data: {
						success: true,
						sql: "SELECT * FROM users",
						params: [],
						dialect: "postgres",
					},
					headers: mockHeaders(),
				})
				.mockResolvedValueOnce({
					data: {
						success: true,
						sql: "SELECT * FROM users",
						params: [],
						dialect: "postgres",
					},
					headers: mockHeaders(),
				});

			mockQueryEngine.validateAndExecute
				.mockRejectedValueOnce(new Error("Syntax error"))
				.mockResolvedValueOnce({
					rows: [],
					fields: [],
				});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				maxRetry: 1,
			});

			const secondCall = mockClient.postWithHeaders.mock.calls[1];
			expect(secondCall[1]).toMatchObject({
				question: "test",
				last_error: "Syntax error",
				previous_sql: "SELECT * FROM users",
			});
		});

		it("should throw error after exhausting retries", async () => {
			mockClient.postWithHeaders.mockResolvedValue({
				data: {
					success: true,
					sql: "INVALID SQL",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});

			mockQueryEngine.validateAndExecute.mockRejectedValue(
				new Error("Persistent error"),
			);

			await expect(
				ask(mockClient, mockQueryEngine, "test", {
					tenantId: "tenant-1",
					maxRetry: 2,
				}),
			).rejects.toThrow("Persistent error");

			expect(mockClient.postWithHeaders).toHaveBeenCalledTimes(3); // Initial + 2 retries
		});

		it("should generate chart when no rows returned", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT id FROM users WHERE 1=0",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: ["id"],
			});

			mockClient.post.mockResolvedValueOnce({
				chart: {
					$schema: "https://vega.github.io/schema/vega-lite/v6.json",
					mark: "bar",
					encoding: {
						x: { field: "id", type: "nominal" },
						y: { aggregate: "count" },
					},
					data: { values: [] },
				},
				notes: "Empty result chart",
			});

			const result = await ask(mockClient, mockQueryEngine as any, "test", {
				tenantId: "tenant-1",
			});

			expect(result.rows).toEqual([]);
			expect(result.chart.vegaLiteSpec).toMatchObject({
				mark: "bar",
				data: { values: [] },
			});
			expect(result.chart.notes).toBe("Empty result chart");
			expect(mockClient.postWithHeaders).toHaveBeenCalledTimes(1);
			expect(mockClient.post).toHaveBeenCalledTimes(1);
			expect(mockClient.post).toHaveBeenCalledWith(
				"/chart",
				expect.objectContaining({
					fields: ["id"],
					rows: [],
				}),
				"tenant-1",
				undefined,
				undefined,
				undefined,
				expect.any(String),
			);
		});

		it("should generate vizspec when no rows returned", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT month, revenue FROM stats WHERE 1=0",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: ["month", "revenue"],
			});

			mockClient.post.mockResolvedValueOnce({
				spec: {
					version: "1.0",
					kind: "chart",
					title: "Revenue",
					data: { sourceId: "main_query" },
					encoding: {
						chartType: "line",
						x: { field: "month", type: "temporal" },
						y: { field: "revenue", type: "quantitative" },
					},
				},
				notes: null,
			});

			const result = await ask(mockClient, mockQueryEngine as any, "monthly revenue", {
				tenantId: "tenant-1",
				chartType: "vizspec",
			});

			expect(result.rows).toEqual([]);
			expect(result.chart.vizSpec?.encoding).toMatchObject({
				chartType: "line",
			});
			expect(result.chart.notes).toBe("Query returned no rows.");
			expect(mockClient.post).toHaveBeenCalledWith(
				"/vizspec",
				expect.objectContaining({
					fields: ["month", "revenue"],
					rows: [],
				}),
				"tenant-1",
				undefined,
				undefined,
				undefined,
				expect.any(String),
			);
		});

		it("should pass through query context", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
					context: [
						{
							source: "docs",
							pageContent: "Example",
							score: 0.9,
						},
					],
				},
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: [],
			});

			const result = await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
			});

			expect(result.context).toHaveLength(1);
			expect(result.context?.[0]).toMatchObject({
				source: "docs",
				pageContent: "Example",
				score: 0.9,
			});
		});

		it("should use custom chart retry count", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [{ id: 1 }],
				fields: ["id"],
			});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				chartMaxRetries: 5,
			});

			const chartCall = mockClient.post.mock.calls[0];
			expect(chartCall[1]).toMatchObject({
				max_retries: 5,
			});
		});

		it("should not retry SQL when chart generation fails with AI provider error", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [{ id: 1 }],
				fields: ["id"],
			});

			mockClient.post.mockRejectedValueOnce(
				new QueryPipelineError(
					"Invalid API key",
					QueryErrorCode.AI_PROVIDER_AUTH_FAILED,
				),
			);

			await expect(
				ask(mockClient, mockQueryEngine, "test", {
					tenantId: "tenant-1",
					maxRetry: 2,
				}),
			).rejects.toMatchObject({
				code: QueryErrorCode.AI_PROVIDER_AUTH_FAILED,
			});

			expect(mockClient.postWithHeaders).toHaveBeenCalledTimes(1);
			expect(mockClient.post).toHaveBeenCalledTimes(1);
		});

		it("should throw QueryPipelineError when chart response contains provider error", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [{ id: 1 }],
				fields: ["id"],
			});

			mockClient.post.mockResolvedValueOnce({
				error: "Rate limited",
				code: QueryErrorCode.AI_PROVIDER_RATE_LIMITED,
			});

			await expect(
				ask(mockClient, mockQueryEngine, "test", {
					tenantId: "tenant-1",
					maxRetry: 2,
				}),
			).rejects.toMatchObject({
				code: QueryErrorCode.AI_PROVIDER_RATE_LIMITED,
			});

			expect(mockClient.postWithHeaders).toHaveBeenCalledTimes(1);
			expect(mockClient.post).toHaveBeenCalledTimes(1);
		});

		it("should send system_prompt only for v2 pipeline", async () => {
			mockClient.postWithHeaders.mockResolvedValueOnce({
				data: {
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				},
				headers: mockHeaders(),
			});
			mockClient.post.mockResolvedValueOnce({
				chart: { mark: "bar" },
				notes: null,
			});

			mockQueryEngineSetup.validateAndExecute.mockResolvedValue({
				rows: [],
				fields: [],
			});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				pipeline: "v2",
				systemPrompt: "Always apply tenant retention window: last 30 days.",
			});

			const call = mockClient.postWithHeaders.mock.calls[0];
			expect(call[0]).toBe("/v2/query");
			expect(call[1]).toMatchObject({
				question: "test",
				system_prompt: "Always apply tenant retention window: last 30 days.",
			});
		});
	});

	describe("anonymizeResults", () => {
		it("should anonymize row values by type", () => {
			const rows = [
				{ id: 123, name: "Alice", active: true, tags: ["admin", "user"] },
				{ id: 456, name: "Bob", active: false, tags: [] },
			];

			const result = anonymizeResults(rows);

			expect(result).toEqual([
				{ id: "number", name: "string", active: "boolean", tags: "array" },
				{ id: "number", name: "string", active: "boolean", tags: "array" },
			]);
		});

		it("should handle null values", () => {
			const rows = [{ id: 1, name: null, age: 25 }];

			const result = anonymizeResults(rows);

			expect(result).toEqual([{ id: "number", name: "null", age: "number" }]);
		});

		it("should return empty array for empty input", () => {
			expect(anonymizeResults([])).toEqual([]);
			expect(anonymizeResults(null as any)).toEqual([]);
			expect(anonymizeResults(undefined as any)).toEqual([]);
		});

		it("should handle objects with no properties", () => {
			const rows = [{}];

			const result = anonymizeResults(rows);

			expect(result).toEqual([{}]);
		});
	});
});
