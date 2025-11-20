import { describe, it, expect, vi, beforeEach } from "vitest";
import { ask, anonymizeResults } from "./query";
import type { ApiClient } from "../core/client";
import type { QueryEngine } from "../core/query-engine";

describe("routes/query", () => {
	let mockClient: ApiClient;
	let mockQueryEngine: QueryEngine;

	beforeEach(() => {
		mockClient = {
			post: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		} as any;

		mockQueryEngine = {
			getDefaultDatabase: vi.fn(() => "default-db"),
			mapGeneratedParams: vi.fn((params) => {
				const record: Record<string, any> = {};
				params.forEach((p: any) => {
					record[p.name] = p.value;
				});
				return record;
			}),
			validateAndExecute: vi.fn(),
		} as any;
	});

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

			vi.mocked(mockClient.post)
				.mockResolvedValueOnce(queryResponse)
				.mockResolvedValueOnce(chartResponse);

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue(
				executionResult,
			);

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
			vi.mocked(mockClient.post).mockResolvedValueOnce({
				success: true,
				sql: "SELECT 1",
				params: [],
				dialect: "postgres",
			});

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue({
				rows: [],
				fields: [],
			});

			await ask(mockClient, mockQueryEngine, "test", {});

			expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			const call = vi.mocked(mockClient.post).mock.calls[0];
			expect(call[0]).toBe("/query");
			expect(call[2]).toBe("default-tenant"); // tenantId
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				ask(mockClient, mockQueryEngine, "test", {}),
			).rejects.toThrow("tenantId is required");
		});

		it("should use database from options if provided", async () => {
			vi.mocked(mockClient.post).mockResolvedValueOnce({
				success: true,
				sql: "SELECT 1",
				params: [],
				dialect: "postgres",
			});

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue({
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

			vi.mocked(mockClient.post)
				.mockResolvedValueOnce(queryResponse1)
				.mockResolvedValueOnce(queryResponse2)
				.mockResolvedValueOnce(chartResponse);

			vi.mocked(mockQueryEngine.validateAndExecute)
				.mockRejectedValueOnce(new Error("Table does not exist"))
				.mockResolvedValueOnce({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

			const result = await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				maxRetry: 1,
			});

			expect(mockClient.post).toHaveBeenCalledTimes(3); // 2 queries + 1 chart
			expect(result.sql).toBe("SELECT * FROM users");
			expect(result.attempts).toBe(2);
		});

		it("should include error context in retry request", async () => {
			vi.mocked(mockClient.post).mockResolvedValue({
				success: true,
				sql: "SELECT * FROM users",
				params: [],
				dialect: "postgres",
			});

			vi.mocked(mockQueryEngine.validateAndExecute)
				.mockRejectedValueOnce(new Error("Syntax error"))
				.mockResolvedValueOnce({
					rows: [],
					fields: [],
				});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				maxRetry: 1,
			});

			const secondCall = vi.mocked(mockClient.post).mock.calls[1];
			expect(secondCall[1]).toMatchObject({
				question: "test",
				last_error: "Syntax error",
				previous_sql: "SELECT * FROM users",
			});
		});

		it("should throw error after exhausting retries", async () => {
			vi.mocked(mockClient.post).mockResolvedValue({
				success: true,
				sql: "INVALID SQL",
				params: [],
				dialect: "postgres",
			});

			vi.mocked(mockQueryEngine.validateAndExecute).mockRejectedValue(
				new Error("Persistent error"),
			);

			await expect(
				ask(mockClient, mockQueryEngine, "test", {
					tenantId: "tenant-1",
					maxRetry: 2,
				}),
			).rejects.toThrow("Persistent error");

			expect(mockClient.post).toHaveBeenCalledTimes(3); // Initial + 2 retries
		});

		it("should not generate chart when no rows returned", async () => {
			vi.mocked(mockClient.post).mockResolvedValueOnce({
				success: true,
				sql: "DELETE FROM users",
				params: [],
				dialect: "postgres",
			});

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue({
				rows: [],
				fields: [],
			});

			const result = await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
			});

			expect(result.chart.vegaLiteSpec).toBeNull();
			expect(result.chart.notes).toBe("Query returned no rows.");
			expect(mockClient.post).toHaveBeenCalledTimes(1); // No chart request
		});

		it("should pass through query context", async () => {
			vi.mocked(mockClient.post).mockResolvedValueOnce({
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
			});

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue({
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
			vi.mocked(mockClient.post)
				.mockResolvedValueOnce({
					success: true,
					sql: "SELECT 1",
					params: [],
					dialect: "postgres",
				})
				.mockResolvedValueOnce({
					chart: { mark: "bar" },
					notes: null,
				});

			vi.mocked(mockQueryEngine.validateAndExecute).mockResolvedValue({
				rows: [{ id: 1 }],
				fields: ["id"],
			});

			await ask(mockClient, mockQueryEngine, "test", {
				tenantId: "tenant-1",
				chartMaxRetries: 5,
			});

			const chartCall = vi.mocked(mockClient.post).mock.calls[1];
			expect(chartCall[1]).toMatchObject({
				max_retries: 5,
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
