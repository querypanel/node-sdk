import { beforeEach, describe, expect, it, type Mock, vi } from "vitest";
import { modifyChart } from "./modify";

describe("routes/modify", () => {
	let mockClient: {
		post: Mock;
		getDefaultTenantId: Mock;
	};
	let mockQueryEngine: {
		getDefaultDatabase: Mock;
		getDatabaseMetadata: Mock;
		mapGeneratedParams: Mock;
		validateAndExecute: Mock;
	};

	beforeEach(() => {
		mockClient = {
			post: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		};

		mockQueryEngine = {
			getDefaultDatabase: vi.fn(() => "default-db"),
			getDatabaseMetadata: vi.fn((name) =>
				name === "default-db" || name === "test-db" || name === "analytics"
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
	});

	describe("modifyChart", () => {
		describe("visualization-only modifications", () => {
			it("should modify chart type without regenerating SQL", async () => {
				const executionResult = {
					rows: [
						{ country: "US", revenue: 1000 },
						{ country: "UK", revenue: 800 },
					],
					fields: ["country", "revenue"],
				};

				const chartResponse = {
					chart: {
						mark: "bar",
						encoding: {
							x: { field: "country" },
							y: { field: "revenue" },
						},
					},
					notes: null,
				};

				mockQueryEngine.validateAndExecute.mockResolvedValue(executionResult);
				mockClient.post.mockResolvedValueOnce(chartResponse);

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT country, revenue FROM sales GROUP BY country",
						question: "revenue by country",
						database: "test-db",
						vizModifications: {
							chartType: "bar",
							xAxis: { field: "country" },
							yAxis: { field: "revenue", aggregate: "sum" },
						},
					},
					{ tenantId: "tenant-1" },
				);

				// SQL should not have changed
				expect(result.sql).toBe(
					"SELECT country, revenue FROM sales GROUP BY country",
				);
				expect(result.modified.sqlChanged).toBe(false);
				expect(result.modified.vizChanged).toBe(true);
				expect(result.rows).toEqual(executionResult.rows);
				expect(result.chart.vegaLiteSpec).toMatchObject({
					mark: "bar",
					data: { values: executionResult.rows },
				});

				// Should NOT have called /query endpoint
				expect(mockClient.post).toHaveBeenCalledTimes(1);
				expect(mockClient.post).toHaveBeenCalledWith(
					"/chart",
					expect.objectContaining({
						encoding_hints: {
							chartType: "bar",
							xAxis: { field: "country" },
							yAxis: { field: "revenue", aggregate: "sum" },
						},
					}),
					"tenant-1",
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should include all viz modification hints", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ x: 1, y: 2, series: "A" }],
					fields: ["x", "y", "series"],
				});
				mockClient.post.mockResolvedValueOnce({
					chart: { mark: "area" },
					notes: null,
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM data",
						question: "test",
						database: "test-db",
						vizModifications: {
							chartType: "area",
							xAxis: { field: "x", timeUnit: "month" },
							yAxis: [
								{ field: "y1", aggregate: "sum" },
								{ field: "y2", aggregate: "avg" },
							],
							series: { field: "category" },
							stacking: "stacked",
							limit: 100,
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/chart",
					expect.objectContaining({
						encoding_hints: {
							chartType: "area",
							xAxis: { field: "x", timeUnit: "month" },
							yAxis: [
								{ field: "y1", aggregate: "sum" },
								{ field: "y2", aggregate: "avg" },
							],
							series: { field: "category" },
							stacking: "stacked",
							limit: 100,
						},
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});
		});

		describe("SQL modifications", () => {
			it("should use customSql directly without calling query endpoint", async () => {
				const customSql =
					"SELECT country, SUM(revenue) FROM orders WHERE status = 'completed' GROUP BY country";

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ country: "US", revenue: 500 }],
					fields: ["country", "revenue"],
				});
				mockClient.post.mockResolvedValueOnce({
					chart: { mark: "bar" },
					notes: null,
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders", // original SQL
						question: "revenue by country",
						database: "test-db",
						sqlModifications: {
							customSql,
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.sql).toBe(customSql);
				expect(result.modified.sqlChanged).toBe(true);
				expect(result.params).toEqual({});

				// Should NOT have called /query endpoint (only /chart)
				expect(mockClient.post).toHaveBeenCalledTimes(1);
				expect(mockClient.post).toHaveBeenCalledWith(
					"/chart",
					expect.any(Object),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should regenerate SQL with time granularity hints", async () => {
				const queryResponse = {
					success: true,
					sql: "SELECT DATE_TRUNC('month', created_at) as month, SUM(revenue) FROM orders GROUP BY 1",
					params: [],
					dialect: "postgres",
					rationale: "Grouped by month as requested",
					queryId: "query-456",
				};

				mockClient.post
					.mockResolvedValueOnce(queryResponse)
					.mockResolvedValueOnce({
						chart: { mark: "line" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ month: "2024-01", revenue: 1000 }],
					fields: ["month", "revenue"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT created_at, revenue FROM orders",
						question: "revenue over time",
						database: "test-db",
						sqlModifications: {
							timeGranularity: "month",
						},
					},
					{ tenantId: "tenant-1" },
				);

				// Should have called /query with modified question
				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.objectContaining({
						question: "revenue over time (group results by month)",
						previous_sql: "SELECT created_at, revenue FROM orders",
					}),
					"tenant-1",
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);

				expect(result.sql).toBe(queryResponse.sql);
				expect(result.modified.sqlChanged).toBe(true);
				expect(result.queryId).toBe("query-456");
			});

			it("should regenerate SQL with date range hints", async () => {
				const queryResponse = {
					success: true,
					sql: "SELECT * FROM orders WHERE created_at BETWEEN '2024-01-01' AND '2024-12-31'",
					params: [],
					dialect: "postgres",
				};

				mockClient.post
					.mockResolvedValueOnce(queryResponse)
					.mockResolvedValueOnce({
						chart: { mark: "bar" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders",
						question: "show orders",
						database: "test-db",
						sqlModifications: {
							dateRange: { from: "2024-01-01", to: "2024-12-31" },
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.objectContaining({
						question:
							"show orders (filter date range from 2024-01-01 to 2024-12-31)",
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should include additional instructions in modified question", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT * FROM orders WHERE status != 'cancelled'",
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "bar" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders",
						question: "show orders",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "exclude cancelled orders",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.objectContaining({
						question: "show orders (exclude cancelled orders)",
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should combine multiple SQL modification hints", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT * FROM orders",
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "line" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders",
						question: "revenue",
						database: "test-db",
						sqlModifications: {
							timeGranularity: "week",
							dateRange: { from: "2024-01-01" },
							additionalInstructions: "only completed orders",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.objectContaining({
						question:
							"revenue (group results by week, filter date range from 2024-01-01, only completed orders)",
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});
		});

		describe("combined modifications", () => {
			it("should apply both SQL and viz modifications", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT month, SUM(revenue) FROM orders GROUP BY month",
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "area" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ month: "Jan", revenue: 100 }],
					fields: ["month", "revenue"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders",
						question: "revenue over time",
						database: "test-db",
						sqlModifications: {
							timeGranularity: "month",
						},
						vizModifications: {
							chartType: "area",
							stacking: "stacked",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.modified.sqlChanged).toBe(true);
				expect(result.modified.vizChanged).toBe(true);

				// Should call /query first
				expect(mockClient.post).toHaveBeenNthCalledWith(
					1,
					"/query",
					expect.any(Object),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);

				// Then /chart with viz hints
				expect(mockClient.post).toHaveBeenNthCalledWith(
					2,
					"/chart",
					expect.objectContaining({
						encoding_hints: {
							chartType: "area",
							stacking: "stacked",
						},
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});
		});

		describe("VizSpec chart type", () => {
			it("should use /vizspec endpoint when chartType is vizspec", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ country: "US", revenue: 1000 }],
					fields: ["country", "revenue"],
				});

				mockClient.post.mockResolvedValueOnce({
					spec: {
						version: "1.0",
						kind: "chart",
						encoding: {
							chartType: "bar",
							x: { field: "country" },
							y: { field: "revenue" },
						},
					},
					notes: null,
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT country, revenue FROM sales",
						question: "revenue by country",
						database: "test-db",
						vizModifications: {
							chartType: "bar",
						},
					},
					{
						tenantId: "tenant-1",
						chartType: "vizspec",
					},
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/vizspec",
					expect.objectContaining({
						encoding_hints: { chartType: "bar" },
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);

				expect(result.chart.specType).toBe("vizspec");
				expect(result.chart.vizSpec).toBeDefined();
			});
		});

		describe("edge cases", () => {
			it("should handle empty rows", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [],
					fields: ["id"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM empty_table",
						question: "show data",
						database: "test-db",
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.rows).toEqual([]);
				expect(result.chart.notes).toBe("Query returned no rows.");
				expect(result.chart.vegaLiteSpec).toBeUndefined();

				// Should NOT call chart generation endpoint
				expect(mockClient.post).not.toHaveBeenCalled();
			});

			it("should throw error if no database specified and no default", async () => {
				mockQueryEngine.getDefaultDatabase.mockReturnValue(undefined);

				await expect(
					modifyChart(
						mockClient,
						mockQueryEngine,
						{
							sql: "SELECT 1",
							question: "test",
							database: undefined as any,
						},
						{ tenantId: "tenant-1" },
					),
				).rejects.toThrow("No database specified");
			});

			it("should throw error if no tenant ID available", async () => {
				mockClient.getDefaultTenantId.mockReturnValue(undefined);

				await expect(
					modifyChart(mockClient, mockQueryEngine, {
						sql: "SELECT 1",
						question: "test",
						database: "test-db",
					}),
				).rejects.toThrow("tenantId is required");
			});

			it("should use default tenant ID from client", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [],
					fields: [],
				});

				await modifyChart(mockClient, mockQueryEngine, {
					sql: "SELECT 1",
					question: "test",
					database: "test-db",
				});

				expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			});

			it("should detect SQL change correctly", async () => {
				const originalSql = "SELECT * FROM orders";
				const newSql = "SELECT * FROM orders WHERE status = 'active'";

				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: newSql,
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "table" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: originalSql,
						question: "show orders",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "only active",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.modified.sqlChanged).toBe(true);
			});

			it("should set sqlChanged to false when regenerated SQL matches original", async () => {
				const sql = "SELECT * FROM orders";

				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql, // Same as original
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "table" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql,
						question: "show orders",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "try something",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.modified.sqlChanged).toBe(false);
			});
		});

		describe("parameters handling", () => {
			it("should preserve and map parameters from query response", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT * FROM orders WHERE status = $1",
						params: [{ name: "status", value: "active" }],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "table" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders",
						question: "active orders",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "only active",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.params).toEqual({ status: "active" });
				expect(result.paramMetadata).toEqual([
					{ name: "status", value: "active" },
				]);
			});

			it("should clear params when using customSql", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});
				mockClient.post.mockResolvedValueOnce({
					chart: { mark: "table" },
					notes: null,
				});

				const result = await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT * FROM orders WHERE status = $1",
						question: "orders",
						database: "test-db",
						params: { status: "pending" },
						sqlModifications: {
							customSql: "SELECT * FROM orders WHERE status = 'active'",
						},
					},
					{ tenantId: "tenant-1" },
				);

				expect(result.params).toEqual({});
				expect(result.paramMetadata).toEqual([]);
			});
		});

		describe("options handling", () => {
			it("should pass maxRetry to query endpoint", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT 1",
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "table" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT 1",
						question: "test",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "test",
						},
					},
					{
						tenantId: "tenant-1",
						maxRetry: 3,
					},
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.objectContaining({
						max_retry: 3,
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should pass chartMaxRetries to chart endpoint", async () => {
				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});
				mockClient.post.mockResolvedValueOnce({
					chart: { mark: "table" },
					notes: null,
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT 1",
						question: "test",
						database: "test-db",
					},
					{
						tenantId: "tenant-1",
						chartMaxRetries: 5,
					},
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/chart",
					expect.objectContaining({
						max_retries: 5,
					}),
					expect.any(String),
					undefined,
					undefined,
					undefined,
					expect.any(String),
				);
			});

			it("should pass userId and scopes to endpoints", async () => {
				mockClient.post
					.mockResolvedValueOnce({
						success: true,
						sql: "SELECT 1",
						params: [],
						dialect: "postgres",
					})
					.mockResolvedValueOnce({
						chart: { mark: "table" },
						notes: null,
					});

				mockQueryEngine.validateAndExecute.mockResolvedValue({
					rows: [{ id: 1 }],
					fields: ["id"],
				});

				await modifyChart(
					mockClient,
					mockQueryEngine,
					{
						sql: "SELECT 1",
						question: "test",
						database: "test-db",
						sqlModifications: {
							additionalInstructions: "test",
						},
					},
					{
						tenantId: "tenant-1",
						userId: "user-123",
						scopes: ["read", "write"],
					},
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/query",
					expect.any(Object),
					"tenant-1",
					"user-123",
					["read", "write"],
					undefined,
					expect.any(String),
				);

				expect(mockClient.post).toHaveBeenCalledWith(
					"/chart",
					expect.any(Object),
					"tenant-1",
					"user-123",
					["read", "write"],
					undefined,
					expect.any(String),
				);
			});
		});
	});
});
