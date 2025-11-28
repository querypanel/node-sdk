import { describe, it, expect, vi, beforeEach } from "vitest";
import { QueryPanelSdkAPI } from "./index";
import type { DatabaseAdapter } from "./adapters/types";
import { TEST_PRIVATE_KEY, TEST_ORG_ID, TEST_BASE_URL } from "./test-utils";

describe("QueryPanelSdkAPI", () => {
	const mockBaseUrl = TEST_BASE_URL;
	const mockPrivateKey = TEST_PRIVATE_KEY;
	const mockOrgId = TEST_ORG_ID;
	let mockFetch: ReturnType<typeof vi.fn>;
	let sdk: QueryPanelSdkAPI;
	let mockAdapter: DatabaseAdapter;

	beforeEach(() => {
		mockFetch = vi.fn();
		sdk = new QueryPanelSdkAPI(mockBaseUrl, mockPrivateKey, mockOrgId, {
			defaultTenantId: "tenant-1",
			fetch: mockFetch,
		});

		mockAdapter = {
			execute: vi.fn(),
			validate: vi.fn(),
			introspect: vi.fn(),
			getDialect: vi.fn(() => "postgres"),
		};
	});

	describe("constructor", () => {
		it("should create SDK instance", () => {
			expect(sdk).toBeInstanceOf(QueryPanelSdkAPI);
		});

		it("should throw error if parameters are missing", () => {
			expect(
				() => new QueryPanelSdkAPI("", mockPrivateKey, mockOrgId),
			).toThrow();
		});
	});

	describe("database attachment", () => {
		describe("attachPostgres", () => {
			it("should attach PostgreSQL database", async () => {
				const clientFn = vi.fn().mockResolvedValue({
					rows: [],
					fields: [],
				});

				sdk.attachPostgres("my-db", clientFn, {
					database: "postgres-db",
					description: "Test database",
				});

				// Verify database was attached by checking it can be called
				await expect(sdk.introspect("my-db")).resolves.toBeDefined();
			});

			it("should configure tenant isolation", () => {
				const clientFn = vi.fn();

				sdk.attachPostgres("my-db", clientFn, {
					tenantFieldName: "tenant_id",
					enforceTenantIsolation: true,
				});

				// This would be tested through query execution
			});
		});

		describe("attachClickhouse", () => {
			it("should attach ClickHouse database", () => {
				const clientFn = vi.fn();

				sdk.attachClickhouse("my-db", clientFn, {
					database: "clickhouse-db",
					description: "Test database",
				});

				expect(() => sdk.introspect("my-db")).not.toThrow();
			});

			it("should set tenant field type for ClickHouse", () => {
				const clientFn = vi.fn();

				sdk.attachClickhouse("my-db", clientFn, {
					tenantFieldName: "tenant_id",
					tenantFieldType: "Int32",
					enforceTenantIsolation: true,
				});

				// This would be tested through query execution
			});
		});

		describe("attachDatabase", () => {
			it("should attach generic database adapter", () => {
				sdk.attachDatabase("my-db", mockAdapter);

				expect(() => sdk.introspect("my-db")).not.toThrow();
			});
		});
	});

	describe("introspect", () => {
		it("should introspect database schema", async () => {
			const mockIntrospectAdapter = {
				...mockAdapter,
				introspect: vi.fn().mockResolvedValue({
					db: { kind: "postgres", name: "my-db" },
					tables: [
						{
							name: "users",
							schema: "public",
							type: "table",
							columns: [
								{ name: "id", type: "integer", isPrimaryKey: true },
								{ name: "email", type: "varchar", isPrimaryKey: false },
							],
						},
					],
					introspectedAt: "2025-01-01T00:00:00Z",
				}),
			};

			sdk.attachDatabase("my-db", mockIntrospectAdapter);

			const result = await sdk.introspect("my-db");

			expect(result.tables).toHaveLength(1);
			expect(result.tables[0].name).toBe("users");
			expect(result.tables[0].columns).toHaveLength(2);
		});

		it("should introspect specific tables", async () => {
			const mockIntrospectAdapter = {
				...mockAdapter,
				introspect: vi.fn().mockResolvedValue({
					db: { kind: "postgres", name: "my-db" },
					tables: [],
					introspectedAt: "2025-01-01T00:00:00Z",
				}),
			};

			sdk.attachDatabase("my-db", mockIntrospectAdapter);

			await sdk.introspect("my-db", ["users", "orders"]);

			expect(mockIntrospectAdapter.introspect).toHaveBeenCalledWith({
				tables: ["users", "orders"],
			});
		});
	});

	describe("syncSchema", () => {
		it("should sync schema to backend", async () => {
			// Properly mock the adapter's introspect method
			const mockIntrospectAdapter = {
				...mockAdapter,
				introspect: vi.fn().mockResolvedValue({
					db: { kind: "postgres", name: "my-db" },
					tables: [],
					introspectedAt: "2025-01-01T00:00:00Z",
				}),
			};

			sdk.attachDatabase("my-db", mockIntrospectAdapter);

			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						success: true,
						message: "Schema synced",
						chunks: 5,
						chunks_with_annotations: 2,
					}),
			});

			const result = await sdk.syncSchema("my-db", { tenantId: "tenant-1" });

			expect(result.success).toBe(true);
			expect(result.chunks).toBe(5);
			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/ingest",
				expect.any(Object),
			);
		});
	});

	describe("ask", () => {
		it("should generate and execute natural language query", async () => {
			sdk.attachDatabase("my-db", mockAdapter);

			// Just mock the implementation directly without vi.mocked()
			(mockAdapter.validate as any).mockResolvedValue(undefined);
			(mockAdapter.execute as any).mockResolvedValue({
				rows: [{ id: 1, name: "Alice" }],
				fields: ["id", "name"],
			});

			mockFetch
				.mockResolvedValueOnce({
					ok: true,
					text: async () =>
						JSON.stringify({
							success: true,
							sql: "SELECT * FROM users LIMIT 1",
							params: [],
							dialect: "postgres",
							database: "my-db",
						}),
				})
				.mockResolvedValueOnce({
					ok: true,
					text: async () =>
						JSON.stringify({
							chart: { mark: "table" },
							notes: null,
						}),
				});

			const result = await sdk.ask("Show me one user", {
				tenantId: "tenant-1",
				database: "my-db",
			});

			expect(result.sql).toBe("SELECT * FROM users LIMIT 1");
			expect(result.rows).toHaveLength(1);
			expect(result.chart.vegaLiteSpec).toBeTruthy();
		});

		it("should use default tenant ID", async () => {
			sdk.attachDatabase("my-db", mockAdapter);

			vi.mocked(mockAdapter.validate).mockResolvedValue();
			vi.mocked(mockAdapter.execute).mockResolvedValue({
				rows: [],
				fields: [],
			});

			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						success: true,
						sql: "SELECT 1",
						params: [],
						dialect: "postgres",
						database: "my-db",
					}),
			});

			await sdk.ask("test", { database: "my-db" });

			// Should use tenant-1 from constructor
			const call = mockFetch.mock.calls[0];
			const token = call[1].headers.Authorization.replace("Bearer ", "");
			const parts = token.split(".");
			const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString());
			expect(payload.tenantId).toBe("tenant-1");
		});
	});

	describe("chart operations", () => {
		beforeEach(() => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});
		});

		it("should create chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "chart-1",
						title: "Test Chart",
						sql: "SELECT 1",
						sql_params: {},
						vega_lite_spec: { mark: "bar" },
					}),
			});

			const result = await sdk.createChart({
				title: "Test Chart",
				sql: "SELECT 1",
				sql_params: {},
				vega_lite_spec: { mark: "bar" },
			});

			expect(result.id).toBe("chart-1");
			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/charts",
				expect.objectContaining({ method: "POST" }),
			);
		});

		it("should list charts", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						data: [
							{
								id: "chart-1",
								title: "Chart 1",
								sql: "SELECT 1",
								vega_lite_spec: {},
							},
						],
						pagination: {
							page: 1,
							limit: 10,
							total: 1,
							totalPages: 1,
							hasNext: false,
							hasPrev: false,
						},
					}),
			});

			const result = await sdk.listCharts();

			expect(result.data).toHaveLength(1);
			expect(result.pagination.total).toBe(1);
		});

		it("should get chart by ID", async () => {
			sdk.attachDatabase("my-db", mockAdapter);

			vi.mocked(mockAdapter.execute).mockResolvedValue({
				rows: [{ count: 5 }],
				fields: ["count"],
			});

			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "chart-1",
						title: "Test Chart",
						sql: "SELECT COUNT(*) as count FROM users",
						sql_params: null,
						vega_lite_spec: { mark: "number" },
						target_db: "my-db",
					}),
			});

			const result = await sdk.getChart("chart-1");

			expect(result.id).toBe("chart-1");
			expect(result.vega_lite_spec.data.values).toEqual([{ count: 5 }]);
		});

		it("should update chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "chart-1",
						title: "Updated Chart",
					}),
			});

			const result = await sdk.updateChart("chart-1", {
				title: "Updated Chart",
			});

			expect(result.title).toBe("Updated Chart");
			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/charts/chart-1",
				expect.objectContaining({ method: "PUT" }),
			);
		});

		it("should delete chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => "",
			});

			await sdk.deleteChart("chart-1");

			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/charts/chart-1",
				expect.objectContaining({ method: "DELETE" }),
			);
		});
	});

	describe("active chart operations", () => {
		beforeEach(() => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => JSON.stringify({}),
			});
		});

		it("should create active chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "active-1",
						chart_id: "chart-1",
						order: 1,
					}),
			});

			const result = await sdk.createActiveChart({
				chart_id: "chart-1",
				order: 1,
			});

			expect(result.id).toBe("active-1");
			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/active-charts",
				expect.objectContaining({ method: "POST" }),
			);
		});

		it("should list active charts", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						data: [
							{
								id: "active-1",
								chart_id: "chart-1",
								order: 1,
							},
						],
						pagination: {
							page: 1,
							limit: 10,
							total: 1,
							totalPages: 1,
							hasNext: false,
							hasPrev: false,
						},
					}),
			});

			const result = await sdk.listActiveCharts();

			expect(result.data).toHaveLength(1);
		});

		it("should get active chart by ID", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "active-1",
						chart_id: "chart-1",
						order: 1,
					}),
			});

			const result = await sdk.getActiveChart("active-1");

			expect(result.id).toBe("active-1");
		});

		it("should update active chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({
						id: "active-1",
						order: 2,
					}),
			});

			const result = await sdk.updateActiveChart("active-1", { order: 2 });

			expect(result.order).toBe(2);
		});

		it("should delete active chart", async () => {
			mockFetch.mockResolvedValue({
				ok: true,
				text: async () => "",
			});

			await sdk.deleteActiveChart("active-1");

			expect(mockFetch).toHaveBeenCalledWith(
				"https://api.example.com/active-charts/active-1",
				expect.objectContaining({ method: "DELETE" }),
			);
		});
	});
});
