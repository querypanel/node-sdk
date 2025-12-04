import { beforeEach, describe, expect, it, type Mock, vi } from "vitest";
import {
	createChart,
	deleteChart,
	getChart,
	listCharts,
	updateChart,
} from "./charts";

describe("routes/charts", () => {
	let mockClient: {
		post: Mock;
		get: Mock;
		put: Mock;
		delete: Mock;
		getDefaultTenantId: Mock;
	};
	let mockQueryEngine: {
		validateAndExecute: Mock;
		getDefaultDatabase: Mock;
	};

	beforeEach(() => {
		mockClient = {
			post: vi.fn(),
			get: vi.fn(),
			put: vi.fn(),
			delete: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		};

		mockQueryEngine = {
			validateAndExecute: vi.fn(),
			getDefaultDatabase: vi.fn(() => "default-db"),
		};
	});

	describe("createChart", () => {
		it("should create a chart", async () => {
			const chartInput = {
				title: "Test Chart",
				sql: "SELECT * FROM users",
				vega_lite_spec: { mark: "bar" },
			};

			const chartResponse = {
				id: "chart-123",
				...chartInput,
				description: null,
				sql_params: null,
				query_id: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			mockClient.post.mockResolvedValue(chartResponse);

			const result = await createChart(mockClient, chartInput, {
				tenantId: "tenant-1",
			});

			expect(result).toEqual(chartResponse);
			expect(mockClient.post).toHaveBeenCalledWith(
				"/charts",
				chartInput,
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(
				createChart(
					mockClient,
					{
						title: "Test",
						sql: "SELECT 1",
						vega_lite_spec: {},
					},
					{},
				),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("listCharts", () => {
		it("should list charts with pagination parameters", async () => {
			const paginatedResponse = {
				data: [
					{
						id: "chart-1",
						title: "Chart 1",
						description: null,
						sql: "SELECT * FROM users",
						sql_params: null,
						vega_lite_spec: { mark: "bar" },
						query_id: null,
						organization_id: null,
						tenant_id: "tenant-1",
						user_id: null,
						created_at: "2024-01-01T00:00:00Z",
						updated_at: "2024-01-01T00:00:00Z",
					},
				],
				pagination: {
					page: 2,
					limit: 50,
					total: 100,
					totalPages: 2,
					hasNext: false,
					hasPrev: true,
				},
			};

			mockClient.get.mockResolvedValue(paginatedResponse);

			const result = await listCharts(mockClient, mockQueryEngine, {
				tenantId: "tenant-1",
				pagination: {
					page: 2,
					limit: 50,
				},
			});

			expect(result).toEqual(paginatedResponse);
			expect(mockClient.get).toHaveBeenCalledWith(
				"/charts?page=2&limit=50",
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should list charts with all filter options", async () => {
			const paginatedResponse = {
				data: [],
				pagination: {
					page: 1,
					limit: 10,
					total: 0,
					totalPages: 0,
					hasNext: false,
					hasPrev: false,
				},
			};

			mockClient.get.mockResolvedValue(paginatedResponse);

			await listCharts(mockClient, mockQueryEngine, {
				tenantId: "tenant-1",
				sortBy: "created_at",
				sortDir: "desc",
				title: "Test Chart",
				userFilter: "user-123",
				createdFrom: "2024-01-01",
				createdTo: "2024-12-31",
				updatedFrom: "2024-01-01",
				updatedTo: "2024-12-31",
			});

			const call = mockClient.get.mock.calls[0];
			expect(call[0]).toContain("sort_by=created_at");
			expect(call[0]).toContain("sort_dir=desc");
			expect(call[0]).toContain("title=Test+Chart");
			expect(call[0]).toContain("user_id=user-123");
			expect(call[0]).toContain("created_from=2024-01-01");
			expect(call[0]).toContain("created_to=2024-12-31");
			expect(call[0]).toContain("updated_from=2024-01-01");
			expect(call[0]).toContain("updated_to=2024-12-31");
		});

		it("should list charts with includeData option", async () => {
			const charts = [
				{
					id: "chart-1",
					title: "Chart 1",
					description: null,
					sql: "SELECT * FROM users WHERE id = :id",
					sql_params: { id: 1 },
					vega_lite_spec: { mark: "bar", encoding: {} },
					query_id: null,
					organization_id: null,
					tenant_id: "tenant-1",
					user_id: null,
					created_at: "2024-01-01T00:00:00Z",
					updated_at: "2024-01-01T00:00:00Z",
					target_db: "custom-db",
				},
				{
					id: "chart-2",
					title: "Chart 2",
					description: null,
					sql: "SELECT * FROM orders",
					sql_params: null,
					vega_lite_spec: { mark: "line" },
					query_id: null,
					organization_id: null,
					tenant_id: "tenant-1",
					user_id: null,
					created_at: "2024-01-01T00:00:00Z",
					updated_at: "2024-01-01T00:00:00Z",
					target_db: null,
				},
			];

			const paginatedResponse = {
				data: charts,
				pagination: {
					page: 1,
					limit: 10,
					total: 2,
					totalPages: 1,
					hasNext: false,
					hasPrev: false,
				},
			};

			const queryResult1 = [
				{ id: 1, name: "Alice" },
				{ id: 2, name: "Bob" },
			];

			const queryResult2 = [
				{ order_id: 1, total: 100 },
				{ order_id: 2, total: 200 },
			];

			mockClient.get.mockResolvedValue(paginatedResponse);
			mockQueryEngine.validateAndExecute
				.mockResolvedValueOnce({ rows: queryResult1, fields: [] })
				.mockResolvedValueOnce({ rows: queryResult2, fields: [] });

			const result = await listCharts(mockClient, mockQueryEngine, {
				tenantId: "tenant-1",
				includeData: true,
			});

			expect(result.data).toHaveLength(2);
			expect(result.data[0].vega_lite_spec).toEqual({
				mark: "bar",
				encoding: {},
				data: {
					values: queryResult1,
				},
			});
			expect(result.data[1].vega_lite_spec).toEqual({
				mark: "line",
				data: {
					values: queryResult2,
				},
			});

			expect(mockQueryEngine.validateAndExecute).toHaveBeenCalledWith(
				"SELECT * FROM users WHERE id = :id",
				{ id: 1 },
				"custom-db",
				"tenant-1",
			);
			expect(mockQueryEngine.validateAndExecute).toHaveBeenCalledWith(
				"SELECT * FROM orders",
				{},
				"default-db", // Using default db since target_db is null
				"tenant-1",
			);
		});

		it("should use default tenant ID if not provided", async () => {
			mockClient.get.mockResolvedValue({
				data: [],
				pagination: {
					page: 1,
					limit: 10,
					total: 0,
					totalPages: 0,
					hasNext: false,
					hasPrev: false,
				},
			});

			await listCharts(mockClient, mockQueryEngine, {});

			expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			const call = mockClient.get.mock.calls[0];
			expect(call[1]).toBe("default-tenant");
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(listCharts(mockClient, mockQueryEngine, {})).rejects.toThrow(
				"tenantId is required",
			);
		});
	});

	describe("getChart", () => {
		it("should get a chart with data hydration", async () => {
			const chart = {
				id: "chart-1",
				title: "Test Chart",
				description: null,
				sql: "SELECT * FROM users",
				sql_params: null,
				vega_lite_spec: { mark: "bar" },
				query_id: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			const queryResult = [
				{ id: 1, name: "Alice" },
				{ id: 2, name: "Bob" },
			];

			mockClient.get.mockResolvedValue(chart);
			mockQueryEngine.validateAndExecute.mockResolvedValue({
				rows: queryResult,
				fields: [],
			});

			const result = await getChart(mockClient, mockQueryEngine, "chart-1", {
				tenantId: "tenant-1",
			});

			expect(result.vega_lite_spec).toEqual({
				mark: "bar",
				data: {
					values: queryResult,
				},
			});
			expect(mockQueryEngine.validateAndExecute).toHaveBeenCalledWith(
				"SELECT * FROM users",
				{},
				"default-db",
				"tenant-1",
			);
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(
				getChart(mockClient, mockQueryEngine, "chart-1", {}),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("updateChart", () => {
		it("should update a chart", async () => {
			const updateInput = {
				title: "Updated Chart",
				description: "New description",
			};

			const updatedChart = {
				id: "chart-1",
				title: "Updated Chart",
				description: "New description",
				sql: "SELECT * FROM users",
				sql_params: null,
				vega_lite_spec: { mark: "bar" },
				query_id: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			mockClient.put.mockResolvedValue(updatedChart);

			const result = await updateChart(mockClient, "chart-1", updateInput, {
				tenantId: "tenant-1",
			});

			expect(result).toEqual(updatedChart);
			expect(mockClient.put).toHaveBeenCalledWith(
				"/charts/chart-1",
				updateInput,
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(
				updateChart(mockClient, "chart-1", { title: "Test" }, {}),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("deleteChart", () => {
		it("should delete a chart", async () => {
			mockClient.delete.mockResolvedValue(undefined);

			await deleteChart(mockClient, "chart-1", { tenantId: "tenant-1" });

			expect(mockClient.delete).toHaveBeenCalledWith(
				"/charts/chart-1",
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			mockClient.getDefaultTenantId.mockReturnValue(undefined);

			await expect(deleteChart(mockClient, "chart-1", {})).rejects.toThrow(
				"tenantId is required",
			);
		});
	});
});
