import { describe, it, expect, vi, beforeEach } from "vitest";
import {
	createActiveChart,
	listActiveCharts,
	getActiveChart,
	updateActiveChart,
	deleteActiveChart,
} from "./active-charts";
import type { ApiClient } from "../core/client";
import type { QueryEngine } from "../core/query-engine";
import * as charts from "./charts";

describe("routes/active-charts", () => {
	let mockClient: ApiClient;
	let mockQueryEngine: QueryEngine;

	beforeEach(() => {
		vi.restoreAllMocks();

		mockClient = {
			post: vi.fn(),
			get: vi.fn(),
			put: vi.fn(),
			delete: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		} as any;

		mockQueryEngine = {
			execute: vi.fn(),
		} as any;
	});

	describe("createActiveChart", () => {
		it("should create an active chart", async () => {
			const activeChartInput = {
				chart_id: "chart-123",
				order: 1,
				meta: { color: "blue" },
			};

			const activeChartResponse = {
				id: "active-chart-1",
				...activeChartInput,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.post).mockResolvedValue(activeChartResponse);

			const result = await createActiveChart(
				mockClient,
				activeChartInput,
				{ tenantId: "tenant-1" },
			);

			expect(result).toEqual(activeChartResponse);
			expect(mockClient.post).toHaveBeenCalledWith(
				"/active-charts",
				activeChartInput,
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				createActiveChart(
					mockClient,
					{
						chart_id: "chart-123",
					},
					{},
				),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("listActiveCharts", () => {
		it("should list active charts with pagination parameters", async () => {
			const paginatedResponse = {
				data: [
					{
						id: "active-1",
						chart_id: "chart-1",
						order: 1,
						meta: null,
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

			vi.mocked(mockClient.get).mockResolvedValue(paginatedResponse);

			const result = await listActiveCharts(
				mockClient,
				mockQueryEngine,
				{
					tenantId: "tenant-1",
					pagination: {
						page: 2,
						limit: 50,
					},
				},
			);

			expect(result).toEqual(paginatedResponse);
			expect(mockClient.get).toHaveBeenCalledWith(
				"/active-charts?page=2&limit=50",
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should list active charts with all filter options", async () => {
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

			vi.mocked(mockClient.get).mockResolvedValue(paginatedResponse);

			await listActiveCharts(
				mockClient,
				mockQueryEngine,
				{
					tenantId: "tenant-1",
					sortBy: "created_at",
					sortDir: "desc",
					title: "Test Chart",
					userFilter: "user-123",
					createdFrom: "2024-01-01",
					createdTo: "2024-12-31",
					updatedFrom: "2024-01-01",
					updatedTo: "2024-12-31",
				},
			);

			const call = vi.mocked(mockClient.get).mock.calls[0];
			expect(call[0]).toContain("sort_by=created_at");
			expect(call[0]).toContain("sort_dir=desc");
			expect(call[0]).toContain("name=Test+Chart");
			expect(call[0]).toContain("user_id=user-123");
			expect(call[0]).toContain("created_from=2024-01-01");
			expect(call[0]).toContain("created_to=2024-12-31");
			expect(call[0]).toContain("updated_from=2024-01-01");
			expect(call[0]).toContain("updated_to=2024-12-31");
		});

		it("should list active charts with withData option", async () => {
			const activeCharts = [
				{
					id: "active-1",
					chart_id: "chart-1",
					order: 1,
					meta: null,
					organization_id: null,
					tenant_id: "tenant-1",
					user_id: null,
					created_at: "2024-01-01T00:00:00Z",
					updated_at: "2024-01-01T00:00:00Z",
					chart: {
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
				},
			];

			const paginatedResponse = {
				data: activeCharts,
				pagination: {
					page: 1,
					limit: 10,
					total: 1,
					totalPages: 1,
					hasNext: false,
					hasPrev: false,
				},
			};

			const chartWithData = {
				id: "chart-1",
				title: "Chart 1",
				description: null,
				sql: "SELECT * FROM users",
				sql_params: null,
				vega_lite_spec: {
					mark: "bar",
					data: {
						values: [{ id: 1, name: "Alice" }],
					},
				},
				query_id: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.get).mockResolvedValue(paginatedResponse);
			vi.spyOn(charts, "getChart").mockResolvedValue(chartWithData);

			const result = await listActiveCharts(
				mockClient,
				mockQueryEngine,
				{
					tenantId: "tenant-1",
					withData: true,
				},
			);

			expect(result.data).toHaveLength(1);
			expect(result.data[0].chart).toEqual(chartWithData);
			expect(charts.getChart).toHaveBeenCalledWith(
				mockClient,
				mockQueryEngine,
				"chart-1",
				expect.objectContaining({
					tenantId: "tenant-1",
					withData: true,
				}),
				undefined,
			);
		});

		it("should handle withData option with null chart", async () => {
			const activeCharts = [
				{
					id: "active-1",
					chart_id: "chart-1",
					order: 1,
					meta: null,
					organization_id: null,
					tenant_id: "tenant-1",
					user_id: null,
					created_at: "2024-01-01T00:00:00Z",
					updated_at: "2024-01-01T00:00:00Z",
					chart: null,
				},
			];

			const paginatedResponse = {
				data: activeCharts,
				pagination: {
					page: 1,
					limit: 10,
					total: 1,
					totalPages: 1,
					hasNext: false,
					hasPrev: false,
				},
			};

			vi.mocked(mockClient.get).mockResolvedValue(paginatedResponse);
			vi.spyOn(charts, "getChart");

			const result = await listActiveCharts(
				mockClient,
				mockQueryEngine,
				{
					tenantId: "tenant-1",
					withData: true,
				},
			);

			expect(result.data).toHaveLength(1);
			expect(result.data[0].chart).toBeNull();
			expect(charts.getChart).not.toHaveBeenCalled();
		});

		it("should use default tenant ID if not provided", async () => {
			vi.mocked(mockClient.get).mockResolvedValue({
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

			await listActiveCharts(mockClient, mockQueryEngine, {});

			expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			const call = vi.mocked(mockClient.get).mock.calls[0];
			expect(call[1]).toBe("default-tenant");
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				listActiveCharts(mockClient, mockQueryEngine, {}),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("getActiveChart", () => {
		it("should get an active chart", async () => {
			const activeChart = {
				id: "active-1",
				chart_id: "chart-1",
				order: 1,
				meta: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.get).mockResolvedValue(activeChart);

			const result = await getActiveChart(
				mockClient,
				mockQueryEngine,
				"active-1",
				{ tenantId: "tenant-1" },
			);

			expect(result).toEqual(activeChart);
			expect(mockClient.get).toHaveBeenCalledWith(
				"/active-charts/active-1",
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should get an active chart with data hydration", async () => {
			const activeChart = {
				id: "active-1",
				chart_id: "chart-1",
				order: 1,
				meta: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			const chartWithData = {
				id: "chart-1",
				title: "Chart 1",
				description: null,
				sql: "SELECT * FROM users",
				sql_params: null,
				vega_lite_spec: {
					mark: "bar",
					data: {
						values: [{ id: 1, name: "Alice" }],
					},
				},
				query_id: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.get).mockResolvedValue(activeChart);
			vi.spyOn(charts, "getChart").mockResolvedValue(chartWithData);

			const result = await getActiveChart(
				mockClient,
				mockQueryEngine,
				"active-1",
				{
					tenantId: "tenant-1",
					withData: true,
				},
			);

			expect(result.chart).toEqual(chartWithData);
			expect(charts.getChart).toHaveBeenCalledWith(
				mockClient,
				mockQueryEngine,
				"chart-1",
				expect.objectContaining({
					tenantId: "tenant-1",
					withData: true,
				}),
				undefined,
			);
		});

		it("should not hydrate data if withData is false", async () => {
			const activeChart = {
				id: "active-1",
				chart_id: "chart-1",
				order: 1,
				meta: null,
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.get).mockResolvedValue(activeChart);
			vi.spyOn(charts, "getChart");

			const result = await getActiveChart(
				mockClient,
				mockQueryEngine,
				"active-1",
				{
					tenantId: "tenant-1",
					withData: false,
				},
			);

			expect(result).toEqual(activeChart);
			expect(charts.getChart).not.toHaveBeenCalled();
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				getActiveChart(mockClient, mockQueryEngine, "active-1", {}),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("updateActiveChart", () => {
		it("should update an active chart", async () => {
			const updateInput = {
				order: 2,
				meta: { color: "red" },
			};

			const updatedActiveChart = {
				id: "active-1",
				chart_id: "chart-1",
				order: 2,
				meta: { color: "red" },
				organization_id: null,
				tenant_id: "tenant-1",
				user_id: null,
				created_at: "2024-01-01T00:00:00Z",
				updated_at: "2024-01-01T00:00:00Z",
			};

			vi.mocked(mockClient.put).mockResolvedValue(updatedActiveChart);

			const result = await updateActiveChart(
				mockClient,
				"active-1",
				updateInput,
				{ tenantId: "tenant-1" },
			);

			expect(result).toEqual(updatedActiveChart);
			expect(mockClient.put).toHaveBeenCalledWith(
				"/active-charts/active-1",
				updateInput,
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				updateActiveChart(mockClient, "active-1", { order: 1 }, {}),
			).rejects.toThrow("tenantId is required");
		});
	});

	describe("deleteActiveChart", () => {
		it("should delete an active chart", async () => {
			vi.mocked(mockClient.delete).mockResolvedValue(undefined);

			await deleteActiveChart(mockClient, "active-1", { tenantId: "tenant-1" });

			expect(mockClient.delete).toHaveBeenCalledWith(
				"/active-charts/active-1",
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
		});

		it("should throw error if no tenant ID available", async () => {
			vi.mocked(mockClient.getDefaultTenantId).mockReturnValue(undefined);

			await expect(
				deleteActiveChart(mockClient, "active-1", {}),
			).rejects.toThrow("tenantId is required");
		});
	});
});
