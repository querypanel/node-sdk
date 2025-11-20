import { describe, it, expect, vi, beforeEach } from "vitest";
import { syncSchema } from "./ingest";
import type { ApiClient } from "../core/client";
import type { QueryEngine } from "../core/query-engine";
import type { DatabaseAdapter } from "../adapters/types";
import type { SchemaIntrospection } from "../schema/types";

describe("routes/ingest", () => {
	let mockClient: ApiClient;
	let mockQueryEngine: QueryEngine;
	let mockAdapter: DatabaseAdapter;

	beforeEach(() => {
		mockClient = {
			post: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		} as any;

		mockAdapter = {
			introspect: vi.fn(),
			getDialect: vi.fn(() => "postgres"),
			execute: vi.fn(),
			validate: vi.fn(),
		} as any;

		mockQueryEngine = {
			getDatabase: vi.fn(() => mockAdapter),
			getDatabaseMetadata: vi.fn(() => undefined),
		} as any;
	});

	describe("syncSchema", () => {
		it("should introspect and sync schema to backend", async () => {
			const introspection: SchemaIntrospection = {
				db: {
					kind: "postgres",
					name: "test-db",
				},
				tables: [
					{
						name: "users",
						schema: "public",
						type: "table",
						comment: "Users table",
						columns: [
							{
								name: "id",
								type: "integer",
								rawType: "int4",
								isPrimaryKey: true,
								comment: "User ID",
							},
							{
								name: "email",
								type: "varchar",
								rawType: "varchar",
								isPrimaryKey: false,
								comment: "Email address",
							},
						],
					},
				],
				introspectedAt: "2025-01-01T00:00:00Z",
			};

			(mockAdapter.introspect as any).mockResolvedValue(introspection);
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 5,
				chunks_with_annotations: 2,
			});

			const result = await syncSchema(
				mockClient,
				mockQueryEngine,
				"test-db",
				{ tenantId: "tenant-1" },
			);

			expect(mockAdapter.introspect).toHaveBeenCalledWith(undefined);
			expect(mockClient.post).toHaveBeenCalledWith(
				"/ingest",
				expect.objectContaining({
					database: "test-db",
					dialect: "postgres",
					tables: [
						{
							table_name: "users",
							description: "Users table",
							columns: [
								{
									name: "id",
									data_type: "int4",
									is_primary_key: true,
									description: "User ID",
								},
								{
									name: "email",
									data_type: "varchar",
									is_primary_key: false,
									description: "Email address",
								},
							],
						},
					],
				}),
				"tenant-1",
				undefined,
				undefined,
				undefined,
				expect.any(String), // session ID
			);
			expect(result.success).toBe(true);
			expect(result.chunks).toBe(5);
		});

		it("should use default tenant ID if not provided", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 0,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {});

			expect(mockClient.getDefaultTenantId).toHaveBeenCalled();
			const call = (mockClient.post as any).mock.calls[0];
			expect(call[0]).toBe("/ingest");
			expect(call[2]).toBe("default-tenant"); // tenantId
		});

		it("should throw error if no tenant ID available", async () => {
			(mockClient.getDefaultTenantId as any).mockReturnValue(undefined);

			await expect(
				syncSchema(mockClient, mockQueryEngine, "test-db", {}),
			).rejects.toThrow("tenantId is required");
		});

		it("should filter tables when tables option provided", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [
					{
						name: "users",
						schema: "public",
						type: "table",
						columns: [],
					},
				],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 1,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
				tables: ["users", "orders"],
			});

			expect(mockAdapter.introspect).toHaveBeenCalledWith({
				tables: ["users", "orders"],
			});
		});

		it("should use table name as description fallback", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [
					{
						name: "users",
						schema: "public",
						type: "table",
						// No comment
						columns: [
							{
								name: "id",
								type: "integer",
								isPrimaryKey: true,
								// No comment
							},
						],
					},
				],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 1,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
			});

			const payload = (mockClient.post as any).mock.calls[0][1] as any;
			expect(payload.tables[0].description).toBe("Table users");
			expect(payload.tables[0].columns[0].description).toBe("");
		});

		it("should use rawType if available, otherwise type", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [
					{
						name: "test",
						schema: "public",
						type: "table",
						columns: [
							{
								name: "col1",
								type: "integer",
								rawType: "int4",
								isPrimaryKey: false,
							},
							{
								name: "col2",
								type: "text",
								isPrimaryKey: false,
							},
						],
					},
				],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 1,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
			});

			const payload = (mockClient.post as any).mock.calls[0][1] as any;
			expect(payload.tables[0].columns[0].data_type).toBe("int4");
			expect(payload.tables[0].columns[1].data_type).toBe("text");
		});

		it("should include userId and scopes in request", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 0,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
				userId: "user-123",
				scopes: ["read", "write"],
			});

			expect(mockClient.post).toHaveBeenCalledWith(
				"/ingest",
				expect.any(Object),
				"tenant-1",
				"user-123",
				["read", "write"],
				undefined,
				expect.any(String),
			);
		});

		it("should pass abort signal", async () => {
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 0,
				chunks_with_annotations: 0,
			});

			const abortController = new AbortController();
			await syncSchema(
				mockClient,
				mockQueryEngine,
				"test-db",
				{ tenantId: "tenant-1" },
				abortController.signal,
			);

			expect(mockClient.post).toHaveBeenCalledWith(
				"/ingest",
				expect.any(Object),
				"tenant-1",
				undefined,
				undefined,
				abortController.signal,
				expect.any(String),
			);
		});

		it("should include tenant_settings in payload when metadata is configured", async () => {
			const metadata = {
				name: "test-db",
				dialect: "postgres" as const,
				tenantFieldName: "tenant_id",
				tenantFieldType: "String",
				enforceTenantIsolation: true,
			};

			(mockQueryEngine.getDatabaseMetadata as any).mockReturnValue(metadata);
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 0,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
			});

			const payload = (mockClient.post as any).mock.calls[0][1] as any;
			expect(payload.tenant_settings).toEqual({
				tenantFieldName: "tenant_id",
				tenantFieldType: "String",
				enforceTenantIsolation: true,
			});
		});

		it("should not include tenant_settings when metadata is not configured", async () => {
			(mockQueryEngine.getDatabaseMetadata as any).mockReturnValue(undefined);
			(mockAdapter.introspect as any).mockResolvedValue({
				db: { kind: "postgres", name: "test-db" },
				tables: [],
				introspectedAt: "2025-01-01T00:00:00Z",
			});
			(mockClient.post as any).mockResolvedValue({
				success: true,
				message: "Schema synced",
				chunks: 0,
				chunks_with_annotations: 0,
			});

			await syncSchema(mockClient, mockQueryEngine, "test-db", {
				tenantId: "tenant-1",
			});

			const payload = (mockClient.post as any).mock.calls[0][1] as any;
			expect(payload.tenant_settings).toBeUndefined();
		});
	});
});
