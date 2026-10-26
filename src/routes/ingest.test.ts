import { describe, it, expect, vi, beforeEach } from "vitest";
import { createMockQueryPanelApi } from "../test-utils";
import { SchemaTooLargeError } from "../errors";
import {
	embedSchemaIngestBatch,
	finalizeSchemaIngest,
	type SchemaSyncProgressEvent,
	splitIngestTablesIntoBatches,
	stageSchemaIngest,
	syncSchema,
	syncSchemaInBatches,
} from "./ingest";
import type { QueryEngine } from "../core/query-engine";
import type { DatabaseAdapter } from "../adapters/types";
import type { SchemaIntrospection } from "../schema/types";

describe("routes/ingest", () => {
	let mockClient: ReturnType<typeof createMockQueryPanelApi>;
	let mockQueryEngine: QueryEngine;
	let mockAdapter: DatabaseAdapter;

	beforeEach(() => {
		mockClient = createMockQueryPanelApi({
			post: vi.fn(),
			getDefaultTenantId: vi.fn(() => "default-tenant"),
		});

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
							schema_name: "public",
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

		it("should include tenant_settings with isolation off", async () => {
			const metadata = {
				name: "test-db",
				dialect: "postgres" as const,
				tenantFieldName: "tenant_id",
				tenantFieldType: "String",
				enforceTenantIsolation: false,
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
				enforceTenantIsolation: false,
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

	describe("splitIngestTablesIntoBatches", () => {
		const table = (name: string, columns: number) => ({
			table_name: name,
			columns: Array.from({ length: columns }, (_, i) => ({ name: `c${i}` })),
		});

		it("splits on the doc limit (1 per table + 1 per column)", () => {
			// docs: 3, 3, 3 -> limit 6 fits two tables per batch
			const batches = splitIngestTablesIntoBatches(
				[table("a", 2), table("b", 2), table("c", 2)],
				6,
				Number.MAX_SAFE_INTEGER,
			);
			expect(batches.map((b) => b.map((t) => t.table_name))).toEqual([
				["a", "b"],
				["c"],
			]);
		});

		it("splits on the serialized byte limit", () => {
			const tables = [table("a", 1), table("b", 1), table("c", 1)];
			const envelope = JSON.stringify({ tables: [] }).length;
			const one = JSON.stringify(tables[0]).length;
			// Exactly fits two tables plus a comma
			const exact = envelope + one * 2 + 1;
			expect(
				splitIngestTablesIntoBatches(tables, 1000, exact).map((b) => b.length),
			).toEqual([2, 1]);
			expect(
				JSON.stringify({ tables: tables.slice(0, 2) }).length,
			).toBe(exact);
			expect(
				splitIngestTablesIntoBatches(tables, 1000, exact - 1).map(
					(b) => b.length,
				),
			).toEqual([1, 1, 1]);
		});

		it("puts an oversized table alone in its own batch", () => {
			const batches = splitIngestTablesIntoBatches(
				[table("small", 1), table("huge", 50), table("tail", 1)],
				10,
				Number.MAX_SAFE_INTEGER,
			);
			expect(batches.map((b) => b.map((t) => t.table_name))).toEqual([
				["small"],
				["huge"],
				["tail"],
			]);
		});

		it("returns a single empty batch when there are no tables", () => {
			expect(splitIngestTablesIntoBatches([])).toEqual([[]]);
		});
	});

	describe("batched ingest sessions", () => {
		const introspection: SchemaIntrospection = {
			db: { kind: "postgres", name: "test-db" },
			tables: [
				{
					name: "users",
					schema: "public",
					type: "table",
					columns: [
						{ name: "id", type: "integer", isPrimaryKey: true },
						{ name: "email", type: "text", isPrimaryKey: false },
					],
				},
				{
					name: "orders",
					schema: "public",
					type: "table",
					columns: [{ name: "id", type: "integer", isPrimaryKey: true }],
				},
				{
					name: "deals",
					schema: "crm",
					type: "table",
					columns: [{ name: "id", type: "integer", isPrimaryKey: true }],
				},
			],
			introspectedAt: "2025-01-01T00:00:00Z",
		};

		const finalResponse = {
			success: true,
			message: "done",
			chunks: 7,
			chunks_with_annotations: 1,
			schema_id: "schema-1",
			schema_hash: "hash",
			drift_detected: true,
		};

		beforeEach(() => {
			(mockAdapter.introspect as any).mockResolvedValue(introspection);
			(mockQueryEngine.getDatabaseMetadata as any).mockReturnValue({
				name: "test-db",
				dialect: "postgres",
				tenantFieldName: "tenant_id",
				tenantFieldType: "String",
			});
			mockClient.post.mockImplementation(async (path: unknown) => {
				const p = String(path);
				if (p === "/ingest/sessions") return { session_id: "sess-1" };
				if (p.endsWith("/commit"))
					return { status: "embedding", total_batches: 2, drift_detected: true };
				if (p.endsWith("/embed")) {
					const index = Number(p.split("/").at(-2));
					return { index, chunks: index + 3, chunks_with_annotations: 0 };
				}
				if (p.endsWith("/finalize")) return finalResponse;
				throw new Error(`unexpected ${p}`);
			});
			mockClient.put.mockImplementation(async (_path: unknown, body: any) => ({
				index: 0,
				tables: body.tables.length,
				columns: 0,
			}));
		});

		it("stages: creates a session, uploads batches in order, and commits", async () => {
			const events: SchemaSyncProgressEvent[] = [];
			const signal = new AbortController().signal;

			const result = await stageSchemaIngest(
				mockClient,
				mockQueryEngine,
				"test-db",
				{
					tenantId: "tenant-1",
					userId: "user-1",
					scopes: ["admin"],
					forceReindex: true,
					tables: ["public.*", "crm.deals"],
					maxBatchDocs: 4,
					onProgress: (e) => events.push(e),
				},
				signal,
			);

			expect(mockAdapter.introspect).toHaveBeenCalledWith({
				tables: ["public.*", "crm.deals"],
			});
			expect(result).toEqual({
				sessionId: "sess-1",
				status: "embedding",
				totalBatches: 2,
				tableCount: 3,
				columnCount: 4,
				driftDetected: true,
			});

			const postPaths = mockClient.post.mock.calls.map((c) => c[0]);
			expect(postPaths).toEqual([
				"/ingest/sessions",
				"/ingest/sessions/sess-1/commit",
			]);
			const [, createBody, tenantId, userId, scopes, sig] =
				mockClient.post.mock.calls[0];
			expect(createBody).toEqual({
				database: "test-db",
				dialect: "postgres",
				tenant_settings: {
					tenantFieldName: "tenant_id",
					tenantFieldType: "String",
					enforceTenantIsolation: true,
				},
				force_reindex: true,
				total_batches: 2,
			});
			expect([tenantId, userId, scopes, sig]).toEqual([
				"tenant-1",
				"user-1",
				["admin"],
				signal,
			]);

			// users (3 docs) alone; orders (2) + deals (2) together under limit 4
			expect(mockClient.put.mock.calls.map((c) => c[0])).toEqual([
				"/ingest/sessions/sess-1/batches/0",
				"/ingest/sessions/sess-1/batches/1",
			]);
			const batchBodies = mockClient.put.mock.calls.map((c) => c[1] as any);
			expect(
				batchBodies.map((b) => b.tables.map((t: any) => t.table_name)),
			).toEqual([["users"], ["orders", "deals"]]);
			// Same table shape as syncSchema
			expect(batchBodies[0].tables[0]).toEqual({
				schema_name: "public",
				table_name: "users",
				description: "Table users",
				columns: [
					{ name: "id", data_type: "integer", is_primary_key: true, description: "" },
					{ name: "email", data_type: "text", is_primary_key: false, description: "" },
				],
			});
			expect(mockClient.put.mock.calls[0].slice(2)).toEqual([
				"tenant-1",
				"user-1",
				["admin"],
				signal,
				"sess-1",
			]);

			expect(events).toEqual([
				{ phase: "introspecting" },
				{ phase: "introspected", tableCount: 3, columnCount: 4 },
				{ phase: "uploading", done: 1, total: 2 },
				{ phase: "uploading", done: 2, total: 2 },
				{ phase: "committing" },
			]);
		});

		it("omits force_reindex and tenant_settings when not configured", async () => {
			(mockQueryEngine.getDatabaseMetadata as any).mockReturnValue(undefined);
			await stageSchemaIngest(mockClient, mockQueryEngine, "test-db", {});
			const createBody = mockClient.post.mock.calls[0][1];
			expect(createBody).toEqual({
				database: "test-db",
				dialect: "postgres",
				total_batches: 1,
			});
			expect(mockClient.post.mock.calls[0][2]).toBe("default-tenant");
		});

		it("returns the skipped result when commit reports no changes", async () => {
			mockClient.post.mockImplementation(async (path: unknown) => {
				if (path === "/ingest/sessions") return { session_id: "sess-1" };
				return {
					status: "skipped",
					success: true,
					message: "Schema unchanged",
					chunks: 0,
					chunks_with_annotations: 0,
					schema_id: "schema-1",
					skipped: true,
					drift_detected: false,
				};
			});

			const staged = await stageSchemaIngest(
				mockClient,
				mockQueryEngine,
				"test-db",
				{ tenantId: "tenant-1" },
			);
			expect(staged.status).toBe("skipped");
			expect(staged.skippedResult).toEqual({
				success: true,
				message: "Schema unchanged",
				chunks: 0,
				chunks_with_annotations: 0,
				schema_id: "schema-1",
				skipped: true,
				drift_detected: false,
			});

			mockClient.post.mockClear();
			const synced = await syncSchemaInBatches(
				mockClient,
				mockQueryEngine,
				"test-db",
				{ tenantId: "tenant-1" },
			);
			expect(synced.skipped).toBe(true);
			expect(synced).not.toHaveProperty("status");
			const paths = mockClient.post.mock.calls.map((c) => String(c[0]));
			expect(paths.some((p) => p.endsWith("/embed"))).toBe(false);
			expect(paths.some((p) => p.endsWith("/finalize"))).toBe(false);
		});

		it("throws SchemaTooLargeError before any API call", async () => {
			const error = await stageSchemaIngest(
				mockClient,
				mockQueryEngine,
				"test-db",
				{ tenantId: "tenant-1", maxColumns: 3 },
			).catch((e) => e);

			expect(error).toBeInstanceOf(SchemaTooLargeError);
			expect(error).toMatchObject({
				tableCount: 3,
				columnCount: 4,
				maxColumns: 3,
			});
			expect(mockClient.post).not.toHaveBeenCalled();
			expect(mockClient.put).not.toHaveBeenCalled();
		});

		it("embedSchemaIngestBatch and finalizeSchemaIngest call session endpoints", async () => {
			await expect(
				embedSchemaIngestBatch(mockClient, "sess/1", 1, {
					tenantId: "tenant-1",
					userId: "user-1",
				}),
			).resolves.toEqual({ index: 1, chunks: 4, chunks_with_annotations: 0 });
			await expect(
				finalizeSchemaIngest(mockClient, "sess-1", {}),
			).resolves.toEqual(finalResponse);

			expect(mockClient.post.mock.calls[0]).toEqual([
				"/ingest/sessions/sess%2F1/batches/1/embed",
				{},
				"tenant-1",
				"user-1",
				undefined,
				undefined,
				"sess/1",
			]);
			expect(mockClient.post.mock.calls[1].slice(0, 3)).toEqual([
				"/ingest/sessions/sess-1/finalize",
				{},
				"default-tenant",
			]);
		});

		it("syncSchemaInBatches stages, embeds every batch, then finalizes", async () => {
			const events: SchemaSyncProgressEvent[] = [];

			const result = await syncSchemaInBatches(
				mockClient,
				mockQueryEngine,
				"test-db",
				{
					tenantId: "tenant-1",
					maxBatchDocs: 4,
					onProgress: (e) => events.push(e),
				},
			);

			expect(result).toEqual(finalResponse);
			expect(mockClient.post.mock.calls.map((c) => c[0])).toEqual([
				"/ingest/sessions",
				"/ingest/sessions/sess-1/commit",
				"/ingest/sessions/sess-1/batches/0/embed",
				"/ingest/sessions/sess-1/batches/1/embed",
				"/ingest/sessions/sess-1/finalize",
			]);
			expect(events.map((e) => e.phase)).toEqual([
				"introspecting",
				"introspected",
				"uploading",
				"uploading",
				"committing",
				"embedding",
				"embedding",
				"finalizing",
			]);
			expect(events.filter((e) => e.phase === "embedding")).toEqual([
				{ phase: "embedding", done: 1, total: 2, chunks: 3 },
				{ phase: "embedding", done: 2, total: 2, chunks: 7 },
			]);
		});
	});
});
