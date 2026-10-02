import { beforeEach, describe, expect, it, vi } from "vitest";
import type { QueryEngine } from "../core/query-engine";
import { QueryPanelSdkAPI } from "../index";
import { createMockQueryPanelApi } from "../test-utils";
import { modifyChart } from "./modify";
import { resolveQuery } from "./queries";

const QUERY_ID = "11111111-1111-4111-8111-111111111111";
const STORED_SQL =
	"SELECT name FROM t WHERE customer_id = {customer_id:String}";
const SERVER_QUERY = {
	queryId: QUERY_ID,
	sql: STORED_SQL,
	params: [{ name: "customer_id", value: "tenant-1" }],
	dialect: "clickhouse",
};

function createEngine(): QueryEngine {
	return {
		getDefaultDatabase: vi.fn(() => "db"),
		getDatabaseMetadata: vi.fn(() => ({ name: "db", dialect: "clickhouse" })),
		mapGeneratedParams: vi.fn((params: Array<Record<string, unknown>>) => {
			const record: Record<string, unknown> = {};
			params.forEach((p) => {
				record[String(p.name)] = p.value;
			});
			return record;
		}),
		validateAndExecute: vi.fn(async () => ({ rows: [], fields: [] })),
	} as unknown as QueryEngine;
}

describe("query id contract (node-sdk)", () => {
	let client: ReturnType<typeof createMockQueryPanelApi>;
	let engine: QueryEngine;

	beforeEach(() => {
		client = createMockQueryPanelApi({
			getDefaultTenantId: vi.fn(() => "tenant-1"),
		});
		engine = createEngine();
	});

	describe("resolveQuery", () => {
		it("fetches the stored query for the tenant and maps params", async () => {
			client.get.mockResolvedValueOnce(SERVER_QUERY);

			const resolved = await resolveQuery(client, engine, QUERY_ID, {
				tenantId: "tenant-1",
			});

			expect(client.get).toHaveBeenCalledWith(
				`/v2/queries/${QUERY_ID}`,
				"tenant-1",
				undefined,
				undefined,
				undefined,
			);
			expect(resolved.sql).toBe(STORED_SQL);
			expect(resolved.params).toEqual({ customer_id: "tenant-1" });
		});

		it("propagates a not-found error", async () => {
			client.get.mockRejectedValueOnce(new Error("Query not found"));
			await expect(
				resolveQuery(client, engine, QUERY_ID, { tenantId: "tenant-1" }),
			).rejects.toThrow("Query not found");
		});
	});

	describe("modifyChart", () => {
		it("works with only queryId, runs the stored sql and ignores client sql/params", async () => {
			client.get.mockResolvedValueOnce(SERVER_QUERY);
			client.post.mockResolvedValueOnce({ chart: { mark: "bar" }, notes: null });

			const result = await modifyChart(
				client,
				engine,
				{
					queryId: QUERY_ID,
					sql: "SELECT version()",
					params: { customer_id: "victim-tenant" },
					question: "names",
					database: "db",
					vizModifications: { chartType: "bar" },
				},
				{ tenantId: "tenant-1" },
			);

			expect(engine.validateAndExecute).toHaveBeenCalledWith(
				STORED_SQL,
				{ customer_id: "tenant-1" },
				"db",
				"tenant-1",
			);
			expect(result.sql).toBe(STORED_SQL);
			expect(result.modified.sqlChanged).toBe(false);
			// The id is preserved when only the visualization changes.
			expect(result.queryId).toBe(QUERY_ID);
		});

		it("regenerates from the stored sql and returns the new queryId", async () => {
			client.get.mockResolvedValueOnce(SERVER_QUERY);
			client.post
				.mockResolvedValueOnce({
					success: true,
					sql: "SELECT name FROM t WHERE customer_id = {customer_id:String} LIMIT 5",
					params: [{ name: "customer_id", value: "tenant-1" }],
					dialect: "clickhouse",
					queryId: "22222222-2222-4222-8222-222222222222",
				})
				.mockResolvedValueOnce({ chart: { mark: "bar" }, notes: null });

			const result = await modifyChart(
				client,
				engine,
				{
					queryId: QUERY_ID,
					question: "names",
					database: "db",
					sqlModifications: { additionalInstructions: "only show top 5" },
				},
				{ tenantId: "tenant-1", pipeline: "v2" },
			);

			const [, body] = client.post.mock.calls[0] as [string, Record<string, unknown>];
			expect(body.previous_sql).toBe(STORED_SQL);
			expect(result.modified.sqlChanged).toBe(true);
			expect(result.queryId).toBe("22222222-2222-4222-8222-222222222222");
		});

		it("drops the queryId when customSql replaces the stored query", async () => {
			client.get.mockResolvedValueOnce(SERVER_QUERY);
			client.post.mockResolvedValueOnce({ chart: { mark: "bar" }, notes: null });

			const result = await modifyChart(
				client,
				engine,
				{
					queryId: QUERY_ID,
					question: "names",
					database: "db",
					sqlModifications: { customSql: "SELECT 1" },
				},
				{ tenantId: "tenant-1" },
			);

			expect(result.queryId).toBeUndefined();
			expect(result.sql).toBe("SELECT 1");
		});

		it("still accepts raw sql without a queryId (backward compatible)", async () => {
			client.post.mockResolvedValueOnce({ chart: { mark: "bar" }, notes: null });

			await modifyChart(
				client,
				engine,
				{
					sql: "SELECT 1",
					question: "one",
					database: "db",
					vizModifications: { chartType: "bar" },
				},
				{ tenantId: "tenant-1" },
			);

			expect(client.get).not.toHaveBeenCalled();
			expect(engine.validateAndExecute).toHaveBeenCalledWith(
				"SELECT 1",
				{},
				"db",
				"tenant-1",
			);
		});

		it("requires either queryId or sql", async () => {
			await expect(
				modifyChart(
					client,
					engine,
					{ question: "x", database: "db" },
					{ tenantId: "tenant-1" },
				),
			).rejects.toThrow("queryId or sql");
		});
	});

	describe("runSqlForDashboard", () => {
		function createSdk() {
			const handler = vi.fn(async () => ({ data: SERVER_QUERY }));
			const sdk = QueryPanelSdkAPI.withCallbacks("org-1", handler, {
				defaultTenantId: "tenant-1",
			});
			const validateAndExecute = vi.fn(async () => ({
				rows: [{ name: "a" }],
				fields: ["name"],
			}));
			(sdk as unknown as { queryEngine: unknown }).queryEngine = {
				validateAndExecute,
				mapGeneratedParams: (params: Array<Record<string, unknown>>) => ({
					[String(params[0]?.name)]: params[0]?.value,
				}),
			};
			return { sdk, handler, validateAndExecute };
		}

		it("executes the stored sql and params and ignores client sql/params", async () => {
			const { sdk, handler, validateAndExecute } = createSdk();

			const result = await sdk.runSqlForDashboard(
				{
					queryId: QUERY_ID,
					sql: "SELECT version()",
					params: { customer_id: "victim-tenant" },
					database: "db",
				},
				{ tenantId: "tenant-1" },
			);

			expect(handler).toHaveBeenCalledWith(
				expect.objectContaining({
					method: "GET",
					path: `/v2/queries/${QUERY_ID}`,
					tenantId: "tenant-1",
				}),
			);
			expect(validateAndExecute).toHaveBeenCalledWith(
				STORED_SQL,
				{ customer_id: "tenant-1" },
				"db",
				"tenant-1",
			);
			expect(result.rows).toEqual([{ name: "a" }]);
		});

		it("keeps the raw sql path when no queryId is sent", async () => {
			const { sdk, handler, validateAndExecute } = createSdk();

			await sdk.runSqlForDashboard(
				{ sql: "SELECT 1", params: { a: 1 }, database: "db" },
				{ tenantId: "tenant-1" },
			);

			expect(handler).not.toHaveBeenCalled();
			expect(validateAndExecute).toHaveBeenCalledWith(
				"SELECT 1",
				{ a: 1 },
				"db",
				"tenant-1",
			);
		});

		it("requires either queryId or sql", async () => {
			const { sdk } = createSdk();
			await expect(
				sdk.runSqlForDashboard({ database: "db" }, { tenantId: "tenant-1" }),
			).rejects.toThrow("queryId or sql");
		});
	});
});
