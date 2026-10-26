import { describe, expect, it, vi } from "vitest";
import { ClickHouseAdapter, type ClickHouseClientFn } from "./clickhouse";

describe("ClickHouseAdapter", () => {
	const createMockClientFn = (): ClickHouseClientFn =>
		vi.fn().mockResolvedValue([{ id: 1 }]);

	describe("validateQueryTables", () => {
		it("should allow queries to tables in the allowed list", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			await expect(
				adapter.execute("SELECT * FROM analytics.events LIMIT 10"),
			).resolves.toBeDefined();
		});

		it("should reject queries to tables not in the allowed list", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			await expect(
				adapter.execute("SELECT * FROM analytics.other_table LIMIT 10"),
			).rejects.toThrow(
				'Query references table "other_table" which is not in the allowed tables list',
			);
		});

		it("should allow ARRAY JOIN on a column from an allowed table", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			const sql = `SELECT
    rule_id,
    avg(amount_usd) AS avg_amount_usd
FROM analytics.events FINAL
ARRAY JOIN applied_tags AS rule_id
GROUP BY rule_id
LIMIT 100`;

			await expect(adapter.execute(sql)).resolves.toBeDefined();
			expect(clientFn).toHaveBeenCalled();
		});

		it("should allow LEFT ARRAY JOIN on a column from an allowed table", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			const sql = `SELECT
    rule_id
FROM analytics.events
LEFT ARRAY JOIN applied_tags AS rule_id
LIMIT 100`;

			await expect(adapter.execute(sql)).resolves.toBeDefined();
		});

		it("should not treat arrayJoin() function calls as table references", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			const sql = `SELECT
    arrayJoin(applied_tags) AS rule_id
FROM analytics.events
LIMIT 100`;

			await expect(adapter.execute(sql)).resolves.toBeDefined();
		});

		it("should still reject disallowed tables in real JOIN clauses", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				allowedTables: ["events"],
			});

			const sql = `SELECT *
FROM analytics.events ev
JOIN analytics.lookup_rules lr ON ev.rule_id = lr.id
LIMIT 100`;

			await expect(adapter.execute(sql)).rejects.toThrow(
				'Query references table "lookup_rules" which is not in the allowed tables list',
			);
		});
	});

	describe("wildcard allowedTables", () => {
		it("treats * and <database>.* as no filter", async () => {
			for (const allowedTables of [["*"], ["analytics.*"]]) {
				const clientFn = vi.fn().mockResolvedValue([]) as ClickHouseClientFn;
				const adapter = new ClickHouseAdapter(clientFn, {
					database: "analytics",
					allowedTables,
				});
				await expect(
					adapter.execute("SELECT * FROM anything"),
				).resolves.toBeDefined();
				await adapter.introspect();
				const introspectCall = vi.mocked(clientFn).mock.calls[1][0];
				expect(introspectCall.query).not.toContain("{tables:Array(String)}");
			}
		});

		it("ignores wildcards for other databases", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, {
				database: "analytics",
				allowedTables: ["other.*", "events"],
			});
			await expect(
				adapter.execute("SELECT * FROM events"),
			).resolves.toBeDefined();
			await expect(adapter.execute("SELECT * FROM users")).rejects.toThrow(
				"not in the allowed tables list",
			);
		});
	});

	describe("catalog listing", () => {
		it("listSchemas returns the bound database with its table count", async () => {
			const clientFn = vi
				.fn()
				.mockResolvedValue([{ table_count: "42" }]) as ClickHouseClientFn;
			const adapter = new ClickHouseAdapter(clientFn, { database: "analytics" });

			await expect(adapter.listSchemas()).resolves.toEqual([
				{ schema: "analytics", tableCount: 42 },
			]);
			const call = vi.mocked(clientFn).mock.calls[0][0];
			expect(call.query).toContain("FROM system.tables");
			expect(call.query_params).toEqual({ db: "analytics" });
		});

		it("listTables binds search and limit and maps engines", async () => {
			const clientFn = vi.fn().mockResolvedValue([
				{ name: "events", engine: "MergeTree" },
				{ name: "events_mv", engine: "MaterializedView" },
				{ name: "events_v", engine: "View" },
			]) as ClickHouseClientFn;
			const adapter = new ClickHouseAdapter(clientFn, { database: "analytics" });

			const tables = await adapter.listTables({ search: "Ev'ents", limit: 10 });

			expect(tables).toEqual([
				{ schema: "analytics", name: "events", type: "table" },
				{ schema: "analytics", name: "events_mv", type: "materialized_view" },
				{ schema: "analytics", name: "events_v", type: "view" },
			]);
			const call = vi.mocked(clientFn).mock.calls[0][0];
			expect(call.query).toContain(
				"positionCaseInsensitive(name, {search:String}) > 0",
			);
			expect(call.query).toContain("LIMIT {limit:UInt32}");
			expect(call.query).not.toContain("Ev'ents");
			expect(call.query_params).toEqual({
				db: "analytics",
				search: "Ev'ents",
				limit: 10,
			});
		});

		it("listTables returns nothing for another schema", async () => {
			const clientFn = createMockClientFn();
			const adapter = new ClickHouseAdapter(clientFn, { database: "analytics" });
			await expect(adapter.listTables({ schema: "other" })).resolves.toEqual([]);
			expect(clientFn).not.toHaveBeenCalled();
		});
	});
});
