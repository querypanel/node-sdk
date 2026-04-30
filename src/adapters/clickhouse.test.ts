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
});
