import { describe, expect, it, vi } from "vitest";
import { PostgresAdapter, type PostgresClientFn } from "./postgres";

describe("PostgresAdapter", () => {
	const createMockClientFn = (): PostgresClientFn =>
		vi.fn().mockResolvedValue({
			rows: [{ id: 1 }],
			fields: [{ name: "id" }],
		});

	describe("validateQueryTables", () => {
		it("should allow queries to tables in the allowed list", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["public.users", "public.orders"],
			});

			await expect(
				adapter.execute("SELECT * FROM users"),
			).resolves.toBeDefined();
			expect(clientFn).toHaveBeenCalled();
		});

		it("should reject queries to tables not in the allowed list", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["public.users"],
			});

			await expect(
				adapter.execute("SELECT * FROM orders"),
			).rejects.toThrow('Query references table "public.orders" which is not in the allowed tables list');
		});

		it("should handle schema-qualified table names", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["analytics.events"],
			});

			await expect(
				adapter.execute("SELECT * FROM analytics.events"),
			).resolves.toBeDefined();
		});

		it("should reject schema-qualified tables not in allowed list", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["public.users"],
			});

			await expect(
				adapter.execute("SELECT * FROM analytics.events"),
			).rejects.toThrow('Query references table "analytics.events" which is not in the allowed tables list');
		});

		it("should handle JOIN clauses", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["public.users", "public.orders"],
			});

			await expect(
				adapter.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id"),
			).resolves.toBeDefined();
		});

		it("should reject JOINs to disallowed tables", async () => {
			const clientFn = createMockClientFn();
			const adapter = new PostgresAdapter(clientFn, {
				allowedTables: ["public.users"],
			});

			await expect(
				adapter.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id"),
			).rejects.toThrow('Query references table "public.orders" which is not in the allowed tables list');
		});

		describe("SQL function FROM keyword handling", () => {
			it("should not confuse EXTRACT(... FROM ...) with table FROM clause", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.netflix_shows"],
				});

				const sql = `SELECT EXTRACT(YEAR FROM "date_added") AS year, COUNT(*) AS shows_added
FROM netflix_shows
WHERE "show_id" IS NOT NULL
AND "tenant_id" = $1
GROUP BY year
ORDER BY year ASC
LIMIT 100`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
				expect(clientFn).toHaveBeenCalled();
			});

			it("should not confuse EXTRACT with MONTH FROM with table FROM clause", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.sales"],
				});

				const sql = `SELECT EXTRACT(MONTH FROM created_at) AS month, SUM(amount)
FROM sales
GROUP BY month`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});

			it("should not confuse SUBSTRING(... FROM ...) with table FROM clause", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.products"],
				});

				const sql = `SELECT SUBSTRING(name FROM 1 FOR 10) AS short_name
FROM products`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});

			it("should not confuse TRIM(... FROM ...) with table FROM clause", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.customers"],
				});

				const sql = `SELECT TRIM(BOTH ' ' FROM name) AS trimmed_name
FROM customers`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});

			it("should not confuse POSITION(... FROM ...) with table FROM clause", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.logs"],
				});

				// Note: POSITION typically uses IN, but some dialects support FROM
				const sql = `SELECT POSITION('error' FROM message) AS error_pos
FROM logs`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});

			it("should handle multiple EXTRACT functions in same query", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.events"],
				});

				const sql = `SELECT
	EXTRACT(YEAR FROM created_at) AS year,
	EXTRACT(MONTH FROM created_at) AS month,
	EXTRACT(DAY FROM created_at) AS day,
	COUNT(*)
FROM events
GROUP BY year, month, day`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});

			it("should still reject disallowed tables when EXTRACT is present", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.allowed_table"],
				});

				const sql = `SELECT EXTRACT(YEAR FROM date_col) AS year
FROM disallowed_table`;

				await expect(adapter.execute(sql)).rejects.toThrow(
					'Query references table "public.disallowed_table" which is not in the allowed tables list',
				);
			});

			it("should handle EXTRACT with quoted column names", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.metrics"],
				});

				const sql = `SELECT EXTRACT(EPOCH FROM "timestamp") AS epoch_time
FROM metrics`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});
		});

		describe("edge cases", () => {
			it("should allow any table when allowedTables is not specified", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {});

				await expect(
					adapter.execute("SELECT * FROM any_table"),
				).resolves.toBeDefined();
			});

			it("should allow any table when allowedTables is empty", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: [],
				});

				await expect(
					adapter.execute("SELECT * FROM any_table"),
				).resolves.toBeDefined();
			});

			it("should handle quoted table names", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.MyTable"],
				});

				await expect(
					adapter.execute('SELECT * FROM "MyTable"'),
				).resolves.toBeDefined();
			});

			it("should handle LEFT JOIN, RIGHT JOIN, INNER JOIN, etc.", async () => {
				const clientFn = createMockClientFn();
				const adapter = new PostgresAdapter(clientFn, {
					allowedTables: ["public.a", "public.b", "public.c", "public.d"],
				});

				const sql = `SELECT * FROM a
LEFT JOIN b ON a.id = b.a_id
RIGHT JOIN c ON b.id = c.b_id
INNER JOIN d ON c.id = d.c_id`;

				await expect(adapter.execute(sql)).resolves.toBeDefined();
			});
		});
	});
});

