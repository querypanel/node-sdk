import { describe, expect, it, vi } from "vitest";
import { BigQueryAdapter, type BigQueryClientFn } from "./bigquery";

describe("BigQueryAdapter", () => {
	const createClientFn = (rows: Array<Record<string, unknown>> = []) =>
		vi.fn().mockResolvedValue({ rows, fields: [] }) as BigQueryClientFn;

	describe("wildcard allowedTables", () => {
		it("treats * and <dataset>.* as no filter", async () => {
			for (const allowedTables of [["*"], ["sales.*"]]) {
				const clientFn = createClientFn();
				const adapter = new BigQueryAdapter(clientFn, {
					projectId: "proj",
					dataset: "sales",
					allowedTables,
				});
				await expect(
					adapter.execute("SELECT * FROM sales.orders"),
				).resolves.toBeDefined();
				await adapter.introspect();
				const tablesCall = vi.mocked(clientFn).mock.calls[1][0];
				expect(tablesCall.query).not.toContain("table_name IN");
			}
		});

		it("ignores wildcards for other datasets", async () => {
			const clientFn = createClientFn();
			const adapter = new BigQueryAdapter(clientFn, {
				projectId: "proj",
				dataset: "sales",
				allowedTables: ["other.*", "orders"],
			});
			await expect(
				adapter.execute("SELECT * FROM sales.orders"),
			).resolves.toBeDefined();
			await expect(
				adapter.execute("SELECT * FROM sales.customers"),
			).rejects.toThrow("not in the allowed tables list");
		});
	});

	describe("catalog listing", () => {
		it("listSchemas returns the dataset with its table count", async () => {
			const clientFn = createClientFn([{ table_count: 7 }]);
			const adapter = new BigQueryAdapter(clientFn, {
				projectId: "proj",
				datasetProjectId: "data-proj",
				dataset: "sales",
			});

			await expect(adapter.listSchemas()).resolves.toEqual([
				{ schema: "sales", tableCount: 7 },
			]);
			const call = vi.mocked(clientFn).mock.calls[0][0];
			expect(call.query).toContain(
				"`data-proj.sales.INFORMATION_SCHEMA.TABLES`",
			);
		});

		it("listTables binds a lowercased, escaped LIKE search", async () => {
			const clientFn = createClientFn([
				{ table_schema: "sales", table_name: "Orders", table_type: "BASE TABLE" },
				{ table_schema: "sales", table_name: "orders_v", table_type: "VIEW" },
			]);
			const adapter = new BigQueryAdapter(clientFn, {
				projectId: "proj",
				dataset: "sales",
			});

			const tables = await adapter.listTables({ search: "Or_d", limit: 20 });

			expect(tables).toEqual([
				{ schema: "sales", name: "Orders", type: "table" },
				{ schema: "sales", name: "orders_v", type: "view" },
			]);
			const call = vi.mocked(clientFn).mock.calls[0][0];
			expect(call.query).toContain("WHERE LOWER(table_name) LIKE @search");
			expect(call.query).toContain("LIMIT 20");
			expect(call.params).toEqual({ search: "%or\\_d%" });
		});

		it("listTables returns nothing for another schema", async () => {
			const clientFn = createClientFn();
			const adapter = new BigQueryAdapter(clientFn, {
				projectId: "proj",
				dataset: "sales",
			});
			await expect(adapter.listTables({ schema: "other" })).resolves.toEqual([]);
			expect(clientFn).not.toHaveBeenCalled();
		});
	});
});
