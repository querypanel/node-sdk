/**
 * Integration tests for ClickHouse adapter
 * Requires: docker-compose -f docker-compose.test.yml up -d
 */

import { type ClickHouseClient, createClient } from "@clickhouse/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { ClickHouseAdapter } from "../../src/adapters/clickhouse";

describe("ClickHouseAdapter Integration", () => {
	let chClient: ClickHouseClient;
	let adapter: ClickHouseAdapter;

	beforeAll(async () => {
		// Connect to test database
		chClient = createClient({
			url: "http://localhost:8124",
			username: "test_user",
			password: "test_password",
			database: "test_db",
		});

		// Wait for ClickHouse to be ready
		await chClient.ping();

		// Create adapter
		adapter = new ClickHouseAdapter(
			async (params) => {
				return await chClient.query(params);
			},
			{
				database: "test_db",
			},
		);
	});

	afterAll(async () => {
		await chClient.close();
	});

	describe("introspect", () => {
		it("should introspect all tables", async () => {
			const schema = await adapter.introspect();

			expect(schema.db.kind).toBe("clickhouse");
			expect(schema.db.name).toBe("test_db");
			expect(schema.tables.length).toBeGreaterThanOrEqual(2);

			const eventsTable = schema.tables.find((t) => t.name === "events");
			expect(eventsTable).toBeDefined();
			expect(eventsTable?.schema).toBe("test_db");
			expect(eventsTable?.type).toBe("table");
			expect(eventsTable?.comment).toBe("Event tracking table");

			const metricsTable = schema.tables.find((t) => t.name === "metrics");
			expect(metricsTable).toBeDefined();
			expect(metricsTable?.comment).toBe("Metrics aggregation table");
		});

		it("should introspect columns with types", async () => {
			const schema = await adapter.introspect();
			const eventsTable = schema.tables.find((t) => t.name === "events");

			expect(eventsTable?.columns.length).toBeGreaterThanOrEqual(5);

			const idColumn = eventsTable?.columns.find((c) => c.name === "id");
			expect(idColumn).toMatchObject({
				name: "id",
				type: "UInt64",
				rawType: "UInt64",
				isPrimaryKey: true, // Part of ORDER BY (tenant_id, timestamp, id)
			});

			const tenantColumn = eventsTable?.columns.find(
				(c) => c.name === "tenant_id",
			);
			expect(tenantColumn).toMatchObject({
				name: "tenant_id",
				type: "String",
				isPrimaryKey: true, // In ORDER BY clause
			});
		});

		it("should detect primary key columns", async () => {
			const schema = await adapter.introspect();
			const metricsTable = schema.tables.find((t) => t.name === "metrics");

			const primaryKeyColumns = metricsTable?.columns.filter(
				(c) => c.isPrimaryKey,
			);

			expect(primaryKeyColumns?.length).toBeGreaterThan(0);
			expect(primaryKeyColumns?.map((c) => c.name)).toEqual(
				expect.arrayContaining(["tenant_id", "metric_name", "timestamp"]),
			);
		});

		it("should filter tables when specified", async () => {
			const schema = await adapter.introspect({ tables: ["events"] });

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0].name).toBe("events");
		});
	});

	describe("execute", () => {
		it("should execute simple SELECT query", async () => {
			const result = await adapter.execute("SELECT * FROM events ORDER BY id");

			expect(result.rows.length).toBeGreaterThanOrEqual(3);
			expect(result.fields).toContain("id");
			expect(result.fields).toContain("event_name");
			expect(result.fields).toContain("tenant_id");

			const firstEvent = result.rows[0];
			expect(firstEvent).toHaveProperty("event_name");
			expect(firstEvent).toHaveProperty("user_id");
		});

		it("should execute query with named parameters", async () => {
			const result = await adapter.execute(
				"SELECT * FROM events WHERE tenant_id = {tenant:String}",
				{ tenant: "tenant-1" },
			);

			expect(result.rows.length).toBeGreaterThanOrEqual(2);
			expect(
				result.rows.every((row: any) => row.tenant_id === "tenant-1"),
			).toBe(true);
		});

		it("should execute aggregate queries", async () => {
			const result = await adapter.execute(`
        SELECT
          tenant_id,
          count() as event_count,
          countDistinct(user_id) as unique_users
        FROM events
        GROUP BY tenant_id
        ORDER BY tenant_id
      `);

			expect(result.rows.length).toBeGreaterThanOrEqual(2);
			expect(result.fields).toEqual(
				expect.arrayContaining(["tenant_id", "event_count", "unique_users"]),
			);

			const tenant1 = result.rows.find(
				(row: any) => row.tenant_id === "tenant-1",
			);
			expect(tenant1).toBeDefined();
			expect(Number(tenant1?.event_count)).toBeGreaterThanOrEqual(2);
		});

		it("should handle array types", async () => {
			const result = await adapter.execute(
				`
        SELECT metric_name, tags
        FROM metrics
        WHERE tenant_id = {tenant:String}
        LIMIT 1
      `,
				{ tenant: "tenant-1" },
			);

			expect(result.rows).toHaveLength(1);
			expect(Array.isArray(result.rows[0].tags)).toBe(true);
		});

		it("should execute window functions", async () => {
			const result = await adapter.execute(`
        SELECT
          metric_name,
          value,
          row_number() OVER (PARTITION BY metric_name ORDER BY timestamp DESC) as rn
        FROM metrics
      `);

			expect(result.rows.length).toBeGreaterThanOrEqual(3);
			expect(result.fields).toContain("rn");
		});
	});

	describe("validate", () => {
		it("should validate correct SQL", async () => {
			await expect(
				adapter.validate("SELECT * FROM events WHERE tenant_id = {t:String}", {
					t: "test",
				}),
			).resolves.not.toThrow();
		});

		it("should reject invalid SQL", async () => {
			await expect(
				adapter.validate("SELECT * FROM nonexistent_table"),
			).rejects.toThrow();
		});

		it("should reject SQL with syntax errors", async () => {
			await expect(adapter.validate("SELECT * FORM events")).rejects.toThrow();
		});
	});

	describe("getDialect", () => {
		it("should return clickhouse dialect", () => {
			expect(adapter.getDialect()).toBe("clickhouse");
		});
	});

	describe("allowed tables", () => {
		it("should restrict queries to allowed tables", async () => {
			const restrictedAdapter = new ClickHouseAdapter(
				async (params) => {
					return await chClient.query(params);
				},
				{
					database: "test_db",
					allowedTables: ["events"],
				},
			);

			// Should allow queries on events table
			await expect(
				restrictedAdapter.execute("SELECT * FROM events LIMIT 1"),
			).resolves.toBeDefined();

			// Should reject queries on metrics table
			await expect(
				restrictedAdapter.execute("SELECT * FROM metrics LIMIT 1"),
			).rejects.toThrow("not in the allowed tables list");
		});

		it("should filter introspection to allowed tables", async () => {
			const restrictedAdapter = new ClickHouseAdapter(
				async (params) => {
					return await chClient.query(params);
				},
				{
					database: "test_db",
					allowedTables: ["events"],
				},
			);

			const schema = await restrictedAdapter.introspect();

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0].name).toBe("events");
		});
	});

	describe("type handling", () => {
		it("should handle various ClickHouse types", async () => {
			const result = await adapter.execute(`
        SELECT
          toUInt32(123) as uint_val,
          'test' as string_val,
          toFloat64(45.67) as float_val,
          today() as date_val,
          now() as datetime_val,
          [1, 2, 3] as array_val
      `);

			expect(result.rows).toHaveLength(1);
			const row = result.rows[0];

			expect(typeof row.uint_val).toBe("number"); // UInt32 fits in JS number
			expect(typeof row.string_val).toBe("string");
			expect(typeof row.float_val).toBe("number");
			expect(Array.isArray(row.array_val)).toBe(true);
		});
	});
});
