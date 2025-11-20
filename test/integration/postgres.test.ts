/**
 * Integration tests for PostgreSQL adapter
 * Requires: docker-compose -f docker-compose.test.yml up -d
 */

import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { PostgresAdapter } from "../../src/adapters/postgres";

describe("PostgresAdapter Integration", () => {
	let pgClient: Client;
	let adapter: PostgresAdapter;

	beforeAll(async () => {
		// Connect to test database
		pgClient = new Client({
			host: "localhost",
			port: 5433,
			database: "test_db",
			user: "test_user",
			password: "test_password",
		});

		await pgClient.connect();

		// Create adapter
		adapter = new PostgresAdapter(
			async (sql: string, params?: unknown[]) => {
				const result = await pgClient.query(sql, params);
				return {
					rows: result.rows,
					fields: result.fields.map((f) => ({ name: f.name })),
				};
			},
			{
				database: "test_db",
				defaultSchema: "public",
			},
		);
	});

	afterAll(async () => {
		await pgClient.end();
	});

	describe("introspect", () => {
		it("should introspect all tables", async () => {
			const schema = await adapter.introspect();

			expect(schema.db.kind).toBe("postgres");
			expect(schema.db.name).toBe("test_db");
			expect(schema.tables).toHaveLength(2);

			const usersTable = schema.tables.find((t) => t.name === "users");
			expect(usersTable).toBeDefined();
			expect(usersTable?.schema).toBe("public");
			expect(usersTable?.type).toBe("table");
			expect(usersTable?.comment).toBe("User accounts table");

			const ordersTable = schema.tables.find((t) => t.name === "orders");
			expect(ordersTable).toBeDefined();
		});

		it("should introspect columns with metadata", async () => {
			const schema = await adapter.introspect();
			const usersTable = schema.tables.find((t) => t.name === "users");

			expect(usersTable?.columns).toHaveLength(6);

			const idColumn = usersTable?.columns.find((c) => c.name === "id");
			expect(idColumn).toMatchObject({
				name: "id",
				type: "integer",
				isPrimaryKey: true,
				comment: "Unique user identifier",
			});

			const emailColumn = usersTable?.columns.find((c) => c.name === "email");
			expect(emailColumn).toMatchObject({
				name: "email",
				type: "character varying",
				isPrimaryKey: false,
				comment: "User email address",
			});

			const tenantColumn = usersTable?.columns.find(
				(c) => c.name === "tenant_id",
			);
			expect(tenantColumn).toMatchObject({
				name: "tenant_id",
				isPrimaryKey: false,
				comment: "Tenant identifier for isolation",
			});
		});

		it("should filter tables when specified", async () => {
			const schema = await adapter.introspect({ tables: ["users"] });

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0].name).toBe("users");
		});

		it("should handle schema-qualified table names", async () => {
			const schema = await adapter.introspect({ tables: ["public.users"] });

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0].name).toBe("users");
			expect(schema.tables[0].schema).toBe("public");
		});
	});

	describe("execute", () => {
		it("should execute simple SELECT query", async () => {
			const result = await adapter.execute("SELECT * FROM users ORDER BY id");

			expect(result.rows).toHaveLength(3);
			expect(result.fields).toContain("id");
			expect(result.fields).toContain("email");
			expect(result.fields).toContain("name");

			expect(result.rows[0]).toMatchObject({
				email: "alice@example.com",
				name: "Alice Smith",
				tenant_id: "tenant-1",
			});
		});

		it("should execute query with named parameters", async () => {
			const result = await adapter.execute(
				"SELECT * FROM users WHERE tenant_id = $1",
				{ "1": "tenant-1" },
			);

			expect(result.rows).toHaveLength(2);
			expect(result.rows.every((row) => row.tenant_id === "tenant-1")).toBe(
				true,
			);
		});

		it("should execute JOIN queries", async () => {
			const result = await adapter.execute(
				`
        SELECT u.name, o.total_amount, o.status
        FROM users u
        JOIN orders o ON u.id = o.user_id
        WHERE u.tenant_id = $1
        ORDER BY o.created_at
      `,
				{ "1": "tenant-1" },
			);

			expect(result.rows).toHaveLength(3);
			expect(result.fields).toEqual(
				expect.arrayContaining(["name", "total_amount", "status"]),
			);
		});

		it("should execute aggregate queries", async () => {
			const result = await adapter.execute(`
        SELECT tenant_id, COUNT(*) as user_count, SUM(CASE WHEN active THEN 1 ELSE 0 END) as active_count
        FROM users
        GROUP BY tenant_id
        ORDER BY tenant_id
      `);

			expect(result.rows).toHaveLength(2);
			expect(result.rows[0]).toMatchObject({
				tenant_id: "tenant-1",
				user_count: "2",
				active_count: "2",
			});
		});
	});

	describe("validate", () => {
		it("should validate correct SQL", async () => {
			await expect(
				adapter.validate("SELECT * FROM users WHERE id = $1", { "1": 1 }),
			).resolves.not.toThrow();
		});

		it("should reject invalid SQL", async () => {
			await expect(
				adapter.validate("SELECT * FROM nonexistent_table"),
			).rejects.toThrow();
		});

		it("should reject SQL with syntax errors", async () => {
			await expect(
				adapter.validate("SELECT * FORM users"), // typo: FORM instead of FROM
			).rejects.toThrow();
		});
	});

	describe("getDialect", () => {
		it("should return postgres dialect", () => {
			expect(adapter.getDialect()).toBe("postgres");
		});
	});

	describe("allowed tables", () => {
		it("should restrict queries to allowed tables", async () => {
			const restrictedAdapter = new PostgresAdapter(
				async (sql: string, params?: unknown[]) => {
					const result = await pgClient.query(sql, params);
					return {
						rows: result.rows,
						fields: result.fields.map((f) => ({ name: f.name })),
					};
				},
				{
					database: "test_db",
					allowedTables: ["users"],
				},
			);

			// Should allow queries on users table
			await expect(
				restrictedAdapter.execute("SELECT * FROM users"),
			).resolves.toBeDefined();

			// Should reject queries on orders table
			await expect(
				restrictedAdapter.execute("SELECT * FROM orders"),
			).rejects.toThrow("not in the allowed tables list");
		});

		it("should filter introspection to allowed tables", async () => {
			const restrictedAdapter = new PostgresAdapter(
				async (sql: string, params?: unknown[]) => {
					const result = await pgClient.query(sql, params);
					return {
						rows: result.rows,
						fields: result.fields.map((f) => ({ name: f.name })),
					};
				},
				{
					database: "test_db",
					allowedTables: ["users"],
				},
			);

			const schema = await restrictedAdapter.introspect();

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0].name).toBe("users");
		});
	});
});
