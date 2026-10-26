/**
 * Integration tests for the Redshift path (PostgresAdapter with dialect "redshift")
 * against Postgres with Redshift catalog views emulated (test/fixtures/redshift-init.sql).
 * Requires: docker-compose -f docker-compose.test.yml up -d redshift_test
 */

import { Client } from "pg";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
	PostgresAdapter,
	type PostgresClientFn,
} from "../../src/adapters/postgres";
import { QueryPanelSdkAPI } from "../../src/index";
import {
	TEST_BASE_URL,
	TEST_ORG_ID,
	TEST_PRIVATE_KEY,
} from "../../src/test-utils";

describe("Redshift Integration (emulated on Postgres)", () => {
	let pgClient: Client;
	let clientFn: PostgresClientFn;

	beforeAll(async () => {
		pgClient = new Client({
			host: "localhost",
			port: 5434,
			database: "dev",
			user: "test_user",
			password: "test_password",
		});
		await pgClient.connect();

		clientFn = async (sql: string, params?: unknown[]) => {
			const result = await pgClient.query(sql, params);
			return {
				rows: result.rows,
				fields: result.fields.map((f) => ({ name: f.name })),
			};
		};
	});

	afterAll(async () => {
		await pgClient.end();
	});

	describe("introspect", () => {
		it("reads tables and views from svv_tables", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
			});

			const schema = await adapter.introspect();

			expect(schema.db).toEqual({ kind: "redshift", name: "dev" });
			expect(schema.tables.map((t) => `${t.schema}.${t.name}`)).toEqual([
				"crm.users",
				"public.active_users",
				"public.events",
				"public.users",
			]);
			expect(
				schema.tables.find((t) => t.name === "active_users")?.type,
			).toBe("view");
			expect(
				schema.tables.find((t) => t.schema === "public" && t.name === "users")
					?.comment,
			).toBe("User accounts table");
		});

		it("keeps same-named tables in different schemas separate", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
			});

			const schema = await adapter.introspect();
			const publicUsers = schema.tables.find(
				(t) => t.schema === "public" && t.name === "users",
			);
			const crmUsers = schema.tables.find(
				(t) => t.schema === "crm" && t.name === "users",
			);

			expect(publicUsers?.columns.map((c) => c.name)).toEqual([
				"id",
				"email",
				"tenant_id",
				"created_at",
			]);
			expect(crmUsers?.columns.map((c) => c.name)).toEqual([
				"id",
				"full_name",
				"tenant_id",
			]);
			expect(
				publicUsers?.columns.find((c) => c.name === "email"),
			).toMatchObject({
				type: "character varying",
				isPrimaryKey: false,
				comment: "User email address",
			});
		});

		it("filters to allowed tables", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
				allowedTables: ["crm.users"],
			});

			const schema = await adapter.introspect();

			expect(schema.tables).toHaveLength(1);
			expect(schema.tables[0]).toMatchObject({ schema: "crm", name: "users" });
		});

		it("postgres dialect also keeps same-named tables separate", async () => {
			const adapter = new PostgresAdapter(clientFn, { database: "dev" });

			const schema = await adapter.introspect({ tables: ["public.users"] });
			const columns = schema.tables[0]?.columns ?? [];

			// Before the schema-aware pg_class join, crm.users comments/joins
			// leaked into public.users and duplicated columns.
			expect(columns.map((c) => c.name)).toEqual([
				"id",
				"email",
				"tenant_id",
				"created_at",
			]);
			expect(columns.find((c) => c.name === "id")?.isPrimaryKey).toBe(true);
		});
	});

	describe("schema wildcard + catalog listing", () => {
		it("introspects every table in crm.* only", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
				allowedTables: ["crm.*"],
			});

			const schema = await adapter.introspect();

			expect(schema.tables.map((t) => `${t.schema}.${t.name}`)).toEqual([
				"crm.users",
			]);
			expect(schema.tables[0]?.columns.map((c) => c.name)).toEqual([
				"id",
				"full_name",
				"tenant_id",
			]);
			await expect(
				adapter.execute("SELECT id FROM crm.users"),
			).resolves.toBeDefined();
			await expect(
				adapter.execute("SELECT id FROM public.users"),
			).rejects.toThrow("not in the allowed tables list");
		});

		it("mixes schema.* with specific tables", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
				allowedTables: ["crm.*", "public.events"],
			});

			const schema = await adapter.introspect();

			expect(schema.tables.map((t) => `${t.schema}.${t.name}`)).toEqual([
				"crm.users",
				"public.events",
			]);
		});

		it("lists schemas with table counts", async () => {
			for (const dialect of ["redshift", "postgres"] as const) {
				const adapter = new PostgresAdapter(clientFn, {
					database: "dev",
					dialect,
				});
				const schemas = await adapter.listSchemas();
				expect(schemas.filter((s) => s.schema !== "redshift_compat")).toEqual([
					{ schema: "crm", tableCount: 1 },
					{ schema: "public", tableCount: 3 },
				]);
			}
		});

		it("lists tables filtered by schema and case-insensitive search", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
			});

			await expect(adapter.listTables({ schema: "public" })).resolves.toEqual([
				{ schema: "public", name: "active_users", type: "view" },
				{ schema: "public", name: "events", type: "table" },
				{ schema: "public", name: "users", type: "table" },
			]);
			await expect(adapter.listTables({ search: "USER" })).resolves.toEqual([
				{ schema: "crm", name: "users", type: "table" },
				{ schema: "public", name: "active_users", type: "view" },
				{ schema: "public", name: "users", type: "table" },
			]);
			// LIKE metacharacters are matched literally
			await expect(adapter.listTables({ search: "_" })).resolves.toEqual([
				{ schema: "public", name: "active_users", type: "view" },
			]);
			await expect(adapter.listTables({ limit: 1 })).resolves.toHaveLength(1);
		});

		it("postgres dialect lists tables the same way", async () => {
			const adapter = new PostgresAdapter(clientFn, { database: "dev" });
			await expect(
				adapter.listTables({ schema: "crm", search: "us" }),
			).resolves.toEqual([{ schema: "crm", name: "users", type: "table" }]);
		});
	});

	describe("execute", () => {
		it("runs Redshift-style SQL with $N tenant parameters", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
			});

			const result = await adapter.execute(
				`SELECT name, COUNT(*) AS event_count
				FROM public.events
				WHERE tenant_id = $1
				  AND created_at >= DATEADD('day', -7, GETDATE())
				GROUP BY name
				ORDER BY name`,
				{ "1": "tenant-1" },
			);

			expect(result.fields).toEqual(["name", "event_count"]);
			expect(result.rows).toEqual([
				{ name: "login", event_count: "1" },
				{ name: "signup", event_count: "1" },
			]);
		});

		it("validates SQL with EXPLAIN", async () => {
			const adapter = new PostgresAdapter(clientFn, {
				database: "dev",
				dialect: "redshift",
			});

			await expect(
				adapter.validate("SELECT id FROM public.users WHERE tenant_id = $1", {
					"1": "tenant-1",
				}),
			).resolves.toBeUndefined();
			await expect(
				adapter.validate("SELECT missing_column FROM public.users"),
			).rejects.toThrow();
		});
	});

	describe("attachRedshift + syncSchema", () => {
		it("ingests the Redshift schema with dialect redshift", async () => {
			const mockFetch = vi.fn().mockResolvedValue({
				ok: true,
				text: async () =>
					JSON.stringify({ success: true, message: "ok", chunks: 4 }),
			});
			const sdk = new QueryPanelSdkAPI(
				TEST_BASE_URL,
				TEST_PRIVATE_KEY,
				TEST_ORG_ID,
				{ fetch: mockFetch as unknown as typeof fetch },
			);

			sdk.attachRedshift("warehouse", clientFn, {
				database: "dev",
				tenantFieldName: "tenant_id",
			});
			await sdk.syncSchema("warehouse", { tenantId: "tenant-1" });

			expect(mockFetch).toHaveBeenCalledWith(
				`${TEST_BASE_URL}/ingest`,
				expect.any(Object),
			);
			const body = JSON.parse(mockFetch.mock.calls[0][1].body as string);
			expect(body.dialect).toBe("redshift");
			expect(
				body.tables.map(
					(t: { schema_name: string; table_name: string }) =>
						`${t.schema_name}.${t.table_name}`,
				),
			).toEqual([
				"crm.users",
				"public.active_users",
				"public.events",
				"public.users",
			]);
			expect(body.tenant_settings).toMatchObject({
				tenantFieldName: "tenant_id",
			});
		});
	});
});
