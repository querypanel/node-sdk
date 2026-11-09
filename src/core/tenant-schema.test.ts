import { describe, expect, it } from "vitest";
import {
	parseTenantSchemaTemplate,
	resolveTenantSchema,
	rewriteTenantSchema,
	TenantSchemaError,
} from "./tenant-schema";

const template = "group_{tenant}_indexed";
const rewrite = (sql: string, tenantId = "1001", sharedSchemas?: string[]) =>
	rewriteTenantSchema(sql, { template, tenantId, sharedSchemas });

describe("parseTenantSchemaTemplate", () => {
	it("accepts one placeholder", () => {
		expect(parseTenantSchemaTemplate("group_{tenant}_indexed")).toEqual({
			prefix: "group_",
			suffix: "_indexed",
		});
	});

	it.each(["group_indexed", "{tenant}_{tenant}", "{tenant}", "bad-name_{tenant}", "x.{tenant}"])(
		"rejects %s",
		(value) => {
			expect(() => parseTenantSchemaTemplate(value)).toThrow(TenantSchemaError);
		},
	);
});

describe("resolveTenantSchema", () => {
	it("lowercases the tenant schema", () => {
		expect(resolveTenantSchema(template, "AbC_1")).toBe("group_abc_1_indexed");
	});

	it.each(["", "a;drop", "a.b", "x".repeat(65), "1001 or 1=1"])("rejects tenant id %j", (id) => {
		expect(() => resolveTenantSchema(template, id)).toThrow(TenantSchemaError);
	});
});

describe("rewriteTenantSchema", () => {
	it("rewrites the template schema to the tenant schema", () => {
		expect(rewrite("SELECT count(*) FROM group_4048_indexed.users u")).toBe(
			"SELECT count(*) FROM group_1001_indexed.users u",
		);
	});

	it("rewrites every reference, quoted ones included, and leaves column qualifiers alone", () => {
		const sql = `WITH a AS (SELECT u.id FROM "group_4048_indexed"."users" u)
SELECT a.id, c.body FROM a JOIN group_4048_indexed.comments c ON c.user_id = a.id`;
		expect(rewrite(sql)).toBe(`WITH a AS (SELECT u.id FROM "group_1001_indexed"."users" u)
SELECT a.id, c.body FROM a JOIN group_1001_indexed.comments c ON c.user_id = a.id`);
	});

	it("forces another tenant's schema to the caller's schema", () => {
		expect(rewrite("select * from group_2002_indexed.users", "1001")).toBe(
			"select * from group_1001_indexed.users",
		);
	});

	it("leaves SQL already on the tenant schema unchanged", () => {
		const sql = "select id from group_1001_indexed.users";
		expect(rewrite(sql)).toBe(sql);
	});

	it("refuses another tenant's schema left in a string or comment", () => {
		expect(() => rewrite("select 'group_2002_indexed' from group_4048_indexed.users")).toThrow(
			/another tenant/,
		);
		expect(() => rewrite("select 1 from group_4048_indexed.users -- group_2002_indexed")).toThrow(
			/another tenant/,
		);
	});

	it("refuses relations outside the tenant schema", () => {
		expect(() => rewrite("select * from public.users")).toThrow(/outside the tenant schema/);
		expect(() => rewrite("select * from pg_catalog.pg_user")).toThrow(/outside/);
		expect(() =>
			rewrite("select * from group_4048_indexed.users u join base.users b on b.id = u.id"),
		).toThrow(/outside/);
		expect(() => rewrite("select * from group_4048_indexed.users, base.users")).toThrow(/outside/);
		expect(() =>
			rewrite("select * from group_4048_indexed.users where id in (select id from base.users)"),
		).toThrow(/outside/);
	});

	it("refuses unqualified tables that are not CTEs", () => {
		expect(() => rewrite("select * from users")).toThrow(/schema-qualified/);
	});

	it("allows shared schemas", () => {
		const sql = "select * from group_4048_indexed.users u join lookups.states s on s.code = u.state";
		expect(rewrite(sql, "1001", ["lookups"])).toBe(
			"select * from group_1001_indexed.users u join lookups.states s on s.code = u.state",
		);
	});

	it("does not treat FROM inside functions as a table", () => {
		const sql = `select extract(year from u.created_at) as y, trim(both ' ' from u.name),
  u.a is distinct from u.b
from group_4048_indexed.users u
where substring(u.zip from 1 for 3) = '100'`;
		expect(() => rewrite(sql)).not.toThrow();
	});

	it("allows subqueries, CTE column lists and table functions", () => {
		const sql = `with recursive t (n) as (select 1 union all select n + 1 from t where n < 3)
select * from (select id from group_4048_indexed.users) x, t
cross join generate_series(1, 3) g`;
		expect(() => rewrite(sql)).not.toThrow();
	});

	it("rejects an unsafe tenant id", () => {
		expect(() => rewrite("select 1 from group_4048_indexed.users", "1; drop")).toThrow(
			TenantSchemaError,
		);
	});
});
