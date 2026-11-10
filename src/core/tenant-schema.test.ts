import { describe, expect, it } from "vitest";
import {
	parseTenantSchemaTemplate,
	resolveTenantSchema,
	rewriteTenantSchema,
	TENANT_SCHEMA_KEY,
	TenantSchemaError,
	tenantSchemaKey,
} from "./tenant-schema";

const template = "group_{tenant}_indexed";
const rewrite = (sql: string, tenantId = "1001") => rewriteTenantSchema(sql, template, tenantId);

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

	it("rejects an unsafe tenant id", () => {
		expect(() => rewrite("select 1 from group_4048_indexed.users", "1; drop")).toThrow(
			TenantSchemaError,
		);
	});
});

describe("tenantSchemaKey", () => {
	it("maps every template schema to one key and leaves others alone", () => {
		const key = tenantSchemaKey(template);
		expect(key("group_4048_indexed")).toBe(TENANT_SCHEMA_KEY);
		expect(key("GROUP_1001_INDEXED")).toBe(TENANT_SCHEMA_KEY);
		expect(key("Public")).toBe("public");
		expect(key("group__indexed")).toBe("group__indexed");
	});
});
