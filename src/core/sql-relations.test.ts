import { describe, expect, it } from "vitest";
import { findRelations } from "./sql-relations";

const names = (sql: string) => findRelations(sql).map((r) => r.parts.join("."));

describe("findRelations", () => {
	it("finds FROM, JOIN and comma-list relations with schemas", () => {
		expect(
			names(`select * from s.a x join s.b on true, "S"."C" left join d.s.e e on true`),
		).toEqual(["s.a", "s.b", "S.C", "d.s.e"]);
	});

	it("skips CTEs, subqueries and table functions", () => {
		const sql = `with recursive t (n) as (select 1 union all select n + 1 from t where n < 3),
  u as (select id from s.users)
select * from (select id from s.orders) x, t, u
cross join generate_series(1, 3) g`;
		expect(names(sql)).toEqual(["s.users", "s.orders"]);
	});

	it("ignores FROM inside functions and IS DISTINCT FROM", () => {
		const sql = `select extract(year from u.created_at), trim(both ' ' from u.name),
  u.a is distinct from u.b, substring(u.zip from 1 for 3)
from s.users u`;
		expect(names(sql)).toEqual(["s.users"]);
	});

	it("ignores names in strings and comments", () => {
		expect(names("select 'from x.y' from s.a -- join p.q\n/* from r.s */")).toEqual(["s.a"]);
	});

	it("finds relations in subqueries", () => {
		expect(names("select * from s.a where id in (select id from base.users)")).toEqual([
			"s.a",
			"base.users",
		]);
	});
});

describe("findRelations FROM lists", () => {
	it("finds a comma item after a JOIN ... ON condition", () => {
		expect(names("select * from s.a join s.b on s.a.id = s.b.id, base.users where x = 1")).toEqual([
			"s.a",
			"s.b",
			"base.users",
		]);
	});

	it("does not treat select-list or function commas as FROM items", () => {
		expect(names("select a, b, coalesce(c, d) from s.a where e in (1, 2) group by a, b")).toEqual([
			"s.a",
		]);
	});
});
