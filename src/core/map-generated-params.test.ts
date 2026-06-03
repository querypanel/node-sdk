import { describe, expect, it } from "vitest";
import { mapGeneratedParams } from "./map-generated-params";

describe("mapGeneratedParams", () => {
	it("assigns numeric keys in array order for PostgreSQL $1, $2", () => {
		const record = mapGeneratedParams([
			{ name: "start_date", value: "2024-01-01" },
			{ name: "end_date", value: "2024-12-31" },
		]);

		expect(record).toEqual({
			"1": "2024-01-01",
			start_date: "2024-01-01",
			"2": "2024-12-31",
			end_date: "2024-12-31",
		});
	});
});
