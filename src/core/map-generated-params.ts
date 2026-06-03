import type { ParamRecord, ParamValue } from "./param-types";

/**
 * Maps v2-style param metadata (array order = $1, $2, $3 in SQL) to a ParamRecord.
 * Numeric keys preserve placeholder order for PostgreSQL; named keys support tenant lookups.
 */
export function mapGeneratedParams(
	params: Array<Record<string, unknown>>,
): ParamRecord {
	const record: ParamRecord = {};

	params.forEach((param, index) => {
		const value = param.value as ParamValue | undefined;
		if (value === undefined) {
			return;
		}

		record[String(index + 1)] = value;

		const nameCandidate =
			(typeof param.name === "string" && param.name.trim()) ||
			(typeof param.placeholder === "string" && param.placeholder.trim()) ||
			(typeof param.position === "number" && String(param.position)) ||
			String(index + 1);
		const key = nameCandidate.replace(/[{}:$]/g, "").trim();
		record[key] = value;
	});

	return record;
}

/**
 * Normalizes run-sql / dashboard request params into a ParamRecord.
 */
export function normalizeSqlParamsInput(
	params: unknown[] | Record<string, unknown> | null | undefined,
): ParamRecord {
	if (params == null) {
		return {};
	}

	if (Array.isArray(params)) {
		if (params.length === 0) {
			return {};
		}

		const first = params[0];
		if (first && typeof first === "object" && "value" in (first as object)) {
			return mapGeneratedParams(params as Array<Record<string, unknown>>);
		}

		const positional: ParamRecord = {};
		params.forEach((value, index) => {
			positional[String(index + 1)] = value as ParamValue;
		});
		return positional;
	}

	if (typeof params === "object") {
		return params as ParamRecord;
	}

	return {};
}
