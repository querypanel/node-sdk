type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

const BLACK_COLOR_VALUES = new Set([
  "#000",
  "#000000",
  "black",
  "rgb(0,0,0)",
  "rgb(0, 0, 0)",
  "rgba(0,0,0,1)",
  "rgba(0, 0, 0, 1)",
]);

function normalizeColorValue(value: JsonValue): JsonValue {
  if (typeof value !== "string") return value;
  return BLACK_COLOR_VALUES.has(value.trim().toLowerCase()) ? "default" : value;
}

function normalizeNode(value: JsonValue, darkMode: boolean): JsonValue {
  if (!darkMode) return value;
  if (Array.isArray(value)) {
    return value.map((item) => normalizeNode(item, darkMode));
  }
  if (!value || typeof value !== "object") return value;

  const next: { [key: string]: JsonValue } = {};
  for (const [key, raw] of Object.entries(value)) {
    if ((key === "textColor" || key === "backgroundColor" || key === "color") && typeof raw === "string") {
      next[key] = normalizeColorValue(raw);
      continue;
    }
    next[key] = normalizeNode(raw as JsonValue, darkMode);
  }
  return next;
}

export function normalizeBlockNoteContent<T>(value: T, darkMode: boolean): T {
  return normalizeNode(value as JsonValue, darkMode) as T;
}

