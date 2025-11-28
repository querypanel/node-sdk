const WRAPPER_REGEX =
  /^(Nullable|LowCardinality|SimpleAggregateFunction)\((.+)\)$/i;

export function isNullableType(type: string): boolean {
  return /Nullable\s*\(/i.test(type);
}

export function unwrapTypeModifiers(type: string): string {
  let current = type.trim();
  let match = WRAPPER_REGEX.exec(current);
  while (match) {
    const inner = match[2];
    if (!inner) {
      break;
    }
    current = inner.trim();
    match = WRAPPER_REGEX.exec(current);
  }
  return current;
}

export function extractPrecisionScale(type: string): {
  precision?: number;
  scale?: number;
} {
  const unwrapped = unwrapTypeModifiers(type);
  const decimalMatch = unwrapped.match(/Decimal(?:\d+)?\((\d+)\s*,\s*(\d+)\)/i);
  if (!decimalMatch) return {};
  const precision = decimalMatch[1];
  const scale = decimalMatch[2];
  if (!precision || !scale) return {};
  return {
    precision: Number.parseInt(precision, 10),
    scale: Number.parseInt(scale, 10),
  };
}

export function extractFixedStringLength(type: string): number | undefined {
  const unwrapped = unwrapTypeModifiers(type);
  const match = unwrapped.match(/^(?:FixedString|StringFixed)\((\d+)\)$/i);
  if (!match) return undefined;
  const length = match[1];
  if (!length) return undefined;
  return Number.parseInt(length, 10);
}

export function parseKeyExpression(expression?: string | null): string[] {
  if (!expression) return [];
  let value = expression.trim();
  if (!value) return [];
  if (/^tuple\s*\(/i.test(value) && value.endsWith(")")) {
    value = value.replace(/^tuple\s*\(/i, "").replace(/\)$/, "");
  }

  const columns: string[] = [];
  let depth = 0;
  let token = "";
  for (const ch of value) {
    if (ch === "(") {
      depth += 1;
      token += ch;
      continue;
    }
    if (ch === ")") {
      depth = Math.max(0, depth - 1);
      token += ch;
      continue;
    }
    if (ch === "," && depth === 0) {
      const col = token.trim();
      if (col) columns.push(stripWrapper(col));
      token = "";
      continue;
    }
    token += ch;
  }
  const last = token.trim();
  if (last) columns.push(stripWrapper(last));
  return columns.filter(Boolean);
}

function stripWrapper(value: string): string {
  const noQuotes = stripQuotes(value);
  const withoutTicks = noQuotes.replace(/`/g, "").trim();
  const parts = withoutTicks.split(".");
  return parts[parts.length - 1]?.trim() ?? "";
}

function stripQuotes(value: string): string {
  if (
    (value.startsWith('"') && value.endsWith('"')) ||
    (value.startsWith("'") && value.endsWith("'"))
  ) {
    return value.slice(1, -1);
  }
  return value;
}
