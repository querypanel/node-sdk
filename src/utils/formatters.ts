interface ValueFormat {
  style?: "number" | "currency" | "percent" | "date" | "time" | "datetime";
  currency?: string;
  minimumFractionDigits?: number;
  maximumFractionDigits?: number;
  dateStyle?: "short" | "medium" | "long";
}

/**
 * Format a value for display; if it can be parsed as a date (string or Date), use the browser's local date/time format.
 */
export function formatTimestampForDisplay(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return String(value);
  try {
    const date = value instanceof Date ? value : new Date(String(value));
    if (Number.isNaN(date.getTime())) return String(value);
    return new Intl.DateTimeFormat(undefined, {
      dateStyle: "short",
      timeStyle: "short",
    }).format(date);
  } catch {
    return String(value);
  }
}

export function formatValue(value: unknown, format?: ValueFormat): string {
  if (value === null || value === undefined) return "—";

  if (!format) return String(value);

  switch (format.style) {
    case "currency":
      return new Intl.NumberFormat(undefined, {
        style: "currency",
        currency: format.currency || "USD",
        minimumFractionDigits: format.minimumFractionDigits,
        maximumFractionDigits: format.maximumFractionDigits,
      }).format(Number(value));

    case "percent":
      return new Intl.NumberFormat(undefined, {
        style: "percent",
        minimumFractionDigits: format.minimumFractionDigits,
        maximumFractionDigits: format.maximumFractionDigits,
      }).format(Number(value) / 100);

    case "date":
      return new Intl.DateTimeFormat(undefined, {
        dateStyle: format.dateStyle || "medium",
      }).format(new Date(String(value)));

    case "datetime":
      return new Intl.DateTimeFormat(undefined, {
        dateStyle: format.dateStyle || "medium",
        timeStyle: "short",
      }).format(new Date(String(value)));

    case "time":
      return new Intl.DateTimeFormat(undefined, {
        timeStyle: format.dateStyle || "medium",
      }).format(new Date(String(value)));

    case "number":
    default:
      return new Intl.NumberFormat(undefined, {
        minimumFractionDigits: format.minimumFractionDigits,
        maximumFractionDigits: format.maximumFractionDigits,
      }).format(Number(value));
  }
}
