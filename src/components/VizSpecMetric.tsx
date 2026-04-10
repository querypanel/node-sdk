import { LineChart, Line, ResponsiveContainer } from "recharts";
import { formatValue } from "../utils/formatters";
import type { ThemeColors } from "../types";

// VizSpec types (copied from querypanel-sdk for now)
type FieldType = "quantitative" | "temporal" | "ordinal" | "nominal" | "boolean";
type AggregateOp = "sum" | "avg" | "min" | "max" | "count" | "distinct";
type TimeUnit = "year" | "quarter" | "month" | "week" | "day" | "hour" | "minute";
type ComparisonMode = "delta" | "deltaPercent" | "ratio";

interface ValueFormat {
  style?: "number" | "currency" | "percent" | "date" | "time" | "datetime";
  currency?: string;
  minimumFractionDigits?: number;
  maximumFractionDigits?: number;
  dateStyle?: "short" | "medium" | "long";
}

interface FieldRef {
  field: string;
  label?: string;
  type?: FieldType;
  format?: ValueFormat;
}

interface AxisField extends FieldRef {
  aggregate?: AggregateOp;
  timeUnit?: TimeUnit;
}

interface MetricField extends FieldRef {
  aggregate?: AggregateOp;
}

interface MetricTrend {
  timeField: AxisField;
  valueField: MetricField;
}

interface MetricEncoding {
  valueField: MetricField;
  comparisonField?: MetricField;
  comparisonMode?: ComparisonMode;
  trend?: MetricTrend;
}

export interface MetricSpec {
  kind: "metric";
  title?: string;
  description?: string;
  encoding: MetricEncoding;
}

export interface VizSpecMetricProps {
  spec: MetricSpec;
  data: Array<Record<string, unknown>>;
  colors: ThemeColors;
}

interface ComparisonResult {
  direction: "up" | "down" | "neutral";
  formatted: string;
}

function calculateComparison(
  mainValue: unknown,
  comparisonValue: unknown,
  mode?: ComparisonMode
): ComparisonResult | null {
  if (
    mainValue === null ||
    mainValue === undefined ||
    comparisonValue === null ||
    comparisonValue === undefined
  ) {
    return null;
  }

  const main = Number(mainValue);
  const comparison = Number(comparisonValue);

  if (isNaN(main) || isNaN(comparison)) return null;

  let delta = 0;
  let formatted = "";

  switch (mode) {
    case "delta":
      delta = main - comparison;
      formatted = delta >= 0 ? `+${delta}` : String(delta);
      break;
    case "deltaPercent":
      if (comparison === 0) return null;
      delta = ((main - comparison) / comparison) * 100;
      formatted = delta >= 0 ? `+${delta.toFixed(1)}%` : `${delta.toFixed(1)}%`;
      break;
    case "ratio": {
      if (comparison === 0) return null;
      const ratio = main / comparison;
      formatted = `${ratio.toFixed(2)}x`;
      delta = ratio - 1;
      break;
    }
    default:
      delta = main - comparison;
      formatted = delta >= 0 ? `+${delta}` : String(delta);
  }

  const direction = delta > 0 ? "up" : delta < 0 ? "down" : "neutral";

  return { direction, formatted };
}

function TrendIcon({ direction }: { direction: "up" | "down" | "neutral" }) {
  const styles = {
    icon: {
      display: "inline-block",
      marginRight: "0.25rem",
      fontSize: "1rem",
    },
  };

  if (direction === "up") {
    return <span style={{ ...styles.icon, color: "#10B981" }}>↑</span>;
  }
  if (direction === "down") {
    return <span style={{ ...styles.icon, color: "#EF4444" }}>↓</span>;
  }
  return <span style={{ ...styles.icon, color: "#94A3B8" }}>→</span>;
}

export function VizSpecMetric({ spec, data, colors }: VizSpecMetricProps) {
  const { valueField, comparisonField, comparisonMode, trend } = spec.encoding;

  // Calculate main value (already aggregated in data)
  const mainValue = data[0]?.[valueField.field];
  const comparisonValue = comparisonField
    ? data[0]?.[comparisonField.field]
    : null;

  // Calculate delta/percent based on comparisonMode
  const delta = comparisonValue
    ? calculateComparison(mainValue, comparisonValue, comparisonMode)
    : null;

  const styles = {
    container: {
      padding: "1.5rem",
      borderRadius: "0.5rem",
      backgroundColor: colors.surface,
      border: `1px solid ${colors.border}`,
    },
    title: {
      fontSize: "0.875rem",
      color: colors.muted,
      marginBottom: "0.5rem",
      textTransform: "uppercase" as const,
      letterSpacing: "0.05em",
    },
    value: {
      fontSize: "2.25rem",
      fontWeight: 700,
      color: colors.text,
      marginBottom: "0.5rem",
    },
    comparison: {
      fontSize: "0.875rem",
      display: "flex",
      alignItems: "center",
      marginBottom: trend ? "1rem" : 0,
    },
    sparkline: {
      height: "60px",
      marginTop: "1rem",
    },
  };

  return (
    <div style={styles.container}>
      {spec.title && <div style={styles.title}>{spec.title}</div>}
      <div style={styles.value}>
        {formatValue(mainValue, valueField.format)}
      </div>
      {delta && (
        <div style={styles.comparison}>
          <TrendIcon direction={delta.direction} />
          <span style={{ color: colors.text }}>{delta.formatted}</span>
        </div>
      )}
      {trend && data.length > 1 && (
        <div style={styles.sparkline}>
          <ResponsiveContainer width="100%" height="100%">
            <LineChart data={data}>
              <Line
                type="monotone"
                dataKey={trend.valueField.field}
                stroke={colors.primary}
                strokeWidth={2}
                dot={false}
              />
            </LineChart>
          </ResponsiveContainer>
        </div>
      )}
    </div>
  );
}
