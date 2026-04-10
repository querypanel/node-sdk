import {
  BarChart,
  Bar,
  LineChart,
  Line,
  AreaChart,
  Area,
  ScatterChart,
  Scatter,
  PieChart,
  Pie,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  Legend,
  ResponsiveContainer,
} from "recharts";
import type { ThemeColors } from "../types";
import { formatTimestampForDisplay } from "../utils/formatters";

// VizSpec types (copied from querypanel-sdk for now)
type FieldType = "quantitative" | "temporal" | "ordinal" | "nominal" | "boolean";
type ChartType = "line" | "bar" | "column" | "area" | "scatter" | "pie";
type AggregateOp = "sum" | "avg" | "min" | "max" | "count" | "distinct";
type TimeUnit = "year" | "quarter" | "month" | "week" | "day" | "hour" | "minute";
type StackingMode = "none" | "stacked" | "percent";

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

interface SortSpec {
  field: string;
  direction?: "asc" | "desc";
}

interface ChartEncoding {
  chartType: ChartType;
  x?: AxisField;
  y?: AxisField | AxisField[];
  series?: FieldRef;
  stacking?: StackingMode;
  sort?: SortSpec;
  limit?: number;
  tooltips?: FieldRef[];
}

export interface ChartSpec {
  kind: "chart";
  title?: string;
  description?: string;
  encoding: ChartEncoding;
}

export interface VizSpecChartProps {
  spec: ChartSpec;
  data: Array<Record<string, unknown>>;
  colors: ThemeColors;
}

export function VizSpecChart({ spec, data, colors }: VizSpecChartProps) {
  const { chartType } = spec.encoding;

  // Apply limit if specified
  const limitedData = spec.encoding.limit
    ? data.slice(0, spec.encoding.limit)
    : data;

  // Coerce quantitative fields from string to number so bars/lines render (API/embed often return strings)
  const normalizedData = normalizeQuantitativeData(limitedData, spec.encoding);

  // Common props for all charts
  const commonMargin = { top: 20, right: 30, left: 20, bottom: 20 };

  switch (chartType) {
    case "column":
      return renderBarChart(spec, normalizedData, colors, commonMargin);
    case "bar":
      // Keep parity with admin chart block, where "bar" is rendered vertically.
      return renderColumnChart(spec, normalizedData, colors, commonMargin);
    case "line":
      return renderLineChart(spec, normalizedData, colors, commonMargin);
    case "area":
      return renderAreaChart(spec, normalizedData, colors, commonMargin);
    case "scatter":
      return renderScatterChart(spec, normalizedData, colors, commonMargin);
    case "pie":
      return renderPieChart(spec, normalizedData, colors);
    default: {
      const _exhaustiveCheck: never = chartType;
      return <div>Unsupported chart type: {_exhaustiveCheck}</div>;
    }
  }
}

function isNumericValue(value: unknown): boolean {
  if (typeof value === "number" && Number.isFinite(value)) return true;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value))) return true;
  return false;
}

/** Coerce encoding quantitative fields from string to number so bars/lines/pie render (API often returns strings). */
function normalizeQuantitativeData(
  data: Array<Record<string, unknown>>,
  encoding: ChartEncoding,
): Array<Record<string, unknown>> {
  const yFields = Array.isArray(encoding.y) ? encoding.y : encoding.y ? [encoding.y] : [];
  const quantitativeFields = new Set<string>();
  if (encoding.x?.type === "quantitative" && encoding.x.field) quantitativeFields.add(encoding.x.field);
  for (const f of yFields) {
    if (f.type === "quantitative" && f.field) quantitativeFields.add(f.field);
  }
  // Pie: coerce value field from tooltips (e.g. order_count)
  if (encoding.chartType === "pie" && encoding.tooltips?.length) {
    for (const t of encoding.tooltips) {
      if (t.type === "quantitative" && t.field) quantitativeFields.add(t.field);
    }
  }
  if (quantitativeFields.size === 0) return data;
  return data.map((row) => {
    const out = { ...row };
    for (const field of quantitativeFields) {
      const v = out[field];
      if (typeof v === "string" && v !== "" && Number.isFinite(Number(v))) {
        out[field] = Number(v);
      }
    }
    return out;
  });
}

function prepareCartesianData(
  data: Array<Record<string, unknown>>,
  xFieldInput?: string,
  yFieldsInput?: AxisField[],
): {
  rows: Array<Record<string, unknown>>;
  xField: string;
  yFields: AxisField[];
} {
  if (data.length === 0) {
    return {
      rows: [],
      xField: xFieldInput || "__category",
      yFields: yFieldsInput ?? [],
    };
  }

  const firstRow = data[0];
  const keys = Object.keys(firstRow);
  const numericKeys = keys.filter((key) => isNumericValue(firstRow[key]));
  const nonNumericKeys = keys.filter((key) => !isNumericValue(firstRow[key]));

  const initialYFields = (yFieldsInput ?? []).filter((field) =>
    data.some((row) => isNumericValue(row[field.field])),
  );

  if (initialYFields.length > 0) {
    const xField =
      xFieldInput && keys.includes(xFieldInput)
        ? xFieldInput
        : nonNumericKeys[0] || xFieldInput || "__category";

    const rows =
      xField === "__category"
        ? data.map((row, index) => ({ ...row, __category: String(index + 1) }))
        : data;

    return { rows, xField, yFields: initialYFields };
  }

  const fallbackNumericKey =
    (xFieldInput && numericKeys.includes(xFieldInput) && xFieldInput) ||
    numericKeys[0] ||
    keys[0];

  const rows = data.map((row, index) => ({
    ...row,
    __category: String(row[fallbackNumericKey] ?? index + 1),
  }));

  return {
    rows,
    xField: "__category",
    yFields: [
      {
        field: fallbackNumericKey,
        label: fallbackNumericKey,
      },
    ],
  };
}

// Column Chart (Vertical Bars)
function renderColumnChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors,
  margin: { top: number; right: number; left: number; bottom: number }
) {
  const { x, y, stacking } = spec.encoding;
  const rawYFields = Array.isArray(y) ? y : y ? [y] : [];
  const normalized = prepareCartesianData(data, x?.field, rawYFields);

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart data={normalized.rows} margin={margin}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.grid} />
        <XAxis
          dataKey={normalized.xField}
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <YAxis stroke={colors.text} tick={{ fill: colors.text }} />
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend />
        {normalized.yFields.map((yField, idx) => (
          <Bar
            key={yField.field}
            dataKey={yField.field}
            name={yField.label || yField.field}
            fill={idx === 0 ? colors.secondary : colors.range[idx % colors.range.length]}
            stackId={stacking === "stacked" || stacking === "percent" ? "stack" : undefined}
          />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

// Bar Chart (Horizontal Bars)
function renderBarChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors,
  margin: { top: number; right: number; left: number; bottom: number }
) {
  const { x, y, stacking } = spec.encoding;
  const rawYFields = Array.isArray(y) ? y : y ? [y] : [];
  const normalized = prepareCartesianData(data, x?.field, rawYFields);

  return (
    <ResponsiveContainer width="100%" height={400}>
      <BarChart data={normalized.rows} layout="vertical" margin={margin}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.grid} />
        <XAxis type="number" stroke={colors.text} tick={{ fill: colors.text }} />
        <YAxis
          dataKey={normalized.xField}
          type="category"
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend />
        {normalized.yFields.map((xField, idx) => (
          <Bar
            key={xField.field}
            dataKey={xField.field}
            name={xField.label || xField.field}
            fill={colors.range[idx % colors.range.length]}
            stackId={stacking === "stacked" || stacking === "percent" ? "stack" : undefined}
          />
        ))}
      </BarChart>
    </ResponsiveContainer>
  );
}

// Line Chart
function renderLineChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors,
  margin: { top: number; right: number; left: number; bottom: number }
) {
  const { x, y } = spec.encoding;
  const yFields = Array.isArray(y) ? y : y ? [y] : [];

  return (
    <ResponsiveContainer width="100%" height={400}>
      <LineChart data={data} margin={margin}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.grid} />
        <XAxis
          dataKey={x?.field}
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <YAxis stroke={colors.text} tick={{ fill: colors.text }} />
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend />
        {yFields.map((yField, idx) => (
          <Line
            key={yField.field}
            type="monotone"
            dataKey={yField.field}
            name={yField.label || yField.field}
            stroke={colors.range[idx % colors.range.length]}
            strokeWidth={2}
            dot={{ fill: colors.range[idx % colors.range.length] }}
          />
        ))}
      </LineChart>
    </ResponsiveContainer>
  );
}

// Area Chart
function renderAreaChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors,
  margin: { top: number; right: number; left: number; bottom: number }
) {
  const { x, y, stacking } = spec.encoding;
  const yFields = Array.isArray(y) ? y : y ? [y] : [];

  return (
    <ResponsiveContainer width="100%" height={400}>
      <AreaChart data={data} margin={margin}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.grid} />
        <XAxis
          dataKey={x?.field}
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <YAxis stroke={colors.text} tick={{ fill: colors.text }} />
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend />
        {yFields.map((yField, idx) => (
          <Area
            key={yField.field}
            type="monotone"
            dataKey={yField.field}
            name={yField.label || yField.field}
            stroke={colors.range[idx % colors.range.length]}
            fill={colors.range[idx % colors.range.length]}
            fillOpacity={0.6}
            stackId={stacking === "stacked" || stacking === "percent" ? "stack" : undefined}
          />
        ))}
      </AreaChart>
    </ResponsiveContainer>
  );
}

// Scatter Chart
function renderScatterChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors,
  margin: { top: number; right: number; left: number; bottom: number }
) {
  const { x, y } = spec.encoding;
  const yField = Array.isArray(y) ? y[0] : y;

  return (
    <ResponsiveContainer width="100%" height={400}>
      <ScatterChart margin={margin}>
        <CartesianGrid strokeDasharray="3 3" stroke={colors.grid} />
        <XAxis
          dataKey={x?.field}
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <YAxis
          dataKey={yField?.field}
          stroke={colors.text}
          tick={{ fill: colors.text }}
          tickFormatter={formatTimestampForDisplay}
        />
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend />
        <Scatter
          name={yField?.label || yField?.field || "value"}
          data={data}
          fill={colors.primary}
        />
      </ScatterChart>
    </ResponsiveContainer>
  );
}

// Pie Chart (backend uses encoding.series for slice label, tooltips[0] or y for slice value)
function renderPieChart(
  spec: ChartSpec,
  data: Array<Record<string, unknown>>,
  colors: ThemeColors
) {
  const { x, y, series, tooltips } = spec.encoding;
  const nameField =
    series?.field || x?.field || "name";
  const valueField =
    (tooltips && tooltips.length > 0 ? tooltips[0].field : undefined) ||
    (Array.isArray(y) ? y[0]?.field : y?.field) ||
    "value";

  return (
    <ResponsiveContainer width="100%" height={400}>
      <PieChart>
        <Pie
          data={data}
          dataKey={valueField}
          nameKey={nameField}
          cx="50%"
          cy="50%"
          outerRadius={120}
          label={({ name }) => formatTimestampForDisplay(name)}
        >
          {data.map((_, index) => (
            <Cell key={`cell-${nameField}-${String(data[index]?.[nameField] ?? index)}`} fill={colors.range[index % colors.range.length]} />
          ))}
        </Pie>
        <Tooltip
          contentStyle={{
            backgroundColor: colors.surface,
            border: `1px solid ${colors.border}`,
            borderRadius: "4px",
          }}
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
        />
        <Legend formatter={(value) => formatTimestampForDisplay(value)} />
      </PieChart>
    </ResponsiveContainer>
  );
}
