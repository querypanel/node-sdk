"use client";

import {
  useState,
  useRef,
  useEffect,
  useLayoutEffect,
  useId,
  useMemo,
  useSyncExternalStore,
} from "react";
import { createPortal } from "react-dom";
import {
  XIcon,
  SendIcon,
  BarChart3Icon,
  SparklesIcon,
  Wand2Icon,
  CircleCheckIcon,
  CircleAlertIcon,
  LoaderCircleIcon,
  SlidersHorizontalIcon,
  ChevronUpIcon,
} from "lucide-react";
import {
  Bar,
  BarChart,
  Line,
  LineChart,
  Area,
  AreaChart,
  Pie,
  PieChart,
  Cell,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from "recharts";
import { DatasourceSelector } from "./DatasourceSelector";
import { formatTimestampForDisplay } from "../utils/formatters";
import "./AIChartModal.css";

/** Option for the SQL / chart LLM model selector (OpenAI model id as `value`). */
export type AIChartModelOption = { value: string; label: string };

/** Sensible defaults; override via `chartModelOptions` if your deployment uses different ids. */
export const DEFAULT_AI_CHART_MODEL_OPTIONS: AIChartModelOption[] = [
  { value: "gpt-5.4-mini", label: "GPT-5.4 mini" },
  { value: "gpt-5.4", label: "GPT-5.4" },
  { value: "gpt-4.1", label: "GPT-4.1" },
  { value: "gpt-4.1-mini", label: "GPT-4.1 mini" },
  { value: "", label: "Server default" },
];

export interface AIChartModalProps {
  isOpen: boolean;
  onClose: () => void;
  onAddChart: (
    chartSpec: unknown,
    rationale?: string,
    sql?: string,
    datasourceIds?: string[],
    sqlParams?: Record<string, unknown> | null,
    tenantFieldName?: string,
    previewTenantId?: string
  ) => void;
  organizationId: string;
  dashboardId: string;
  initialPrompt?: string;
  dashboardType?: "customer" | "internal";
  /** URL for SQL chart generation (default: /api/ai/generate-chart-with-sql) */
  generateChartWithSqlUrl?: string;
  /** URL for datasources (default: /api/datasources) */
  datasourcesUrl?: string;
  /** Extra headers for API requests (e.g. x-organization-id is always sent) */
  headers?: Record<string, string>;
  /** Whether to render dark theme styles for inline-styled children */
  darkMode?: boolean;
  /** When set, only these datasource IDs are shown (e.g. from dashboard.available_datasource_ids). */
  availableDatasourceIds?: string[] | null;
  /** Default tenant field name (e.g. from dashboard.tenant_field_name). */
  defaultTenantFieldName?: string | null;
  /** Per-datasource tenant field name (e.g. from dashboard.tenant_field_by_datasource). Overrides default for the selected datasource. */
  tenantFieldByDatasource?: Record<string, string> | null;
  /** When true, hide tenant field name and preview tenant ID inputs (e.g. in customer embed; tenant comes from JWT only). */
  hideTenantInputs?: boolean;
  /** Whitelabel: modal title (default: "AI Chart Generator") */
  title?: string;
  /** Whitelabel: empty-state heading (default: "Create a Chart") */
  createTitle?: string;
  /** Model dropdown options (`value` = OpenAI model id, empty string = server default). */
  chartModelOptions?: AIChartModelOption[];
  /** Initial selected model `value` when the modal opens (default: `gpt-5.4-mini`) */
  defaultChartModel?: string;
}

type Message = {
  id: string;
  role: "user" | "assistant";
  content: string;
  chartSpec?: unknown;
  rationale?: string;
  sql?: string;
  sqlParams?: Record<string, unknown> | null;
  timestamp: Date;
};

type MastraStreamChunk = {
  type?: string;
  textDelta?: string;
  toolName?: string;
  result?: unknown;
  payload?: Record<string, unknown>;
  response?: {
    messages?: Array<{
      role?: string;
      content?: Array<{
        type?: string;
        text?: string;
        result?: unknown;
        toolName?: string;
      }>;
    }>;
    uiMessages?: Array<{
      role?: string;
      metadata?: Record<string, unknown>;
    }>;
  };
};

const COLORS = ["#3b82f6", "#10b981", "#8b5cf6", "#f59e0b", "#ec4899"];
const EMPTY_HEADERS: Record<string, string> = {};

type ToolEventStatus = "running" | "succeeded" | "failed";
type ToolEvent = {
  id: string;
  toolName: string;
  status: ToolEventStatus;
  startedAt: number;
  endedAt?: number;
  error?: string;
};

type SqlExecutionArtifact = {
  fields: string[];
  rows: Array<Record<string, unknown>>;
  rowCount: number;
  database?: string;
  dialect?: string;
  datasource?: { id: string; name: string; dialect: string };
};

function isMastraAgentStreamUrl(url: string) {
  return /\/api\/agents\/[^/]+\/stream(?:[/?#]|$)/.test(url);
}

const AI_CHART_MODAL_NARROW_QUERY = "(max-width: 52rem)";

function subscribeAiChartModalNarrow(onChange: () => void) {
  if (typeof window === "undefined") return () => {};
  const mq = window.matchMedia(AI_CHART_MODAL_NARROW_QUERY);
  mq.addEventListener("change", onChange);
  return () => mq.removeEventListener("change", onChange);
}

function getAiChartModalNarrowSnapshot() {
  if (typeof window === "undefined") return false;
  return window.matchMedia(AI_CHART_MODAL_NARROW_QUERY).matches;
}

function useAiChartModalNarrowLayout() {
  return useSyncExternalStore(
    subscribeAiChartModalNarrow,
    getAiChartModalNarrowSnapshot,
    () => false
  );
}

/**
 * Mastra stream resolves models like AI Gateway (`provider/model`, e.g. `openai/gpt-5.4-mini`).
 * The v2 SQL pipeline uses bare OpenAI ids via @ai-sdk/openai — keep those unchanged for generate-chart-with-sql.
 */
function normalizeModelForMastraStreamBody(model: string): string {
  const t = model.trim();
  if (!t) return "";
  if (t.includes("/")) return t;
  return `openai/${t}`;
}

function mapGeneratedParams(params: unknown) {
  if (!Array.isArray(params)) {
    return null;
  }

  return params.reduce<Record<string, unknown>>((acc, param, index) => {
    if (!param || typeof param !== "object") {
      return acc;
    }

    const record = param as Record<string, unknown>;
    const value = record.value;
    if (value === undefined) {
      return acc;
    }

    const key =
      (typeof record.name === "string" && record.name.trim()) ||
      (typeof record.placeholder === "string" && record.placeholder.trim()) ||
      (typeof record.position === "number" && String(record.position)) ||
      String(index + 1);

    acc[key.replace(/[{}]/g, "").replace(/(.+):.*$/, "$1").replace(/^[:$]/, "").trim()] = value;
    return acc;
  }, {});
}

function extractAssistantTextFromChunk(chunk: MastraStreamChunk) {
  const assistantMessage = chunk.response?.messages?.find((message) => message.role === "assistant");
  const textPart = assistantMessage?.content?.find((part) => part.type === "text" && typeof part.text === "string");
  return textPart?.text;
}

function extractToolResultsFromChunk(chunk: MastraStreamChunk) {
  const results: Array<{ toolName: string; result: Record<string, unknown> }> = [];

  for (const message of chunk.response?.messages ?? []) {
    for (const part of message.content ?? []) {
      if (
        typeof part.toolName === "string" &&
        part.toolName.trim().length > 0 &&
        part.result &&
        typeof part.result === "object" &&
        !Array.isArray(part.result)
      ) {
        results.push({
          toolName: part.toolName.trim(),
          result: part.result as Record<string, unknown>,
        });
      }
    }
  }

  return results;
}

function formatToolStatus(toolName: string) {
  switch (toolName) {
    case "search_schema":
    case "search_relevant_schema":
      return "Searching schema";
    case "generate_sql":
      return "Generating SQL";
    case "execute_sql":
      return "Running SQL";
    case "generate_visualization":
      return "Building chart";
    default:
      return toolName
        .split("_")
        .filter(Boolean)
        .map((part, index) =>
          index === 0
            ? part.charAt(0).toUpperCase() + part.slice(1).toLowerCase().replace(/s$/, "ing")
            : part.toLowerCase()
        )
        .join(" ");
  }
}

function isNumericChartValue(value: unknown): boolean {
  if (typeof value === "number" && Number.isFinite(value)) return true;
  if (typeof value === "string" && value !== "" && Number.isFinite(Number(value))) return true;
  return false;
}

function withVizSpecDataFallback(
  chartSpec: unknown,
  rows: Array<Record<string, unknown>> | null
) {
  if (!rows || rows.length === 0) return chartSpec;

  try {
    const spec =
      typeof chartSpec === "string"
        ? (JSON.parse(chartSpec) as unknown)
        : chartSpec;
    if (!spec || typeof spec !== "object") return chartSpec;

    const s = spec as Record<string, unknown>;
    if (s.kind === "chart") {
      const currentData = s.data;
      if (Array.isArray(currentData) && currentData.length > 0) return s;
      return { ...s, data: rows };
    }

    const data = s.data;
    if (data && typeof data === "object" && !Array.isArray(data)) {
      const values = (data as { values?: unknown }).values;
      if (Array.isArray(values) && values.length > 0) return s;
      return { ...s, data: { ...(data as Record<string, unknown>), values: rows } };
    }

    return { ...s, data: { values: rows } };
  } catch {
    return chartSpec;
  }
}

function normalizeMastraChunk(raw: MastraStreamChunk): MastraStreamChunk {
  const payload = raw.payload;
  if (payload && typeof payload === "object" && !Array.isArray(payload)) {
    const merged = { ...raw, ...payload } as MastraStreamChunk;
    delete (merged as { payload?: unknown }).payload;
    if (
      merged.type === "text-delta" &&
      typeof merged.textDelta !== "string" &&
      typeof (payload as { text?: unknown }).text === "string"
    ) {
      merged.textDelta = (payload as { text: string }).text;
    }
    if (
      merged.type === "step-finish" &&
      !merged.response &&
      Array.isArray((payload as { messages?: unknown }).messages)
    ) {
      merged.response = {
        messages: (payload as { messages: NonNullable<MastraStreamChunk["response"]>["messages"] }).messages,
      };
    }
    return merged;
  }
  return raw;
}

function getToolEventLabel(toolName: string) {
  return formatToolStatus(toolName);
}

function ToolEventStatusIcon({ status }: { status: ToolEventStatus }) {
  if (status === "succeeded") return <CircleCheckIcon className="w-4 h-4" />;
  if (status === "failed") return <CircleAlertIcon className="w-4 h-4" />;
  return <LoaderCircleIcon className="w-4 h-4 qp-ai-modal-spin" />;
}

function ToolEventChip({
  event,
  active,
  onClick,
}: {
  event: ToolEvent;
  active?: boolean;
  onClick?: () => void;
}) {
  return (
    <button
      type="button"
      className={`qp-ai-modal-toolchip ${event.status} ${active ? "active" : ""}`}
      onClick={onClick}
    >
      <ToolEventStatusIcon status={event.status} />
      <span className="qp-ai-modal-toolchip-label">{getToolEventLabel(event.toolName)}</span>
    </button>
  );
}

function ChartPreview({
  chartSpec,
  darkMode = false,
}: {
  chartSpec: unknown;
  darkMode?: boolean;
}) {
  const isNarrowChart = useAiChartModalNarrowLayout();
  const parsed = useMemo(() => {
    try {
      const spec = typeof chartSpec === "string" ? JSON.parse(chartSpec) : chartSpec;
      if (!spec || typeof spec !== "object") {
        return {
          error: "Invalid chart specification",
          spec: null,
          chartType: "bar",
          chartData: [] as Record<string, unknown>[],
        };
      }

      const record = spec as Record<string, unknown>;
      if (record.kind === "chart") {
        return {
          error: null,
          spec: record,
          chartType:
            typeof (record.encoding as { chartType?: unknown } | undefined)?.chartType === "string"
              ? ((record.encoding as { chartType: string }).chartType || "bar")
              : "bar",
          chartData: Array.isArray(record.data) ? (record.data as Record<string, unknown>[]) : [],
        };
      }

      const mark = record.mark;
      const chartType =
        typeof mark === "string"
          ? mark
          : mark && typeof mark === "object" && typeof (mark as { type?: unknown }).type === "string"
            ? (mark as { type: string }).type
            : "bar";

      const data = record.data;
      const chartData =
        data && typeof data === "object" && Array.isArray((data as { values?: unknown }).values)
          ? ((data as { values: Record<string, unknown>[] }).values ?? [])
          : [];

      return { error: null, spec: record, chartType, chartData };
    } catch (err) {
      return {
        error: err instanceof Error ? err.message : "Failed to render chart",
        spec: null,
        chartType: "bar",
        chartData: [] as Record<string, unknown>[],
      };
    }
  }, [chartSpec]);

  const { error, spec, chartType, chartData } = parsed;

  const gridStroke = darkMode ? "#3f3f46" : "#e2e8f0";
  const tickFill = darkMode ? "#a1a1aa" : "#64748b";
  const tickFontSize = isNarrowChart ? 12 : 10;
  const xAxisAngle = isNarrowChart && chartData.length > 3 ? -38 : 0;
  const xAxisHeight = isNarrowChart ? (chartData.length > 5 ? 64 : chartData.length > 3 ? 52 : 32) : 30;
  const chartMargins = isNarrowChart
    ? { top: 10, right: 6, left: 2, bottom: chartData.length > 3 ? Math.max(36, xAxisHeight - 8) : 16 }
    : { top: 6, right: 8, left: 0, bottom: 6 };
  const tooltipContentStyle = isNarrowChart
    ? { fontSize: 13, padding: "10px 12px", borderRadius: 10 }
    : undefined;

  if (error || !chartData || chartData.length === 0) {
    return (
      <div
        className={`qp-ai-modal-chart-preview qp-ai-modal-chart-preview-empty${isNarrowChart ? " qp-ai-modal-chart-preview--narrow" : ""}`}
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          fontSize: isNarrowChart ? 13 : 12,
          color: darkMode ? "#fca5a5" : "#dc2626",
          background: darkMode ? "rgba(239,68,68,0.18)" : "#fef2f2",
          borderRadius: isNarrowChart ? 10 : 4,
          border: darkMode ? "1px solid rgba(239,68,68,0.35)" : "1px solid #fecaca",
          padding: isNarrowChart ? 14 : 8,
        }}
      >
        <div style={{ textAlign: "center" }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>{error ? "Chart Error" : "No Data"}</div>
          <div style={{ opacity: 0.75 }}>{error || "No data available"}</div>
        </div>
      </div>
    );
  }

  // Use encoding from vizspec when present (kind === "chart"), so x/y/series/tooltips map correctly
  const encoding =
    spec?.kind === "chart" && spec.encoding && typeof spec.encoding === "object"
      ? (spec.encoding as Record<string, unknown>)
      : null;
  const xRef =
    encoding?.x && typeof encoding.x === "object"
      ? (encoding.x as { field?: unknown; type?: unknown })
      : null;
  const yRef =
    encoding?.y && typeof encoding.y === "object"
      ? (encoding.y as { field?: unknown; type?: unknown })
      : null;
  const xField = typeof xRef?.field === "string" ? xRef.field : undefined;
  const yField = typeof yRef?.field === "string" ? yRef.field : undefined;
  const yType = typeof yRef?.type === "string" ? yRef.type : undefined;
  const seriesRef = encoding?.series;
  const seriesField =
    typeof seriesRef === "string"
      ? seriesRef
      : (seriesRef && typeof seriesRef === "object" && "field" in seriesRef && typeof (seriesRef as { field?: unknown }).field === "string")
        ? (seriesRef as { field: string }).field
        : undefined;
  const tooltipFields = Array.isArray(encoding?.tooltips) ? encoding.tooltips : [];
  const firstTooltipField =
    tooltipFields.length > 0 && typeof tooltipFields[0] === "object" && tooltipFields[0] !== null && "field" in tooltipFields[0]
      ? (tooltipFields[0] as { field?: string }).field
      : undefined;

  const firstRow = chartData[0] ?? {};
  const firstRowKeys = Object.keys(firstRow);

  // For pie, backend uses encoding.series (slice label) and tooltips[0] (slice size)
  const isPie = chartType === "pie" || chartType === "arc";
  let categoryKey: string;
  let dataKey: string;
  if (isPie) {
    if (typeof seriesField === "string" && seriesField in firstRow) {
      categoryKey = seriesField;
    } else {
      categoryKey = firstRowKeys.find((k) => !isNumericChartValue(firstRow[k])) ?? firstRowKeys[0] ?? "name";
    }
    if (typeof firstTooltipField === "string" && firstTooltipField in firstRow) {
      dataKey = firstTooltipField;
    } else if (typeof yField === "string" && yField in firstRow) {
      dataKey = yField;
    } else {
      dataKey = firstRowKeys.find((k) => isNumericChartValue(firstRow[k])) ?? firstRowKeys.find((k) => k !== categoryKey) ?? "value";
    }
  } else {
    categoryKey =
      typeof xField === "string" && xField in firstRow
        ? xField
        : "category" in firstRow
          ? "category"
          : "date" in firstRow
            ? "date"
            : firstRowKeys[0] ?? "category";
    dataKey =
      typeof yField === "string" && yField in firstRow
        ? yField
        : firstRowKeys.find((k) => k !== "category" && k !== "date") ?? "value";
  }
  if (isPie && categoryKey === dataKey && firstRowKeys.length > 1) {
    const valueKey = firstRowKeys.find((k) => isNumericChartValue(firstRow[k]));
    const labelKey = firstRowKeys.find((k) => !isNumericChartValue(firstRow[k]));
    if (valueKey && labelKey) {
      dataKey = valueKey;
      categoryKey = labelKey;
    } else {
      const other = firstRowKeys.find((k) => k !== dataKey);
      if (other !== undefined) categoryKey = other;
    }
  }

  // Coerce quantitative values to numbers (API often returns strings); for pie always coerce value field
  const valueNeedsCoerce =
    yType === "quantitative" ||
    (isPie && (tooltipFields[0]?.type === "quantitative" || isNumericChartValue(firstRow[dataKey])));
  const chartDataNormalized =
    valueNeedsCoerce && dataKey
      ? chartData.map((row) => {
          const r = { ...row } as Record<string, unknown>;
          const v = r[dataKey];
          if (typeof v === "string" && v !== "" && Number.isFinite(Number(v))) {
            r[dataKey] = Number(v);
          }
          return r;
        })
      : chartData;

  const renderChart = () => {
    const data = chartDataNormalized;
    if (chartType === "pie" || chartType === "arc") {
      return (
        <PieChart>
          <Tooltip
            formatter={(value, name) => [formatTimestampForDisplay(value), name]}
            contentStyle={tooltipContentStyle}
          />
          <Pie
            data={data}
            dataKey={dataKey}
            nameKey={categoryKey}
            cx="50%"
            cy="50%"
            innerRadius={isNarrowChart ? "18%" : 0}
            outerRadius={isNarrowChart ? "78%" : 60}
            paddingAngle={isNarrowChart ? 1.5 : 0}
            label={
              isNarrowChart
                ? ({ name, percent }) =>
                    `${formatTimestampForDisplay(name)} ${((percent ?? 0) * 100).toFixed(0)}%`
                : ({ name }) => formatTimestampForDisplay(name)
            }
            labelLine={isNarrowChart}
          >
            {data.map((entry, index) => {
              const item = entry as Record<string, unknown>;
              const key = String(item[categoryKey] ?? item.category ?? item.date ?? index);
              return <Cell key={key} fill={COLORS[index % COLORS.length]} />;
            })}
          </Pie>
        </PieChart>
      );
    }
    if (chartType === "line") {
      return (
        <LineChart data={data} margin={chartMargins}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
          <XAxis
            dataKey={categoryKey}
            tick={{ fontSize: tickFontSize, fill: tickFill }}
            tickFormatter={formatTimestampForDisplay}
            angle={xAxisAngle}
            textAnchor={xAxisAngle ? "end" : "middle"}
            height={xAxisHeight}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: tickFontSize, fill: tickFill }}
            width={isNarrowChart ? 44 : 36}
            tickFormatter={(v) => (typeof v === "number" && Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : String(v))}
          />
          <Tooltip
            formatter={(value, name) => [formatTimestampForDisplay(value), name]}
            contentStyle={tooltipContentStyle}
          />
          <Line
            type="monotone"
            dataKey={dataKey}
            stroke={COLORS[0]}
            strokeWidth={isNarrowChart ? 2.5 : 2}
            dot={isNarrowChart ? { r: 3, strokeWidth: 1, fill: COLORS[0] } : false}
            activeDot={{ r: isNarrowChart ? 6 : 5 }}
          />
        </LineChart>
      );
    }
    if (chartType === "area") {
      return (
        <AreaChart data={data} margin={chartMargins}>
          <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
          <XAxis
            dataKey={categoryKey}
            tick={{ fontSize: tickFontSize, fill: tickFill }}
            tickFormatter={formatTimestampForDisplay}
            angle={xAxisAngle}
            textAnchor={xAxisAngle ? "end" : "middle"}
            height={xAxisHeight}
            interval="preserveStartEnd"
          />
          <YAxis
            tick={{ fontSize: tickFontSize, fill: tickFill }}
            width={isNarrowChart ? 44 : 36}
            tickFormatter={(v) => (typeof v === "number" && Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : String(v))}
          />
          <Tooltip
            formatter={(value, name) => [formatTimestampForDisplay(value), name]}
            contentStyle={tooltipContentStyle}
          />
          <Area
            type="monotone"
            dataKey={dataKey}
            stroke={COLORS[0]}
            fill={COLORS[0]}
            fillOpacity={0.6}
            strokeWidth={isNarrowChart ? 2 : 1.5}
          />
        </AreaChart>
      );
    }
    return (
      <BarChart data={data} margin={chartMargins} barCategoryGap={isNarrowChart ? "18%" : "10%"}>
        <CartesianGrid strokeDasharray="3 3" stroke={gridStroke} vertical={false} />
        <XAxis
          dataKey={categoryKey}
          tick={{ fontSize: tickFontSize, fill: tickFill }}
          tickFormatter={formatTimestampForDisplay}
          angle={xAxisAngle}
          textAnchor={xAxisAngle ? "end" : "middle"}
          height={xAxisHeight}
          interval="preserveStartEnd"
        />
        <YAxis
          tick={{ fontSize: tickFontSize, fill: tickFill }}
          width={isNarrowChart ? 44 : 36}
          tickFormatter={(v) => (typeof v === "number" && Math.abs(v) >= 1000 ? `${(v / 1000).toFixed(v % 1000 === 0 ? 0 : 1)}k` : String(v))}
        />
        <Tooltip
          formatter={(value, name) => [formatTimestampForDisplay(value), name]}
          contentStyle={tooltipContentStyle}
        />
        <Bar
          dataKey={dataKey}
          fill={COLORS[0]}
          radius={isNarrowChart ? [6, 6, 2, 2] : [4, 4, 0, 0]}
          maxBarSize={isNarrowChart ? 48 : 56}
        />
      </BarChart>
    );
  };

  return (
    <div
      className={`qp-ai-modal-chart-preview${isNarrowChart ? " qp-ai-modal-chart-preview--narrow" : ""}`}
    >
      <ResponsiveContainer width="100%" height="100%">
        {renderChart()}
      </ResponsiveContainer>
    </div>
  );
}

export function AIChartModal({
  isOpen,
  onClose,
  onAddChart,
  organizationId,
  dashboardId,
  initialPrompt,
  dashboardType = "customer",
  generateChartWithSqlUrl = "/api/ai/generate-chart-with-sql",
  datasourcesUrl = "/api/datasources",
  headers = EMPTY_HEADERS,
  darkMode = false,
  availableDatasourceIds,
  defaultTenantFieldName,
  tenantFieldByDatasource,
  hideTenantInputs = false,
  title: titleProp,
  createTitle: createTitleProp,
  chartModelOptions = DEFAULT_AI_CHART_MODEL_OPTIONS,
  defaultChartModel = "gpt-5.4-mini",
}: AIChartModalProps) {
  const modalTitle = titleProp ?? "AI Chart Generator";
  const createTitle = createTitleProp ?? "Create a Chart";
  const useMastraStream = isMastraAgentStreamUrl(generateChartWithSqlUrl);

  const getResolvedTenantField = (
    selectedIds: string[],
    byDs: Record<string, string> | null | undefined,
    defaultName: string | null | undefined
  ) => {
    const firstId = selectedIds[0];
    if (firstId && byDs && byDs[firstId]?.trim()) return byDs[firstId].trim();
    return defaultName?.trim() || "tenant_id";
  };

  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const [transientStatus, setTransientStatus] = useState<string | null>(null);
  const [selectedDatasourceIds, setSelectedDatasourceIds] = useState<string[]>([]);
  const [tenantFieldName, setTenantFieldName] = useState(() =>
    getResolvedTenantField([], tenantFieldByDatasource, defaultTenantFieldName)
  );
  const [previewTenantId, setPreviewTenantId] = useState("");
  const [querypanelSessionId, setQuerypanelSessionId] = useState<string | null>(null);
  const [toolEvents, setToolEvents] = useState<ToolEvent[]>([]);
  const [activeAssistantMessageId, setActiveAssistantMessageId] = useState<string | null>(null);
  const [lastSqlExecution, setLastSqlExecution] = useState<SqlExecutionArtifact | null>(null);
  const [chartModel, setChartModel] = useState(() => {
    const allowed = new Set(chartModelOptions.map((o) => o.value));
    return allowed.has(defaultChartModel) ? defaultChartModel : (chartModelOptions[0]?.value ?? "");
  });
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const promptInputRef = useRef<HTMLInputElement>(null);
  const tenantFieldId = useId();
  const previewTenantIdFieldId = useId();
  const chartModelId = useId();
  const mobileSettingsSheetTitleId = useId();
  const mobileSettingsDoneRef = useRef<HTMLButtonElement>(null);
  const mobileSettingsTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileSheetWasOpenRef = useRef(false);
  const isNarrowLayout = useAiChartModalNarrowLayout();
  const [mobileSettingsOpen, setMobileSettingsOpen] = useState(false);

  // Sync with document theme when modal is open so switching theme (e.g. next-themes) updates the header immediately.
  const [docDarkMode, setDocDarkMode] = useState(false);
  useLayoutEffect(() => {
    if (!isOpen || typeof document === "undefined") return;
    const html = document.documentElement;
    const detect = () =>
      html.classList.contains("dark") ||
      html.getAttribute("data-theme") === "dark" ||
      html.style.colorScheme === "dark";
    setDocDarkMode(detect());
    const observer = new MutationObserver(() => setDocDarkMode(detect()));
    observer.observe(html, {
      attributes: true,
      attributeFilter: ["class", "data-theme", "style"],
    });
    return () => observer.disconnect();
  }, [isOpen]);

  // When open, use document theme so toggling theme updates the modal; otherwise use prop.
  // Read from document on render when open so first paint has correct theme (no flash).
  const docDarkLive =
    isOpen &&
    typeof document !== "undefined" &&
    (document.documentElement.classList.contains("dark") ||
      document.documentElement.getAttribute("data-theme") === "dark" ||
      document.documentElement.style.colorScheme === "dark");
  const effectiveDarkMode = isOpen ? (docDarkLive ?? docDarkMode) : (darkMode ?? false);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (isOpen && promptInputRef.current) {
      setTimeout(() => promptInputRef.current?.focus(), 100);
    }
  }, [isOpen]);

  useEffect(() => {
    if (isOpen && initialPrompt && messages.length === 0) {
      setInputValue(initialPrompt);
    }
  }, [isOpen, initialPrompt, messages.length]);

  useEffect(() => {
    if (!isOpen) return;
    const allowed = new Set(chartModelOptions.map((o) => o.value));
    const next = allowed.has(defaultChartModel)
      ? defaultChartModel
      : (chartModelOptions[0]?.value ?? "");
    setChartModel(next);
  }, [isOpen, defaultChartModel, chartModelOptions]);

  useEffect(() => {
    if (!isOpen) {
      setMobileSettingsOpen(false);
      setTimeout(() => {
        setMessages([]);
        setInputValue("");
        setIsLoading(false);
        setTransientStatus(null);
        setQuerypanelSessionId(null);
        setToolEvents([]);
        setActiveAssistantMessageId(null);
        setLastSqlExecution(null);
      }, 300);
    }
  }, [isOpen]);

  useEffect(() => {
    if (!isNarrowLayout) setMobileSettingsOpen(false);
  }, [isNarrowLayout]);

  useEffect(() => {
    if (!isNarrowLayout || !mobileSettingsOpen) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.preventDefault();
        e.stopPropagation();
        setMobileSettingsOpen(false);
      }
    };
    document.addEventListener("keydown", onKeyDown, true);
    return () => document.removeEventListener("keydown", onKeyDown, true);
  }, [isNarrowLayout, mobileSettingsOpen]);

  useLayoutEffect(() => {
    if (!isOpen) {
      mobileSheetWasOpenRef.current = false;
      return;
    }
    if (!isNarrowLayout) {
      mobileSheetWasOpenRef.current = false;
      return;
    }
    if (mobileSettingsOpen) {
      mobileSheetWasOpenRef.current = true;
      mobileSettingsDoneRef.current?.focus({ preventScroll: true });
      return;
    }
    if (mobileSheetWasOpenRef.current) {
      mobileSheetWasOpenRef.current = false;
      mobileSettingsTriggerRef.current?.focus({ preventScroll: true });
    }
  }, [isOpen, isNarrowLayout, mobileSettingsOpen]);

  useEffect(() => {
    if (isOpen) {
      const firstId = selectedDatasourceIds[0];
      const resolved =
        firstId && tenantFieldByDatasource?.[firstId]?.trim()
          ? tenantFieldByDatasource[firstId].trim()
          : defaultTenantFieldName?.trim() || "tenant_id";
      setTenantFieldName(resolved);
    }
  }, [isOpen, defaultTenantFieldName, tenantFieldByDatasource, selectedDatasourceIds]);

  const handleSendMessage = async (prompt?: string) => {
    const messageText = prompt || inputValue.trim();
    if (!messageText || isLoading) return;
    if (useMastraStream && selectedDatasourceIds.length !== 1) {
      setMessages((prev) => [
        ...prev,
        {
          id: `error-${Date.now()}`,
          role: "assistant",
          content: "Select exactly one datasource before generating a chart with the native Mastra agent.",
          timestamp: new Date(),
        },
      ]);
      return;
    }

    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: "user",
      content: messageText,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInputValue("");
    setIsLoading(true);
    setTransientStatus("Thinking");
    setToolEvents([]);
    setLastSqlExecution(null);
    let assistantMessageId: string | null = null;

    try {
      if (useMastraStream) {
        const threadId = querypanelSessionId || `chart-${dashboardId}-${Date.now()}`;
        const currentAssistantMessageId = `assistant-${Date.now()}`;
        assistantMessageId = currentAssistantMessageId;
        setActiveAssistantMessageId(currentAssistantMessageId);
        const resourceId = `dashboard-${organizationId}-${dashboardId}`;

        setMessages((prev) => [
          ...prev,
          {
            id: currentAssistantMessageId,
            role: "assistant",
            content: "",
            sqlParams: null,
            timestamp: new Date(),
          },
        ]);
        setQuerypanelSessionId(threadId);

        const response = await fetch(generateChartWithSqlUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-organization-id": organizationId,
            ...headers,
          },
          body: JSON.stringify({
            messages: messageText,
            memory: {
              thread: threadId,
              resource: resourceId,
            },
            requestContext: {
              organizationId,
              datasourceId: selectedDatasourceIds[0],
              ...(hideTenantInputs ? {} : { tenantId: previewTenantId.trim() || undefined }),
            },
            savePerStep: true,
            ...(chartModel.trim()
              ? { model: normalizeModelForMastraStreamBody(chartModel) }
              : {}),
          }),
        });

        if (!response.ok || !response.body) {
          throw new Error("Failed to generate chart");
        }

        const decoder = new TextDecoder();
        const reader = response.body.getReader();
        let buffer = "";
        let streamDone = false;

        const updateAssistantMessage = (updates: Partial<Message> | ((current: Message) => Message)) => {
          setMessages((prev) =>
            prev.map((message) => {
              if (message.id !== currentAssistantMessageId) {
                return message;
              }

              return typeof updates === "function" ? updates(message) : { ...message, ...updates };
            })
          );
        };

        const applyMastraChunk = (rawChunk: MastraStreamChunk) => {
          const chunk = normalizeMastraChunk(rawChunk);

          if (typeof chunk.toolName === "string" && chunk.toolName.trim().length > 0) {
            setTransientStatus(formatToolStatus(chunk.toolName));
            const toolName = chunk.toolName.trim();
            setToolEvents((prev) => {
              const existingRunning = prev.find(
                (event) => event.toolName === toolName && event.status === "running"
              );
              if (existingRunning) return prev;
              const id = `tool-${toolName}-${Date.now()}`;
              const next: ToolEvent[] = [
                ...prev,
                { id, toolName, status: "running", startedAt: Date.now() },
              ];
              return next;
            });
          }

          const chunkError =
            (chunk as { error?: unknown }).error && typeof (chunk as { error?: unknown }).error === "object"
              ? ((chunk as { error?: { message?: unknown } }).error?.message ?? null)
              : null;
          if (typeof chunkError === "string" && chunkError.trim().length > 0) {
            setTransientStatus(null);
            const toolName = typeof chunk.toolName === "string" ? chunk.toolName : undefined;
            setToolEvents((prev) =>
              prev.map((event) =>
                toolName && event.toolName === toolName && event.status === "running"
                  ? { ...event, status: "failed", endedAt: Date.now(), error: chunkError.trim() }
                  : event
              )
            );
          }

          const applyToolResult = (toolName: string, result: Record<string, unknown>) => {
            if (toolName === "generate_sql") {
              console.log("[AIChartModal] generated sql", {
                sql: typeof result.sql === "string" ? result.sql : null,
                params: mapGeneratedParams(result.params),
              });
              setToolEvents((prev) =>
                prev.map((event) =>
                  event.toolName === "generate_sql" && event.status === "running"
                    ? { ...event, status: "succeeded", endedAt: Date.now() }
                    : event
                )
              );
              updateAssistantMessage((current) => ({
                ...current,
                sql: typeof result.sql === "string" ? result.sql : current.sql,
                rationale: typeof result.rationale === "string" ? result.rationale : current.rationale,
                sqlParams: mapGeneratedParams(result.params) ?? current.sqlParams ?? null,
              }));
              return true;
            }

            if (toolName === "execute_sql") {
              const rows = Array.isArray(result.rows)
                ? (result.rows as Array<Record<string, unknown>>)
                : [];
              const fields = Array.isArray(result.fields) ? (result.fields as string[]) : [];
              console.log("[AIChartModal] query data", {
                rowCount: typeof result.rowCount === "number" ? result.rowCount : rows.length,
                fields,
                rows,
              });
              setToolEvents((prev) =>
                prev.map((event) =>
                  event.toolName === "execute_sql" && event.status === "running"
                    ? { ...event, status: "succeeded", endedAt: Date.now() }
                    : event
                )
              );
              setLastSqlExecution({
                rows,
                fields,
                rowCount: typeof result.rowCount === "number" ? result.rowCount : rows.length,
                database: typeof result.database === "string" ? result.database : undefined,
                dialect: typeof result.dialect === "string" ? result.dialect : undefined,
                datasource:
                  result.datasource && typeof result.datasource === "object"
                    ? (result.datasource as { id: string; name: string; dialect: string })
                    : undefined,
              });
              return true;
            }

            if (toolName === "generate_visualization") {
              setTransientStatus(null);
              setToolEvents((prev) =>
                prev.map((event) =>
                  event.toolName === "generate_visualization" && event.status === "running"
                    ? { ...event, status: "succeeded", endedAt: Date.now() }
                    : event
                )
              );
              updateAssistantMessage((current) => ({
                ...current,
                chartSpec: result.spec ?? current.chartSpec,
                rationale:
                  typeof result.notes === "string" && result.notes.trim().length > 0
                    ? result.notes
                    : current.rationale,
              }));
              return true;
            }

            return false;
          };

          if (chunk.type === "text-delta" && chunk.textDelta) {
            setTransientStatus(null);
            updateAssistantMessage((current) => ({
              ...current,
              content: `${current.content}${chunk.textDelta}`,
            }));
            return;
          }

          if (chunk.type === "tool-result" && chunk.toolName === "generate_sql" && chunk.result && typeof chunk.result === "object") {
            applyToolResult("generate_sql", chunk.result as Record<string, unknown>);
            return;
          }

          if (chunk.type === "tool-result" && chunk.toolName === "execute_sql" && chunk.result && typeof chunk.result === "object") {
            applyToolResult("execute_sql", chunk.result as Record<string, unknown>);
            return;
          }

          if (chunk.type === "tool-result" && chunk.toolName === "generate_visualization" && chunk.result && typeof chunk.result === "object") {
            applyToolResult("generate_visualization", chunk.result as Record<string, unknown>);
            return;
          }

          if (chunk.type === "step-finish") {
            setTransientStatus(null);
            setToolEvents((prev) =>
              prev.map((event) =>
                event.status === "running" ? { ...event, status: "succeeded", endedAt: Date.now() } : event
              )
            );
            for (const embeddedResult of extractToolResultsFromChunk(chunk)) {
              applyToolResult(embeddedResult.toolName, embeddedResult.result);
            }
            const assistantText = extractAssistantTextFromChunk(chunk);
            if (assistantText && assistantText.trim().length > 0) {
              updateAssistantMessage((current) => ({
                ...current,
                content: current.content.trim().length > 0 ? current.content : assistantText,
              }));
            }
          }
        };

        while (!streamDone) {
          const { done, value } = await reader.read();
          streamDone = done;
          buffer += decoder.decode(value ?? new Uint8Array(), { stream: !done });

          let boundaryIndex = buffer.indexOf("\n\n");
          while (boundaryIndex !== -1) {
            const rawEvent = buffer.slice(0, boundaryIndex);
            buffer = buffer.slice(boundaryIndex + 2);
            boundaryIndex = buffer.indexOf("\n\n");

            const payload = rawEvent
              .split("\n")
              .filter((line) => line.startsWith("data:"))
              .map((line) => line.slice(5).trim())
              .join("\n");

            if (!payload) {
              continue;
            }

            if (payload === "[DONE]") {
              streamDone = true;
              break;
            }

            try {
              applyMastraChunk(JSON.parse(payload) as MastraStreamChunk);
            } catch (error) {
              console.error("Failed to parse Mastra stream chunk:", error);
            }
          }
        }

        updateAssistantMessage((current) => ({
          ...current,
          content:
            current.content.trim().length > 0
              ? current.content
              : current.chartSpec
                ? "I've created a chart for you."
                : "I processed your request.",
        }));
        setTransientStatus(null);
      } else {
        const response = await fetch(generateChartWithSqlUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-organization-id": organizationId,
            ...headers,
          },
          body: JSON.stringify({
            prompt: messageText,
            dashboardId,
            datasourceIds: selectedDatasourceIds.length > 0 ? selectedDatasourceIds : undefined,
            tenantFieldName: tenantFieldName.trim() || undefined,
            ...(hideTenantInputs ? {} : { previewTenantId: previewTenantId.trim() || undefined }),
            conversationHistory: messages.map((m) => ({ role: m.role, content: m.content })),
            ...(querypanelSessionId ? { querypanelSessionId } : {}),
            ...(chartModel.trim() ? { model: chartModel.trim() } : {}),
          }),
        });

        if (!response.ok) {
          throw new Error("Failed to generate chart");
        }

        const data = await response.json();

        const assistantMessage: Message = {
          id: `assistant-${Date.now()}`,
          role: "assistant",
          content: data.message || "I've created a chart for you!",
          chartSpec: data.chartSpec,
          rationale: data.rationale,
          sql: data.sql,
          sqlParams: (data.params as Record<string, unknown> | null) ?? null,
          timestamp: new Date(),
        };

        setMessages((prev) => [...prev, assistantMessage]);
        setTransientStatus(null);
        if (data.sessionId) setQuerypanelSessionId(data.sessionId);
      }
    } catch {
      setTransientStatus(null);
      setMessages((prev) =>
        prev.filter((message) => message.id !== assistantMessageId)
      );
      setToolEvents((prev) =>
        prev.map((event) =>
          event.status === "running" ? { ...event, status: "failed", endedAt: Date.now(), error: "Failed" } : event
        )
      );

      const errorMessage: Message = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content:
          "Sorry, I couldn't generate that chart. Please try again or rephrase your request.",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleAddChartToEditor = (message: Message) => {
    onAddChart(
      message.chartSpec,
      message.rationale,
      message.sql,
      selectedDatasourceIds,
      message.sqlParams,
      tenantFieldName.trim() || undefined,
      hideTenantInputs ? undefined : previewTenantId.trim() || undefined
    );
  };

  const handlePromptKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleSendMessage();
    } else if (e.key === "Escape") {
      if (isNarrowLayout && mobileSettingsOpen) {
        e.preventDefault();
        setMobileSettingsOpen(false);
        return;
      }
      onClose();
    }
  };

  if (!isOpen) return null;

  const tenantSettingsVisible = dashboardType === "customer" && !hideTenantInputs;
  const datasourceSummaryShort =
    selectedDatasourceIds.length === 0
      ? "Tap to choose a datasource"
      : selectedDatasourceIds.length === 1
        ? "1 datasource selected"
        : `${selectedDatasourceIds.length} datasources selected`;

  const settingsPaneInner = (
    <>
      <div className="qp-ai-modal-settings-block">
        <span className="qp-ai-modal-settings-heading">Datasource</span>
        <DatasourceSelector
          organizationId={organizationId}
          selectedIds={selectedDatasourceIds}
          onSelectionChange={setSelectedDatasourceIds}
          selectionMode={useMastraStream ? "single" : "multiple"}
          datasourcesUrl={datasourcesUrl}
          headers={headers}
          darkMode={effectiveDarkMode}
          allowedIds={availableDatasourceIds}
          fullWidth
        />
      </div>

      {tenantSettingsVisible && (
        <div className="qp-ai-modal-settings-block qp-ai-modal-settings-tenant">
          <div>
            <label htmlFor={tenantFieldId} className="qp-ai-modal-label">
              Tenant field name (optional)
            </label>
            <input
              id={tenantFieldId}
              type="text"
              value={tenantFieldName}
              onChange={(e) => setTenantFieldName(e.target.value)}
              placeholder="tenant_id"
              className="qp-ai-modal-input"
            />
          </div>
          <div>
            <label htmlFor={previewTenantIdFieldId} className="qp-ai-modal-label">
              Preview as tenant ID (optional)
            </label>
            <input
              id={previewTenantIdFieldId}
              type="text"
              value={previewTenantId}
              onChange={(e) => setPreviewTenantId(e.target.value)}
              placeholder="tenant_a"
              className="qp-ai-modal-input"
            />
          </div>
        </div>
      )}
    </>
  );

  const modalContent = (
    <div
      data-qp-ai-modal
      data-theme={effectiveDarkMode ? "dark" : "light"}
      data-qp-ai-mobile-sheet-open={isNarrowLayout && mobileSettingsOpen ? "true" : undefined}
    >
      <button
        type="button"
        className="qp-ai-modal-backdrop"
        onClick={onClose}
        aria-label="Close modal"
      />

      <div className="qp-ai-modal-wrap">
        <div className="qp-ai-modal-dialog" role="dialog" aria-modal="true">
          <div className="qp-ai-modal-header">
            <div className="qp-ai-modal-header-inner">
              <div className="qp-ai-modal-header-brand">
                <div className="qp-ai-modal-header-icon">
                  <SparklesIcon className="w-4 h-4" style={{ color: "#fff" }} />
                </div>
                <div>
                  <h2 className="qp-ai-modal-title">{modalTitle}</h2>
                  <p className="qp-ai-modal-subtitle">
                    Describe your visualization in natural language
                  </p>
                </div>
              </div>
              <div className="qp-ai-modal-header-actions">
                {isLoading && (
                  <div className="qp-ai-modal-status-pill" aria-live="polite">
                    <span className="qp-ai-modal-status-dot" />
                    <span className="qp-ai-modal-status-text">{transientStatus ?? "Working"}</span>
                  </div>
                )}
                {!isLoading && querypanelSessionId && (
                  <div className="qp-ai-modal-session-pill" title={`Session: ${querypanelSessionId}`}>
                    <Wand2Icon className="w-4 h-4" />
                    <span className="qp-ai-modal-session-pill-text">Session</span>
                  </div>
                )}
              <button
                type="button"
                onClick={onClose}
                className="qp-ai-modal-close"
                aria-label="Close"
              >
                <XIcon className="w-4 h-4" />
              </button>
              </div>
            </div>
          </div>

          <div
            className={`qp-ai-modal-body qp-ai-modal-body-split${isNarrowLayout ? " qp-ai-modal-body-narrow" : ""}`}
          >
            <div
              className="qp-ai-modal-chat-pane"
              inert={isNarrowLayout && mobileSettingsOpen ? true : undefined}
            >
              <div className="qp-ai-modal-messages">
                {messages.length === 0 && (
                  <div className="qp-ai-modal-empty">
                    <div className="qp-ai-modal-empty-inner">
                      <div className="qp-ai-modal-empty-icon-wrap">
                        <SparklesIcon className="qp-ai-modal-empty-icon w-8 h-8" />
                      </div>
                      <h3 className="qp-ai-modal-empty-title">{createTitle}</h3>
                      <p className="qp-ai-modal-empty-text">
                        Describe your visualization in natural language
                      </p>
                      <div className="qp-ai-modal-suggestions">
                        <button
                          type="button"
                          className="qp-ai-modal-suggestion"
                          onClick={() => handleSendMessage("Show revenue by month as a line chart")}
                          disabled={isLoading}
                        >
                          Revenue by month
                        </button>
                        <button
                          type="button"
                          className="qp-ai-modal-suggestion"
                          onClick={() => handleSendMessage("Show top 10 customers by total spend as a bar chart")}
                          disabled={isLoading}
                        >
                          Top customers
                        </button>
                        <button
                          type="button"
                          className="qp-ai-modal-suggestion"
                          onClick={() => handleSendMessage("Break down signups by plan as a stacked bar chart")}
                          disabled={isLoading}
                        >
                          Signups by plan
                        </button>
                      </div>
                    </div>
                  </div>
                )}

                {messages.map((message) => {
                  const isActiveAssistant =
                    message.role === "assistant" && message.id === activeAssistantMessageId;
                  const hasVisibleAssistantState =
                    message.content.trim().length > 0 ||
                    Boolean(message.chartSpec) ||
                    (isActiveAssistant &&
                      (toolEvents.length > 0 || Boolean(lastSqlExecution)));

                  if (message.role === "assistant" && !hasVisibleAssistantState) {
                    return null;
                  }

                  return (
                    <div
                      key={message.id}
                      className={`qp-ai-modal-msg-row ${message.role === "user" ? "user" : ""}`}
                    >
                      <div className={`qp-ai-modal-msg-bubble ${message.role}`}>
                        {message.content.trim().length > 0 && <div>{message.content}</div>}

                        {message.role === "assistant" && message.id === activeAssistantMessageId && toolEvents.length > 0 && (
                          <div className="qp-ai-modal-toolrail" aria-label="Agent steps">
                            {toolEvents.map((event) => (
                              <ToolEventChip key={event.id} event={event} />
                            ))}
                          </div>
                        )}

                        {message.role === "assistant" && message.id === activeAssistantMessageId && lastSqlExecution && (
                          <div className="qp-ai-modal-data-card" aria-label="Query results preview">
                            <div className="qp-ai-modal-data-card-head">
                              <div className="qp-ai-modal-data-summary">
                                <span className="qp-ai-modal-data-pill">
                                  Rows: {lastSqlExecution.rowCount}
                                </span>
                                <span className="qp-ai-modal-data-pill secondary">
                                  Columns: {(lastSqlExecution.fields.length || Object.keys(lastSqlExecution.rows[0] ?? {}).length)}
                                </span>
                                {lastSqlExecution.database && (
                                  <span className="qp-ai-modal-data-pill secondary">
                                    DB: {lastSqlExecution.database}
                                  </span>
                                )}
                              </div>
                            </div>
                            <div className="qp-ai-modal-table-wrap">
                              <table className="qp-ai-modal-table">
                                <thead>
                                  <tr>
                                    {(lastSqlExecution.fields.length
                                      ? lastSqlExecution.fields
                                      : Object.keys(lastSqlExecution.rows[0] ?? {})
                                    ).map((field) => (
                                      <th key={field}>{field}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {lastSqlExecution.rows.slice(0, 3).map((row, idx) => (
                                    <tr key={idx}>
                                      {(lastSqlExecution.fields.length
                                        ? lastSqlExecution.fields
                                        : Object.keys(lastSqlExecution.rows[0] ?? {})
                                      ).map((field) => (
                                        <td key={field}>{String(row[field] ?? "")}</td>
                                      ))}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </div>
                        )}

                        {Boolean(message.chartSpec) && (
                          <div className="qp-ai-modal-chart-card">
                            <div className="qp-ai-modal-chart-card-head">
                              <div className="qp-ai-modal-chart-card-badge">
                                <BarChart3Icon className="w-3 h-3" style={{ color: "#fff" }} />
                              </div>
                              <span className="qp-ai-modal-chart-card-title">Ready to add</span>
                              <button
                                type="button"
                                onClick={() => handleAddChartToEditor(message)}
                                className="qp-ai-modal-add-chart-btn"
                              >
                                Add Chart
                              </button>
                            </div>
                            <div className="qp-ai-modal-chart-card-preview">
                              <ChartPreview
                                chartSpec={withVizSpecDataFallback(
                                  message.chartSpec,
                                  message.id === activeAssistantMessageId
                                    ? (lastSqlExecution?.rows ?? null)
                                    : null
                                )}
                                darkMode={effectiveDarkMode}
                              />
                            </div>
                          </div>
                        )}

                      </div>
                    </div>
                  );
                })}

                <div ref={messagesEndRef} />
              </div>

              {isNarrowLayout && (
                <div className="qp-ai-modal-mobile-settings-strip">
                  <button
                    ref={mobileSettingsTriggerRef}
                    type="button"
                    className={`qp-ai-modal-mobile-settings-trigger${
                      selectedDatasourceIds.length === 0 ? " qp-ai-modal-mobile-settings-trigger-attention" : ""
                    }`}
                    onClick={() => setMobileSettingsOpen(true)}
                    aria-expanded={mobileSettingsOpen}
                    aria-controls="qp-ai-chart-mobile-settings-sheet"
                  >
                    <span className="qp-ai-modal-mobile-settings-trigger-icon" aria-hidden>
                      <SlidersHorizontalIcon className="w-4 h-4" />
                    </span>
                    <span className="qp-ai-modal-mobile-settings-trigger-copy">
                      <span className="qp-ai-modal-mobile-settings-trigger-kicker">Data & preview</span>
                      <span className="qp-ai-modal-mobile-settings-trigger-line">{datasourceSummaryShort}</span>
                    </span>
                    {tenantSettingsVisible && previewTenantId.trim() ? (
                      <span className="qp-ai-modal-mobile-settings-tenant-pill" title="Preview tenant">
                        {previewTenantId.trim()}
                      </span>
                    ) : null}
                    <ChevronUpIcon className="qp-ai-modal-mobile-settings-open-hint w-4 h-4" aria-hidden />
                  </button>
                </div>
              )}

              <div className="qp-ai-modal-footer">
                <div className="qp-ai-modal-pill-compose">
                  <input
                    ref={promptInputRef}
                    type="text"
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    onKeyDown={handlePromptKeyDown}
                    placeholder="What chart do you want?"
                    className="qp-ai-modal-pill-input"
                    aria-label="Chart prompt"
                    autoComplete="off"
                  />
                  {chartModelOptions.length > 0 && (
                    <select
                      id={chartModelId}
                      className="qp-ai-modal-pill-model"
                      value={chartModel}
                      onChange={(e) => setChartModel(e.target.value)}
                      disabled={isLoading}
                      title="Model"
                      aria-label="Model"
                    >
                      {chartModelOptions.map((opt, i) => (
                        <option key={`${i}-${opt.label}`} value={opt.value}>
                          {opt.label}
                        </option>
                      ))}
                    </select>
                  )}
                  <button
                    type="button"
                    onClick={() => handleSendMessage()}
                    disabled={!inputValue.trim() || isLoading}
                    className="qp-ai-modal-pill-send"
                    aria-label="Send message"
                  >
                    <SendIcon className="w-4 h-4" style={{ color: "#fff" }} />
                  </button>
                </div>
              </div>
            </div>

            {!isNarrowLayout && (
              <aside className="qp-ai-modal-settings-pane" aria-label="Chart settings">
                {settingsPaneInner}
              </aside>
            )}

            {isNarrowLayout && mobileSettingsOpen ? (
              <div
                className="qp-ai-modal-mobile-sheet-root"
                role="presentation"
                onClick={() => setMobileSettingsOpen(false)}
              >
                <div
                  id="qp-ai-chart-mobile-settings-sheet"
                  className="qp-ai-modal-mobile-sheet"
                  role="region"
                  aria-labelledby={mobileSettingsSheetTitleId}
                  onClick={(e) => e.stopPropagation()}
                >
                  <div className="qp-ai-modal-mobile-sheet-handle-wrap" aria-hidden>
                    <span className="qp-ai-modal-mobile-sheet-handle" />
                  </div>
                  <div className="qp-ai-modal-mobile-sheet-toolbar">
                    <h3 className="qp-ai-modal-mobile-sheet-title" id={mobileSettingsSheetTitleId}>
                      Data & preview
                    </h3>
                    <button
                      ref={mobileSettingsDoneRef}
                      type="button"
                      className="qp-ai-modal-mobile-sheet-done"
                      onClick={() => setMobileSettingsOpen(false)}
                    >
                      Done
                    </button>
                  </div>
                  <p className="qp-ai-modal-mobile-sheet-subtitle">
                    Pick where the chart runs and how tenant preview is applied.
                  </p>
                  <div className="qp-ai-modal-mobile-sheet-body">{settingsPaneInner}</div>
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </div>
  );

  if (typeof document === "undefined" || !document.body) {
    return modalContent;
  }
  return createPortal(modalContent, document.body);
}
