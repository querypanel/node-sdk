/* eslint-disable react-hooks/rules-of-hooks */
"use client";

import { createReactBlockSpec } from "@blocknote/react";
import { useEffect, useMemo, useState } from "react";
import { VizSpecRenderer, type VizSpec } from "../VizSpecRenderer";
import { useThemeContext } from "../../context/ThemeContext";
import { getColorsForMode } from "../../themes";
import type { ThemeColors } from "../../types";
import { runDedupedRequest } from "../../utils/requestDedup";

type ChartBlockOptions = {
  apiBaseUrl: string;
  colors: ThemeColors;
  /** Override URL for run-sql (e.g. /api/dashboards/run-sql for admin). If not set, uses apiBaseUrl + /query/run-sql */
  runSqlUrl?: string;
  organizationId?: string;
  dashboardId?: string;
  headers?: Record<string, string>;
};

const EMPTY_HEADERS: Record<string, string> = {};

function getHeadersSignature(headers: Record<string, string>) {
  return JSON.stringify(
    Object.entries(headers).sort(([a], [b]) => a.localeCompare(b))
  );
}

function isVizSpec(value: unknown): value is VizSpec {
  if (!value || typeof value !== "object") return false;
  const kind = (value as { kind?: unknown }).kind;
  return kind === "chart" || kind === "table" || kind === "metric";
}

export function createChartBlockSpec({ apiBaseUrl, colors, runSqlUrl, organizationId, dashboardId, headers = EMPTY_HEADERS }: ChartBlockOptions) {
  return createReactBlockSpec(
    {
      type: "chart",
      propSchema: {
        chartSpec: {
          default: undefined,
          type: "string" as const,
        },
        title: {
          default: "Chart",
        },
        rationale: {
          default: "",
        },
        sql: {
          default: "",
        },
        sqlParams: {
          default: "{}",
          type: "string" as const,
        },
        datasourceIds: {
          default: "[]",
          type: "string" as const,
        },
        tenantFieldName: {
          default: "",
        },
        previewTenantId: {
          default: "",
        },
        tenantId: {
          default: "",
        },
        width: {
          default: "100%",
        },
        height: {
          default: 400,
        },
      },
      content: "none",
    },
    {
      render: (props) => {
        const { darkMode } = useThemeContext();
        const resolvedColors = getColorsForMode(colors, darkMode);
        const [data, setData] = useState<Array<Record<string, unknown>> | null>(null);
        const [error, setError] = useState<string | null>(null);
        const [isLoading, setIsLoading] = useState(false);

        const chartHeight = (() => {
          const raw = props.block.props.height;
          if (typeof raw === "number" && Number.isFinite(raw)) return raw;
          if (typeof raw === "string") {
            const parsed = Number.parseInt(raw, 10);
            if (Number.isFinite(parsed)) return parsed;
          }
          return 400;
        })();

        const parsedSpec = useMemo(() => {
          if (!props.block.props.chartSpec) return null;
          try {
            const rawSpec = JSON.parse(props.block.props.chartSpec) as VizSpec & {
              data?: Array<Record<string, unknown>>;
            };

            if (!isVizSpec(rawSpec)) {
              throw new Error("Unsupported chart specification");
            }
            return {
              spec: rawSpec,
              embeddedData: Array.isArray(rawSpec.data) ? rawSpec.data : [],
            };
          } catch (err) {
            setError(err instanceof Error ? err.message : "Invalid chart spec");
            return null;
          }
        }, [props.block.props.chartSpec]);

        const sqlParams = useMemo(() => {
          try {
            return JSON.parse(props.block.props.sqlParams || "{}") as Record<string, unknown>;
          } catch {
            return {};
          }
        }, [props.block.props.sqlParams]);

        const datasourceIds = useMemo(() => {
          try {
            return JSON.parse(props.block.props.datasourceIds || "[]") as string[];
          } catch {
            return [];
          }
        }, [props.block.props.datasourceIds]);

        // biome-ignore lint/correctness/useExhaustiveDependencies: values come from createChartBlockSpec closure
        useEffect(() => {
          if (!parsedSpec) return;
          if (Array.isArray(parsedSpec.embeddedData) && parsedSpec.embeddedData.length > 0) {
            setData(parsedSpec.embeddedData);
            return;
          }

          if (!props.block.props.sql || datasourceIds.length === 0) {
            setData([]);
            return;
          }

          const url = runSqlUrl ?? `${apiBaseUrl.replace(/\/+$/, "")}/query/run-sql`;
          const tenantFieldName = props.block.props.tenantFieldName?.trim() || undefined;
          const previewTenantId =
            props.block.props.previewTenantId?.trim() || props.block.props.tenantId?.trim() || undefined;
          const requestPayload = {
            sql: props.block.props.sql,
            datasourceIds,
            ...(dashboardId ? { dashboardId } : {}),
            params: sqlParams,
            ...(tenantFieldName && { tenantFieldName }),
            ...(previewTenantId && { previewTenantId }),
          };
          const requestKey = `run-sql:${url}:${JSON.stringify(requestPayload)}:${getHeadersSignature(headers)}`;

          let cancelled = false;
          const fetchData = async () => {
            setIsLoading(true);
            setError(null);
            try {
              const payload = await runDedupedRequest(
                requestKey,
                async () => {
                  const response = await fetch(url, {
                  method: "POST",
                  headers: {
                    "Content-Type": "application/json",
                    ...(organizationId ? { "x-organization-id": organizationId } : {}),
                    ...headers,
                  },
                  body: JSON.stringify(requestPayload),
                });

                const parsed = await response.json().catch(() => ({}));
                if (!response.ok) {
                  throw new Error(parsed?.error || "Failed to load chart data");
                }
                return parsed;
              },
                { cacheMs: 60_000 }
              );

              if (!cancelled) {
                setData(Array.isArray(payload?.rows) ? payload.rows : []);
              }
            } catch (err) {
              if (!cancelled) {
                setError(err instanceof Error ? err.message : "Failed to load chart data");
              }
            } finally {
              if (!cancelled) {
                setIsLoading(false);
              }
            }
          };

          void fetchData();
          return () => {
            cancelled = true;
          };
        }, [
          datasourceIds,
          sqlParams,
          parsedSpec,
          props.block.props.sql,
          props.block.props.tenantFieldName,
          props.block.props.previewTenantId,
          props.block.props.tenantId,
        ]);

        // Use embedded data immediately when present so first paint shows the chart (no wait for useEffect)
        const hasEmbeddedData =
          Array.isArray(parsedSpec?.embeddedData) && parsedSpec.embeddedData.length > 0;
        const effectiveData = hasEmbeddedData && parsedSpec
          ? parsedSpec.embeddedData
          : (data ?? []);

        if (!parsedSpec) {
          return (
            <div
              style={{
                width: "100%",
                minWidth: 0,
                padding: "1rem",
                color: resolvedColors.error,
                background: resolvedColors.surface,
                border: `1px solid ${resolvedColors.border}`,
                borderRadius: "0.75rem",
              }}
            >
              Invalid chart specification
            </div>
          );
        }

        if (isLoading && !hasEmbeddedData && !data) {
          return (
            <div
              style={{
                width: "100%",
                minWidth: 0,
                padding: "1rem",
                color: resolvedColors.muted,
                background: resolvedColors.surface,
                border: `1px solid ${resolvedColors.border}`,
                borderRadius: "0.75rem",
              }}
            >
              Loading chart data...
            </div>
          );
        }

        if (error) {
          return (
            <div
              style={{
                width: "100%",
                minWidth: 0,
                padding: "1rem",
                color: resolvedColors.error,
                background: resolvedColors.surface,
                border: `1px solid ${resolvedColors.border}`,
                borderRadius: "0.75rem",
              }}
            >
              {error}
            </div>
          );
        }

        return (
          <div
            style={{
              width: "100%",
              minWidth: 0,
              padding: "0.75rem",
              boxSizing: "border-box",
              background: resolvedColors.surface,
              border: `1px solid ${resolvedColors.border}`,
              borderRadius: "0.75rem",
            }}
          >
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: "0.5rem",
                color: resolvedColors.text,
              }}
            >
              <div style={{ fontSize: "0.875rem", fontWeight: 600 }}>
                {props.block.props.title || "Chart"}
              </div>
              <div style={{ fontSize: "0.75rem", color: resolvedColors.muted }}>
                {datasourceIds.length > 0
                  ? `${datasourceIds.length} datasource${datasourceIds.length > 1 ? "s" : ""}`
                  : ""}
              </div>
            </div>
            <div
              style={{
                width: "100%",
                minWidth: 0,
                height: `${chartHeight}px`,
                borderRadius: "0.5rem",
                background: resolvedColors.background,
                border: `1px solid ${resolvedColors.border}`,
                padding: "0.5rem",
                boxSizing: "border-box",
              }}
            >
              <VizSpecRenderer
                spec={parsedSpec.spec}
                data={effectiveData}
                colors={resolvedColors}
              />
            </div>
            {props.block.props.rationale ? (
              <div
                style={{
                  marginTop: "0.75rem",
                  padding: "0.75rem",
                  borderRadius: "0.5rem",
                  border: `1px solid ${resolvedColors.border}`,
                  color: resolvedColors.muted,
                  fontSize: "0.8rem",
                  lineHeight: 1.45,
                }}
              >
                <div style={{ marginBottom: "0.25rem", fontWeight: 600, color: resolvedColors.text }}>
                  Rationale
                </div>
                {props.block.props.rationale}
              </div>
            ) : null}
          </div>
        );
      },
    }
  );
}
