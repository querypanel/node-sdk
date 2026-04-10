/* eslint-disable react-hooks/rules-of-hooks */
"use client";

import { createReactBlockSpec } from "@blocknote/react";
import { useMemo, useState } from "react";
import type { ThemeColors } from "../../types";
import { useThemeContext } from "../../context/ThemeContext";
import { getColorsForMode } from "../../themes";
import { PersistedSpecRenderer } from "../generative-ui/PersistedSpecRenderer";

type JsonRenderChartBlockOptions = {
  colors: ThemeColors;
  queryResultBaseUrl?: string;
};

export function createJsonRenderChartBlockSpec({
  colors,
  queryResultBaseUrl = "/api/query-results",
}: JsonRenderChartBlockOptions) {
  return createReactBlockSpec(
    {
      type: "json-render-chart",
      propSchema: {
        jsonRenderSpec: {
          default: undefined,
          type: "string" as const,
        },
        title: {
          default: "Chart",
        },
        description: {
          default: "",
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
        resultId: {
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
        const [error, setError] = useState<string | null>(null);

        const parsedSpec = useMemo(() => {
          if (!props.block.props.jsonRenderSpec) return null;

          try {
            return JSON.parse(props.block.props.jsonRenderSpec as string);
          } catch (err) {
            setError(err instanceof Error ? err.message : "Invalid JSON Render spec");
            return null;
          }
        }, [props.block.props.jsonRenderSpec]);

        const datasourceIds = useMemo(() => {
          try {
            return JSON.parse(props.block.props.datasourceIds || "[]") as string[];
          } catch {
            return [];
          }
        }, [props.block.props.datasourceIds]);

        const chartHeight = (() => {
          const raw = props.block.props.height;
          if (typeof raw === "number" && Number.isFinite(raw)) return raw;
          if (typeof raw === "string") {
            const parsed = Number.parseInt(raw, 10);
            if (Number.isFinite(parsed)) return parsed;
          }
          return 400;
        })();

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
              {error ?? "Invalid JSON Render specification"}
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
                minHeight: `${chartHeight}px`,
                borderRadius: "0.5rem",
                background: resolvedColors.background,
                border: `1px solid ${resolvedColors.border}`,
                padding: "0.5rem",
                boxSizing: "border-box",
              }}
            >
              <PersistedSpecRenderer
                spec={parsedSpec}
                queryResultBaseUrl={queryResultBaseUrl}
              />
            </div>

            {props.block.props.rationale ? (
              <div
                style={{
                  marginTop: "0.75rem",
                  padding: "0.75rem",
                  fontSize: "0.8125rem",
                  lineHeight: 1.5,
                  color: resolvedColors.muted,
                  border: `1px solid ${resolvedColors.border}`,
                  borderRadius: "0.5rem",
                }}
              >
                {props.block.props.rationale}
              </div>
            ) : null}
          </div>
        );
      },
    }
  );
}
