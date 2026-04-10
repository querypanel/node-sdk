"use client";

import type { QueryResult as QueryResultType, ThemeColors, ColorPreset, SqlModifications, VizModifications } from "../types";
import { VizSpecRenderer } from "./VizSpecRenderer";
import type { VizSpec } from "./VizSpecRenderer";
import { DataTable } from "./DataTable";
import { ChartControls } from "./ChartControls";
import { getColorsByPreset } from "../themes";

export interface QueryResultProps {
  /** Query result data */
  result: QueryResultType;
  /** Original query string */
  query?: string;
  /** Whether modifications are loading */
  isLoading?: boolean;
  /** Current color preset */
  colorPreset?: ColorPreset;
  /** Callback when chart is modified */
  onModify?: (options: {
    sqlModifications?: SqlModifications;
    vizModifications?: VizModifications;
    colorPreset?: ColorPreset;
  }) => void;
  /** Theme colors override */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Show chart controls */
  showControls?: boolean;
  /** Show SQL section */
  showSql?: boolean;
  /** Show data table */
  showTable?: boolean;
  /** Show Vega spec */
  showSpec?: boolean;
  /** Custom copy handler */
  onCopy?: (content: string, type: "sql" | "spec") => void;
}

const defaultColors: Partial<ThemeColors> = {
  primary: "#8B5CF6",
  secondary: "#3B82F6",
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.2)",
  surface: "rgba(0,0,0,0.4)",
  background: "#0a0612",
  error: "#EF4444",
};

export function QueryResult({
  result,
  query,
  isLoading = false,
  colorPreset = "default",
  onModify,
  colors = defaultColors,
  className = "",
  showControls = true,
  showSql = true,
  showTable = true,
  showSpec = false,
}: QueryResultProps) {
  const mergedColors = { ...defaultColors, ...colors };
  const chartColors = { ...mergedColors, ...getColorsByPreset(colorPreset) };

  const styles = {
    container: {
      display: "flex",
      flexDirection: "column" as const,
      gap: "1.5rem",
    },
    card: {
      borderRadius: "1rem",
      overflow: "hidden",
      backgroundColor: mergedColors.surface,
      border: `1px solid ${mergedColors.border}`,
    },
    cardHeader: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      padding: "1.5rem",
      borderBottom: `1px solid ${mergedColors.border}`,
    },
    cardHeaderLeft: {
      display: "flex",
      alignItems: "center",
      gap: "0.75rem",
    },
    iconBox: {
      width: "2.5rem",
      height: "2.5rem",
      borderRadius: "0.5rem",
      background: `linear-gradient(135deg, ${mergedColors.primary}33, ${mergedColors.secondary}33)`,
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
    },
    cardTitle: {
      fontSize: "1.125rem",
      fontWeight: 600,
      color: mergedColors.text,
    },
    cardSubtitle: {
      fontSize: "0.875rem",
      color: mergedColors.muted,
    },
    badge: {
      display: "inline-flex",
      alignItems: "center",
      gap: "0.25rem",
      padding: "0.25rem 0.75rem",
      borderRadius: "9999px",
      backgroundColor: "rgba(16, 185, 129, 0.2)",
      color: "#34D399",
      fontSize: "0.75rem",
      fontWeight: 500,
    },
    cardBody: {
      padding: "1.5rem",
      minHeight: "400px",
      position: "relative" as const,
    },
    loadingOverlay: {
      position: "absolute" as const,
      inset: 0,
      borderRadius: "0.5rem",
      backgroundColor: "rgba(0,0,0,0.5)",
      backdropFilter: "blur(4px)",
      display: "flex",
      alignItems: "center" as const,
      justifyContent: "center" as const,
      zIndex: 10,
    },
    loadingText: {
      fontSize: "0.875rem",
      color: mergedColors.text,
    },
    sqlCard: {
      borderRadius: "1rem",
      backgroundColor: mergedColors.surface,
      border: `1px solid ${mergedColors.border}`,
      overflow: "hidden",
    },
    sqlHeader: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      padding: "1rem 1.25rem",
      borderBottom: `1px solid ${mergedColors.border}`,
    },
    sqlTitle: {
      display: "flex",
      alignItems: "center",
      gap: "0.5rem",
      fontWeight: 500,
      color: mergedColors.text,
    },
    sqlBody: {
      padding: "1.25rem",
    },
    pre: {
      fontSize: "0.875rem",
      color: mergedColors.muted,
      fontFamily: "monospace",
      whiteSpace: "pre-wrap" as const,
      wordBreak: "break-word" as const,
      margin: 0,
    },
    rationale: {
      fontSize: "0.75rem",
      color: mergedColors.muted,
      fontStyle: "italic",
      borderTop: `1px solid ${mergedColors.border}`,
      padding: "1rem 1.25rem",
    },
    tableCard: {
      borderRadius: "1rem",
      backgroundColor: mergedColors.surface,
      border: `1px solid ${mergedColors.border}`,
      overflow: "hidden",
    },
    tableHeader: {
      display: "flex",
      alignItems: "center",
      gap: "0.75rem",
      padding: "1rem 1.25rem",
      borderBottom: `1px solid ${mergedColors.border}`,
    },
    rowBadge: {
      fontSize: "0.75rem",
      padding: "0.125rem 0.5rem",
      borderRadius: "9999px",
      border: `1px solid ${mergedColors.border}`,
      color: mergedColors.primary,
    },
  };

  const hasChart = result.chart?.vizSpec;
  const hasData = result.rows && result.rows.length > 0 && result.fields;

  return (
    <div className={className} style={styles.container}>
      {/* Chart Card */}
      {hasChart && (
        <div style={styles.card}>
          <div style={styles.cardHeader}>
            <div style={styles.cardHeaderLeft}>
              <div style={styles.iconBox}>
                <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke={mergedColors.primary} strokeWidth="2">
                  <rect x="3" y="3" width="18" height="18" rx="2" />
                  <path d="M3 9h18M9 21V9" />
                </svg>
              </div>
              <div>
                <div style={styles.cardTitle}>Visualization</div>
                {query && <div style={styles.cardSubtitle}>&ldquo;{query}&rdquo;</div>}
              </div>
            </div>
            <div style={styles.badge}>
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <polyline points="20 6 9 17 4 12" />
              </svg>
              Generated
            </div>
          </div>
          <div style={styles.cardBody}>
            {showControls && result.fields && onModify && (
              <ChartControls
                fields={result.fields}
                disabled={isLoading}
                onApply={onModify}
                colors={mergedColors}
              />
            )}
            <div style={{ position: "relative" }}>
              <VizSpecRenderer
                spec={result.chart!.vizSpec as unknown as VizSpec}
                data={result.rows || []}
                colors={chartColors as ThemeColors}
              />
              {isLoading && (
                <div style={styles.loadingOverlay}>
                  <div style={styles.loadingText}>Applying changes...</div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* SQL Card */}
      {showSql && result.sql && (
        <div style={styles.sqlCard}>
          <div style={styles.sqlHeader}>
            <div style={styles.sqlTitle}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#3B82F6" strokeWidth="2">
                <polyline points="16 18 22 12 16 6" />
                <polyline points="8 6 2 12 8 18" />
              </svg>
              Generated SQL
            </div>
          </div>
          <div style={styles.sqlBody}>
            <pre style={styles.pre}>{result.sql}</pre>
          </div>
          {result.rationale && (
            <div style={styles.rationale}>{result.rationale}</div>
          )}
        </div>
      )}

      {/* Data Table Card */}
      {showTable && hasData && (
        <div style={styles.tableCard}>
          <div style={styles.tableHeader}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={mergedColors.primary} strokeWidth="2">
              <rect x="3" y="3" width="7" height="7" />
              <rect x="14" y="3" width="7" height="7" />
              <rect x="14" y="14" width="7" height="7" />
              <rect x="3" y="14" width="7" height="7" />
            </svg>
            <span style={{ fontWeight: 500, color: mergedColors.text }}>Results</span>
            <span style={styles.rowBadge}>{result.rows!.length} rows</span>
          </div>
          <DataTable rows={result.rows!} fields={result.fields!} colors={mergedColors} />
        </div>
      )}

      {/* VizSpec Card */}
      {showSpec && hasChart && (
        <div style={styles.sqlCard}>
          <div style={styles.sqlHeader}>
            <div style={styles.sqlTitle}>
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke={mergedColors.muted} strokeWidth="2">
                <polyline points="16 18 22 12 16 6" />
                <polyline points="8 6 2 12 8 18" />
              </svg>
              VizSpec
            </div>
          </div>
          <div style={{ ...styles.sqlBody, maxHeight: "16rem", overflow: "auto" }}>
            <pre style={{ ...styles.pre, fontSize: "0.75rem" }}>
              {JSON.stringify(result.chart!.vizSpec, null, 2)}
            </pre>
          </div>
        </div>
      )}
    </div>
  );
}
