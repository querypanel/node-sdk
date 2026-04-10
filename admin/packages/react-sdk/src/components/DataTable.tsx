"use client";

import type { ThemeColors } from "../types";
import { formatTimestampForDisplay } from "../utils/formatters";

export interface DataTableProps {
  /** Array of row data */
  rows: Array<Record<string, unknown>>;
  /** Column field names */
  fields: string[];
  /** Maximum rows to display (default: 10) */
  maxRows?: number;
  /** Theme colors */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Custom cell renderer */
  renderCell?: (value: unknown, field: string, row: Record<string, unknown>) => React.ReactNode;
}

const defaultColors: Partial<ThemeColors> = {
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.2)",
  surface: "rgba(0,0,0,0.4)",
  primary: "#8B5CF6",
};

export function DataTable({
  rows,
  fields,
  maxRows = 10,
  colors = defaultColors,
  className = "",
  renderCell,
}: DataTableProps) {
  const displayRows = rows.slice(0, maxRows);
  const mergedColors = { ...defaultColors, ...colors };

  const styles = {
    container: {
      overflowX: "auto" as const,
    },
    table: {
      width: "100%",
      fontSize: "0.875rem",
      borderCollapse: "collapse" as const,
    },
    th: {
      padding: "0.75rem 1rem",
      textAlign: "left" as const,
      fontWeight: 600,
      color: mergedColors.text,
      textTransform: "uppercase" as const,
      fontSize: "0.75rem",
      letterSpacing: "0.05em",
      borderBottom: `1px solid ${mergedColors.border}`,
    },
    td: {
      padding: "0.75rem 1rem",
      color: mergedColors.text,
      borderBottom: `1px solid rgba(255,255,255,0.05)`,
    },
    tr: {
      transition: "background-color 0.15s",
    },
    footer: {
      fontSize: "0.75rem",
      color: mergedColors.muted,
      marginTop: "0.75rem",
      paddingLeft: "1rem",
    },
  };

  return (
    <div className={className} style={styles.container}>
      <table style={styles.table}>
        <thead>
          <tr>
            {fields.map((field) => (
              <th key={field} style={styles.th}>
                {field}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {displayRows.map((row, idx) => {
            const rowKey =
              fields.map((f) => String(row[f] ?? "")).join("-") || `row-${idx}`;
            return (
              <tr
                key={rowKey}
                style={styles.tr}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = `${mergedColors.primary}10`;
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = "transparent";
                }}
              >
                {fields.map((field) => (
                  <td key={field} style={styles.td}>
                    {renderCell
                      ? renderCell(row[field], field, row)
                      : formatTimestampForDisplay(row[field])}
                  </td>
                ))}
              </tr>
            );
          })}
        </tbody>
      </table>
      {rows.length > maxRows && (
        <p style={styles.footer}>
          Showing {maxRows} of {rows.length} rows
        </p>
      )}
    </div>
  );
}
