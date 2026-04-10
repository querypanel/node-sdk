"use client";

import { useState } from "react";
import type {
  ChartType,
  TimeUnit,
  ColorPreset,
  SqlModifications,
  VizModifications,
  ThemeColors,
} from "../types";

export interface ChartControlsProps {
  /** Available field names from the query result */
  fields: string[];
  /** Whether controls are disabled */
  disabled?: boolean;
  /** Callback when changes are applied */
  onApply: (options: {
    sqlModifications?: SqlModifications;
    vizModifications?: VizModifications;
    colorPreset?: ColorPreset;
  }) => void;
  /** Theme colors */
  colors?: Partial<ThemeColors>;
  /** Additional class name */
  className?: string;
  /** Show color preset selector */
  showColorPresets?: boolean;
}

const CHART_TYPES: { label: string; value: ChartType }[] = [
  { label: "Column (Vertical)", value: "column" },
  { label: "Bar (Horizontal)", value: "bar" },
  { label: "Line", value: "line" },
  { label: "Area", value: "area" },
  { label: "Scatter", value: "scatter" },
  { label: "Pie", value: "pie" },
];

const TIME_GRAIN: { label: string; value: TimeUnit }[] = [
  { label: "Day", value: "day" },
  { label: "Week", value: "week" },
  { label: "Month", value: "month" },
  { label: "Quarter", value: "quarter" },
  { label: "Year", value: "year" },
];

const COLOR_PRESETS: { label: string; value: ColorPreset }[] = [
  { label: "Default", value: "default" },
  { label: "Sunset", value: "sunset" },
  { label: "Emerald", value: "emerald" },
  { label: "Ocean", value: "ocean" },
];

const defaultColors: Partial<ThemeColors> = {
  primary: "#8B5CF6",
  text: "#F1F5F9",
  muted: "#94A3B8",
  border: "rgba(139,92,246,0.3)",
  surface: "rgba(0,0,0,0.4)",
  background: "#0a0612",
};

export function ChartControls({
  fields,
  disabled = false,
  onApply,
  colors = defaultColors,
  className = "",
  showColorPresets = true,
}: ChartControlsProps) {
  const [chartType, setChartType] = useState<ChartType | "">("");
  const [xField, setXField] = useState<string>("");
  const [yField, setYField] = useState<string>("");
  const [timeGranularity, setTimeGranularity] = useState<TimeUnit | "">("");
  const [dateFrom, setDateFrom] = useState<string>("");
  const [dateTo, setDateTo] = useState<string>("");
  const [colorPreset, setColorPreset] = useState<ColorPreset>("default");

  const mergedColors = { ...defaultColors, ...colors };

  const handleApply = () => {
    const vizModifications: VizModifications = {};
    const sqlModifications: SqlModifications = {};

    if (chartType) vizModifications.chartType = chartType;
    if (xField) vizModifications.xAxis = { field: xField };
    if (yField) vizModifications.yAxis = { field: yField };
    if (timeGranularity) sqlModifications.timeGranularity = timeGranularity;
    if (dateFrom || dateTo) {
      sqlModifications.dateRange = {
        from: dateFrom || undefined,
        to: dateTo || undefined,
      };
    }

    onApply({
      vizModifications: Object.keys(vizModifications).length > 0 ? vizModifications : undefined,
      sqlModifications: Object.keys(sqlModifications).length > 0 ? sqlModifications : undefined,
      colorPreset,
    });
  };

  const handleReset = () => {
    setChartType("");
    setXField("");
    setYField("");
    setTimeGranularity("");
    setDateFrom("");
    setDateTo("");
    setColorPreset("default");
    onApply({ colorPreset: "default" });
  };

  const styles = {
    container: {
      marginBottom: "1.5rem",
      borderRadius: "0.75rem",
      border: `1px solid ${mergedColors.border}`,
      backgroundColor: mergedColors.surface,
      padding: "0.75rem 1rem",
    },
    header: {
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      gap: "0.75rem",
      marginBottom: "0.75rem",
    },
    title: {
      fontSize: "0.875rem",
      fontWeight: 500,
      color: mergedColors.text,
    },
    buttonGroup: {
      display: "flex",
      alignItems: "center",
      gap: "0.5rem",
    },
    button: {
      height: "1.75rem",
      padding: "0 0.75rem",
      fontSize: "0.75rem",
      borderRadius: "0.375rem",
      border: "none",
      cursor: disabled ? "not-allowed" : "pointer",
      opacity: disabled ? 0.5 : 1,
      transition: "all 0.15s",
    },
    resetButton: {
      backgroundColor: "transparent",
      color: mergedColors.muted,
    },
    applyButton: {
      background: `linear-gradient(to right, ${mergedColors.primary}, #3B82F6)`,
      color: "white",
    },
    grid: {
      display: "grid",
      gridTemplateColumns: "repeat(auto-fit, minmax(150px, 1fr))",
      gap: "0.75rem",
    },
    fieldGroup: {
      display: "flex",
      flexDirection: "column" as const,
      gap: "0.25rem",
    },
    label: {
      fontSize: "0.6875rem",
      textTransform: "uppercase" as const,
      letterSpacing: "0.05em",
      color: mergedColors.muted,
    },
    select: {
      width: "100%",
      borderRadius: "0.5rem",
      backgroundColor: mergedColors.background,
      border: `1px solid ${mergedColors.border}`,
      fontSize: "0.75rem",
      color: mergedColors.text,
      padding: "0.375rem 0.5rem",
      outline: "none",
    },
    input: {
      width: "100%",
      borderRadius: "0.5rem",
      backgroundColor: mergedColors.background,
      border: `1px solid ${mergedColors.border}`,
      fontSize: "0.625rem",
      color: mergedColors.text,
      padding: "0.375rem 0.5rem",
      outline: "none",
    },
    dateRow: {
      display: "grid",
      gridTemplateColumns: "1fr 1fr",
      gap: "0.375rem",
    },
  };

  return (
    <div className={className} style={styles.container}>
      <div style={styles.header}>
        <span style={styles.title}>Chart Controls</span>
        <div style={styles.buttonGroup}>
          <button
            type="button"
            disabled={disabled}
            onClick={handleReset}
            style={{ ...styles.button, ...styles.resetButton }}
          >
            Reset
          </button>
          <button
            type="button"
            disabled={disabled}
            onClick={handleApply}
            style={{ ...styles.button, ...styles.applyButton }}
          >
            Apply
          </button>
        </div>
      </div>

      <div style={styles.grid}>
        {/* Chart Type */}
        <div style={styles.fieldGroup}>
          <span style={styles.label}>Chart</span>
          <select
            value={chartType}
            onChange={(e) => setChartType(e.target.value as ChartType | "")}
            disabled={disabled}
            style={styles.select}
          >
            <option value="">Auto</option>
            {CHART_TYPES.map((t) => (
              <option key={t.value} value={t.value}>
                {t.label}
              </option>
            ))}
          </select>
        </div>

        {/* X Axis */}
        <div style={styles.fieldGroup}>
          <span style={styles.label}>X Axis</span>
          <select
            value={xField}
            onChange={(e) => setXField(e.target.value)}
            disabled={disabled}
            style={styles.select}
          >
            <option value="">Auto</option>
            {fields.map((f) => (
              <option key={f} value={f}>
                {f}
              </option>
            ))}
          </select>
        </div>

        {/* Y Axis */}
        <div style={styles.fieldGroup}>
          <span style={styles.label}>Y Axis</span>
          <select
            value={yField}
            onChange={(e) => setYField(e.target.value)}
            disabled={disabled}
            style={styles.select}
          >
            <option value="">Auto</option>
            {fields.map((f) => (
              <option key={f} value={f}>
                {f}
              </option>
            ))}
          </select>
        </div>

        {/* Time Granularity */}
        <div style={styles.fieldGroup}>
          <span style={styles.label}>Time</span>
          <select
            value={timeGranularity}
            onChange={(e) => setTimeGranularity(e.target.value as TimeUnit | "")}
            disabled={disabled}
            style={styles.select}
          >
            <option value="">Auto</option>
            {TIME_GRAIN.map((g) => (
              <option key={g.value} value={g.value}>
                {g.label}
              </option>
            ))}
          </select>
          <div style={styles.dateRow}>
            <input
              type="date"
              value={dateFrom}
              onChange={(e) => setDateFrom(e.target.value)}
              disabled={disabled}
              style={styles.input}
            />
            <input
              type="date"
              value={dateTo}
              onChange={(e) => setDateTo(e.target.value)}
              disabled={disabled}
              style={styles.input}
            />
          </div>
        </div>

        {/* Color Preset */}
        {showColorPresets && (
          <div style={styles.fieldGroup}>
            <span style={styles.label}>Colors</span>
            <select
              value={colorPreset}
              onChange={(e) => setColorPreset(e.target.value as ColorPreset)}
              disabled={disabled}
              style={styles.select}
            >
              {COLOR_PRESETS.map((p) => (
                <option key={p.value} value={p.value}>
                  {p.label}
                </option>
              ))}
            </select>
          </div>
        )}
      </div>
    </div>
  );
}
