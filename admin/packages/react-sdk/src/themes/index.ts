import type { Theme, ThemeColors, ColorPreset } from "../types";

/** Default dark theme colors */
export const defaultColors: ThemeColors = {
  primary: "#8B5CF6",
  secondary: "#3B82F6",
  tertiary: "#6366F1",
  accent: "#A855F7",
  range: [
    "#8B5CF6",
    "#3B82F6",
    "#6366F1",
    "#A855F7",
    "#2DD4BF",
    "#FBBF24",
    "#F87171",
    "#4ADE80",
  ],
  text: "#F1F5F9",
  muted: "#94A3B8",
  grid: "rgba(139,92,246,0.1)",
  background: "#0a0612",
  surface: "rgba(0,0,0,0.4)",
  border: "rgba(139,92,246,0.2)",
  error: "#EF4444",
};

/** Sunset theme colors */
export const sunsetColors: ThemeColors = {
  primary: "#fb923c",
  secondary: "#f97316",
  tertiary: "#facc15",
  accent: "#f97316",
  range: [
    "#fb923c",
    "#f97316",
    "#facc15",
    "#ef4444",
    "#eab308",
    "#fbbf24",
    "#f97316",
    "#fb7185",
  ],
  text: "#F9FAFB",
  muted: "#E5E7EB",
  grid: "rgba(248, 113, 113, 0.12)",
  background: "#0a0612",
  surface: "rgba(0,0,0,0.4)",
  border: "rgba(251,146,60,0.2)",
  error: "#EF4444",
};

/** Emerald theme colors */
export const emeraldColors: ThemeColors = {
  primary: "#10b981",
  secondary: "#22c55e",
  tertiary: "#14b8a6",
  accent: "#22c55e",
  range: [
    "#10b981",
    "#22c55e",
    "#14b8a6",
    "#06b6d4",
    "#65a30d",
    "#4ade80",
    "#22c55e",
    "#84cc16",
  ],
  text: "#ECFDF5",
  muted: "#A7F3D0",
  grid: "rgba(16, 185, 129, 0.12)",
  background: "#0a0612",
  surface: "rgba(0,0,0,0.4)",
  border: "rgba(16,185,129,0.2)",
  error: "#EF4444",
};

/** Ocean theme colors */
export const oceanColors: ThemeColors = {
  primary: "#0ea5e9",
  secondary: "#6366f1",
  tertiary: "#22d3ee",
  accent: "#38bdf8",
  range: [
    "#0ea5e9",
    "#6366f1",
    "#22d3ee",
    "#38bdf8",
    "#4f46e5",
    "#14b8a6",
    "#0ea5e9",
    "#1e3a8a",
  ],
  text: "#E0F2FE",
  muted: "#93C5FD",
  grid: "rgba(56, 189, 248, 0.12)",
  background: "#0a0612",
  surface: "rgba(0,0,0,0.4)",
  border: "rgba(14,165,233,0.2)",
  error: "#EF4444",
};

/** Light-mode overrides for UI surfaces and text. Chart colors (primary, range, etc.) stay the same. */
export function getColorsForMode(base: ThemeColors, darkMode: boolean): ThemeColors {
  if (darkMode) return base;
  return {
    ...base,
    text: "#0f172a",
    muted: "#64748b",
    grid: "rgba(15, 23, 42, 0.08)",
    background: "#ffffff",
    surface: "#f8fafc",
    border: "rgba(15, 23, 42, 0.12)",
  };
}

/** Get colors by preset name */
export function getColorsByPreset(preset: ColorPreset): ThemeColors {
  switch (preset) {
    case "sunset":
      return sunsetColors;
    case "emerald":
      return emeraldColors;
    case "ocean":
      return oceanColors;
    default:
      return defaultColors;
  }
}

/** Default theme */
export const defaultTheme: Theme = {
  name: "default",
  colors: defaultColors,
  borderRadius: "0.75rem",
  fontFamily: "system-ui, -apple-system, sans-serif",
};

/** Create a custom theme by merging with defaults */
export function createTheme(overrides: Partial<Theme>): Theme {
  return {
    ...defaultTheme,
    ...overrides,
    colors: {
      ...defaultTheme.colors,
      ...overrides.colors,
    },
  };
}
