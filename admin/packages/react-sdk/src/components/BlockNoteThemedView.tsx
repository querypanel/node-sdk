import type React from "react";
import { useState } from "react";
import { BlockNoteView } from "@blocknote/mantine";
import type { ThemeColors } from "../types";
import { defaultColors } from "../themes";

export interface BlockNoteThemedViewProps {
  /** BlockNote editor instance */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  editor: any;
  /** Whether editor is editable */
  editable?: boolean;
  /** Optional view class name */
  className?: string;
  /** Optional inline styles */
  style?: React.CSSProperties;
  /** Optional child UI overrides */
  children?: React.ReactNode;
  /** Whether to use dark mode palette mapping */
  darkMode?: boolean;
  /** QueryPanel theme colors used to skin BlockNote */
  themeColors?: ThemeColors;
  /** Optional font family for editor text/UI */
  fontFamily?: string;
  /** Disable default slash menu when using custom SuggestionMenuController */
  slashMenu?: boolean;
}

function toRgba(color: string, alpha: number): string {
  const trimmed = color.trim();
  const hexMatch = /^#([A-Fa-f0-9]{6})$/.exec(trimmed);
  if (!hexMatch) return color;

  const hex = hexMatch[1];
  const r = Number.parseInt(hex.slice(0, 2), 16);
  const g = Number.parseInt(hex.slice(2, 4), 16);
  const b = Number.parseInt(hex.slice(4, 6), 16);
  return `rgba(${r}, ${g}, ${b}, ${alpha})`;
}

function buildCssVariables(colors: ThemeColors, darkMode: boolean, fontFamily?: string): React.CSSProperties {
  const textColor = darkMode ? "#ffffff" : "#0f172a";
  const editorBackground = darkMode ? colors.background : "#ffffff";
  const menuBackground = darkMode ? colors.surface : "#ffffff";
  const border = darkMode ? colors.border : toRgba(colors.primary, 0.3);
  const shadow = darkMode ? "0 8px 24px rgba(0,0,0,0.45)" : "0 8px 24px rgba(15,23,42,0.12)";
  const hovered = darkMode ? toRgba(colors.primary, 0.2) : toRgba(colors.primary, 0.12);
  const selected = darkMode ? toRgba(colors.primary, 0.3) : toRgba(colors.primary, 0.2);

  return {
    "--bn-border-radius": "8px",
    "--bn-font-family": fontFamily || "system-ui, -apple-system, sans-serif",
    "--bn-colors-editor-background": editorBackground,
    "--bn-colors-editor-text": textColor,
    "--bn-colors-menu-background": menuBackground,
    "--bn-colors-menu-text": textColor,
    "--bn-colors-tooltip-background": menuBackground,
    "--bn-colors-tooltip-text": textColor,
    "--bn-colors-hovered-background": hovered,
    "--bn-colors-selected-background": selected,
    "--bn-colors-disabled-text": colors.muted,
    "--bn-colors-border": border,
    "--bn-colors-side-menu": colors.primary,
    "--bn-colors-shadow": shadow,
    "--bn-colors-highlights-blue-background": toRgba(colors.secondary, darkMode ? 0.26 : 0.18),
    "--bn-colors-highlights-green-background": toRgba(colors.tertiary, darkMode ? 0.26 : 0.18),
    "--bn-colors-highlights-purple-background": toRgba(colors.primary, darkMode ? 0.26 : 0.18),
    "--bn-colors-highlights-red-background": toRgba(colors.error, darkMode ? 0.26 : 0.18),
    "--bn-colors-highlights-yellow-background": "rgba(251, 191, 36, 0.22)",
    "--bn-colors-highlights-blue-text": textColor,
    "--bn-colors-highlights-green-text": textColor,
    "--bn-colors-highlights-purple-text": textColor,
    "--bn-colors-highlights-red-text": textColor,
    "--bn-colors-highlights-yellow-text": textColor,
  } as React.CSSProperties;
}

/**
 * Wrapper around BlockNoteView that maps QueryPanel theme colors to BlockNote UI variables.
 */
export function BlockNoteThemedView({
  darkMode = false,
  themeColors = defaultColors,
  fontFamily,
  style,
  ...props
}: BlockNoteThemedViewProps) {
  const cssVariables = buildCssVariables(themeColors, darkMode, fontFamily);

  // Lock BlockNote's theme to the initial value to avoid flushSync errors.
  // Dark/light styling changes are handled entirely via CSS variables above,
  // so there's no visual impact from keeping the theme prop stable.
  // useState initializer only runs once, so subsequent darkMode prop changes
  // won't update this value.
  const [lockedTheme] = useState<"dark" | "light">(() => (darkMode ? "dark" : "light"));

  const stableKey = [
    fontFamily || "",
    themeColors.primary,
    themeColors.secondary,
    themeColors.tertiary,
    themeColors.background,
    themeColors.surface,
    themeColors.border,
    themeColors.text,
  ].join("|");

  return (
    <BlockNoteView
      key={stableKey}
      {...props}
      theme={lockedTheme}
      style={{ ...cssVariables, ...style }}
    />
  );
}
