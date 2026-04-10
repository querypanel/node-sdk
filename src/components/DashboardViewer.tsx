import { useEffect, useState, useMemo } from "react";
import type { Block } from "@blocknote/core";
import { BlockNoteSchema, defaultBlockSpecs } from "@blocknote/core";
import { useCreateBlockNote } from "@blocknote/react";
import { createChartBlockSpec } from "./blocks/ChartBlock";
import { defaultColors, defaultTheme } from "../themes";
import type { ThemeColors } from "../types";
import { BlockNoteThemedView } from "./BlockNoteThemedView";
import { ThemeProvider } from "../context/ThemeContext";
import { normalizeBlockNoteContent } from "./blocknoteContent";

function getViewerStyles(): string {
  return `
    .bn-container {
      min-height: 100%;
      border-color: transparent;
    }
    .bn-editor {
      min-height: 100%;
    }
    .ProseMirror {
      min-height: 100%;
      padding: 1.25rem;
    }
  `;
}

export interface DashboardViewerProps {
  /** BlockNote content as JSON string */
  content: string;
  /** Customer backend base URL */
  apiBaseUrl?: string;
  /** Whether to use dark theme */
  darkMode?: boolean;
  /** Theme colors used by BlockNote UI and chart blocks */
  themeColors?: ThemeColors;
  /** Font family override for editor UI/text */
  fontFamily?: string;
  /** Custom CSS class */
  className?: string;
}

/**
 * Read-only dashboard viewer using BlockNote
 */
export function DashboardViewer({
  content,
  apiBaseUrl = "",
  darkMode = false,
  themeColors = defaultColors,
  fontFamily = defaultTheme.fontFamily,
  className = "",
}: DashboardViewerProps) {
  const [mounted, setMounted] = useState(false);

  // Parse content once when it changes
  const parsedContent = useMemo(() => {
    try {
      if (content) {
        return normalizeBlockNoteContent(JSON.parse(content), darkMode);
      }
    } catch (e) {
      console.error("Failed to parse dashboard content:", e);
    }
    // Return default empty content if parsing fails
    return [
      {
        type: "paragraph",
        content: [],
      },
    ];
  }, [content, darkMode]);

  const chartBlockSpec = useMemo(
    () => createChartBlockSpec({ apiBaseUrl, colors: themeColors }),
    [apiBaseUrl, themeColors]
  );
  const viewerStyles = useMemo(() => getViewerStyles(), []);

  const schema = useMemo(
    () =>
      BlockNoteSchema.create({
        blockSpecs: {
          ...defaultBlockSpecs,
          chart: chartBlockSpec,
        },
      }),
    [chartBlockSpec]
  );

  // Create editor with parsed content
  const editor = useCreateBlockNote({
    schema,
    initialContent: parsedContent as Block[],
  });

  // Client-side only rendering to avoid hydration mismatch
  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setMounted(true);
  }, []);

  if (!mounted) {
    return (
      <div className={`flex items-center justify-center py-16 ${className}`}>
        <div
          className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin"
          style={{ borderColor: "currentColor" }}
        />
      </div>
    );
  }

  return (
    <ThemeProvider darkMode={darkMode}>
      <div className={className} data-theme={darkMode ? "dark" : "light"}>
        <style>{viewerStyles}</style>
        <BlockNoteThemedView
          editor={editor}
          editable={false}
          darkMode={darkMode}
          themeColors={themeColors}
          fontFamily={fontFamily}
        />
      </div>
    </ThemeProvider>
  );
}
