import { useState, useEffect, useMemo } from "react";
import type { Block } from "@blocknote/core";
import { BlockNoteSchema, defaultBlockSpecs } from "@blocknote/core";
import { useCreateBlockNote } from "@blocknote/react";
import type { ThemeColors } from "../types";
import { defaultTheme, defaultColors } from "../themes";
import { BlockNoteThemedView } from "./BlockNoteThemedView";
import { createChartBlockSpec } from "./blocks/ChartBlock";
import { ThemeProvider } from "../context/ThemeContext";
import { normalizeBlockNoteContent } from "./blocknoteContent";

export interface DashboardEditorProps {
  /** Initial BlockNote content as JSON string */
  initialContent: string;
  /** Customer backend base URL */
  apiBaseUrl?: string;
  /** Callback when save is clicked */
  onSave: (content: string) => Promise<void>;
  /** Whether to use dark theme */
  darkMode?: boolean;
  /** Theme colors used by BlockNote UI */
  themeColors?: ThemeColors;
  /** Font family override for editor UI/text */
  fontFamily?: string;
  /** Custom CSS class */
  className?: string;
}

/**
 * Editable dashboard editor using BlockNote
 */
export function DashboardEditor({
  initialContent,
  apiBaseUrl = "",
  onSave,
  darkMode = false,
  themeColors = defaultColors,
  fontFamily = defaultTheme.fontFamily,
  className = "",
}: DashboardEditorProps) {
  const [isSaving, setIsSaving] = useState(false);
  const [mounted, setMounted] = useState(false);

  // Parse content once when it changes
  const parsedContent = useMemo(() => {
    try {
      if (initialContent) {
        return normalizeBlockNoteContent(JSON.parse(initialContent), darkMode);
      }
    } catch (e) {
      console.error("Failed to parse initial content:", e);
    }
    // Return default empty content if parsing fails
    return [
      {
        type: "paragraph",
        content: [],
      },
    ];
  }, [initialContent, darkMode]);

  const chartBlockSpec = useMemo(
    () => createChartBlockSpec({ apiBaseUrl, colors: themeColors }),
    [apiBaseUrl, themeColors]
  );

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

  useEffect(() => {
    setMounted(true);
  }, []);

  const handleSave = async () => {
    setIsSaving(true);
    try {
      const content = JSON.stringify(editor.document);
      await onSave(content);
    } finally {
      setIsSaving(false);
    }
  };

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
    <div className={className}>
      {/* Save button */}
      <div className="flex justify-end mb-4">
        <button
          type="button"
          onClick={handleSave}
          disabled={isSaving}
          className="px-4 py-2 text-sm font-medium text-white disabled:opacity-60 rounded-lg transition-opacity"
          style={{ backgroundColor: themeColors.primary }}
        >
          {isSaving ? "Saving..." : "Save Changes"}
        </button>
      </div>

      {/* Editor */}
      <ThemeProvider darkMode={darkMode}>
        <div
          className="border rounded-lg overflow-hidden"
          data-theme={darkMode ? "dark" : "light"}
          style={{ borderColor: themeColors.border }}
        >
          <BlockNoteThemedView
            editor={editor}
            darkMode={darkMode}
            themeColors={themeColors}
            fontFamily={fontFamily}
          />
        </div>
      </ThemeProvider>
    </div>
  );
}
