"use client";

import { useState, useEffect, useCallback, useMemo, useRef } from "react";
import type { Block } from "@blocknote/core";
import { RocketIcon, Save, SparklesIcon } from "lucide-react";
import { BlockNoteSchema, defaultBlockSpecs, filterSuggestionItems } from "@blocknote/core";
import {
  useCreateBlockNote,
  getDefaultReactSlashMenuItems,
  SuggestionMenuController,
} from "@blocknote/react";
import type { DefaultReactSuggestionItem } from "@blocknote/react";
import type { ThemeColors, EmbedBranding } from "../types";
import { defaultColors } from "../themes";
import { BlockNoteThemedView } from "./BlockNoteThemedView";
import { createChartBlockSpec } from "./blocks/ChartBlock";
import { AIChartModal } from "./AIChartModal";
import { DeploySuccessModal } from "./DeploySuccessModal";
import { ThemeProvider } from "../context/ThemeContext";
import { normalizeBlockNoteContent } from "./blocknoteContent";

const EMPTY_HEADERS: Record<string, string> = {};

function getSafeInitialBlocks(initialContent: string, darkMode: boolean) {
  try {
    if (initialContent) {
      const parsed = normalizeBlockNoteContent(JSON.parse(initialContent), darkMode);
      if (Array.isArray(parsed) && parsed.length > 0) {
        return parsed;
      }
    }
  } catch (error) {
    console.error("Failed to parse initial content:", error);
  }

  return [{ type: "paragraph", content: [] }];
}

const editorStyles = `
  .bn-container {
    min-height: calc(100vh - 16rem);
  }
  .bn-editor {
    min-height: calc(100vh - 16rem);
  }
  .ProseMirror {
    min-height: calc(100vh - 16rem);
    padding: 2rem;
  }
  [data-theme="light"] .bn-container,
  [data-theme="light"] .bn-editor,
  [data-theme="light"] .ProseMirror {
    background-color: hsl(0 0% 100%);
    color: hsl(222.2 84% 4.9%);
  }
  [data-theme="dark"] .bn-container,
  [data-theme="dark"] .bn-editor,
  [data-theme="dark"] .ProseMirror {
    background-color: hsl(222.2 84% 4.9%);
    color: hsl(210 40% 98%);
  }
  [data-theme="dark"] .bn-container {
    border-color: hsl(217.2 32.6% 17.5%);
  }
  [data-theme="dark"] .bn-menu-dropdown,
  [data-theme="dark"] .bn-suggestion-menu {
    background-color: hsl(217.2 32.6% 17.5%);
    border-color: hsl(217.2 32.6% 17.5%);
  }
  [data-theme="dark"] .bn-menu-item:hover {
    background-color: hsl(217.2 32.6% 25%);
  }
`;

export interface DashboardAiEditorProps {
  /** Initial BlockNote content as JSON string */
  initialContent: string;
  /** Callback when save or deploy is triggered */
  onSave: (content: string, options?: { deploy?: boolean }) => Promise<void>;
  /** Organization ID (required for AI modal) */
  organizationId?: string;
  /** Dashboard ID (required for AI modal) */
  dashboardId?: string;
  /** Dashboard type for tenant preview UI */
  dashboardType?: "customer" | "internal";
  /** Show deploy button (admin flow) */
  showDeployButton?: boolean;
  /** Show save button in editor toolbar. Set false when parent provides Save (e.g. embed toolbar). */
  showSaveButton?: boolean;
  /** Whether editor is editable. Set false for viewer mode. */
  editable?: boolean;
  /** Customer/embed API base URL. When empty, uses admin defaults (/api/...) */
  apiBaseUrl?: string;
  /** Override run-sql URL (e.g. /api/dashboards/run-sql for admin) */
  runSqlUrl?: string;
  /** Override AI generate-chart-with-sql URL */
  generateChartWithSqlUrl?: string;
  /** Override datasources URL */
  datasourcesUrl?: string;
  /** Dark mode. When undefined, auto-detects from DOM (html class/data-theme). */
  darkMode?: boolean;
  /** Theme colors */
  themeColors?: ThemeColors;
  /** Font family */
  fontFamily?: string;
  /** Extra headers for AI/datasource requests */
  headers?: Record<string, string>;
  /** CSS class */
  className?: string;
  /** Optional reset key to force reloading initialContent */
  contentResetKey?: string | number;
  /** Optional ref to trigger save from parent (e.g. toolbar Save button) */
  saveRef?: React.MutableRefObject<(() => void | Promise<void>) | null>;
  /** Optional extra content in toolbar (e.g. "Datasources & tenant" button) */
  toolbarExtra?: React.ReactNode;
  /** When set, only these datasource IDs are offered in AI chart modal (e.g. from dashboard.available_datasource_ids). */
  availableDatasourceIds?: string[] | null;
  /** Default tenant field name in AI chart modal (e.g. from dashboard.tenant_field_name). */
  tenantFieldName?: string | null;
  /** Per-datasource tenant field name (e.g. from dashboard.tenant_field_by_datasource). Overrides tenantFieldName for each datasource. */
  tenantFieldByDatasource?: Record<string, string> | null;
  /** When true, AI modal does not show tenant field / preview tenant ID (e.g. customer embed; tenant from JWT only). */
  hideTenantInputsInAiModal?: boolean;
  /** Whitelabel strings for embed (e.g. AI modal title). Passed from QuerypanelEmbedded branding. */
  embedBranding?: EmbedBranding;
}

export function DashboardAiEditor({
  initialContent,
  onSave,
  organizationId,
  dashboardId,
  dashboardType = "customer",
  showDeployButton = true,
  showSaveButton = true,
  editable = true,
  apiBaseUrl = "",
  runSqlUrl,
  generateChartWithSqlUrl = "/api/ai/generate-chart-with-sql",
  datasourcesUrl = "/api/datasources",
  darkMode,
  themeColors = defaultColors,
  fontFamily,
  headers = EMPTY_HEADERS,
  className = "",
  contentResetKey,
  saveRef,
  toolbarExtra,
  availableDatasourceIds,
  tenantFieldName: tenantFieldNameProp,
  tenantFieldByDatasource,
  hideTenantInputsInAiModal = false,
  embedBranding,
}: DashboardAiEditorProps) {
  const [isDeploying, setIsDeploying] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [mounted, setMounted] = useState(false);
  const [isAIModalOpen, setIsAIModalOpen] = useState(false);
  const [showDeploySuccess, setShowDeploySuccess] = useState(false);
  const lastLoadedSignatureRef = useRef<string | null>(null);

  // Detect dark mode from DOM to avoid flushSync when parent re-renders on theme change.
  // When darkMode prop is provided, use it as override; otherwise auto-detect from DOM.
  const [detectedDarkMode, setDetectedDarkMode] = useState(false);

  useEffect(() => {
    if (typeof document === "undefined") return;

    const html = document.documentElement;
    const detect = () => {
      const isDark =
        html.classList.contains("dark") ||
        html.getAttribute("data-theme") === "dark" ||
        html.style.colorScheme === "dark";
      return isDark;
    };

    // Sync initial value
    setDetectedDarkMode(detect());

    const observer = new MutationObserver(() => {
      const isDark = detect();
      // Use queueMicrotask so the state update runs outside React's current render
      queueMicrotask(() => setDetectedDarkMode(isDark));
    });

    observer.observe(html, {
      attributes: true,
      attributeFilter: ["class", "data-theme", "style"],
    });

    return () => observer.disconnect();
  }, []);

  // Use prop when explicitly provided; otherwise use DOM detection
  const effectiveDarkMode = darkMode !== undefined ? darkMode : detectedDarkMode;

  const resolvedRunSqlUrl = runSqlUrl ?? (apiBaseUrl ? `${apiBaseUrl.replace(/\/+$/, "")}/query/run-sql` : "/api/dashboards/run-sql");
  const resolvedGenerateChartWithSqlUrl = apiBaseUrl ? `${apiBaseUrl.replace(/\/+$/, "")}/ai/generate-chart-with-sql` : generateChartWithSqlUrl;
  const resolvedDatasourcesUrl = apiBaseUrl ? `${apiBaseUrl.replace(/\/+$/, "")}/datasources` : datasourcesUrl;

  const chartBlockSpec = useMemo(
    () =>
      createChartBlockSpec({
        apiBaseUrl: apiBaseUrl || "",
        colors: themeColors,
        runSqlUrl: resolvedRunSqlUrl,
        organizationId,
        dashboardId,
        headers,
      }),
    [apiBaseUrl, themeColors, resolvedRunSqlUrl, organizationId, dashboardId, headers]
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

  const contentSignature = `${String(contentResetKey ?? "")}::${initialContent}::${effectiveDarkMode ? "dark" : "light"}`;
  const parsedInitialContent = useMemo(
    () => getSafeInitialBlocks(initialContent, effectiveDarkMode) as Block[],
    [initialContent, effectiveDarkMode]
  );

  const editor = useCreateBlockNote({
    schema,
    initialContent: parsedInitialContent,
  }, [schema, contentSignature]);

  useEffect(() => {
    if (!mounted) return;
    lastLoadedSignatureRef.current = contentSignature;
  }, [mounted, contentSignature]);

  const insertAIChartItem = useCallback(
    (): DefaultReactSuggestionItem => ({
      title: "AI Chart Assistant",
      onItemClick: () => setIsAIModalOpen(true),
      aliases: ["ai", "chart", "assistant", "generate", "visualization", "graph"],
      group: "AI Tools",
      icon: <SparklesIcon size={18} />,
      subtext: "Generate charts with AI assistance",
      badge: "AI",
    }),
    []
  );

  const getCustomSlashMenuItems = useCallback(
    (): DefaultReactSuggestionItem[] => [
      ...getDefaultReactSlashMenuItems(editor),
      insertAIChartItem(),
    ],
    [editor, insertAIChartItem]
  );

  const handleAddChart = useCallback(
    (
      chartSpec: unknown,
      rationale?: string,
      sql?: string,
      datasourceIds?: string[],
      sqlParams?: Record<string, unknown> | null,
      tenantFieldName?: string,
      previewTenantId?: string
    ) => {
      editor.insertBlocks(
        [
          {
            type: "chart",
            props: {
              chartSpec: JSON.stringify(chartSpec),
              title: (chartSpec as { description?: string }).description || "Chart",
              rationale: rationale || "",
              sql: sql || "",
              sqlParams: JSON.stringify(sqlParams || {}),
              datasourceIds: JSON.stringify(datasourceIds || []),
              tenantFieldName: tenantFieldName || "",
              previewTenantId: previewTenantId || "",
            },
          },
        ],
        editor.getTextCursorPosition().block,
        "after"
      );
      setIsAIModalOpen(false);
    },
    [editor]
  );

  const stripDataFromChartBlocks = useCallback(() => {
    return editor.document.map((block) => {
      if (block.type === "chart" && block.props.chartSpec) {
        try {
          const spec = JSON.parse(block.props.chartSpec as string);
          if (spec.data) {
            const specWithoutData = { ...spec };
            delete specWithoutData.data;
            return {
              ...block,
              props: {
                ...block.props,
                chartSpec: JSON.stringify(specWithoutData),
              },
            };
          }
        } catch {
          // keep original on parse failure
        }
      }
      return block;
    });
  }, [editor]);

  const handleDeploy = async () => {
    setIsDeploying(true);
    try {
      const cleanedDocument = stripDataFromChartBlocks();
      const content = JSON.stringify(cleanedDocument);
      await onSave(content, { deploy: true });
      setShowDeploySuccess(true);
      setTimeout(() => setShowDeploySuccess(false), 3000);
    } finally {
      setIsDeploying(false);
    }
  };

  const handleSave = useCallback(async () => {
    setIsSaving(true);
    try {
      const cleanedDocument = stripDataFromChartBlocks();
      const content = JSON.stringify(cleanedDocument);
      await onSave(content);
    } finally {
      setIsSaving(false);
    }
  }, [stripDataFromChartBlocks, onSave]);

  useEffect(() => {
    if (saveRef) {
      saveRef.current = handleSave;
      return () => {
        saveRef.current = null;
      };
    }
  }, [saveRef, handleSave]);

  useEffect(() => {
    setMounted(true);
  }, []);

  if (!mounted) {
    return (
      <div
        className={`flex items-center justify-center py-16 ${className}`}
        style={{ minHeight: 200 }}
      >
        <div
          className="w-6 h-6 border-2 border-t-transparent rounded-full animate-spin"
          style={{ borderColor: themeColors.primary }}
        />
      </div>
    );
  }

  return (
    <>
      <style>{editorStyles}</style>
      <div className={`space-y-4 ${className}`}>
        {editable && (showDeployButton || showSaveButton || toolbarExtra) && (
          <div className="flex justify-end gap-2 flex-wrap">
            {toolbarExtra}
            {!showDeployButton && showSaveButton && (
              <button
                type="button"
                onClick={handleSave}
                disabled={isSaving}
                className="px-4 py-2 text-sm font-semibold rounded-lg border transition-opacity disabled:opacity-60"
                style={{
                  backgroundColor: themeColors.primary,
                  color: "#fff",
                  borderColor: themeColors.primary,
                }}
              >
                <Save className="w-4 h-4 inline-block mr-2 align-middle" />
                {isSaving ? "Saving..." : "Save"}
              </button>
            )}
            {showDeployButton && (
              <button
                type="button"
                onClick={handleDeploy}
                disabled={isDeploying}
                className="px-4 py-2 text-sm font-semibold rounded-lg transition-opacity disabled:opacity-60"
                style={{
                  background: "linear-gradient(to right, #9333ea, #4f46e5, #9333ea)",
                  color: "#fff",
                }}
              >
                <RocketIcon className="w-4 h-4 inline-block mr-2 align-middle" />
                {isDeploying ? "Deploying..." : "Deploy to Customers"}
              </button>
            )}
          </div>
        )}

        <ThemeProvider darkMode={effectiveDarkMode}>
          <div
            className="border rounded-lg overflow-hidden"
            data-theme={effectiveDarkMode ? "dark" : "light"}
            style={{ borderColor: themeColors.border }}
          >
            <BlockNoteThemedView
              editor={editor}
              editable={editable}
              darkMode={effectiveDarkMode}
              themeColors={themeColors}
              fontFamily={fontFamily}
              style={{ minHeight: "calc(100vh - 16rem)" }}
              slashMenu={false}
            >
              {editable && (
                <SuggestionMenuController
                  triggerCharacter="/"
                  getItems={async (query) =>
                    filterSuggestionItems(getCustomSlashMenuItems(), query)
                  }
                />
              )}
            </BlockNoteThemedView>
          </div>
        </ThemeProvider>
      </div>

      {editable && organizationId && dashboardId && (
        <AIChartModal
          isOpen={isAIModalOpen}
          onClose={() => setIsAIModalOpen(false)}
          onAddChart={handleAddChart}
          organizationId={organizationId}
          dashboardId={dashboardId}
          dashboardType={dashboardType}
          generateChartWithSqlUrl={resolvedGenerateChartWithSqlUrl}
          datasourcesUrl={resolvedDatasourcesUrl}
          headers={headers}
          darkMode={effectiveDarkMode}
          availableDatasourceIds={availableDatasourceIds}
          defaultTenantFieldName={tenantFieldNameProp}
          tenantFieldByDatasource={tenantFieldByDatasource}
          hideTenantInputs={hideTenantInputsInAiModal}
          title={embedBranding?.aiChartModalTitle}
          createTitle={embedBranding?.aiChartModalCreateTitle}
        />
      )}

      <DeploySuccessModal
        open={showDeploySuccess}
        onOpenChange={setShowDeploySuccess}
      />
    </>
  );
}
