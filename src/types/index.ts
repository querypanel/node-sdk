// VizSpec types are now exported from VizSpecRenderer component

/** Chart type options */
export type ChartType =
  | "bar" // horizontal bars (categorical on Y-axis)
  | "column" // vertical bars (categorical on X-axis)
  | "line"
  | "area"
  | "scatter"
  | "pie";

/** Time granularity for date-based queries */
export type TimeUnit = "day" | "week" | "month" | "quarter" | "year";

/** SQL modification options */
export interface SqlModifications {
  timeGranularity?: TimeUnit;
  dateRange?: {
    from?: string;
    to?: string;
  };
}

/** Visualization modification options */
export interface VizModifications {
  chartType?: ChartType;
  xAxis?: { field: string };
  yAxis?: { field: string };
}

/** Chart response from API */
export interface ChartResponse {
  vegaLiteSpec?: Record<string, unknown> | null;
  vizSpec?: Record<string, unknown> | null;
  specType: "vega-lite" | "vizspec";
  notes: string | null;
}

/** Full query result from API */
export interface QueryResult {
  success: boolean;
  sql?: string;
  params?: Record<string, unknown>;
  rationale?: string;
  rows?: Array<Record<string, unknown>>;
  fields?: string[];
  chart?: ChartResponse;
  modified?: {
    sqlChanged: boolean;
    vizChanged: boolean;
  };
  error?: string;
}

/** Color theme preset names */
export type ColorPreset = "default" | "sunset" | "emerald" | "ocean";

/** Theme color configuration */
export interface ThemeColors {
  primary: string;
  secondary: string;
  tertiary: string;
  accent: string;
  range: string[];
  text: string;
  muted: string;
  grid: string;
  background: string;
  surface: string;
  border: string;
  error: string;
}

/** Full theme configuration */
export interface Theme {
  name: string;
  colors: ThemeColors;
  borderRadius: string;
  fontFamily: string;
}

/** Provider configuration */
export interface QueryPanelConfig {
  /** API endpoint for ask queries */
  askEndpoint: string;
  /** API endpoint for chart modifications */
  modifyEndpoint?: string;
  /** Default color preset */
  colorPreset?: ColorPreset;
  /** Custom theme override */
  theme?: Partial<Theme>;
  /** Custom fetch function for API calls */
  fetcher?: (url: string, options: RequestInit) => Promise<Response>;
}

/** Prompt chip configuration */
export interface PromptChip {
  text: string;
  key: string;
  icon?: React.ComponentType<{ className?: string }>;
  emoji?: string;
}

/**
 * Optional overrides for user-visible strings in QuerypanelEmbedded (whitelabel).
 * Omit any field to keep the default text.
 */
export interface EmbedBranding {
  /** Toolbar badge when the user has a customized fork (default: "Customized") */
  customizedBadge?: string;
  /** Button to start customizing (default: "Customize Dashboard") */
  customizeButton?: string;
  /** Button to enter edit mode (default: "Edit") */
  editButton?: string;
  /** Button to reset fork to original (default: "Reset to Original") */
  resetButton?: string;
  /** Save button label (default: "Save") */
  saveButton?: string;
  /** Save button label while saving (default: "Saving...") */
  savingLabel?: string;
  /** Cancel button label (default: "Cancel") */
  cancelButton?: string;
  /** Confirm dialog message for reset (default: "Reset to original dashboard? Your customizations will be lost.") */
  resetConfirmMessage?: string;
  /** AI Chart modal title (default: "AI Chart Generator") */
  aiChartModalTitle?: string;
  /** AI Chart modal empty-state heading (default: "Create a Chart") */
  aiChartModalCreateTitle?: string;
}

/** Dashboard from API */
export interface Dashboard {
  id: string;
  organization_id: string;
  name: string;
  description: string | null;
  status: "draft" | "deployed";
  dashboard_type?: "customer" | "internal";
  content_json: string | null;
  widget_config: Record<string, unknown> | null;
  editor_type: "blocknote" | "custom";
  is_customer_fork: boolean;
  forked_from_dashboard_id: string | null;
  tenant_id: string | null;
  datasource_id: string | null;
  /** Datasource IDs allowed for this dashboard when customers add/modify charts. Null = all org datasources. */
  available_datasource_ids?: string[] | null;
  /** Default column name for tenant isolation. Used when no per-datasource override is set. */
  tenant_field_name?: string | null;
  /** Per-datasource tenant column name: { [datasourceId]: "tenant_id" | "customer_id" }. Overrides tenant_field_name per datasource. */
  tenant_field_by_datasource?: Record<string, string> | null;
  version: number;
  deployed_at: string | null;
  created_at: string;
  updated_at: string;
  created_by: string | null;
}
