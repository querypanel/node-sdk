// Modal and other component styles (so hosts get CSS when they import the SDK)
import "./components/AIChartModal.css";

// Components
export { VizSpecRenderer, type VizSpecRendererProps, type VizSpec } from "./components/VizSpecRenderer";
export { VizSpecChart, type VizSpecChartProps, type ChartSpec } from "./components/VizSpecChart";
export { VizSpecTable, type VizSpecTableProps, type TableSpec } from "./components/VizSpecTable";
export { VizSpecMetric, type VizSpecMetricProps, type MetricSpec } from "./components/VizSpecMetric";
export { DataTable, type DataTableProps } from "./components/DataTable";
export { ChartControls, type ChartControlsProps } from "./components/ChartControls";
export { QueryInput, type QueryInputProps } from "./components/QueryInput";
export { QueryResult, type QueryResultProps } from "./components/QueryResult";
export { LoadingState, type LoadingStateProps } from "./components/LoadingState";
export { ErrorState, type ErrorStateProps } from "./components/ErrorState";
export { EmptyState, type EmptyStateProps } from "./components/EmptyState";
export { QuerypanelEmbedded, type QuerypanelEmbeddedProps } from "./components/QuerypanelEmbedded";
export { DashboardViewer, type DashboardViewerProps } from "./components/DashboardViewer";
export { DashboardEditor, type DashboardEditorProps } from "./components/DashboardEditor";
export { DashboardAiEditor, type DashboardAiEditorProps } from "./components/DashboardAiEditor";
export {
  AIChartModal,
  DEFAULT_AI_CHART_MODEL_OPTIONS,
  type AIChartModalProps,
  type AIChartModelOption,
} from "./components/AIChartModal";
export { DatasourceSelector, type DatasourceSelectorProps } from "./components/DatasourceSelector";
export { DeploySuccessModal, type DeploySuccessModalProps } from "./components/DeploySuccessModal";
export { BlockNoteThemedView, type BlockNoteThemedViewProps } from "./components/BlockNoteThemedView";

// Utilities
export { formatValue } from "./utils/formatters";

// Context & Hooks
export {
  QueryPanelProvider,
  useQueryPanel,
  type QueryPanelProviderProps,
} from "./context/QueryPanelContext";

// Types
export type {
  ChartType,
  TimeUnit,
  SqlModifications,
  VizModifications,
  ChartResponse,
  QueryResult as QueryResultType,
  ColorPreset,
  ThemeColors,
  Theme,
  QueryPanelConfig,
  PromptChip,
  Dashboard,
  EmbedBranding,
} from "./types";

// Themes
export {
  defaultTheme,
  defaultColors,
  sunsetColors,
  emeraldColors,
  oceanColors,
  getColorsByPreset,
  getColorsForMode,
  createTheme,
} from "./themes";

// Theme context (for chart blocks and other theme-aware components)
export { ThemeProvider, useThemeContext } from "./context/ThemeContext";
