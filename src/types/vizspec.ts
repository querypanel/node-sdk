/**
 * VizSpec types for flexible visualization specifications
 * Supports chart, table, and metric visualizations
 */

// ============================================================================
// Field Types and Formatting
// ============================================================================

export type FieldType =
  | 'quantitative' // numbers you sum/avg
  | 'temporal'     // dates/times
  | 'ordinal'      // ordered categories (1st, 2nd, 3rd)
  | 'nominal'      // unordered categories (names, IDs)
  | 'boolean';

export interface ValueFormat {
  style?: 'number' | 'currency' | 'percent' | 'date' | 'time' | 'datetime';
  currency?: string; // e.g. "USD"
  minimumFractionDigits?: number;
  maximumFractionDigits?: number;
  dateStyle?: 'short' | 'medium' | 'long';
}

export interface FieldRef {
  field: string;        // column name from the SQL result
  label?: string;       // human-friendly label
  type?: FieldType;
  format?: ValueFormat; // optional formatting hint
}

export type AggregateOp = 'sum' | 'avg' | 'min' | 'max' | 'count' | 'distinct';

export type TimeUnit = 'year' | 'quarter' | 'month' | 'week' | 'day' | 'hour' | 'minute';

export interface AxisField extends FieldRef {
  aggregate?: AggregateOp; // usually on y
  timeUnit?: TimeUnit;     // usually on temporal x
}

export interface MetricField extends FieldRef {
  aggregate?: AggregateOp;
}

export interface SortSpec {
  field: string;            // column to sort by
  direction?: 'asc' | 'desc'; // default "asc"
}

// ============================================================================
// Base Spec
// ============================================================================

export interface VizSpecBase {
  version: '1.0';
  kind: 'chart' | 'table' | 'metric';
  title?: string;
  description?: string;
  data: {
    sourceId: string; // Identifier for the SQL result this spec refers to
  };
}

// ============================================================================
// Chart Spec
// ============================================================================

export type ChartType = 'line' | 'bar' | 'area' | 'scatter' | 'pie';

export type StackingMode = 'none' | 'stacked' | 'percent';

export interface ChartEncoding {
  chartType: ChartType;

  // Axes / measure fields
  x?: AxisField;              // usually categorical or temporal
  y?: AxisField | AxisField[]; // one or more measures

  // Optional: split into multiple series (colors/lines)
  series?: FieldRef;          // e.g. "region"

  // Optional: how to stack if multiple series on same axis
  stacking?: StackingMode;

  // Display-only concerns
  sort?: SortSpec;            // order of categories / points
  limit?: number;             // max rows to display

  // Tooltip fields to show on hover
  tooltips?: FieldRef[];
}

export interface ChartSpec extends VizSpecBase {
  kind: 'chart';
  encoding: ChartEncoding;
}

// ============================================================================
// Table Spec
// ============================================================================

export type TextAlign = 'left' | 'right' | 'center';

export interface TableColumn extends FieldRef {
  width?: number;      // optional, in px
  align?: TextAlign;   // default: left; numeric often right
  isHidden?: boolean;  // if true, don't render this column
}

export interface TableEncoding {
  columns: TableColumn[];
  sort?: SortSpec;
  limit?: number; // max rows to show (e.g., top 50)
}

export interface TableSpec extends VizSpecBase {
  kind: 'table';
  encoding: TableEncoding;
}

// ============================================================================
// Metric Spec
// ============================================================================

export type ComparisonMode = 'delta' | 'deltaPercent' | 'ratio';

export interface MetricTrend {
  timeField: AxisField;   // e.g. "date"
  valueField: MetricField; // value over time for sparkline
}

export interface MetricEncoding {
  valueField: MetricField; // main KPI

  // optional comparison (e.g. vs previous period)
  comparisonField?: MetricField;
  comparisonMode?: ComparisonMode; // default could be "deltaPercent"

  // optional tiny timeseries/sparkline
  trend?: MetricTrend;
}

export interface MetricSpec extends VizSpecBase {
  kind: 'metric';
  encoding: MetricEncoding;
}

// ============================================================================
// Union Type
// ============================================================================

export type VizSpec = ChartSpec | TableSpec | MetricSpec;
