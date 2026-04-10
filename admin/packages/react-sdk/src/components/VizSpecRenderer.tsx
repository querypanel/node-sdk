import type { ThemeColors } from "../types";
import { VizSpecChart } from "./VizSpecChart";
import type { ChartSpec } from "./VizSpecChart";
import { VizSpecTable } from "./VizSpecTable";
import type { TableSpec } from "./VizSpecTable";
import { VizSpecMetric } from "./VizSpecMetric";
import type { MetricSpec } from "./VizSpecMetric";

// VizSpec union type
export type VizSpec = ChartSpec | TableSpec | MetricSpec;

export interface VizSpecRendererProps {
  spec: VizSpec;
  data: Array<Record<string, unknown>>;
  colors: ThemeColors;
}

export function VizSpecRenderer({ spec, data, colors }: VizSpecRendererProps) {
  switch (spec.kind) {
    case "chart":
      return <VizSpecChart spec={spec} data={data} colors={colors} />;
    case "table":
      return <VizSpecTable spec={spec} data={data} colors={colors} />;
    case "metric":
      return <VizSpecMetric spec={spec} data={data} colors={colors} />;
    default:
      return (
        <div style={{ padding: "1rem", color: colors.error }}>
          Unsupported visualization type
        </div>
      );
  }
}
