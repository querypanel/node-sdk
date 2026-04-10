import { defineRegistry } from "@json-render/react";
import { catalog } from "./catalog";
import {
  BarChartComponent,
  DataTable,
  LineChartComponent,
  MetricCard,
  PieChartComponent,
} from "./charts";

export const { registry } = defineRegistry(catalog, {
  components: {
    Metric: MetricCard,
    DataTable,
    BarChart: BarChartComponent,
    LineChart: LineChartComponent,
    PieChart: PieChartComponent,
  },
});
