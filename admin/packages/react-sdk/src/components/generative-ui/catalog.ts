import { defineCatalog } from "@json-render/core";
import { schema } from "@json-render/react/schema";
import { z } from "zod";

const dataPointSchema = z.object({
  label: z.string(),
  value: z.number(),
});

export const catalog = defineCatalog(schema, {
  components: {
    Metric: {
      props: z.object({
        label: z.string(),
        value: z.string(),
        description: z.string().nullable(),
      }),
      slots: [] as string[],
      description: "A metric card showing a label, large value, and optional description.",
      example: {
        label: "Revenue",
        value: "$12,345",
        description: "+12% from last month",
      },
    },
    DataTable: {
      props: z.object({
        resultId: z.string().nullable().optional(),
        headers: z.array(z.string()).optional(),
        rows: z.array(z.array(z.string())).optional(),
        caption: z.string().nullable().optional(),
      }),
      slots: [] as string[],
      description: "A data table. Use resultId when the dataset is larger than a small inline preview.",
      example: {
        resultId: "abc123",
        caption: "Query results",
      },
    },
    BarChart: {
      props: z.object({
        resultId: z.string().nullable().optional(),
        title: z.string().nullable().optional(),
        data: z.array(dataPointSchema).optional(),
        xLabel: z.string().nullable().optional(),
        yLabel: z.string().nullable().optional(),
      }),
      slots: [] as string[],
      description: "A bar chart visualizing categorical data.",
      example: {
        resultId: "abc123",
        title: "Sales by Region",
      },
    },
    LineChart: {
      props: z.object({
        resultId: z.string().nullable().optional(),
        title: z.string().nullable().optional(),
        data: z.array(dataPointSchema).optional(),
        xLabel: z.string().nullable().optional(),
        yLabel: z.string().nullable().optional(),
      }),
      slots: [] as string[],
      description: "A line chart visualizing trends over time or sequential data.",
      example: {
        resultId: "abc123",
        title: "Monthly Revenue",
      },
    },
    PieChart: {
      props: z.object({
        resultId: z.string().nullable().optional(),
        title: z.string().nullable().optional(),
        data: z.array(dataPointSchema).optional(),
      }),
      slots: [] as string[],
      description: "A pie chart visualizing proportional data.",
      example: {
        resultId: "abc123",
        title: "Market Share",
      },
    },
  },
  actions: {},
});
