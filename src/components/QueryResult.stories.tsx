import type { Meta, StoryObj } from "@storybook/react";
import { QueryResult } from "./QueryResult";
import { defaultColors, sunsetColors } from "../themes";
import type { QueryResult as QueryResultType } from "../types";

const meta: Meta<typeof QueryResult> = {
  title: "Components/QueryResult",
  component: QueryResult,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Combined display of chart, SQL, and data table. The main result component that brings everything together.",
      },
    },
  },
  argTypes: {
    result: {
      description: "Query result data from the API",
      control: "object",
    },
    query: {
      description: "Original query string",
      control: "text",
    },
    isLoading: {
      description: "Whether modifications are loading",
      control: "boolean",
    },
    colorPreset: {
      description: "Current color preset",
      control: "select",
      options: ["default", "sunset", "emerald", "ocean"],
    },
    showControls: {
      description: "Show chart controls",
      control: "boolean",
    },
    showSql: {
      description: "Show SQL section",
      control: "boolean",
    },
    showTable: {
      description: "Show data table",
      control: "boolean",
    },
    showSpec: {
      description: "Show Vega spec",
      control: "boolean",
    },
  },
};

export default meta;
type Story = StoryObj<typeof QueryResult>;

const sampleResult: QueryResultType = {
  success: true,
  sql: `SELECT 
  release_year,
  COUNT(*) as count
FROM netflix_shows
WHERE tenant_id = $1
GROUP BY release_year
ORDER BY release_year DESC
LIMIT 10;`,
  rationale: "This query groups Netflix content by release year and counts the number of titles for each year.",
  rows: [
    { release_year: 2021, count: 245 },
    { release_year: 2020, count: 312 },
    { release_year: 2019, count: 278 },
    { release_year: 2018, count: 198 },
    { release_year: 2017, count: 167 },
  ],
  fields: ["release_year", "count"],
  chart: {
    vegaLiteSpec: {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      data: {
        values: [
          { release_year: 2021, count: 245 },
          { release_year: 2020, count: 312 },
          { release_year: 2019, count: 278 },
          { release_year: 2018, count: 198 },
          { release_year: 2017, count: 167 },
        ],
      },
      mark: "bar",
      encoding: {
        x: { field: "release_year", type: "ordinal", title: "Year" },
        y: { field: "count", type: "quantitative", title: "Number of Titles" },
      },
      title: "Netflix Titles by Release Year",
    },
    specType: "vega-lite",
    notes: null,
  },
};

const pieChartResult: QueryResultType = {
  success: true,
  sql: `SELECT type, COUNT(*) as count FROM netflix_shows GROUP BY type;`,
  rows: [
    { type: "Movie", count: 6131 },
    { type: "TV Show", count: 2676 },
  ],
  fields: ["type", "count"],
  chart: {
    vegaLiteSpec: {
      $schema: "https://vega.github.io/schema/vega-lite/v5.json",
      data: {
        values: [
          { type: "Movie", count: 6131 },
          { type: "TV Show", count: 2676 },
        ],
      },
      mark: { type: "arc", innerRadius: 50 },
      encoding: {
        theta: { field: "count", type: "quantitative" },
        color: { field: "type", type: "nominal" },
      },
      title: "Movies vs TV Shows",
    },
    specType: "vega-lite",
    notes: null,
  },
};

export const Default: Story = {
  args: {
    result: sampleResult,
    query: "Shows added by year",
    colorPreset: "default",
    colors: defaultColors,
    showControls: true,
    showSql: true,
    showTable: true,
    showSpec: false,
    onModify: (options) => console.log("Modify:", options),
  },
};

export const WithAllSections: Story = {
  args: {
    result: sampleResult,
    query: "Shows added by year",
    colorPreset: "default",
    colors: defaultColors,
    showControls: true,
    showSql: true,
    showTable: true,
    showSpec: true,
    onModify: (options) => console.log("Modify:", options),
  },
  parameters: {
    docs: {
      description: {
        story: "Full result display including the Vega-Lite spec.",
      },
    },
  },
};

export const ChartOnly: Story = {
  args: {
    result: sampleResult,
    query: "Shows added by year",
    colorPreset: "default",
    colors: defaultColors,
    showControls: false,
    showSql: false,
    showTable: false,
    showSpec: false,
  },
  parameters: {
    docs: {
      description: {
        story: "Minimal display showing only the chart.",
      },
    },
  },
};

export const PieChart: Story = {
  args: {
    result: pieChartResult,
    query: "Movies vs TV Shows distribution",
    colorPreset: "default",
    colors: defaultColors,
    showControls: true,
    showSql: true,
    showTable: true,
    onModify: (options) => console.log("Modify:", options),
  },
};

export const Loading: Story = {
  args: {
    result: sampleResult,
    query: "Shows added by year",
    isLoading: true,
    colorPreset: "default",
    colors: defaultColors,
    showControls: true,
    showSql: true,
    showTable: true,
    onModify: (options) => console.log("Modify:", options),
  },
  parameters: {
    docs: {
      description: {
        story: "Result with loading overlay while modifications are being applied.",
      },
    },
  },
};

export const SunsetTheme: Story = {
  args: {
    result: sampleResult,
    query: "Shows added by year",
    colorPreset: "sunset",
    colors: sunsetColors,
    showControls: true,
    showSql: true,
    showTable: true,
    onModify: (options) => console.log("Modify:", options),
  },
};
