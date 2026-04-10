import type { Meta, StoryObj } from "@storybook/react";
import { DataTable } from "./DataTable";
import { defaultColors, sunsetColors } from "../themes";

const meta: Meta<typeof DataTable> = {
  title: "Components/DataTable",
  component: DataTable,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Displays query results in a styled table with hover effects and row limiting.",
      },
    },
  },
  argTypes: {
    rows: {
      description: "Array of row data objects",
      control: "object",
    },
    fields: {
      description: "Column field names to display",
      control: "object",
    },
    maxRows: {
      description: "Maximum rows to display (default: 10)",
      control: { type: "number", min: 1, max: 50 },
    },
    colors: {
      description: "Theme colors",
      control: "object",
    },
  },
};

export default meta;
type Story = StoryObj<typeof DataTable>;

const sampleData = [
  { title: "Stranger Things", type: "TV Show", country: "United States", year: 2016 },
  { title: "Money Heist", type: "TV Show", country: "Spain", year: 2017 },
  { title: "The Crown", type: "TV Show", country: "United Kingdom", year: 2016 },
  { title: "Squid Game", type: "TV Show", country: "South Korea", year: 2021 },
  { title: "Wednesday", type: "TV Show", country: "United States", year: 2022 },
  { title: "Dark", type: "TV Show", country: "Germany", year: 2017 },
  { title: "Lupin", type: "TV Show", country: "France", year: 2021 },
  { title: "Narcos", type: "TV Show", country: "Colombia", year: 2015 },
];

const largeDataset = Array.from({ length: 25 }, (_, i) => ({
  id: i + 1,
  title: `Show ${i + 1}`,
  rating: (Math.random() * 2 + 3).toFixed(1),
  views: Math.floor(Math.random() * 10000000),
}));

export const Default: Story = {
  args: {
    rows: sampleData,
    fields: ["title", "type", "country", "year"],
    colors: defaultColors,
  },
};

export const LimitedRows: Story = {
  args: {
    rows: largeDataset,
    fields: ["id", "title", "rating", "views"],
    maxRows: 5,
    colors: defaultColors,
  },
  parameters: {
    docs: {
      description: {
        story: "Shows only 5 rows with a count indicator for remaining rows.",
      },
    },
  },
};

export const SunsetTheme: Story = {
  args: {
    rows: sampleData,
    fields: ["title", "type", "country", "year"],
    colors: sunsetColors,
  },
};

export const CustomCellRenderer: Story = {
  args: {
    rows: sampleData,
    fields: ["title", "type", "country", "year"],
    colors: defaultColors,
    renderCell: (value, field) => {
      if (field === "year") {
        return <span style={{ color: "#8B5CF6", fontWeight: 600 }}>{String(value)}</span>;
      }
      if (field === "type") {
        return (
          <span
            style={{
              padding: "2px 8px",
              borderRadius: "4px",
              backgroundColor: "rgba(139, 92, 246, 0.2)",
              fontSize: "12px",
            }}
          >
            {String(value)}
          </span>
        );
      }
      return String(value ?? "");
    },
  },
  parameters: {
    docs: {
      description: {
        story: "Custom cell rendering with styled year and type columns.",
      },
    },
  },
};
