import type { Meta, StoryObj } from "@storybook/react";
import { EmptyState } from "./EmptyState";
import { defaultColors, oceanColors } from "../themes";

const meta: Meta<typeof EmptyState> = {
  title: "Components/EmptyState",
  component: EmptyState,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Empty state display with customizable title, description, and feature badges. Shown before any query is made.",
      },
    },
  },
  argTypes: {
    title: {
      description: "Title text",
      control: "text",
    },
    description: {
      description: "Description text",
      control: "text",
    },
    features: {
      description: "Feature badges to display",
      control: "object",
    },
  },
};

export default meta;
type Story = StoryObj<typeof EmptyState>;

export const Default: Story = {
  args: {
    title: "Ready to explore",
    description: "Ask anything about your data. Type a question or try one of the example prompts.",
    colors: defaultColors,
  },
};

export const CustomFeatures: Story = {
  args: {
    title: "Start querying",
    description: "Your database is connected and ready.",
    features: [
      { label: "PostgreSQL", color: "#336791" },
      { label: "10 tables", color: "#10B981" },
      { label: "AI-ready", color: "#8B5CF6" },
    ],
    colors: defaultColors,
  },
};

export const OceanTheme: Story = {
  args: {
    title: "Explore your data",
    description: "Natural language queries powered by AI.",
    colors: oceanColors,
  },
};

export const WithCustomIcon: Story = {
  args: {
    title: "No results yet",
    description: "Run a query to see visualizations here.",
    colors: defaultColors,
    icon: (
      <svg width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="#8B5CF6" strokeWidth="2">
        <circle cx="11" cy="11" r="8" />
        <path d="M21 21l-4.35-4.35" />
      </svg>
    ),
  },
  parameters: {
    docs: {
      description: {
        story: "Empty state with a custom search icon.",
      },
    },
  },
};

export const MinimalFeatures: Story = {
  args: {
    title: "Welcome",
    description: "Get started by asking a question.",
    features: [{ label: "Ready", color: "#10B981" }],
    colors: defaultColors,
  },
};
