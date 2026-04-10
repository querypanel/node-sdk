import type { Meta, StoryObj } from "@storybook/react";
import { ChartControls } from "./ChartControls";
import { defaultColors, oceanColors } from "../themes";

const meta: Meta<typeof ChartControls> = {
  title: "Components/ChartControls",
  component: ChartControls,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Controls for modifying chart type, axes, time granularity, and color presets. Emits modification options on apply.",
      },
    },
  },
  argTypes: {
    fields: {
      description: "Available field names from the query result",
      control: "object",
    },
    disabled: {
      description: "Whether controls are disabled",
      control: "boolean",
    },
    showColorPresets: {
      description: "Show color preset selector",
      control: "boolean",
    },
    onApply: {
      description: "Callback when changes are applied",
      action: "applied",
    },
  },
};

export default meta;
type Story = StoryObj<typeof ChartControls>;

const sampleFields = ["title", "year", "country", "rating", "type", "duration"];

export const Default: Story = {
  args: {
    fields: sampleFields,
    colors: defaultColors,
    onApply: (options) => console.log("Applied:", options),
  },
};

export const Disabled: Story = {
  args: {
    fields: sampleFields,
    disabled: true,
    colors: defaultColors,
    onApply: (options) => console.log("Applied:", options),
  },
  parameters: {
    docs: {
      description: {
        story: "Controls in disabled state (e.g., while loading).",
      },
    },
  },
};

export const WithoutColorPresets: Story = {
  args: {
    fields: sampleFields,
    showColorPresets: false,
    colors: defaultColors,
    onApply: (options) => console.log("Applied:", options),
  },
  parameters: {
    docs: {
      description: {
        story: "Controls without the color preset selector.",
      },
    },
  },
};

export const OceanTheme: Story = {
  args: {
    fields: sampleFields,
    colors: oceanColors,
    onApply: (options) => console.log("Applied:", options),
  },
};

export const FewFields: Story = {
  args: {
    fields: ["date", "value"],
    colors: defaultColors,
    onApply: (options) => console.log("Applied:", options),
  },
  parameters: {
    docs: {
      description: {
        story: "Controls with minimal field options.",
      },
    },
  },
};
