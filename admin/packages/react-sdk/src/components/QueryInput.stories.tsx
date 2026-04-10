import type { Meta, StoryObj } from "@storybook/react";
import { useState } from "react";
import { QueryInput } from "./QueryInput";
import { defaultColors, oceanColors } from "../themes";
import type { PromptChip } from "../types";

const meta: Meta<typeof QueryInput> = {
  title: "Components/QueryInput",
  component: QueryInput,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Search input with prompt chips for quick queries. Supports controlled and uncontrolled modes.",
      },
    },
  },
  argTypes: {
    value: {
      description: "Controlled input value",
      control: "text",
    },
    placeholder: {
      description: "Input placeholder text",
      control: "text",
    },
    disabled: {
      description: "Whether input is disabled",
      control: "boolean",
    },
    isLoading: {
      description: "Whether query is loading",
      control: "boolean",
    },
    chips: {
      description: "Prompt chips to display",
      control: "object",
    },
  },
};

export default meta;
type Story = StoryObj<typeof QueryInput>;

const sampleChips: PromptChip[] = [
  { text: "Shows added by year", key: "year", emoji: "📈" },
  { text: "Top 10 countries", key: "countries", emoji: "🌍" },
  { text: "Movies vs TV Shows", key: "type", emoji: "🎬" },
  { text: "Content ratings", key: "ratings", emoji: "⭐" },
];

export const Default: Story = {
  args: {
    placeholder: "Ask a question about your data...",
    chips: sampleChips,
    colors: defaultColors,
    onSubmit: (query) => console.log("Submitted:", query),
    onChipClick: (chip) => console.log("Chip clicked:", chip),
  },
};

export const WithValue: Story = {
  args: {
    value: "Show me the top 10 movies by rating",
    placeholder: "Ask a question...",
    colors: defaultColors,
    onSubmit: (query) => console.log("Submitted:", query),
  },
};

export const Loading: Story = {
  args: {
    value: "Analyzing Netflix catalog...",
    isLoading: true,
    colors: defaultColors,
  },
  parameters: {
    docs: {
      description: {
        story: "Input in loading state with spinner.",
      },
    },
  },
};

export const Disabled: Story = {
  args: {
    placeholder: "Input is disabled",
    disabled: true,
    chips: sampleChips,
    colors: defaultColors,
  },
};

export const NoChips: Story = {
  args: {
    placeholder: "Ask anything...",
    colors: defaultColors,
    onSubmit: (query) => console.log("Submitted:", query),
  },
  parameters: {
    docs: {
      description: {
        story: "Input without prompt chips.",
      },
    },
  },
};

export const OceanTheme: Story = {
  args: {
    placeholder: "Ask a question...",
    chips: sampleChips,
    colors: oceanColors,
    onSubmit: (query) => console.log("Submitted:", query),
  },
};

export const CustomSubmitLabel: Story = {
  args: {
    placeholder: "Search...",
    colors: defaultColors,
    submitLabel: "🔍 Search",
    onSubmit: (query) => console.log("Submitted:", query),
  },
};

// Interactive controlled example
const ControlledTemplate = () => {
  const [value, setValue] = useState("");
  const [submitted, setSubmitted] = useState<string | null>(null);

  return (
    <div style={{ width: "100%", maxWidth: 600 }}>
      <QueryInput
        value={value}
        onChange={setValue}
        onSubmit={(q) => setSubmitted(q)}
        placeholder="Type and submit..."
        chips={sampleChips}
        colors={defaultColors}
      />
      {submitted && (
        <div
          style={{
            marginTop: 16,
            padding: 12,
            borderRadius: 8,
            backgroundColor: "rgba(139, 92, 246, 0.1)",
            color: "#F1F5F9",
          }}
        >
          Submitted: <strong>{submitted}</strong>
        </div>
      )}
    </div>
  );
};

export const Controlled: Story = {
  render: () => <ControlledTemplate />,
  parameters: {
    docs: {
      description: {
        story: "Fully controlled input with state management.",
      },
    },
  },
};
