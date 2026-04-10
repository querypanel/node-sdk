import type { Meta, StoryObj } from "@storybook/react";
import { LoadingState } from "./LoadingState";
import { defaultColors, sunsetColors, emeraldColors } from "../themes";

const meta: Meta<typeof LoadingState> = {
  title: "Components/LoadingState",
  component: LoadingState,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Loading indicator with customizable message and icon. Used while queries are being processed.",
      },
    },
  },
  argTypes: {
    message: {
      description: "Primary loading message",
      control: "text",
    },
    submessage: {
      description: "Secondary message",
      control: "text",
    },
    colors: {
      description: "Theme colors",
      control: "object",
    },
  },
};

export default meta;
type Story = StoryObj<typeof LoadingState>;

export const Default: Story = {
  args: {
    message: "Generating your visualization...",
    submessage: "AI is analyzing your data",
    colors: defaultColors,
  },
};

export const CustomMessage: Story = {
  args: {
    message: "Processing query...",
    submessage: "This may take a few seconds",
    colors: defaultColors,
  },
};

export const SunsetTheme: Story = {
  args: {
    message: "Loading results...",
    submessage: "Please wait",
    colors: sunsetColors,
  },
};

export const EmeraldTheme: Story = {
  args: {
    message: "Fetching data...",
    submessage: "Almost there",
    colors: emeraldColors,
  },
};

export const WithCustomIcon: Story = {
  args: {
    message: "Analyzing...",
    submessage: "Running AI models",
    colors: defaultColors,
    icon: (
      <svg width="32" height="32" viewBox="0 0 24 24" fill="none" stroke="#8B5CF6" strokeWidth="2">
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
    ),
  },
  parameters: {
    docs: {
      description: {
        story: "Loading state with a custom icon.",
      },
    },
  },
};
