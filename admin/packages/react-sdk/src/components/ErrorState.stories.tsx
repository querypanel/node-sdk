import type { Meta, StoryObj } from "@storybook/react";
import { ErrorState } from "./ErrorState";
import { defaultColors } from "../themes";

const meta: Meta<typeof ErrorState> = {
  title: "Components/ErrorState",
  component: ErrorState,
  tags: ["autodocs"],
  parameters: {
    layout: "padded",
    docs: {
      description: {
        component:
          "Error display with customizable title, message, and optional retry button.",
      },
    },
  },
  argTypes: {
    title: {
      description: "Error title",
      control: "text",
    },
    message: {
      description: "Error message",
      control: "text",
    },
    helpText: {
      description: "Additional help text",
      control: "text",
    },
    onRetry: {
      description: "Retry callback (shows retry button if provided)",
      action: "retry",
    },
  },
};

export default meta;
type Story = StoryObj<typeof ErrorState>;

export const Default: Story = {
  args: {
    message: "Failed to connect to the database. Please check your connection settings.",
    colors: defaultColors,
  },
};

export const WithTitle: Story = {
  args: {
    title: "Connection Error",
    message: "Unable to reach the API server.",
    colors: defaultColors,
  },
};

export const WithHelpText: Story = {
  args: {
    title: "Query Failed",
    message: "The SQL query returned an error.",
    helpText: "Make sure your query syntax is correct and try again.",
    colors: defaultColors,
  },
};

export const WithRetryButton: Story = {
  args: {
    title: "Request Timeout",
    message: "The server took too long to respond.",
    helpText: "This might be due to high traffic. Please try again.",
    onRetry: () => console.log("Retry clicked"),
    colors: defaultColors,
  },
  parameters: {
    docs: {
      description: {
        story: "Error state with a retry button.",
      },
    },
  },
};

export const NetworkError: Story = {
  args: {
    title: "Network Error",
    message: "No internet connection detected.",
    helpText: "Please check your network settings and try again.",
    onRetry: () => console.log("Retry clicked"),
    colors: defaultColors,
  },
};

export const AuthError: Story = {
  args: {
    title: "Authentication Failed",
    message: "Your session has expired.",
    helpText: "Please log in again to continue.",
    colors: defaultColors,
  },
};
