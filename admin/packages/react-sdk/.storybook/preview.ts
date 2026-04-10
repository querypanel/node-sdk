import type { Preview } from "@storybook/react";
// BlockNote editor and block node styles (required for dashboard/editor stories)
import "@blocknote/mantine/style.css";
import "./preview.css";

const preview: Preview = {
  parameters: {
    controls: {
      matchers: {
        color: /(background|color)$/i,
        date: /Date$/i,
      },
    },
    backgrounds: {
      default: "dark",
      values: [
        { name: "dark", value: "#0a0612" },
        { name: "light", value: "#ffffff" },
        { name: "gray", value: "#1a1a2e" },
      ],
    },
    layout: "centered",
  },
};

export default preview;
