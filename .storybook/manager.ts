import { addons } from "@storybook/manager-api";
import { create } from "@storybook/theming/create";

const theme = create({
  base: "dark",
  brandTitle: "QueryPanel React SDK",
  brandUrl: "https://querypanel.io",
  brandTarget: "_blank",

  // Colors
  colorPrimary: "#8B5CF6",
  colorSecondary: "#6366F1",

  // UI
  appBg: "#0a0612",
  appContentBg: "#0f0a1e",
  appBorderColor: "rgba(139, 92, 246, 0.2)",
  appBorderRadius: 8,

  // Text colors
  textColor: "#F1F5F9",
  textInverseColor: "#0a0612",

  // Toolbar
  barTextColor: "#94A3B8",
  barSelectedColor: "#8B5CF6",
  barBg: "#0f0a1e",

  // Form colors
  inputBg: "#1a1a2e",
  inputBorder: "rgba(139, 92, 246, 0.3)",
  inputTextColor: "#F1F5F9",
  inputBorderRadius: 6,
});

addons.setConfig({
  theme,
});
