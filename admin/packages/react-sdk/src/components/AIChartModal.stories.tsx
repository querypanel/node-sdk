import type { Meta, StoryObj } from "@storybook/react";
import { useState } from "react";
import { AIChartModal } from "./AIChartModal";

const meta: Meta<typeof AIChartModal> = {
  title: "Components/AIChartModal",
  component: AIChartModal,
  tags: ["autodocs"],
  parameters: {
    layout: "fullscreen",
  },
};

export default meta;

type Story = StoryObj<typeof AIChartModal>;

function AIChartModalWrapper(props: {
  defaultOpen?: boolean;
  darkMode?: boolean;
  dashboardType?: "customer" | "internal";
  hideTenantInputs?: boolean;
}) {
  const { defaultOpen = true, darkMode = true, dashboardType = "internal", hideTenantInputs = false } = props;
  const [open, setOpen] = useState(defaultOpen);

  return (
    <div className={darkMode ? "dark bg-gray-950 min-h-screen" : "min-h-screen"} style={darkMode ? { background: "#0a0612" } : undefined}>
      <button
        type="button"
        onClick={() => setOpen(true)}
        className="m-4 px-4 py-2 rounded-lg bg-blue-600 text-white"
      >
        Open AI Chart Generator
      </button>
      <AIChartModal
        isOpen={open}
        onClose={() => setOpen(false)}
        onAddChart={(spec, _rationale, _sql, _datasourceIds, _params, _tenantField, _previewTenant) => {
          console.log("Add chart:", spec);
          setOpen(false);
        }}
        organizationId="org-storybook"
        dashboardId="dashboard-storybook"
        dashboardType={dashboardType}
        darkMode={darkMode}
        hideTenantInputs={hideTenantInputs}
      />
    </div>
  );
}

export const Default: Story = {
  render: () => <AIChartModalWrapper defaultOpen />,
  parameters: {
    backgrounds: { default: "dark" },
    docs: {
      description: {
        story: "AI Chart Generator modal (matches product appearance). Open from the button or via the dashboard slash menu.",
      },
    },
  },
};

export const LightMode: Story = {
  render: () => <AIChartModalWrapper defaultOpen darkMode={false} />,
  parameters: {
    backgrounds: { default: "light" },
    docs: {
      description: {
        story: "AI Chart Generator in light mode.",
      },
    },
  },
};

export const CustomerWithTenantInputs: Story = {
  render: () => (
    <AIChartModalWrapper defaultOpen darkMode dashboardType="customer" hideTenantInputs={false} />
  ),
  parameters: {
    docs: {
      description: {
        story: "Customer dashboard type with tenant field and preview tenant ID inputs visible.",
      },
    },
  },
};

export const ClosedByDefault: Story = {
  render: () => <AIChartModalWrapper defaultOpen={false} />,
  parameters: {
    backgrounds: { default: "dark" },
    docs: {
      description: {
        story: "Modal closed initially; use the button to open it.",
      },
    },
  },
};
