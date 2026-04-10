import type { Meta, StoryObj } from "@storybook/react";
import { QuerypanelEmbedded } from "./QuerypanelEmbedded";
import type { Dashboard } from "../types";

// Mock dashboard content
const mockDashboardContent = JSON.stringify([
  {
    type: "heading",
    props: { level: 1 },
    content: [{ type: "text", text: "Sales Dashboard", styles: {} }],
  },
  {
    type: "paragraph",
    content: [
      {
        type: "text",
        text: "This is a sample dashboard showing key metrics and visualizations.",
        styles: {},
      },
    ],
  },
  {
    type: "heading",
    props: { level: 2 },
    content: [{ type: "text", text: "Key Metrics", styles: {} }],
  },
  {
    type: "paragraph",
    content: [
      {
        type: "text",
        text: "Revenue: $125,430",
        styles: { bold: true },
      },
    ],
  },
  {
    type: "paragraph",
    content: [
      {
        type: "text",
        text: "New Customers: 1,234",
        styles: { bold: true },
      },
    ],
  },
  {
    type: "heading",
    props: { level: 2 },
    content: [{ type: "text", text: "Charts & Visualizations", styles: {} }],
  },
  {
    type: "paragraph",
    content: [
      {
        type: "text",
        text: "Here you would see embedded widgets with charts and data visualizations.",
        styles: {},
      },
    ],
  },
]);

// Mock dashboard data
const mockDashboard: Dashboard = {
  id: "mock-dashboard-id",
  organization_id: "org-123",
  name: "Sales Dashboard",
  description: "Monthly sales metrics and KPIs",
  status: "deployed",
  content_json: mockDashboardContent,
  widget_config: {},
  editor_type: "blocknote",
  is_customer_fork: false,
  forked_from_dashboard_id: null,
  tenant_id: null,
  datasource_id: null,
  version: 1,
  deployed_at: new Date().toISOString(),
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString(),
  created_by: "admin@example.com",
};

const mockFork: Dashboard = {
  ...mockDashboard,
  id: "mock-fork-id",
  name: "Sales Dashboard (customer-123)",
  is_customer_fork: true,
  forked_from_dashboard_id: "mock-dashboard-id",
  tenant_id: "customer-123",
};

// Mock fetch function
const createMockFetch = (mockData: Dashboard, delay = 300) => {
  return async (url: string | URL | Request, options?: RequestInit): Promise<Response> => {
    // Simulate network delay
    await new Promise((resolve) => setTimeout(resolve, delay));

    // Convert to string URL
    const urlString = typeof url === "string" ? url : url instanceof Request ? url.url : url.toString();
    const urlObj = new URL(urlString);
    
    console.log("[Mock Fetch]", options?.method || "GET", urlObj.pathname);
    
    // Handle fork endpoint - POST /dashboards/:id/fork
    if (urlObj.pathname.includes("/fork") && options?.method === "POST") {
      console.log("[Mock Fetch] Returning fork response");
      return new Response(JSON.stringify(mockFork), {
        status: 201,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Handle rollback endpoint - POST /dashboards/forks/:id/rollback
    if (urlObj.pathname.includes("/rollback") && options?.method === "POST") {
      console.log("[Mock Fetch] Returning rollback response");
      return new Response(JSON.stringify(mockDashboard), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    }

    // Handle update fork endpoint - PUT /dashboards/forks/:id
    if (urlObj.pathname.includes("/forks/") && options?.method === "PUT") {
      console.log("[Mock Fetch] Returning update fork response");
      const body = options.body ? JSON.parse(options.body as string) : {};
      return new Response(
        JSON.stringify({ ...mockFork, content_json: body.content_json || mockFork.content_json }),
        {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }
      );
    }

    // Handle get dashboard - GET /dashboards/:id/for-tenant
    console.log("[Mock Fetch] Returning dashboard response");
    return new Response(JSON.stringify(mockData), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  };
};

const meta: Meta<typeof QuerypanelEmbedded> = {
  title: "Components/QuerypanelEmbedded",
  component: QuerypanelEmbedded,
  tags: ["autodocs"],
  args: {
    apiBaseUrl: "https://mock-api.querypanel.com",
    jwt: "demo.jwt.token",
  },
  parameters: {
    layout: "padded",
  },
  decorators: [
    (Story, context) => {
      // Setup mock fetch before rendering
      const originalFetch = globalThis.fetch;
      
      // Determine which mock data to use based on story
      const mockData = context.name === "Customer Fork" ? mockFork : mockDashboard;
      const mockFetch = createMockFetch(mockData, 300);
      
      // Override global fetch
      globalThis.fetch = mockFetch as typeof fetch;
      
      // Cleanup on unmount using useEffect equivalent
      const cleanup = () => {
        globalThis.fetch = originalFetch;
      };
      
      // Store cleanup for later
      if (typeof window !== "undefined") {
        const storybookWindow = window as Window & {
          __storybookMockCleanup?: () => void;
        };
        storybookWindow.__storybookMockCleanup = cleanup;
      }
      
      return <Story />;
    },
  ],
};

export default meta;
type Story = StoryObj<typeof QuerypanelEmbedded>;

export const ReadOnly: Story = {
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: false,
    darkMode: false,
  },
  parameters: {
    docs: {
      description: {
        story: "Read-only dashboard view through a customer backend API wrapper.",
      },
    },
  },
};

export const WithCustomization: Story = {
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: true,
    darkMode: true,
    colorPreset: "ocean",
  },
  parameters: {
    backgrounds: { default: "dark" },
    docs: {
      description: {
        story:
          "Dashboard with customization enabled. Customers can fork and edit via backend-wrapped endpoints. AI Chart Generator modal matches product (dark theme).",
      },
    },
  },
};

export const CustomTheme: Story = {
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: true,
    darkMode: true,

    theme: {
      name: "custom-rose",
      colors: {
        primary: "#e11d48",
        secondary: "#fb7185",
        tertiary: "#f97316",
        accent: "#ec4899",
        range: ["#e11d48", "#fb7185", "#f97316", "#ec4899", "#06b6d4", "#22c55e", "#eab308", "#8b5cf6"],
        text: "#ffe4e6",
        muted: "#fecdd3",
        grid: "rgba(225,29,72,0.14)",
        background: "#18050d",
        surface: "rgba(24,5,13,0.82)",
        border: "rgba(251,113,133,0.35)",
        error: "#ef4444",
      },
      fontFamily: "Georgia, Cambria, serif",
      borderRadius: "0.75rem",
    },

    colorPreset: "default"
  },
  parameters: {
    backgrounds: { default: "dark" },
    docs: {
      description: {
        story: "Dashboard with a fully custom theme applied to the BlockNote editor surface and menus.",
      },
    },
  },
};

export const DarkMode: Story = {
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: true,
    darkMode: true,
  },
  parameters: {
    backgrounds: { default: "dark" },
    docs: {
      description: {
        story: "Dashboard in dark mode with customization enabled.",
      },
    },
  },
};

export const WithCallbacks: Story = {
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: true,
    onLoad: (dashboard) => {
      console.log("Dashboard loaded:", dashboard);
    },
    onCustomize: (fork) => {
      console.log("Dashboard forked:", fork);
    },
    onError: (error) => {
      console.error("Error:", error);
    },
  },
  parameters: {
    docs: {
      description: {
        story: "Dashboard with event callbacks for load, customize, and error events.",
      },
    },
  },
};

export const CustomerFork: Story = {
  name: "Customer Fork",
  args: {
    dashboardId: "mock-dashboard-id",
    apiBaseUrl: "https://mock-api.querypanel.com",
    allowCustomization: true,
    darkMode: false,
  },
  parameters: {
    docs: {
      description: {
        story: "Dashboard showing a customer fork (customized version).",
      },
    },
  },
};
