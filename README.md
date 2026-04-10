# @querypanel/react-sdk

React components for QueryPanel - AI-powered data visualization.

## Installation

```bash
npm install @querypanel/react-sdk
# or
yarn add @querypanel/react-sdk
# or
pnpm add @querypanel/react-sdk
```

## Quick Start

### Using the Provider (Recommended)

```tsx
import {
  QueryPanelProvider,
  QueryInput,
  QueryResult,
  LoadingState,
  EmptyState,
  ErrorState,
  useQueryPanel,
} from "@querypanel/react-sdk";

function App() {
  return (
    <QueryPanelProvider
      config={{
        askEndpoint: "/api/demo/ask",
        modifyEndpoint: "/api/demo/modify",
        colorPreset: "default",
      }}
    >
      <Dashboard />
    </QueryPanelProvider>
  );
}

function Dashboard() {
  const { query, result, isLoading, error, ask, modify, colorPreset } = useQueryPanel();

  return (
    <div>
      <QueryInput
        value={query}
        onSubmit={ask}
        isLoading={isLoading}
        chips={[
          { key: "sales", text: "Show sales by month", emoji: "📈" },
          { key: "top", text: "Top 10 products", emoji: "🏆" },
        ]}
      />

      {isLoading && !result && <LoadingState />}
      {error && <ErrorState message={error} />}
      {!isLoading && !error && !result && <EmptyState />}
      {result && (
        <QueryResult
          result={result}
          query={query}
          isLoading={isLoading}
          colorPreset={colorPreset}
          onModify={modify}
        />
      )}
    </div>
  );
}
```

### Using Individual Components

```tsx
import { VegaChart, DataTable, ChartControls } from "@querypanel/react-sdk";
import { getColorsByPreset } from "@querypanel/react-sdk/themes";

function MyChart({ spec, data, fields }) {
  const colors = getColorsByPreset("ocean");

  return (
    <div>
      <ChartControls
        fields={fields}
        onApply={(options) => console.log(options)}
      />
      <VegaChart spec={spec} colors={colors} />
      <DataTable rows={data} fields={fields} />
    </div>
  );
}
```

## Components

### `QuerypanelEmbedded`
Embeds a deployed dashboard by calling your backend wrapper (not QueryPanel API directly from the browser).

```tsx
import { QuerypanelEmbedded } from "@querypanel/react-sdk";

function CustomerPage() {
  return (
    <QuerypanelEmbedded
      dashboardId="3ed3b98f-..."
      apiBaseUrl="https://customer-api.example.com"
      allowCustomization={true}
    />
  );
}
```

Notes:
- Browser requests go to your backend URL.
- Backend handles auth and tenant context server-side.
- `token` prop was removed; migrate to backend-managed auth/session.

### `QueryInput`
Search input with prompt chips for quick queries.

### `VegaChart`
Renders Vega-Lite specifications with automatic theming.

### `DataTable`
Displays query results in a styled table.

### `ChartControls`
Controls for modifying chart type, axes, time granularity, and colors.

### `QueryResult`
Combined display of chart, SQL, and data table.

### `LoadingState`, `ErrorState`, `EmptyState`
UI states for loading, errors, and empty results.

## Theming

### Color Presets

Built-in presets: `default`, `sunset`, `emerald`, `ocean`

```tsx
import { getColorsByPreset } from "@querypanel/react-sdk/themes";

const colors = getColorsByPreset("sunset");
```

### Custom Theme

```tsx
import { createTheme } from "@querypanel/react-sdk/themes";

const customTheme = createTheme({
  colors: {
    primary: "#FF6B6B",
    secondary: "#4ECDC4",
    // ... other colors
  },
  borderRadius: "1rem",
  fontFamily: "Inter, sans-serif",
});
```

## White-Labeling

All components accept a `colors` prop for custom styling:

```tsx
<VegaChart
  spec={spec}
  colors={{
    primary: "#YOUR_BRAND_COLOR",
    range: ["#color1", "#color2", "#color3"],
    text: "#ffffff",
    muted: "#888888",
    // ...
  }}
/>
```

## Types

```typescript
interface ThemeColors {
  primary: string;
  secondary: string;
  tertiary: string;
  accent: string;
  range: string[];
  text: string;
  muted: string;
  grid: string;
  background: string;
  surface: string;
  border: string;
  error: string;
}

interface QueryResult {
  success: boolean;
  sql?: string;
  rows?: Array<Record<string, unknown>>;
  fields?: string[];
  chart?: {
    vegaLiteSpec?: Record<string, unknown>;
    specType: "vega-lite" | "vizspec";
  };
}
```

## License

MIT
