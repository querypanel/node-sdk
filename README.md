# QueryPanel Node SDK

A TypeScript-first client for the QueryPanel Bun/Hono API. Its primary function is to **generate SQL from natural language**, but it also signs JWTs with your service private key, syncs database schemas, enforces tenant isolation, and wraps every public route under `src/routes/` (query, ingest, charts, active charts, and knowledge base).

## Installation

```bash
bun add @querypanel/sdk
# or
npm install @querypanel/sdk
```

> **Runtime:** Node.js 18+, Deno, or Bun. The SDK uses Web Crypto API for JWT signing and the native `fetch` API, making it compatible with modern JavaScript runtimes.

## Quickstart

```ts
import { QueryPanelSdkAPI } from "@querypanel/sdk";
import { Pool } from "pg";

const qp = new QueryPanelSdkAPI(
  process.env.QUERYPANEL_URL!,
  process.env.PRIVATE_KEY!,
  process.env.QUERYPANEL_WORKSPACE_ID!,
  {
    defaultTenantId: process.env.DEFAULT_TENANT_ID,
  },
);

const pool = new Pool({ connectionString: process.env.POSTGRES_URL });

const createPostgresClient = () => async (sql: string, params?: unknown[]) => {
  const client = await pool.connect();
  try {
    const result = await client.query(sql, params);
    return {
      rows: result.rows,
      fields: result.fields.map((field) => ({ name: field.name })),
    };
  } finally {
    client.release();
  }
};

	// Attach PostgreSQL database using the SDK's PostgresAdapter
	// The SDK will automatically handle tenant isolation when tenantFieldName is provided
	qp.attachPostgres(
  "pg_demo",  // a uniq identifier for QueryPanel
  createPostgresClientFn(),
  {
		database: "pg_demo", // database name
		description: "PostgreSQL demo database", // some description that QueryPanel can use
		tenantFieldName: "tenant_id", // SDK will automatically filter by tenant_id
		enforceTenantIsolation: true, // Ensures all queries include tenant_id filter
		allowedTables: ["orders"], // Only sync 'orders' table - 'users' will be excluded
	});

qp.attachClickhouse(
  "clicks", // uniq identifier for QueryPanel
  (params) => clickhouse.query(params),
  {
    database: "analytics", // database name
    tenantFieldName: "customer_id", // SDK will automatically filter by tenant_id
    tenantFieldType: "String", // SDK will use it in the clickhouse query as {customer_id::String}
  },
);

// Syncs schema. Skips embedding if schema hasn't changed (no drift).
// Pass { forceReindex: true } to force re-embedding.
await qp.syncSchema("analytics", { tenantId: "tenant_123" });

const response = await qp.ask("Top countries by revenue", {
  tenantId: "tenant_123",
  database: "analytics",
});

console.log(response.sql);
console.log(response.params);
console.table(response.rows);
console.log(response.chart.vegaLiteSpec);
```

### Custom system instructions (v2 pipeline)

If you need to enforce tenant-specific policies that should apply to every query (for example, data retention windows), you can inject additional **system prompt** instructions into the v2 pipeline:

```ts
const response = await qp.ask("Revenue by product", {
  tenantId: "tenant_123",
  database: "analytics",
  pipeline: "v2",
  systemPrompt: "Retention policy: only query data from the last 30 days.",
});
```

### Restricting VizSpec chart types

If your UI only supports a subset of VizSpec chart kinds (for example no area or scatter), set **`supportedChartTypes`** on the SDK constructor or on each `ask` / `modifyChart` call. The API’s `/vizspec` step then steers generation toward allowed types only (`line`, `bar`, `column`, `area`, `scatter`, `pie`). This applies whenever you use **`chartType: "vizspec"`** (including the default v2 flow after SQL runs).

```ts
import { QueryPanelSdkAPI, ALL_VIZ_CHART_TYPES, type ChartType } from "@querypanel/sdk";

const allowed: ChartType[] = ["line", "bar", "column", "pie"];

const qp = new QueryPanelSdkAPI(url, privateKey, workspaceId, {
  supportedChartTypes: allowed,
});

// Or per request:
await qp.ask("Revenue by month", {
  tenantId: "t1",
  database: "analytics",
  chartType: "vizspec",
  pipeline: "v2",
  supportedChartTypes: allowed,
});
```

Use `ALL_VIZ_CHART_TYPES` when you need the full list (for example to derive `allowed` with `.filter(...)`).

## Session History & Context-Aware Queries

The SDK can link related questions into a session so follow-ups like “filter that to Europe” use prior context. The backend generates a QueryPanel session ID for every query and returns it in the response so you can reuse it.

```ts
const first = await qp.ask("Revenue by country", {
  tenantId: "tenant_123",
  database: "analytics",
});

const querypanelSessionId = first.querypanelSessionId;

const followUp = await qp.ask("Now filter that to Europe", {
  tenantId: "tenant_123",
  database: "analytics",
  querypanelSessionId, // same QueryPanel session keeps context
});

console.log(followUp.sql);
```

### Managing Session History

```ts
// List sessions
const sessions = await qp.listSessions({
  tenantId: "tenant_123",
  pagination: { page: 1, limit: 20 },
  sortBy: "updated_at",
});

// Get a session with its turns
const session = await qp.getSession("session_abc123", {
  tenantId: "tenant_123",
  includeTurns: true,
});

// Update session title
await qp.updateSession(
  "session_abc123",
  { title: "Q4 Revenue Analysis" },
  { tenantId: "tenant_123" },
);

// Delete a session
await qp.deleteSession("session_abc123", { tenantId: "tenant_123" });
```

## Saving & Managing Charts

The SDK allows you to save generated charts to the QueryPanel system.

> **Privacy Note:** QueryPanel only stores the chart definition (SQL query, parameters, and Vega-Lite spec). We **never** store the actual result data rows. The data is fetched live from your database whenever the chart is rendered or refreshed.

```ts
// 1. Ask a question to generate a chart
const response = await qp.ask("Show revenue by country", {
  tenantId: "tenant_123",
  database: "analytics",
});

if (response.chart.vegaLiteSpec) {
  // 2. Save the chart (only stores SQL + metadata, no data)
  const savedChart = await qp.createChart({
    title: "Revenue by Country",
    prompt: "Show revenue by country",
    sql: response.sql,
    sql_params: response.params,
    vega_lite_spec: response.chart.vegaLiteSpec,
    query_id: response.queryId,
    target_db: response.target_db,
  }, {
    tenantId: "tenant_123",
    userId: "user_456" // Optional: associate with a user
  });

  console.log(`Chart saved with ID: ${savedChart.id}`);
}

// 3. List saved charts (History)
const charts = await qp.listCharts({ tenantId: "tenant_123" });
```

Saved charts now include the original `prompt` so you can show the question
alongside chart history or reuse it in follow-up workflows.

## Modifying Charts

The `modifyChart()` method allows you to edit SQL and/or visualization settings, then re-execute and regenerate charts. It works with both fresh `ask()` responses and saved charts.

### Changing Visualization Settings

Modify chart type, axes, or series without regenerating SQL:

```ts
// Start with an ask() response
const response = await qp.ask("revenue by country", {
  tenantId: "tenant_123",
  database: "analytics",
});

// Change to a bar chart with specific axis configuration
const modified = await qp.modifyChart({
  sql: response.sql,
  question: "revenue by country",
  database: "analytics",
  vizModifications: {
    chartType: "bar",
    xAxis: { field: "country", label: "Country" },
    yAxis: { field: "revenue", label: "Total Revenue", aggregate: "sum" },
  },
}, { tenantId: "tenant_123" });

console.log(modified.chart); // New chart spec with bar visualization
console.log(modified.modified.vizChanged); // true
console.log(modified.modified.sqlChanged); // false
```

### Changing Time Granularity and Date Range

These modifications trigger SQL regeneration:

```ts
// Change from daily to monthly aggregation
const monthly = await qp.modifyChart({
  sql: response.sql,
  question: "revenue over time",
  database: "analytics",
  sqlModifications: {
    timeGranularity: "month",
    dateRange: { from: "2024-01-01", to: "2024-12-31" },
  },
}, {
  tenantId: "tenant_123",
  querypanelSessionId: response.querypanelSessionId, // preserve follow-up context
});

console.log(monthly.sql); // New SQL with monthly GROUP BY
console.log(monthly.modified.sqlChanged); // true
```

### Direct SQL Editing

Provide custom SQL that will be executed directly:

```ts
const customized = await qp.modifyChart({
  sql: response.sql,
  question: "revenue by country",
  database: "analytics",
  sqlModifications: {
    customSql: `
      SELECT country, SUM(revenue) as total_revenue
      FROM orders
      WHERE status = 'completed' AND created_at > '2024-01-01'
      GROUP BY country
      ORDER BY total_revenue DESC
      LIMIT 10
    `,
  },
}, { tenantId: "tenant_123" });

// Optionally save the modified chart
if (customized.chart.vegaLiteSpec) {
  await qp.createChart({
    title: "Top 10 Countries by Revenue (Completed Orders)",
    sql: customized.sql,
    sql_params: customized.params,
    vega_lite_spec: customized.chart.vegaLiteSpec,
    target_db: customized.target_db,
  }, { tenantId: "tenant_123" });
}
```

### Combining SQL and Visualization Changes

Apply both types of modifications in a single call:

```ts
const combined = await qp.modifyChart({
  sql: response.sql,
  question: "revenue over time",
  database: "analytics",
  sqlModifications: {
    timeGranularity: "week",
    additionalInstructions: "exclude refunded orders",
  },
  vizModifications: {
    chartType: "area",
    stacking: "stacked",
  },
}, { tenantId: "tenant_123" });
```

### Modifying Saved Charts

Load a saved chart and modify it:

If you do not have a previous `querypanelSessionId` (common for persisted charts),
the SDK starts a new QueryPanel session and sends the full original question with
your modification hints. If you have one from an earlier `ask()` call, pass it to
keep follow-up context.

```ts
// Load a saved chart
const savedChart = await qp.getChart("chart_id_123", {
  tenantId: "tenant_123",
});

// Modify it
const modified = await qp.modifyChart({
  sql: savedChart.sql,
  question: savedChart.prompt ?? "original question",
  database: savedChart.target_db ?? "analytics",
  params: savedChart.sql_params as Record<string, unknown>,
  vizModifications: {
    chartType: "line",
  },
}, { tenantId: "tenant_123" });
```

## Building a Dashboard (Active Charts)

While `createChart` and `listCharts` manage your **history** of saved queries, "Active Charts" are designed for building **dashboards**. You can "pin" a saved chart to a dashboard, control its order, and fetch it with live data in a single call.

```ts
// 1. Pin a saved chart to the dashboard
const activeChart = await qp.createActiveChart({
  chart_id: "saved_chart_id_from_history",
  order: 1, // Optional: for sorting in UI
  meta: { width: "full", variant: "dark" } // Optional: UI layout hints
}, {
  tenantId: "tenant_123"
});

// 2. Load the dashboard with live data
// Passing { withData: true } executes the SQL for each chart immediately
const dashboard = await qp.listActiveCharts({
  tenantId: "tenant_123",
  withData: true
});

dashboard.data.forEach(item => {
  console.log(`Chart: ${item.chart?.title}`);
  console.log(`Data points: ${item.chart?.vega_lite_spec.data.values.length}`);
});
```

## Deno Support

The SDK is fully compatible with Deno (including Supabase Edge Functions) thanks to its use of Web Crypto API for JWT signing. No additional configuration needed:

```ts
import { QueryPanelSdkAPI } from "https://esm.sh/@querypanel/sdk";

const qp = new QueryPanelSdkAPI(
  Deno.env.get("QUERYPANEL_URL")!,
  Deno.env.get("PRIVATE_KEY")!,
  Deno.env.get("QUERYPANEL_WORKSPACE_ID")!,
);

// Use the SDK as normal - JWT signing works automatically
const response = await qp.ask("Show top products", {
  tenantId: "tenant_123",
});
```

## Building locally

```bash
cd node-sdk
bun install
bun run build
```

This runs `tsup` which emits dual ESM/CJS bundles plus type declarations to `dist/`.

## Authentication model

Every request is signed with `RS256` using the private key you pass to the constructor. The payload always includes `organizationId` and `tenantId`; `userId` and `scopes` are added when provided per call. If you still need service tokens or custom middleware, pass additional headers via the constructor.

## Error handling

- HTTP errors propagate as thrown `Error` instances that include `status` (and `details` when available).
- `syncSchema` automatically skips embedding if the schema hasn't changed (drift detection). Use `syncSchema(..., { forceReindex: true })` to force updates.
- `ask()` raises immediately for guardrail/moderation errors because `/query` responds with 4xx/5xx.

### Automatic SQL repair and retry

When SQL execution fails (e.g., invalid column name, syntax error), the SDK can automatically retry with a repaired query:

```ts
const response = await qp.ask("Show revenue by country", {
  tenantId: "tenant_123",
  maxRetry: 3,  // Automatically retry up to 3 times on execution error
});

console.log(`Query succeeded after ${response.attempts} attempt(s)`);
console.table(response.rows);
```

The SDK will:
1. Execute the generated SQL
2. If execution fails, send the error back to the server with the failed SQL
3. Get a repaired SQL query from the server
4. Execute the repaired query
5. Repeat up to `maxRetry` times

Without `maxRetry`, execution errors throw immediately (default behavior).

## Need more?

Open an issue or extend `node-sdk/src/index.ts`—every route lives in one file. Pull requests are welcome for additional adapters, richer param coercion, or convenience helpers around charts/annotations.
