# QueryPanel Node SDK

`@querypanel/node-sdk` is the headless, server-side SDK for [QueryPanel](https://querypanel.io) embedded analytics. Use it in your Node.js (18+), Bun, or Deno backend to mint RS256 tenant JWTs, sync your Postgres, Amazon Redshift, ClickHouse, or BigQuery schema, and turn natural-language questions into SQL (natural-language-to-SQL / text-to-SQL) for multi-tenant SaaS products. The generated SQL runs against your database through a client function you provide, so database credentials and query results stay on your servers. For chart generation the SDK sends column names and value types, not row values (see `anonymizeResults`).

- Docs: [querypanel.io/docs/node-sdk](https://querypanel.io/docs/node-sdk)
- Tenant isolation: [querypanel.io/docs/tenant-isolation](https://querypanel.io/docs/tenant-isolation)
- JWT setup: [querypanel.io/docs/jwt](https://querypanel.io/docs/jwt)

## When to use the Node SDK vs the React SDK

- **[React SDK](https://www.npmjs.com/package/@querypanel/react-sdk) (headful, start here for React apps):** `@querypanel/react-sdk` renders customer-facing dashboards with an AI assistant and per-tenant customization directly in your React tree, not an iframe ([why that matters](https://querypanel.io/blog/iframe-vs-native-react-embedded-analytics-2026)). It still needs this package on your server to mint the tenant JWT (`createJwt`).
- **Node SDK (this package, headless):** use it alone when you build your own UI and want `ask()` to return SQL, rows, and a chart spec from your API routes.

How it compares to other embedded analytics tools: [querypanel.io/compare](https://querypanel.io/compare).

## Overview

A TypeScript-first client for the QueryPanel Bun/Hono API. Its primary function is to **generate SQL from natural language**, but it also signs JWTs with your service private key, syncs database schemas, enforces tenant isolation, and wraps every public route under `src/routes/` (query, ingest, charts, active charts, and knowledge base).

## Installation

```bash
bun add @querypanel/node-sdk
# or
npm install @querypanel/node-sdk
```

> **Runtime:** Node.js 18+, Deno, or Bun. The SDK uses Web Crypto API for JWT signing and the native `fetch` API, making it compatible with modern JavaScript runtimes.

## Quickstart

```ts
import { QueryPanelSdkAPI } from "@querypanel/node-sdk";
import { Pool } from "pg";
import { createClient } from "@clickhouse/client";

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
  "pg_demo", // a unique identifier for QueryPanel
  createPostgresClient(),
  {
    database: "pg_demo", // database name
    description: "PostgreSQL demo database", // some description that QueryPanel can use
    tenantFieldName: "tenant_id", // SDK will automatically filter by tenant_id
    enforceTenantIsolation: true, // Ensures all queries include tenant_id filter
    allowedTables: ["orders"], // Only sync 'orders' table - 'users' will be excluded
  },
);

// Amazon Redshift speaks the Postgres wire protocol, so a pg-based client works.
// attachRedshift introspects via Redshift's svv_* views and generates Redshift SQL.
qp.attachRedshift("warehouse", createPostgresClient(), {
  database: "dev",
  tenantFieldName: "tenant_id",
});

const clickhouse = createClient({ url: process.env.CLICKHOUSE_URL });

qp.attachClickhouse(
  "analytics", // unique identifier for QueryPanel
  (params) => clickhouse.query(params),
  {
    database: "analytics", // database name
    tenantFieldName: "customer_id", // SDK will automatically filter by tenant_id
    tenantFieldType: "String", // SDK will use it in the clickhouse query as {customer_id::String}
  },
);

const response = await qp.ask("Top countries by revenue", {
  tenantId: "tenant_123",
  database: "analytics",
});

console.log(response.sql);
console.log(response.params);
console.table(response.rows);
console.log(response.chart.vegaLiteSpec);
```

Run `syncSchema` separately — see [Schema sync](#schema-sync).

### Schema sync

`syncSchema` introspects an attached database and uploads table/column metadata so natural-language questions can generate accurate SQL. Treat it like a **migration**, not part of `ask()`:

- Run it once when you first connect a database.
- Run it again when tables or columns change.
- Do not call it on every request — put it in a setup/deploy script instead.

```ts
// scripts/sync-schema.ts — run once at setup, or after schema changes
await qp.syncSchema("pg_demo", { tenantId: "tenant_123" });
await qp.syncSchema("analytics", { tenantId: "tenant_123" });

// Unchanged schemas skip re-embedding. Force a rebuild only when needed:
await qp.syncSchema("analytics", {
  tenantId: "tenant_123",
  forceReindex: true,
});
```

Optionally limit the payload with `tables: ["orders"]`.

#### Large databases

`allowedTables` (and `tables`) accept `schema.*` to include every table in a schema on Postgres/Redshift, e.g. `["crm.*", "public.orders"]`. On ClickHouse and BigQuery the adapter is bound to one database/dataset, so `*` or `<database>.*` means "all tables".

Browse the catalog cheaply (no columns; not restricted by `allowedTables`) to build a table picker:

```ts
await qp.listSchemas("warehouse"); // [{ schema: "crm", tableCount: 12 }, ...]
await qp.listTables("warehouse", { schema: "crm", search: "user", limit: 100 });
// [{ schema: "crm", name: "users", type: "table" }, ...]  (default limit 500, max 5000)
```

For schemas too large for a single request, `syncSchemaInBatches` uploads tables in batches (≤ `maxBatchDocs` docs, default 1000, and ≤ `maxBatchBytes`, default 2 MB) and embeds them batch by batch:

```ts
await qp.syncSchemaInBatches("warehouse", {
  tenantId: "tenant_123",
  maxColumns: 50_000, // throw SchemaTooLargeError before uploading if exceeded
  onProgress: (e) => console.log(e.phase, e),
});
```

To spread the work across requests (e.g. serverless time limits), call `stageSchemaIngest` (introspect + upload + commit), then `embedSchemaIngestBatch(sessionId, i, ...)` for each batch, then `finalizeSchemaIngest(sessionId, ...)`.

### Custom system prompt (`systemPrompt`)

Pass `systemPrompt` on `ask()` to inject extra instructions for SQL generation. Use it when a **client** needs policies QueryPanel does not know about — retention windows, a max date range, excluded event types, and similar product rules. Look up that client’s config in your backend and pass it on every question.

The text is appended to the SQL generator (and reflection) prompts. When it conflicts with generic defaults (for example the usual 30-day trend window), the caller instructions win. It does **not** replace tenant isolation — still attach with `tenantFieldName` and pass `tenantId`. Max length is 8,000 characters. There is no constructor default; `modifyChart()` does not take `systemPrompt`.

```ts
function systemPromptForClient(client: { retentionDays: number }) {
  return [
    `Data retention: only query rows from the last ${client.retentionDays} days.`,
    `Never return a date range longer than ${client.retentionDays} days, even if the user asks for all time or a wider window.`,
    "Clamp any requested range to that maximum and bind start/end dates as parameters.",
  ].join(" ");
}

const client = await loadClient(req); // your auth + billing/retention settings

const response = await qp.ask("Revenue by product over all time", {
  tenantId: client.tenantId,
  database: "analytics",
  systemPrompt: systemPromptForClient(client),
});
```

Other per-client instructions:

```ts
const response = await qp.ask("Show order volume by week", {
  tenantId: client.tenantId,
  database: "analytics",
  systemPrompt: [
    "Default time range: last 90 days unless the user names a shorter window.",
    "Maximum lookback: 90 days from today.",
    "Exclude orders with status refunded or cancelled.",
  ].join(" "),
});
```

### Restricting chart types

If your UI only supports a subset of chart kinds (for example no area or scatter), set **`supportedChartTypes`** on the SDK constructor or on each `ask` / `modifyChart` call (`line`, `bar`, `column`, `area`, `scatter`, `pie`).

```ts
import { QueryPanelSdkAPI, ALL_VIZ_CHART_TYPES, type ChartType } from "@querypanel/node-sdk";

const allowed: ChartType[] = ["line", "bar", "column", "pie"];

const qp = new QueryPanelSdkAPI(url, privateKey, workspaceId, {
  supportedChartTypes: allowed,
});

// Or per request:
await qp.ask("Revenue by month", {
  tenantId: "t1",
  database: "analytics",
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

### Saved charts via `/charts/all`

`listAllCharts()` calls **`GET /charts/all`**. It accepts the **same options as `listCharts()`** (including **`pagination`** with `page` and `limit`, filters, and sort).

**Breaking change:** Older SDK versions returned a **plain array** of charts from `listAllCharts()`. Current versions return **`{ data, pagination }`**, the same **`PaginatedResponse`** shape as **`listCharts()`**. Update callers from `const charts = await qp.listAllCharts(...)` to **`const { data: charts, pagination } = await qp.listAllCharts(...)`** (or use **`result.data`** only).

```ts
const { data, pagination } = await qp.listAllCharts({
  tenantId: "tenant_123",
  pagination: { page: 1, limit: 50 },
  includeData: true,
});

// Walk every page (same pattern as listCharts)
let page = 1;
const limit = 50;
let allCharts: typeof data = [];
for (;;) {
  const res = await qp.listAllCharts({
    tenantId: "tenant_123",
    pagination: { page, limit },
  });
  allCharts = allCharts.concat(res.data);
  if (!res.pagination.hasNext) break;
  page += 1;
}
```

### Bulk fetch saved charts by ID

To load specific saved charts by their **chart UUIDs** (from `createChart` or list responses), use `getChartsByIds()`. It calls **`GET /charts/bulk`** with `ids` as repeated or comma-separated query params. The API returns **`{ data, missingIds }`**. Up to **100** UUIDs per request.

```ts
const { data, missingIds } = await qp.getChartsByIds(
  ["550e8400-e29b-41d4-a716-446655440000"],
  { tenantId: "tenant_123", includeData: true },
);
```

Saved charts now include the original `prompt` so you can show the question
alongside chart history or reuse it in follow-up workflows.

## Referencing Queries by `queryId` (recommended)

`ask()` and `modifyChart()` return a server-issued `queryId`. The server stores the generated SQL and bind params for that id, scoped to the authenticated workspace and tenant. **Pass the id instead of SQL** so your clients never send executable SQL or bind params:

```ts
const response = await qp.ask("revenue by country", { tenantId, database: "analytics" });

// Modify: SQL and params are loaded from the server, client `sql` / `params` are ignored
const modified = await qp.modifyChart(
  { queryId: response.queryId, question: "revenue by country", database: "analytics",
    vizModifications: { chartType: "bar" } },
  { tenantId },
);

// Run a stored query (e.g. in a dashboard)
await qp.runSqlForDashboard({ queryId: response.queryId, database: "analytics" }, { tenantId });

// Save a chart: the server stores the SQL and params it holds for `query_id`
await qp.createChart({ title: "Revenue", query_id: modified.queryId, vega_lite_spec: spec }, { tenantId });
```

- When `queryId` / `query_id` is present it **takes priority**: any `sql` / `sql_params` you also send is ignored.
- An id that belongs to another tenant, or does not exist, is rejected (`404`).
- `sql` is still accepted when no id is sent, for backward compatibility. Charts saved without a `query_id` (for example older charts) keep using their stored SQL.
- `modifyChart()` returns the input `queryId` when only visualization changes, and a new `queryId` when SQL is regenerated. Custom SQL (`customSql`) has no id.
- Generation fails if the server cannot record the query, so a successful `ask()` always returns a `queryId`.
- Requires an API that supports `GET /v2/queries/:id`; upgrade the API before the SDK.

## Modifying Charts

The `modifyChart()` method allows you to edit SQL and/or visualization settings, then re-execute and regenerate charts. It works with both fresh `ask()` responses and saved charts. Prefer `queryId` over `sql` (see above).

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

### All active charts (no pagination)

`listActiveCharts()` is **paginated** (default page size is small). To load every pinned chart for a tenant in one round trip, use `listAllActiveCharts()`, which calls **`GET /active-charts/all`** and returns `{ data }` from the API as a plain array.

```ts
const all = await qp.listAllActiveCharts({
  tenantId: "tenant_123",
  withData: true, // optional: same as listActiveCharts — runs SQL per chart via your attached DB
});
```

### Bulk fetch by active-chart IDs

To load only specific dashboard tiles by their **active chart row IDs** (UUIDs from `createActiveChart` / `listActiveCharts`), use `getActiveChartsByIds()`. It calls **`GET /active-charts/bulk`** with query params `ids` (repeat or comma-separated). The API returns **`{ data, missingIds }`**: rows you are allowed to see, plus any requested IDs that were not found or not in scope.

```ts
const { data, missingIds } = await qp.getActiveChartsByIds(
  ["550e8400-e29b-41d4-a716-446655440000", "550e8400-e29b-41d4-a716-446655440001"],
  {
    tenantId: "tenant_123",
    withData: true, // optional
  },
);

if (missingIds.length) {
  console.warn("Unknown or inaccessible active charts:", missingIds);
}
```

The API accepts up to **100** UUIDs per request. Examples: `?ids=a&ids=b` or `?ids=a,b`.

## Deno Support

The SDK is fully compatible with Deno (including Supabase Edge Functions) thanks to its use of Web Crypto API for JWT signing. No additional configuration needed:

```ts
import { QueryPanelSdkAPI } from "https://esm.sh/@querypanel/node-sdk";

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

BigQuery is attached the same way with `attachBigQuery(name, clientFn, options)`. Any other engine can be plugged in with `attachDatabase(name, adapter)` and your own `DatabaseAdapter` implementation.

## One schema per tenant

If each tenant has its own schema with the same tables (for example `group_4048_indexed`, `group_4049_indexed`), attach with `tenantSchema` instead of `tenantFieldName`. Sync the schema of one tenant; the SQL QueryPanel generates names that schema, and the SDK rewrites it to the caller's schema (`{tenant}` replaced by `tenantId`) before running it:

```ts
qp.attachRedshift("analytics", createClient, {
  tenantSchema: {
    template: "group_{tenant}_indexed",
    sharedSchemas: ["lookups"], // optional: schemas every tenant may also read
  },
});

await qp.ask("How many members joined last month?", { tenantId: "4048" });
// runs against group_4048_indexed
```

Queries that read anything outside the tenant's schema, a shared schema or a CTE are refused, so a tenant cannot reach another tenant's schema. `allowedTables` written for the template schema (`group_4048_indexed.users`) applies to every tenant's schema. `tenantId` must be 1–64 letters, digits or `_`. Postgres and Redshift only. Adding a tenant needs no QueryPanel call: create its schema, then sign JWTs with its `tenantId`.

For embedded dashboards, set the same template as **Tenant schema template** on the datasource in QueryPanel and mint the usual JWT; `tenantId` is only the part that fills `{tenant}` (`"4048"`, not `"group_4048_indexed"`). See [Tenant isolation](https://querypanel.io/docs/tenant-isolation#schema-per-tenant).

## Building locally

```bash
cd admin/packages/node-sdk
bun install
bun run build
```

This runs `tsup` which emits dual ESM/CJS bundles plus type declarations to `dist/`.

## Authentication model

Every request is signed with `RS256` using the private key you pass to the constructor. The payload always includes `organizationId` and `tenantId`; `userId` and `scopes` are added when provided per call. If you still need service tokens or custom middleware, pass additional headers via the constructor.

## Error handling

- HTTP errors propagate as thrown `Error` instances that include `status` (and `details` when available).
- `syncSchema` automatically skips embedding if the schema hasn't changed (drift detection). Use `syncSchema(..., { forceReindex: true })` to force updates.
- `ask()` raises immediately for guardrail/moderation errors because `/v2/query` responds with 4xx/5xx. `pipeline: "v1"` is not accepted.

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

#### Retry when the query returns nothing

A query can run fine and still return nothing because a filter value does not match how the data stores it (`state = 'approved'` when the column holds `APPROVE`). With `retryOnEmptyResult`, the SDK treats an empty result, or a single row of only zeros/NULLs such as `COUNT(*) = 0`, like an execution error and asks for a repaired query within the same `maxRetry` budget:

```ts
const response = await qp.ask("How many transactions were declined?", {
  tenantId: "tenant_123",
  maxRetry: 1,
  retryOnEmptyResult: true,
});
```

Only the fact that the result was empty and the previous SQL are sent back; no rows or values leave your backend. If the last attempt is still empty, it is returned as is (no error). Off by default; each retry is one more generation round, so expect a few extra seconds on those questions.

## Need more?

Open an issue or extend the SDK: the public class is in `src/index.ts` and each API route has a module under `src/routes/`. Pull requests are welcome for additional adapters, richer param coercion, or convenience helpers around charts/annotations.
