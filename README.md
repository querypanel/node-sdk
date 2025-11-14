# QueryPanel Node SDK

A TypeScript-first client for the QueryPanel Bun/Hono API. It signs JWTs with your service private key, syncs database schemas, enforces tenant isolation, and wraps every public route under `src/routes/` (query, ingest, charts, active charts, and knowledge base).

## Installation

```bash
bun add @querypanel/sdk
# or
npm install @querypanel/sdk
```

> **Runtime:** Node.js 18+ (or Bun). The SDK relies on the native `fetch` API.

## Quickstart

```ts
import { QueryPanelSdkAPI } from "@querypanel/sdk";
import { Pool } from "pg";

const qp = new QueryPanelSdkAPI(
  process.env.QUERYPANEL_URL!,
  process.env.PRIVATE_KEY!,
  process.env.ORGANIZATION_ID!,
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

qp.attachPostgres("analytics", createPostgresClient(), {
  description: "Primary analytics warehouse",
  tenantFieldName: "tenant_id",
});

qp.attachClickhouse(
  "clicks",
  (params) => clickhouse.query(params),
  {
    database: "analytics",
    tenantFieldName: "customer_id",
    tenantFieldType: "String",
  },
);

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
- Schema ingestion failures are logged to `console.warn` during auto-sync, but you can call `syncSchema(..., { force: true })` to surface them directly.
- `ask()` raises immediately for guardrail/moderation errors because `/query` responds with 4xx/5xx.

## Need more?

Open an issue or extend `node-sdk/src/index.ts`—every route lives in one file. Pull requests are welcome for additional adapters, richer param coercion, or convenience helpers around charts/annotations.
