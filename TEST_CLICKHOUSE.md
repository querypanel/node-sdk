# ClickHouse Adapter Test Guide

This guide helps you test the ClickHouse adapter in the node-sdk.

## Prerequisites

1. **ClickHouse running locally**
   ```bash
   docker-compose up clickhouse_demo -d
   ```

2. **Environment variables** (create `.env` in project root if not exists)
   ```bash
   # ClickHouse connection
   CLICKHOUSE_URL=http://localhost:8123
   CLICKHOUSE_USER=demo
   CLICKHOUSE_PASSWORD=demo123
   CLICKHOUSE_DATABASE=demo

   # QueryPanel API (get these from scripts/generate-token.ts)
   API_BASE_URL=http://localhost:3000
   ORGANIZATION_ID=your-org-id
   PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n...\n-----END PRIVATE KEY-----"
   TENANT_ID=test-tenant
   ```

3. **Install dependencies**
   ```bash
   bun install
   ```

## Running Tests

### Run all tests
```bash
bun run node-sdk/test-clickhouse.ts
```

### Run specific test
```bash
# Test schema introspection only
bun run node-sdk/test-clickhouse.ts introspect

# Test direct query execution
bun run node-sdk/test-clickhouse.ts query

# Test schema sync to API
bun run node-sdk/test-clickhouse.ts sync

# Test AI query generation
bun run node-sdk/test-clickhouse.ts ask
```

## Test Descriptions

### 1. Schema Introspection (`introspect`)
Tests the adapter's ability to read ClickHouse schema metadata:
- Lists all tables in the database
- Shows table engines, row counts, and byte sizes
- Displays column information (name, type, nullable, primary key)

### 2. Direct Query Execution (`query`)
Tests executing SQL queries directly through the adapter:
- Runs a simple query with parameters
- Validates parameter binding
- Shows query results

### 3. Schema Sync (`sync`)
Tests syncing the schema to the QueryPanel API:
- Sends schema metadata to the API
- Handles schema drift detection
- Shows chunk counts and annotations

### 4. AI Query Generation (`ask`)
Tests the full AI-powered query generation flow:
- Sends a natural language question
- Gets back generated SQL
- Executes the query
- Optionally generates a chart

## Troubleshooting

### ClickHouse connection fails
```
❌ Failed to connect to ClickHouse
```
**Solution:** Make sure ClickHouse is running:
```bash
docker-compose up clickhouse_demo -d
docker-compose ps  # verify it's running
```

### Missing environment variables
```
❌ Missing required environment variables
```
**Solution:** Create a `.env` file with the required variables (see Prerequisites above)

### API connection fails
```
❌ Schema sync failed: fetch failed
```
**Solution:** Make sure the QueryPanel API server is running:
```bash
bun run dev  # in another terminal
```

### TypeScript errors about @clickhouse/client
The test file expects `@clickhouse/client` to be installed. If you see import errors:
```bash
bun add -D @clickhouse/client
```

## Example Output

```
🚀 ClickHouse Adapter Test Suite
==================================================
✅ Connected to ClickHouse
   URL: http://localhost:8123
   Database: demo

📋 Testing Schema Introspection
==================================================

🔍 Introspecting schema...

✅ Found 1 tables:

  📊 orders (table)
     Engine: MergeTree
     Columns: 5
     Rows: 50
     Bytes: 2048
       - id: UInt32 NOT NULL [PK]
       - tenant_id: String NOT NULL
       - user_id: String NOT NULL
       ... and 2 more columns

✅ Test suite complete!
```

## Next Steps

After verifying the adapter works:

1. **Seed test data** (if needed):
   ```bash
   bun run scripts/clickhouse-demo.ts
   ```

2. **Try the full demo**:
   ```bash
   bun run scripts/clickhouse-demo.ts
   ```

3. **Integrate into your application**:
   ```typescript
   import { QueryPanelSdkAPI } from "@querypanel/node-sdk";
   import { createClient } from "@clickhouse/client";

   const client = createClient({ /* config */ });
   const sdk = new QueryPanelSdkAPI(baseUrl, privateKey, orgId);

   sdk.attachClickhouse("mydb", async (params) => {
     return await client.query(params);
   });
   ```
