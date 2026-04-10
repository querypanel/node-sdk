# Test Suite

This directory contains integration tests and demo scripts for the QueryPanel SDK.

## Full Flow Demo

The `full-flow-demo.ts` is a **real executable demo** (not a vitest test) that demonstrates the complete SDK workflow with a local API server and Docker ClickHouse database.

### What it Does

1. **Starts a local Bun.serve() API server** on port 3000 that mocks QueryPanel API endpoints
2. **Initializes the QueryPanel SDK** pointing to the local server
3. **Connects to Docker ClickHouse** database
4. **Runs the complete flow**:
   - Schema introspection (reads database structure)
   - Schema sync (sends schema to API)
   - Natural language query (generates and executes SQL)

### Quick Start

```bash
# 1. Start the Docker ClickHouse database
bun run docker:test:up

# 2. Wait a few seconds for the database to be ready
sleep 5

# 3. Run the demo
bun run demo:full-flow

# 4. Clean up when done
bun run docker:test:down
```

### Environment Variables

The demo uses these environment variables (all have sensible defaults):

```bash
# ClickHouse connection (defaults work with docker:test:up)
CLICKHOUSE_URL=http://localhost:8124
CLICKHOUSE_USER=test_user
CLICKHOUSE_PASSWORD=test_password
CLICKHOUSE_DATABASE=test_db

# QueryPanel credentials (you can override these)
PRIVATE_KEY=your-private-key-here
ORGANIZATION_ID=23011c66-b1dd-40f3-bc88-4065c6357d39
TENANT_ID=tenant-1
```

### Output Example

```
🚀 Mock API Server started on http://localhost:3000

📊 Testing ClickHouse Connection
✅ Connected to ClickHouse
   URL: http://localhost:8124
   Database: test_db

📋 STEP 1: Schema Introspection
✅ Found 2 tables in database:
  📊 events (MergeTree)
     Columns: 6
     💬 Event tracking table

🔄 STEP 2: Schema Sync to API
✅ Schema synced successfully!

🤖 STEP 3: Natural Language Query
❓ Question: "What are the most common events by type?"
✅ AI-Generated Response:
   SQL Query: SELECT event_name, COUNT(*) as count FROM events...
```

## Integration Tests

The `integration/` directory contains vitest integration tests that run against real Docker databases.

See the main [TESTING.md](../TESTING.md) for full testing documentation.

## Test Data

The `fixtures/` directory contains SQL initialization scripts that populate the test databases with sample data.

- `postgres-init.sql` - PostgreSQL test schema and data
- `clickhouse-init.sql` - ClickHouse test schema and data
