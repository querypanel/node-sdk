# Integration Tests

Integration tests for database adapters using real PostgreSQL and ClickHouse instances via Docker Compose.

## Prerequisites

- Docker and Docker Compose installed
- Bun runtime

## Running Integration Tests

### 1. Start Test Databases

```bash
bun run docker:test:up
```

This starts:
- PostgreSQL 17 on port `5433`
- ClickHouse 24 on port `8124`

Both databases are initialized with test schemas and data from `test/fixtures/`.

### 2. Run Integration Tests

```bash
# Run all integration tests
bun run test:integration

# Watch mode
bun run test:integration:watch

# Run specific test file
TEST_TYPE=integration bun vitest run test/integration/postgres.test.ts
```

### 3. Stop Test Databases

```bash
bun run docker:test:down
```

## Test Structure

### PostgreSQL Tests (`postgres.test.ts`)
Tests for `PostgresAdapter`:
- Schema introspection (tables, columns, comments)
- SQL execution (SELECT, JOIN, aggregates)
- SQL validation
- Parameter conversion (named → positional)
- Table filtering (allowed tables)

**Test Data:**
- `users` table (3 rows with tenant isolation)
- `orders` table (4 rows with foreign keys)

### ClickHouse Tests (`clickhouse.test.ts`)
Tests for `ClickHouseAdapter`:
- Schema introspection (tables, columns, engines, primary keys)
- SQL execution (SELECT, aggregates, window functions)
- SQL validation
- Named parameter support
- Type handling (arrays, timestamps, etc.)
- Table filtering (allowed tables)

**Test Data:**
- `events` table (3 rows with tenant isolation)
- `metrics` table (3 rows with array fields)

## Database Connections

### PostgreSQL
```
Host: localhost
Port: 5433
Database: test_db
User: test_user
Password: test_password
```

### ClickHouse
```
Host: localhost
Port: 8124 (HTTP)
Database: test_db
User: test_user
Password: test_password
```

## Adding New Tests

1. Add test cases to existing test files
2. If you need new test data, update SQL fixtures in `test/fixtures/`
3. Restart containers to apply fixture changes:
   ```bash
   bun run docker:test:down
   bun run docker:test:up
   ```

## Troubleshooting

### Check database logs
```bash
bun run docker:test:logs
```

### Connect to databases manually

**PostgreSQL:**
```bash
docker exec -it sdk-test-postgres psql -U test_user -d test_db
```

**ClickHouse:**
```bash
docker exec -it sdk-test-clickhouse clickhouse-client --user test_user --password test_password --database test_db
```

### Health checks
The containers have health checks configured. Wait for them to be healthy:
```bash
docker ps
```

Look for `(healthy)` status.

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: oven-sh/setup-bun@v1

      - name: Start test databases
        run: bun run docker:test:up

      - name: Wait for databases
        run: sleep 10

      - name: Run unit tests
        run: bun test

      - name: Run integration tests
        run: bun test:integration

      - name: Stop databases
        run: bun run docker:test:down
        if: always()
```

## Coverage

Integration tests complement unit tests by covering:
- Real SQL query execution
- Database-specific behavior (Postgres vs ClickHouse)
- Introspection of actual database schemas
- Parameter binding and type conversions
- Error messages from real database engines

Unit tests cover business logic and mocking.
Integration tests cover database adapter implementation.
