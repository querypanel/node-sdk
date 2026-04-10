// Note: @clickhouse/client must be installed to run this test
// Run: bun add -D @clickhouse/client
import { createClient, type QueryParams } from "@clickhouse/client";
import { type ClickHouseClientFn, QueryPanelSdkAPI } from "./src/index";

/**
 * Test file for trying out the ClickHouse adapter
 *
 * Prerequisites:
 * 1. ClickHouse running (docker-compose up clickhouse_demo -d)
 * 2. Environment variables set (see .env.example)
 *
 * Usage:
 *   bun run node-sdk/test-clickhouse.ts
 */

// ClickHouse client configuration
const client = createClient({
	url: process.env.CLICKHOUSE_URL || "http://localhost:8123",
	username: process.env.CLICKHOUSE_USER || "demo",
	password: process.env.CLICKHOUSE_PASSWORD || "demo123",
	database: process.env.CLICKHOUSE_DATABASE || "demo",
});

// Create a ClickHouseClientFn for the SDK
const createClickHouseClientFn = (): ClickHouseClientFn => {
	return async (params: QueryParams) => {
		const resultSet = await client.query({
			query: params.query,
			format: params.format || "JSONEachRow",
			query_params: params.query_params,
			clickhouse_settings: params.clickhouse_settings,
		});
		return resultSet;
	};
};

async function testIntrospection() {
	console.log("\n📋 Testing Schema Introspection");
	console.log("=".repeat(50));

	const baseUrl = process.env.API_BASE_URL || "http://localhost:3000";
	const privateKey = process.env.PRIVATE_KEY || "";
	const organizationId = process.env.ORGANIZATION_ID || "";
	const tenantId = process.env.TENANT_ID || "test-tenant";

	if (!privateKey || !organizationId) {
		console.error("❌ Missing required environment variables:");
		console.error("   PRIVATE_KEY, ORGANIZATION_ID");
		console.error("   See .env.example for details");
		return;
	}

	const sdk = new QueryPanelSdkAPI(baseUrl, privateKey, organizationId, {
		defaultTenantId: tenantId,
	});

	// Attach ClickHouse database
	sdk.attachClickhouse("demo", createClickHouseClientFn(), {
		database: process.env.CLICKHOUSE_DATABASE || "demo",
		description: "ClickHouse test database",
		tenantFieldName: "customer_id",
		tenantFieldType: "Int32",
	});

	try {
		console.log("\n🔍 Introspecting schema...");
		const schema = await sdk.introspect("demo");

		console.log(`\n✅ Found ${schema.tables.length} tables:`);
		for (const table of schema.tables) {
			console.log(`\n  📊 ${table.name} (${table.type})`);
			console.log(`     Columns: ${table.columns.length}`);
			if (table.comment) {
				console.log(`     Comment: ${table.comment}`);
			}

			// Show first 3 columns
			const columnsToShow = table.columns.slice(0, 3);
			for (const col of columnsToShow) {
				const pk = col.isPrimaryKey ? " [PK]" : "";
				console.log(`       - ${col.name}: ${col.type}${pk}`);
				if (col.comment) {
					console.log(`         "${col.comment}"`);
				}
			}
			if (table.columns.length > 3) {
				console.log(`       ... and ${table.columns.length - 3} more columns`);
			}
		}
	} catch (error) {
		console.error("\n❌ Introspection failed:", error);
	}
}

async function testDirectQuery() {
	console.log("\n🔍 Testing Direct Query Execution");
	console.log("=".repeat(50));

	const baseUrl = process.env.API_BASE_URL || "http://localhost:3000";
	const privateKey = process.env.PRIVATE_KEY || "";
	const organizationId = process.env.ORGANIZATION_ID || "";
	const tenantId = process.env.TENANT_ID || "3";

	if (!privateKey || !organizationId) {
		console.error("❌ Missing required environment variables");
		return;
	}

	const sdk = new QueryPanelSdkAPI(baseUrl, privateKey, organizationId, {
		defaultTenantId: tenantId,
	});

	sdk.attachClickhouse("demo", createClickHouseClientFn(), {
		database: process.env.CLICKHOUSE_DATABASE || "demo",
		tenantFieldName: "tenant_id",
	});

	try {
		// Test a simple query
		console.log("\n📊 Running test query...");
		const sql =
			"SELECT name, engine FROM system.tables WHERE database = {db:String} LIMIT 5";
		const params = { db: process.env.CLICKHOUSE_DATABASE || "demo" };

		console.log(`   SQL: ${sql}`);
		console.log(`   Params:`, params);

		// Use the introspect method to test database access
		// (We can't access the adapter directly through the public API)
		const schema = await sdk.introspect("demo");
		console.log(
			`\n✅ Database accessible, found ${schema.tables.length} tables`,
		);

		if (schema.tables.length > 0) {
			console.log("\n   First table:");
			const firstTable = schema.tables[0];
			console.log(`   Name: ${firstTable.name}`);
			console.log(`   Type: ${firstTable.type}`);
			console.log(`   Columns: ${firstTable.columns.length}`);
		}
	} catch (error) {
		console.error("\n❌ Query execution failed:", error);
	}
}

async function testSchemaSync() {
	console.log("\n🔄 Testing Schema Sync");
	console.log("=".repeat(50));

	const baseUrl = process.env.API_BASE_URL || "http://localhost:3000";
	const privateKey = process.env.PRIVATE_KEY || "";
	const organizationId = process.env.ORGANIZATION_ID || "";
	const tenantId = process.env.TENANT_ID || "test-tenant";

	if (!privateKey || !organizationId) {
		console.error("❌ Missing required environment variables");
		return;
	}

	const sdk = new QueryPanelSdkAPI(baseUrl, privateKey, organizationId, {
		defaultTenantId: tenantId,
	});

	sdk.attachClickhouse("demo", createClickHouseClientFn(), {
		database: process.env.CLICKHOUSE_DATABASE || "demo",
		description: "ClickHouse test database",
		tenantFieldName: "tenant_id",
	});

	try {
		console.log("\n📤 Syncing schema to API...");
		const result = await sdk.syncSchema("demo", { tenantId });

		console.log("\n✅ Schema sync completed:");
		console.log(`   Success: ${result.success}`);
		console.log(`   Message: ${result.message}`);
		console.log(`   Chunks: ${result.chunks}`);
		console.log(
			`   Chunks with annotations: ${result.chunks_with_annotations}`,
		);

		if (result.schema_hash) {
			console.log(`   Schema hash: ${result.schema_hash.substring(0, 16)}...`);
		}
		if (result.drift_detected) {
			console.log(`   ⚠️  Drift detected: ${result.drift_detected}`);
		}
		if (result.skipped) {
			console.log(`   ℹ️  Skipped (no changes)`);
		}
	} catch (error) {
		const err = error as Error & { details?: unknown };
		console.error("\n❌ Schema sync failed:", err.message);
		if (err.details) {
			console.error("   Details:", err.details);
		}
	}
}

async function testAskQuery() {
	console.log("\n🤖 Testing AI Query Generation");
	console.log("=".repeat(50));

	const baseUrl = process.env.API_BASE_URL || "http://localhost:3000";
	const privateKey = process.env.PRIVATE_KEY || "";
	const organizationId = process.env.ORGANIZATION_ID || "";
	const tenantId = process.env.TENANT_ID || "test-tenant";

	if (!privateKey || !organizationId) {
		console.error("❌ Missing required environment variables");
		return;
	}

	const sdk = new QueryPanelSdkAPI(baseUrl, privateKey, organizationId, {
		defaultTenantId: tenantId,
	});

	sdk.attachClickhouse("demo", createClickHouseClientFn(), {
		database: process.env.CLICKHOUSE_DATABASE || "demo",
		description: "ClickHouse test database",
		tenantFieldName: "tenant_id",
	});

	try {
		const question = "Show me the first 5 tables in the database";
		console.log(`\n❓ Question: "${question}"`);

		const response = await sdk.ask(question, {
			tenantId,
			database: "demo",
			maxRetry: 1,
		});

		console.log("\n✅ AI Response:");
		console.log(`   SQL: ${response.sql}`);
		console.log(`   Dialect: ${response.dialect}`);

		if (response.rationale) {
			console.log(`   Rationale: ${response.rationale}`);
		}

		console.log(`   Rows returned: ${response.rows.length}`);
		console.log(`   Fields: ${response.fields.join(", ")}`);

		if (response.rows.length > 0) {
			console.log("\n   Results:");
			console.table(response.rows.slice(0, 5));
		}

		if (response.chart.vegaLiteSpec) {
			console.log(
				`\n   📈 Chart generated: ${response.chart.vegaLiteSpec.mark}`,
			);
		} else if (response.chart.notes) {
			console.log(`\n   ℹ️  Chart notes: ${response.chart.notes}`);
		}
	} catch (error) {
		const err = error as Error & { details?: unknown };
		console.error("\n❌ AI query failed:", err.message);
		if (err.details) {
			console.error("   Details:", err.details);
		}
	}
}

async function main() {
	console.log("🚀 ClickHouse Adapter Test Suite");
	console.log("=".repeat(50));

	// Check ClickHouse connection
	try {
		await client.ping();
		console.log("✅ Connected to ClickHouse");
		console.log(
			`   URL: ${process.env.CLICKHOUSE_URL || "http://localhost:8123"}`,
		);
		console.log(`   Database: ${process.env.CLICKHOUSE_DATABASE || "demo"}`);
	} catch {
		console.error("❌ Failed to connect to ClickHouse");
		console.error("   Make sure ClickHouse is running:");
		console.error("   docker-compose up clickhouse_demo -d");
		process.exit(1);
	}

	// Run tests based on command line argument
	const testName = process.argv[2];

	try {
		switch (testName) {
			case "introspect":
				await testIntrospection();
				break;
			case "query":
				await testDirectQuery();
				break;
			case "sync":
				await testSchemaSync();
				break;
			case "ask":
				await testAskQuery();
				break;
			case "all":
			default:
				await testIntrospection();
				await testDirectQuery();
				await testSchemaSync();
				await testAskQuery();
				break;
		}
	} catch (error) {
		const err = error as Error;
		console.error("\n❌ Test failed:", err.message);
		process.exit(1);
	} finally {
		await client.close();
		console.log("\n✅ Test suite complete!");
	}
}

// Run the tests
main().catch((error) => {
	console.error("Fatal error:", error);
	process.exit(1);
});
