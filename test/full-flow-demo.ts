/**
 * Full Flow Demo - End-to-End SDK Test with Local API Server
 *
 * This demo:
 * 1. Starts a local Bun.serve() API server on port 3000
 * 2. Connects QueryPanel SDK to the local server
 * 3. Attaches Docker ClickHouse database
 * 4. Runs the complete flow: introspect → sync schema → ask natural language query
 *
 * Prerequisites:
 * - Start ClickHouse: bun run docker:test:up
 * - Set environment variables (see below)
 *
 * Usage:
 *   bun node-sdk/test/full-flow-demo.ts
 */

import type { QueryParams } from "@clickhouse/client";
import { createClient } from "@clickhouse/client";
import { type ClickHouseClientFn, QueryPanelSdkAPI } from "../src/index";

// ============================================================================
// Configuration
// ============================================================================

const PORT = 3001;
const CLICKHOUSE_CONFIG = {
	url: process.env.CLICKHOUSE_URL || "http://localhost:8124",
	username: process.env.CLICKHOUSE_USER || "test_user",
	password: process.env.CLICKHOUSE_PASSWORD || "test_password",
	database: process.env.CLICKHOUSE_DATABASE || "test_db",
};

// These should match your real QueryPanel credentials
const PRIVATE_KEY = process.env.JWT_PRIVATE_KEY;
if (!PRIVATE_KEY) {
	throw new Error("JWT_PRIVATE_KEY is required");
}
const ORGANIZATION_ID =
	process.env.ORGANIZATION_ID || "23011c66-b1dd-40f3-bc88-4065c6357d39";
const TENANT_ID = process.env.TENANT_ID || "tenant-1";

// ============================================================================
// Mock API Server
// ============================================================================

interface IngestRequest {
	database: string;
	schema: {
		name: string;
		dialect: string;
		tables: Array<{
			name: string;
			columns: Array<{
				name: string;
				type: string;
				isPrimaryKey?: boolean;
				comment?: string;
			}>;
			comment?: string;
		}>;
	};
	tenantId: string;
}

interface AskRequest {
	question: string;
	database: string;
	tenantId: string;
	maxRetry?: number;
	context?: Array<{
		text: string;
		metadata?: Record<string, unknown>;
	}>;
}

/**
 * Mock QueryPanel API Server
 * Simulates the real API endpoints for local testing
 */
function startMockApiServer() {
	const server = Bun.serve({
		port: PORT,
		development: false,

		async fetch(req) {
			const url = new URL(req.url);
			console.log(`[API] ${req.method} ${url.pathname}`);

			// CORS headers
			if (req.method === "OPTIONS") {
				return new Response(null, {
					headers: {
						"Access-Control-Allow-Origin": "*",
						"Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
						"Access-Control-Allow-Headers": "Content-Type, Authorization",
					},
				});
			}

			// POST /v1/ingest - Schema Sync
			if (url.pathname === "/v1/ingest" && req.method === "POST") {
				try {
					const body = (await req.json()) as IngestRequest;
					console.log(
						`[API] Schema sync for database: ${body.database}, ${body.schema.tables.length} tables`,
					);

					// Simulate successful schema sync
					return Response.json({
						success: true,
						message: "Schema synced successfully",
						chunks: body.schema.tables.length,
						chunks_with_annotations: body.schema.tables.filter((t) =>
							t.columns.some((c) => c.comment),
						).length,
						schema_hash: crypto.randomUUID(),
						drift_detected: false,
					});
				} catch (error) {
					console.error("[API] Ingest error:", error);
					return Response.json(
						{ error: "Failed to process schema sync" },
						{ status: 500 },
					);
				}
			}

			// POST /v1/query/ask - Natural Language Query
			if (url.pathname === "/v1/query/ask" && req.method === "POST") {
				try {
					const body = (await req.json()) as AskRequest;
					console.log(`[API] AI Query: "${body.question}"`);

					// Simulate AI-generated SQL response
					const mockSql = `SELECT event_name, COUNT(*) as count FROM events WHERE tenant_id = '${body.tenantId}' GROUP BY event_name ORDER BY count DESC LIMIT 10`;

					return Response.json({
						sql: mockSql,
						dialect: "clickhouse",
						rationale: `Generated SQL to answer: "${body.question}"`,
						parameterMapping: {},
						chart: {
							notes:
								"This would typically include a Vega-Lite chart specification",
						},
					});
				} catch (error) {
					console.error("[API] Query error:", error);
					return Response.json(
						{ error: "Failed to process query" },
						{ status: 500 },
					);
				}
			}

			// Health check
			if (url.pathname === "/health") {
				return Response.json({
					status: "ok",
					timestamp: new Date().toISOString(),
				});
			}

			// 404 for unhandled routes
			return Response.json(
				{ error: "Not found", path: url.pathname },
				{ status: 404 },
			);
		},
	});

	console.log(`\n🚀 Mock API Server started on http://localhost:${PORT}`);
	console.log(`   Health: http://localhost:${PORT}/health\n`);

	return server;
}

// ============================================================================
// SDK Demo Flow
// ============================================================================

/**
 * Creates a ClickHouse client function for the SDK
 */
function createClickHouseClient(): ClickHouseClientFn {
	const client = createClient(CLICKHOUSE_CONFIG);

	return async (params: QueryParams) => {
		const resultSet = await client.query({
			query: params.query,
			format: params.format || "JSONEachRow",
			query_params: params.query_params,
			clickhouse_settings: params.clickhouse_settings,
		});
		return resultSet;
	};
}

/**
 * Test ClickHouse connection
 */
async function testClickHouseConnection() {
	console.log("📊 Testing ClickHouse Connection");
	console.log("=".repeat(60));

	const client = createClient(CLICKHOUSE_CONFIG);

	try {
		await client.ping();
		console.log("✅ Connected to ClickHouse");
		console.log(`   URL: ${CLICKHOUSE_CONFIG.url}`);
		console.log(`   Database: ${CLICKHOUSE_CONFIG.database}`);
		console.log(`   User: ${CLICKHOUSE_CONFIG.username}\n`);
		await client.close();
		return true;
	} catch (error) {
		console.error("❌ Failed to connect to ClickHouse:", error);
		console.error("\n💡 Start the database with:");
		console.error("   bun run docker:test:up\n");
		await client.close();
		return false;
	}
}

/**
 * Step 1: Introspect database schema
 */
async function step1_Introspect(sdk: QueryPanelSdkAPI) {
	console.log("\n📋 STEP 1: Schema Introspection");
	console.log("=".repeat(60));

	const schema = await sdk.introspect("demo");

	console.log(`\n✅ Found ${schema.tables.length} tables in database:\n`);

	for (const table of schema.tables) {
		console.log(`  📊 ${table.name} (${table.type})`);
		console.log(`     Columns: ${table.columns.length}`);

		if (table.comment) {
			console.log(`     💬 ${table.comment}`);
		}

		// Show first 3 columns
		const preview = table.columns.slice(0, 3);
		for (const col of preview) {
			const pk = col.isPrimaryKey ? " [PRIMARY KEY]" : "";
			console.log(`       • ${col.name}: ${col.type}${pk}`);
			if (col.comment) {
				console.log(`         💬 "${col.comment}"`);
			}
		}

		if (table.columns.length > 3) {
			console.log(`       ... +${table.columns.length - 3} more columns`);
		}
		console.log();
	}

	return schema;
}

/**
 * Step 2: Sync schema to API
 */
async function step2_SyncSchema(sdk: QueryPanelSdkAPI) {
	console.log("\n🔄 STEP 2: Schema Sync to API");
	console.log("=".repeat(60));

	const result = await sdk.syncSchema("demo", {
		tenantId: TENANT_ID,
	});

	console.log("\n✅ Schema synced successfully!");
	console.log(`   Chunks sent: ${result.chunks}`);
	console.log(`   Chunks with annotations: ${result.chunks_with_annotations}`);
	console.log(`   Message: ${result.message}`);

	if (result.schema_hash) {
		console.log(`   Schema Hash: ${result.schema_hash.substring(0, 24)}...`);
	}

	if (result.drift_detected) {
		console.log(`   ⚠️  Drift detected: Schema changed since last sync`);
	}

	return result;
}

/**
 * Step 3: Execute natural language query
 */
async function step3_AskQuery(sdk: QueryPanelSdkAPI) {
	console.log("\n🤖 STEP 3: Natural Language Query");
	console.log("=".repeat(60));

	const question = "What are the most common events by type?";
	console.log(`\n❓ Question: "${question}"\n`);

	const response = await sdk.ask(question, {
		database: "demo",
		tenantId: TENANT_ID,
		maxRetry: 1,
	});

	console.log("✅ AI-Generated Response:");
	console.log(`\n   SQL Query:`);
	console.log(
		`   ${response.sql
			.split("\n")
			.map((line) => `   ${line}`)
			.join("\n")}`,
	);
	console.log(`\n   Dialect: ${response.dialect}`);

	if (response.rationale) {
		console.log(`   Rationale: ${response.rationale}`);
	}

	console.log(`\n   📊 Results: ${response.rows.length} rows returned`);
	console.log(`   📑 Fields: ${response.fields.join(", ")}`);

	if (response.rows.length > 0) {
		console.log("\n   Top Results:");
		const preview = response.rows.slice(0, 5);
		console.table(preview);
	}

	if (response.chart.vegaLiteSpec) {
		console.log(
			`\n   📈 Chart Type: ${response.chart.vegaLiteSpec.mark || "auto"}`,
		);
	} else if (response.chart.notes) {
		console.log(`\n   💡 Chart Notes: ${response.chart.notes}`);
	}

	return response;
}

// ============================================================================
// Main Flow
// ============================================================================

async function main() {
	console.log("\n");
	console.log("=".repeat(60));
	console.log("  QueryPanel SDK - Full Flow Demo");
	console.log("=".repeat(60));

	// Step 0: Check ClickHouse connection
	const connected = await testClickHouseConnection();
	if (!connected) {
		process.exit(1);
	}

	// Start mock API server
	const server = startMockApiServer();

	// Give server time to start
	await new Promise((resolve) => setTimeout(resolve, 100));

	try {
		// Initialize SDK
		console.log("⚙️  Initializing QueryPanel SDK");
		console.log("=".repeat(60));
		console.log(`   Base URL: http://localhost:3000`);
		console.log(`   Organization ID: ${ORGANIZATION_ID}`);
		console.log(`   Tenant ID: ${TENANT_ID}\n`);

		const sdk = new QueryPanelSdkAPI(
			`http://localhost:3000`,
			PRIVATE_KEY!,
			ORGANIZATION_ID,
			{
				defaultTenantId: TENANT_ID,
			},
		);

		// Attach ClickHouse database
		console.log("🔗 Attaching ClickHouse database...");
		sdk.attachClickhouse("demo", createClickHouseClient(), {
			database: CLICKHOUSE_CONFIG.database,
			description: "Demo ClickHouse database for testing",
			tenantFieldName: "tenant_id",
			tenantFieldType: "String",
			enforceTenantIsolation: true,
		});
		console.log("✅ Database attached: demo\n");

		// Run the full flow
		await step1_Introspect(sdk);
		await step2_SyncSchema(sdk);
		await step3_AskQuery(sdk);

		console.log("\n");
		console.log("=".repeat(60));
		console.log("  ✅ Full Flow Demo Complete!");
		console.log("=".repeat(60));
		console.log("\n💡 Key Takeaways:");
		console.log("   • SDK connected to local API server successfully");
		console.log("   • Schema introspection works with ClickHouse");
		console.log("   • Schema sync sends data to API endpoints");
		console.log("   • Natural language queries generate and execute SQL");
		console.log("   • Tenant isolation is enforced throughout\n");
	} catch (error) {
		console.error("\n❌ Demo failed:", error);
		process.exit(1);
	} finally {
		// Cleanup
		server.stop();
		console.log("🛑 Server stopped\n");
	}
}

// Run the demo
main().catch((error) => {
	console.error("Fatal error:", error);
	process.exit(1);
});
