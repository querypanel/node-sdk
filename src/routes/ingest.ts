import crypto from 'node:crypto';
import type { ApiClient } from "../core/client";
import type { QueryEngine } from "../core/query-engine";
import type { SchemaIntrospection } from "../schema/types";

export interface IngestResponse {
	success: boolean;
	message: string;
	chunks: number;
	chunks_with_annotations: number;
	schema_id?: string;
	schema_hash?: string;
	drift_detected?: boolean;
	skipped?: boolean;
}

export interface SchemaSyncOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	tables?: string[];
	forceReindex?: boolean;
}

interface SchemaIngestColumn {
	name: string;
	data_type: string;
	is_primary_key: boolean;
	description: string;
}

interface SchemaIngestTable {
	table_name: string;
	description: string;
	columns: SchemaIngestColumn[];
}

interface SchemaIngestRequest {
	database: string;
	dialect: string;
	tables: SchemaIngestTable[];
	force_reindex?: boolean;
	tenant_settings?: {
		tenantFieldName: string;
		tenantFieldType: string;
		enforceTenantIsolation: boolean;
	};
}

/**
 * Route module for schema ingestion
 * Handles introspection and sync to backend
 */
export async function syncSchema(
	client: ApiClient,
	queryEngine: QueryEngine,
	databaseName: string,
	options: SchemaSyncOptions,
	signal?: AbortSignal,
): Promise<IngestResponse> {
	const tenantId = resolveTenantId(client, options.tenantId);
	const adapter = queryEngine.getDatabase(databaseName);
	const metadata = queryEngine.getDatabaseMetadata(databaseName);

	const introspection = await adapter.introspect(
		options.tables ? { tables: options.tables } : undefined,
	);

	const payload = buildSchemaRequest(databaseName, adapter, introspection, metadata);
	if (options.forceReindex) {
		payload.force_reindex = true;
	}

	// Generate a session id so backend telemetry can correlate all work for this sync
	const sessionId = crypto.randomUUID();

	const response = await client.post<IngestResponse>(
		"/ingest",
		payload,
		tenantId,
		options.userId,
		options.scopes,
		signal,
		sessionId,
	);

	return response;
}

function resolveTenantId(client: ApiClient, tenantId?: string): string {
	const resolved = tenantId ?? client.getDefaultTenantId();
	if (!resolved) {
		throw new Error(
			"tenantId is required. Provide it per request or via defaultTenantId option.",
		);
	}
	return resolved;
}

function buildSchemaRequest(
	databaseName: string,
	adapter: { getDialect: () => string },
	introspection: SchemaIntrospection,
	metadata?: {
		tenantFieldName?: string;
		tenantFieldType?: string;
		enforceTenantIsolation?: boolean;
	},
): SchemaIngestRequest {
	const dialect = adapter.getDialect();
	const tables: SchemaIngestTable[] = introspection.tables.map((table) => ({
		table_name: table.name,
		description: table.comment ?? `Table ${table.name}`,
		columns: table.columns.map((column) => ({
			name: column.name,
			data_type: column.rawType ?? column.type,
			is_primary_key: Boolean(column.isPrimaryKey),
			description: column.comment ?? "",
		})),
	}));

	const request: SchemaIngestRequest = {
		database: databaseName,
		dialect,
		tables,
	};

	// Include tenant_settings if configured in the database metadata
	if (
		metadata?.tenantFieldName &&
		metadata?.tenantFieldType &&
		metadata?.enforceTenantIsolation !== undefined
	) {
		request.tenant_settings = {
			tenantFieldName: metadata.tenantFieldName,
			tenantFieldType: metadata.tenantFieldType,
			enforceTenantIsolation: metadata.enforceTenantIsolation,
		};
	}

	return request;
}
