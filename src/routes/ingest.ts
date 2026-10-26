import crypto from 'node:crypto';
import type { IQueryPanelApi } from "../core/api-types";
import type { QueryEngine } from "../core/query-engine";
import { SchemaTooLargeError } from "../errors";
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
	schema_name?: string;
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
	client: IQueryPanelApi,
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

// ---------------------------------------------------------------------------
// Batched ingest (session protocol): stage -> embed each batch -> finalize
// ---------------------------------------------------------------------------

export const DEFAULT_MAX_BATCH_DOCS = 1000;
export const DEFAULT_MAX_BATCH_BYTES = 2_000_000;

/** Progress events emitted by stageSchemaIngest / syncSchemaInBatches. */
export type SchemaSyncProgressEvent =
	| { phase: "introspecting" }
	| { phase: "introspected"; tableCount: number; columnCount: number }
	| { phase: "uploading"; done: number; total: number }
	| { phase: "committing" }
	| { phase: "embedding"; done: number; total: number; chunks: number }
	| { phase: "finalizing" };

export interface SchemaIngestRequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

export interface StageSchemaIngestOptions extends SchemaSyncOptions {
	/** Max docs per batch (1 per table + 1 per column). Default 1000. */
	maxBatchDocs?: number;
	/** Max serialized JSON bytes per batch. Default 2_000_000. */
	maxBatchBytes?: number;
	/** Throw SchemaTooLargeError (before uploading) above this many columns. */
	maxColumns?: number;
	onProgress?: (event: SchemaSyncProgressEvent) => void;
}

export type SyncSchemaInBatchesOptions = StageSchemaIngestOptions;

export interface StageSchemaIngestResult {
	sessionId: string;
	status: "skipped" | "embedding";
	totalBatches: number;
	tableCount: number;
	columnCount: number;
	/** Present when status is "skipped" (schema unchanged). */
	skippedResult?: IngestResponse;
	driftDetected?: boolean;
}

export interface SchemaIngestBatchEmbedResponse {
	index: number;
	chunks: number;
	chunks_with_annotations: number;
}

interface IngestSessionCreateResponse {
	session_id: string;
}

type IngestSessionCommitResponse =
	| ({ status: "skipped" } & IngestResponse)
	| { status: "embedding"; total_batches: number; drift_detected?: boolean };

const utf8 = new TextEncoder();

function jsonBytes(value: unknown): number {
	return utf8.encode(JSON.stringify(value)).length;
}

const BATCH_ENVELOPE_BYTES = jsonBytes({ tables: [] });

/**
 * Split tables (keeping order) into batches bounded by doc count and JSON size.
 * A single table larger than either limit is placed alone in its own batch.
 * Always returns at least one batch (possibly empty) so a session has >= 1 batch.
 */
export function splitIngestTablesIntoBatches<T extends { columns: unknown[] }>(
	tables: T[],
	maxBatchDocs: number = DEFAULT_MAX_BATCH_DOCS,
	maxBatchBytes: number = DEFAULT_MAX_BATCH_BYTES,
): T[][] {
	const batches: T[][] = [];
	let current: T[] = [];
	let currentDocs = 0;
	let currentBytes = BATCH_ENVELOPE_BYTES;

	for (const table of tables) {
		const docs = 1 + table.columns.length;
		const bytes = jsonBytes(table);
		// +1 for the separating comma when appended to a non-empty batch.
		if (
			current.length > 0 &&
			(currentDocs + docs > maxBatchDocs ||
				currentBytes + 1 + bytes > maxBatchBytes)
		) {
			batches.push(current);
			current = [];
			currentDocs = 0;
			currentBytes = BATCH_ENVELOPE_BYTES;
		}
		currentBytes += bytes + (current.length > 0 ? 1 : 0);
		current.push(table);
		currentDocs += docs;
	}

	if (current.length > 0 || batches.length === 0) {
		batches.push(current);
	}
	return batches;
}

function sessionPath(sessionId: string, suffix: string): string {
	return `/ingest/sessions/${encodeURIComponent(sessionId)}${suffix}`;
}

/**
 * Introspect, upload the schema in batches to a new ingest session, and commit it.
 * Embedding is left to embedSchemaIngestBatch / finalizeSchemaIngest so callers
 * (e.g. serverless routes) can spread the work over several requests.
 */
export async function stageSchemaIngest(
	client: IQueryPanelApi,
	queryEngine: QueryEngine,
	databaseName: string,
	options: StageSchemaIngestOptions,
	signal?: AbortSignal,
): Promise<StageSchemaIngestResult> {
	const tenantId = resolveTenantId(client, options.tenantId);
	const adapter = queryEngine.getDatabase(databaseName);
	const metadata = queryEngine.getDatabaseMetadata(databaseName);
	const emit = options.onProgress ?? (() => {});

	emit({ phase: "introspecting" });
	const introspection = await adapter.introspect(
		options.tables ? { tables: options.tables } : undefined,
	);
	const { tables, ...sessionFields } = buildSchemaRequest(
		databaseName,
		adapter,
		introspection,
		metadata,
	);
	const tableCount = tables.length;
	const columnCount = tables.reduce((sum, t) => sum + t.columns.length, 0);
	emit({ phase: "introspected", tableCount, columnCount });

	if (options.maxColumns !== undefined && columnCount > options.maxColumns) {
		throw new SchemaTooLargeError(tableCount, columnCount, options.maxColumns);
	}

	const batches = splitIngestTablesIntoBatches(
		tables,
		options.maxBatchDocs ?? DEFAULT_MAX_BATCH_DOCS,
		options.maxBatchBytes ?? DEFAULT_MAX_BATCH_BYTES,
	);
	const totalBatches = batches.length;

	const created = await client.post<IngestSessionCreateResponse>(
		"/ingest/sessions",
		{
			...sessionFields,
			...(options.forceReindex ? { force_reindex: true } : {}),
			total_batches: totalBatches,
		},
		tenantId,
		options.userId,
		options.scopes,
		signal,
		crypto.randomUUID(),
	);
	const sessionId = created.session_id;

	for (let index = 0; index < totalBatches; index += 1) {
		await client.put(
			sessionPath(sessionId, `/batches/${index}`),
			{ tables: batches[index] },
			tenantId,
			options.userId,
			options.scopes,
			signal,
			sessionId,
		);
		emit({ phase: "uploading", done: index + 1, total: totalBatches });
	}

	emit({ phase: "committing" });
	const committed = await client.post<IngestSessionCommitResponse>(
		sessionPath(sessionId, "/commit"),
		{},
		tenantId,
		options.userId,
		options.scopes,
		signal,
		sessionId,
	);

	if (committed.status === "skipped") {
		const { status: _status, ...skippedResult } = committed;
		return {
			sessionId,
			status: "skipped",
			totalBatches,
			tableCount,
			columnCount,
			skippedResult,
			driftDetected: skippedResult.drift_detected,
		};
	}

	return {
		sessionId,
		status: "embedding",
		totalBatches: committed.total_batches ?? totalBatches,
		tableCount,
		columnCount,
		driftDetected: committed.drift_detected,
	};
}

/** Embed one committed batch of an ingest session. */
export async function embedSchemaIngestBatch(
	client: IQueryPanelApi,
	sessionId: string,
	index: number,
	options: SchemaIngestRequestOptions,
	signal?: AbortSignal,
): Promise<SchemaIngestBatchEmbedResponse> {
	const tenantId = resolveTenantId(client, options.tenantId);
	return await client.post<SchemaIngestBatchEmbedResponse>(
		sessionPath(sessionId, `/batches/${index}/embed`),
		{},
		tenantId,
		options.userId,
		options.scopes,
		signal,
		sessionId,
	);
}

/** Finalize an ingest session once every batch is embedded. */
export async function finalizeSchemaIngest(
	client: IQueryPanelApi,
	sessionId: string,
	options: SchemaIngestRequestOptions,
	signal?: AbortSignal,
): Promise<IngestResponse> {
	const tenantId = resolveTenantId(client, options.tenantId);
	return await client.post<IngestResponse>(
		sessionPath(sessionId, "/finalize"),
		{},
		tenantId,
		options.userId,
		options.scopes,
		signal,
		sessionId,
	);
}

/**
 * Stage, embed every batch, and finalize in-process. For server-side callers
 * without per-request time limits; returns the skipped result when unchanged.
 */
export async function syncSchemaInBatches(
	client: IQueryPanelApi,
	queryEngine: QueryEngine,
	databaseName: string,
	options: SyncSchemaInBatchesOptions,
	signal?: AbortSignal,
): Promise<IngestResponse> {
	const staged = await stageSchemaIngest(
		client,
		queryEngine,
		databaseName,
		options,
		signal,
	);
	if (staged.status === "skipped") {
		return staged.skippedResult as IngestResponse;
	}

	const emit = options.onProgress ?? (() => {});
	const requestOptions: SchemaIngestRequestOptions = {
		tenantId: options.tenantId,
		userId: options.userId,
		scopes: options.scopes,
	};
	let chunks = 0;
	for (let index = 0; index < staged.totalBatches; index += 1) {
		const embedded = await embedSchemaIngestBatch(
			client,
			staged.sessionId,
			index,
			requestOptions,
			signal,
		);
		chunks += embedded.chunks ?? 0;
		emit({
			phase: "embedding",
			done: index + 1,
			total: staged.totalBatches,
			chunks,
		});
	}

	emit({ phase: "finalizing" });
	return await finalizeSchemaIngest(
		client,
		staged.sessionId,
		requestOptions,
		signal,
	);
}

function resolveTenantId(client: IQueryPanelApi, tenantId?: string): string {
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
		schema_name: table.schema,
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
	if (metadata?.enforceTenantIsolation === false) {
		request.tenant_settings = {
			tenantFieldName: metadata.tenantFieldName ?? "",
			tenantFieldType: metadata.tenantFieldType ?? "String",
			enforceTenantIsolation: false,
		};
	} else if (metadata?.tenantFieldName && metadata?.tenantFieldType) {
		request.tenant_settings = {
			tenantFieldName: metadata.tenantFieldName,
			tenantFieldType: metadata.tenantFieldType,
			enforceTenantIsolation: true,
		};
	}

	return request;
}
