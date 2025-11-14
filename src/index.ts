import { createHash, randomUUID } from "node:crypto";
import { importPKCS8, SignJWT } from "jose";
import {
	ClickHouseAdapter,
	type ClickHouseAdapterOptions,
	type ClickHouseClientFn,
} from "./adapters/clickhouse";
import {
	PostgresAdapter,
	type PostgresAdapterOptions,
	type PostgresClientFn,
} from "./adapters/postgres";
import type { DatabaseAdapter, DatabaseDialect } from "./adapters/types";
import type { SchemaIntrospection } from "./schema/types";

export { ClickHouseAdapter, PostgresAdapter };

export type {
	ClickHouseAdapterOptions,
	ClickHouseClientFn,
	DatabaseAdapter,
	DatabaseDialect,
	PostgresAdapterOptions,
	PostgresClientFn,
	SchemaIntrospection,
};

export type ParamValue = string | number | boolean | string[] | number[];
export type ParamRecord = Record<string, ParamValue>;

export interface ContextDocument {
	source?: string;
	pageContent: string;
	metadata?: Record<string, unknown>;
	score?: number;
}

export interface ChartEnvelope {
	vegaLiteSpec: Record<string, unknown> | null;
	notes: string | null;
}

export interface AskOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	database?: string;
	lastError?: string;
	previousSql?: string;
	maxRetry?: number;
	chartMaxRetries?: number;
	disableAutoSync?: boolean;
}

export interface AskResponse {
	sql: string;
	params: ParamRecord;
	paramMetadata: Array<Record<string, unknown>>;
	rationale?: string;
	dialect: string;
	queryId?: string;
	rows: Array<Record<string, unknown>>;
	fields: string[];
	chart: ChartEnvelope;
	context?: ContextDocument[];
}

export interface SchemaSyncOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	tables?: string[];
	force?: boolean;
}

export interface KnowledgeBaseAnnotation {
	id: string;
	organization_id: string;
	target_identifier: string;
	content: string;
	created_by: string;
	updated_by: string;
	created_at: string;
	updated_at: string;
}

export interface KnowledgeBaseAnnotationInput {
	targetIdentifier: string;
	content: string;
	userId: string;
	tenantId?: string;
}

export interface KnowledgeBaseChunkTable {
	table_name: string;
	gold_sql?: Array<{ sql: string; description?: string; name?: string }>;
	glossary?: Array<{ term: string; definition: string }>;
}

export interface KnowledgeBaseChunkRequest {
	database: string;
	dialect: string;
	tables: KnowledgeBaseChunkTable[];
	tenantId?: string;
}

export interface KnowledgeBaseChunksResponse {
	success: boolean;
	message: string;
	chunks: {
		total: number;
		gold_sql: number;
		glossary: number;
		chunks_with_annotations: number;
	};
}

export interface SdkChart {
	id: string;
	title: string;
	description: string | null;
	sql: string;
	sql_params: Record<string, unknown> | null;
	vega_lite_spec: Record<string, unknown>;
	query_id: string | null;
	organization_id: string | null;
	tenant_id: string | null;
	user_id: string | null;
	created_at: string | null;
	updated_at: string | null;
	active?: boolean;
	database?: string | null;
}

export interface SdkActiveChart {
	id: string;
	chart_id: string;
	order: number | null;
	meta: Record<string, unknown> | null;
	organization_id: string | null;
	tenant_id: string | null;
	user_id: string | null;
	created_at: string | null;
	updated_at: string | null;
	chart?: SdkChart | null;
}

export interface ChartCreateInput {
	title: string;
	description?: string;
	sql: string;
	sql_params?: Record<string, unknown>;
	vega_lite_spec: Record<string, unknown>;
	query_id?: string;
	database?: string;
}

export interface ChartUpdateInput {
	title?: string;
	description?: string;
	sql?: string;
	sql_params?: Record<string, unknown>;
	vega_lite_spec?: Record<string, unknown>;
	database?: string;
}

export interface ActiveChartCreateInput {
	chart_id: string;
	order?: number;
	meta?: Record<string, unknown>;
}

export interface ActiveChartUpdateInput {
	chart_id?: string;
	order?: number;
	meta?: Record<string, unknown>;
}

export interface PaginationQuery {
	page?: number;
	limit?: number;
}

export interface PaginationInfo {
	page: number;
	limit: number;
	total: number;
	totalPages: number;
	hasNext: boolean;
	hasPrev: boolean;
}

export interface PaginatedResponse<T> {
	data: T[];
	pagination: PaginationInfo;
}

export interface ChartListOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
	pagination?: PaginationQuery;
	sortBy?: "title" | "user_id" | "created_at" | "updated_at";
	sortDir?: "asc" | "desc";
	title?: string;
	userFilter?: string;
	createdFrom?: string;
	createdTo?: string;
	updatedFrom?: string;
	updatedTo?: string;
	includeData?: boolean;
}

export interface ActiveChartListOptions extends ChartListOptions {
	withData?: boolean;
}

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

interface DatabaseMetadata {
	name: string;
	dialect: DatabaseDialect;
	description?: string;
	tags?: string[];
	tenantFieldName?: string;
	tenantFieldType?: string;
	enforceTenantIsolation?: boolean;
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
}

interface ServerQueryResponse {
	success: boolean;
	sql: string;
	params?: Array<Record<string, unknown>>;
	dialect: string;
	rationale?: string;
	queryId?: string;
	context?: ContextDocument[];
}

interface ServerChartResponse {
	chart: Record<string, unknown> | null;
	notes: string | null;
}

interface RequestOptions {
	tenantId?: string;
	userId?: string;
	scopes?: string[];
}

export class QueryPanelSdkAPI {
	private readonly baseUrl: string;
	private readonly privateKey: string;
	private readonly organizationId: string;
	private readonly defaultTenantId?: string;
	private readonly additionalHeaders?: Record<string, string>;
	private readonly fetchImpl: typeof fetch;
	private cachedPrivateKey?: Awaited<ReturnType<typeof importPKCS8>>;

	private databases = new Map<string, DatabaseAdapter>();
	private databaseMetadata = new Map<string, DatabaseMetadata>();
	private defaultDatabase?: string;
	private lastSyncHashes = new Map<string, string>();
	private syncedDatabases = new Set<string>();

	constructor(
		baseUrl: string,
		privateKey: string,
		organizationId: string,
		options?: {
			defaultTenantId?: string;
			additionalHeaders?: Record<string, string>;
			fetch?: typeof fetch;
		},
	) {
		if (!baseUrl) {
			throw new Error("Base URL is required");
		}
		if (!privateKey) {
			throw new Error("Private key is required");
		}
		if (!organizationId) {
			throw new Error("Organization ID is required");
		}

		this.baseUrl = baseUrl.replace(/\/+$/, "");
		this.privateKey = privateKey;
		this.organizationId = organizationId;
		this.defaultTenantId = options?.defaultTenantId;
		this.additionalHeaders = options?.additionalHeaders;
		this.fetchImpl = options?.fetch ?? globalThis.fetch;

		if (!this.fetchImpl) {
			throw new Error(
				"Fetch implementation not found. Provide options.fetch or use Node 18+.",
			);
		}
	}

	attachClickhouse(
		name: string,
		clientFn: ClickHouseClientFn,
		options?: ClickHouseAdapterOptions & {
			description?: string;
			tags?: string[];
			tenantFieldName?: string;
			tenantFieldType?: string;
			enforceTenantIsolation?: boolean;
		},
	): void {
		const adapter = new ClickHouseAdapter(clientFn, options);
		this.attachDatabase(name, adapter);

		const metadata: DatabaseMetadata = {
			name,
			dialect: "clickhouse",
			description: options?.description,
			tags: options?.tags,
			tenantFieldName: options?.tenantFieldName,
			tenantFieldType: options?.tenantFieldType ?? "String",
			enforceTenantIsolation: options?.tenantFieldName
				? (options?.enforceTenantIsolation ?? true)
				: undefined,
		};

		this.databaseMetadata.set(name, metadata);
	}

	attachPostgres(
		name: string,
		clientFn: PostgresClientFn,
		options?: PostgresAdapterOptions & {
			description?: string;
			tags?: string[];
			tenantFieldName?: string;
			enforceTenantIsolation?: boolean;
		},
	): void {
		const adapter = new PostgresAdapter(clientFn, options);
		this.attachDatabase(name, adapter);

		const metadata: DatabaseMetadata = {
			name,
			dialect: "postgres",
			description: options?.description,
			tags: options?.tags,
			tenantFieldName: options?.tenantFieldName,
			enforceTenantIsolation: options?.tenantFieldName
				? (options?.enforceTenantIsolation ?? true)
				: undefined,
		};

		this.databaseMetadata.set(name, metadata);
	}

	attachDatabase(name: string, adapter: DatabaseAdapter): void {
		this.databases.set(name, adapter);
		if (!this.defaultDatabase) {
			this.defaultDatabase = name;
		}
	}

	async syncSchema(
		databaseName: string,
		options: SchemaSyncOptions,
		signal?: AbortSignal,
	): Promise<IngestResponse> {
		const tenantId = this.resolveTenantId(options.tenantId);
		const adapter = this.getDatabase(databaseName);
		const introspection = await adapter.introspect(
			options.tables ? { tables: options.tables } : undefined,
		);

		const payload = this.buildSchemaRequest(
			databaseName,
			adapter,
			introspection,
		);
		const hash = this.hashSchemaRequest(payload);
		const previousHash = this.lastSyncHashes.get(databaseName);

		if (!options.force && previousHash === hash) {
			return {
				success: true,
				message: "Schema unchanged; skipping ingestion",
				chunks: 0,
				chunks_with_annotations: 0,
				schema_hash: hash,
				skipped: true,
			};
		}

		// Generate a session id so backend telemetry can correlate all work for this sync
		const sessionId = randomUUID();

		const response = await this.post<IngestResponse>(
			"/ingest",
			payload,
			tenantId,
			options.userId,
			options.scopes,
			signal,
			sessionId,
		);

		this.lastSyncHashes.set(databaseName, hash);
		this.syncedDatabases.add(databaseName);

		return response;
	}

	async introspect(
		databaseName: string,
		tables?: string[],
	): Promise<SchemaIntrospection> {
		const adapter = this.getDatabase(databaseName);
		return await adapter.introspect(tables ? { tables } : undefined);
	}

	async ask(
		question: string,
		options: AskOptions,
		signal?: AbortSignal,
	): Promise<AskResponse> {
		const tenantId = this.resolveTenantId(options.tenantId);

		await this.ensureSchemasSynced(
			tenantId,
			options.userId,
			options.scopes,
			options.disableAutoSync,
		);

		const sessionId = randomUUID();

		const queryResponse = await this.post<ServerQueryResponse>(
			"/query",
			{
				question,
				...(options.lastError ? { last_error: options.lastError } : {}),
				...(options.previousSql ? { previous_sql: options.previousSql } : {}),
				...(options.maxRetry ? { max_retry: options.maxRetry } : {}),
			},
			tenantId,
			options.userId,
			options.scopes,
			signal,
			sessionId,
		);

		const databaseName = options.database ?? this.defaultDatabase;
		if (!databaseName) {
			throw new Error(
				"No database attached. Call attachPostgres/attachClickhouse first.",
			);
		}

		const adapter = this.getDatabase(databaseName);
		const paramMetadata = Array.isArray(queryResponse.params)
			? queryResponse.params
			: [];
		const paramValues = this.mapGeneratedParams(paramMetadata);

		const metadata = this.databaseMetadata.get(databaseName);
		if (metadata) {
			queryResponse.sql = this.ensureTenantIsolation(
				queryResponse.sql,
				paramValues,
				metadata,
				tenantId,
			);
		}

		await adapter.validate(queryResponse.sql, paramValues);
		const execution = await adapter.execute(queryResponse.sql, paramValues);
		const rows = execution.rows ?? [];

		let chart: ChartEnvelope = {
			vegaLiteSpec: null,
			notes: rows.length === 0 ? "Query returned no rows." : null,
		};

		if (rows.length > 0) {
			const chartResponse = await this.post<ServerChartResponse>(
				"/chart",
				{
					question,
					sql: queryResponse.sql,
					rationale: queryResponse.rationale,
					fields: execution.fields,
					rows: anonymizeResults(rows),
					max_retries: options.chartMaxRetries ?? 3,
					query_id: queryResponse.queryId,
				},
				tenantId,
				options.userId,
				options.scopes,
				signal,
				sessionId,
			);

			chart = {
				vegaLiteSpec: chartResponse.chart
					? {
							...chartResponse.chart,
							data: { values: rows },
						}
					: null,
				notes: chartResponse.notes,
			};
		}

		return {
			sql: queryResponse.sql,
			params: paramValues,
			paramMetadata,
			rationale: queryResponse.rationale,
			dialect: queryResponse.dialect,
			queryId: queryResponse.queryId,
			rows,
			fields: execution.fields,
			chart,
			context: queryResponse.context,
		};
	}

	async createChart(
		body: ChartCreateInput,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<SdkChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		return await this.post<SdkChart>(
			"/charts",
			body,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	async listCharts(
		options?: ChartListOptions,
		signal?: AbortSignal,
	): Promise<PaginatedResponse<SdkChart>> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		const params = new URLSearchParams();
		if (options?.pagination?.page)
			params.set("page", `${options.pagination.page}`);
		if (options?.pagination?.limit)
			params.set("limit", `${options.pagination.limit}`);
		if (options?.sortBy) params.set("sort_by", options.sortBy);
		if (options?.sortDir) params.set("sort_dir", options.sortDir);
		if (options?.title) params.set("title", options.title);
		if (options?.userFilter) params.set("user_id", options.userFilter);
		if (options?.createdFrom) params.set("created_from", options.createdFrom);
		if (options?.createdTo) params.set("created_to", options.createdTo);
		if (options?.updatedFrom) params.set("updated_from", options.updatedFrom);
		if (options?.updatedTo) params.set("updated_to", options.updatedTo);

		const response = await this.get<PaginatedResponse<SdkChart>>(
			`/charts${params.toString() ? `?${params.toString()}` : ""}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);

		if (options?.includeData) {
			response.data = await Promise.all(
				response.data.map(async (chart) => ({
					...chart,
					vega_lite_spec: {
						...chart.vega_lite_spec,
						data: {
							values: await this.runSafeQueryOnClient(
								chart.sql,
								chart.database ?? undefined,
								(chart.sql_params as ParamRecord | null) ?? undefined,
							),
						},
					},
				})),
			);
		}

		return response;
	}

	async getChart(
		id: string,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<SdkChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		const chart = await this.get<SdkChart>(
			`/charts/${encodeURIComponent(id)}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);

		return {
			...chart,
			vega_lite_spec: {
				...chart.vega_lite_spec,
				data: {
					values: await this.runSafeQueryOnClient(
						chart.sql,
						chart.database ?? undefined,
						(chart.sql_params as ParamRecord | null) ?? undefined,
					),
				},
			},
		};
	}

	async updateChart(
		id: string,
		body: ChartUpdateInput,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<SdkChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		return await this.put<SdkChart>(
			`/charts/${encodeURIComponent(id)}`,
			body,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	async deleteChart(
		id: string,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<void> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		await this.delete(
			`/charts/${encodeURIComponent(id)}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	async createActiveChart(
		body: ActiveChartCreateInput,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<SdkActiveChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		return await this.post<SdkActiveChart>(
			"/active-charts",
			body,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	async listActiveCharts(
		options?: ActiveChartListOptions,
		signal?: AbortSignal,
	): Promise<PaginatedResponse<SdkActiveChart>> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		const params = new URLSearchParams();
		if (options?.pagination?.page)
			params.set("page", `${options.pagination.page}`);
		if (options?.pagination?.limit)
			params.set("limit", `${options.pagination.limit}`);
		if (options?.sortBy) params.set("sort_by", options.sortBy);
		if (options?.sortDir) params.set("sort_dir", options.sortDir);
		if (options?.title) params.set("name", options.title);
		if (options?.userFilter) params.set("user_id", options.userFilter);
		if (options?.createdFrom) params.set("created_from", options.createdFrom);
		if (options?.createdTo) params.set("created_to", options.createdTo);
		if (options?.updatedFrom) params.set("updated_from", options.updatedFrom);
		if (options?.updatedTo) params.set("updated_to", options.updatedTo);

		const response = await this.get<PaginatedResponse<SdkActiveChart>>(
			`/active-charts${params.toString() ? `?${params.toString()}` : ""}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);

		if (options?.withData) {
			response.data = await Promise.all(
				response.data.map(async (active) => ({
					...active,
					chart: active.chart
						? await this.getChart(active.chart_id, options, signal)
						: null,
				})),
			);
		}

		return response;
	}

	async getActiveChart(
		id: string,
		options?: ActiveChartListOptions,
		signal?: AbortSignal,
	): Promise<SdkActiveChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		const active = await this.get<SdkActiveChart>(
			`/active-charts/${encodeURIComponent(id)}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);

		if (options?.withData && active.chart_id) {
			return {
				...active,
				chart: await this.getChart(active.chart_id, options, signal),
			};
		}

		return active;
	}

	async updateActiveChart(
		id: string,
		body: ActiveChartUpdateInput,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<SdkActiveChart> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		return await this.put<SdkActiveChart>(
			`/active-charts/${encodeURIComponent(id)}`,
			body,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	async deleteActiveChart(
		id: string,
		options?: RequestOptions,
		signal?: AbortSignal,
	): Promise<void> {
		const tenantId = this.resolveTenantId(options?.tenantId);
		await this.delete(
			`/active-charts/${encodeURIComponent(id)}`,
			tenantId,
			options?.userId,
			options?.scopes,
			signal,
		);
	}

	private getDatabase(name?: string): DatabaseAdapter {
		const dbName = name ?? this.defaultDatabase;
		if (!dbName) {
			throw new Error("No database attached.");
		}
		const adapter = this.databases.get(dbName);
		if (!adapter) {
			throw new Error(
				`Database '${dbName}' not found. Attached: ${Array.from(
					this.databases.keys(),
				).join(", ")}`,
			);
		}
		return adapter;
	}

	private async ensureSchemasSynced(
		tenantId: string,
		userId?: string,
		scopes?: string[],
		disableAutoSync?: boolean,
	): Promise<void> {
		if (disableAutoSync) return;
		const unsynced = Array.from(this.databases.keys()).filter(
			(name) => !this.syncedDatabases.has(name),
		);
		await Promise.all(
			unsynced.map((name) =>
				this.syncSchema(name, { tenantId, userId, scopes }).catch((error) => {
					console.warn(`Failed to sync schema for ${name}:`, error);
				}),
			),
		);
	}

	private resolveTenantId(tenantId?: string): string {
		const resolved = tenantId ?? this.defaultTenantId;
		if (!resolved) {
			throw new Error(
				"tenantId is required. Provide it per request or via defaultTenantId option.",
			);
		}
		return resolved;
	}

	private async headers(
		tenantId: string,
		userId?: string,
		scopes?: string[],
		includeJson: boolean = true,
		sessionId?: string,
	): Promise<Record<string, string>> {
		const token = await this.generateJWT(tenantId, userId, scopes);
		const headers: Record<string, string> = {
			Authorization: `Bearer ${token}`,
			Accept: "application/json",
		};
		if (includeJson) {
			headers["Content-Type"] = "application/json";
		}
		if (sessionId) {
			headers["x-session-id"] = sessionId;
		}
		if (this.additionalHeaders) {
			Object.assign(headers, this.additionalHeaders);
		}
		return headers;
	}

	private async request<T>(path: string, init: RequestInit): Promise<T> {
		const response = await this.fetchImpl(`${this.baseUrl}${path}`, init);
		const text = await response.text();
		let json: any;
		try {
			json = text ? JSON.parse(text) : undefined;
		} catch {
			json = undefined;
		}

		if (!response.ok) {
			const error = new Error(
				json?.error || response.statusText || "Request failed",
			);
			(error as any).status = response.status;
			if (json?.details) (error as any).details = json.details;
			throw error;
		}

		return json as T;
	}

	private async get<T>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "GET",
			headers: await this.headers(tenantId, userId, scopes, false, sessionId),
			signal,
		});
	}

	private async post<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "POST",
			headers: await this.headers(tenantId, userId, scopes, true, sessionId),
			body: JSON.stringify(body ?? {}),
			signal,
		});
	}

	private async put<T>(
		path: string,
		body: unknown,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "PUT",
			headers: await this.headers(tenantId, userId, scopes, true, sessionId),
			body: JSON.stringify(body ?? {}),
			signal,
		});
	}

	private async delete<T = void>(
		path: string,
		tenantId: string,
		userId?: string,
		scopes?: string[],
		signal?: AbortSignal,
		sessionId?: string,
	): Promise<T> {
		return await this.request<T>(path, {
			method: "DELETE",
			headers: await this.headers(tenantId, userId, scopes, false, sessionId),
			signal,
		});
	}

	private async generateJWT(
		tenantId: string,
		userId?: string,
		scopes?: string[],
	): Promise<string> {
		if (!this.cachedPrivateKey) {
			this.cachedPrivateKey = await importPKCS8(this.privateKey, "RS256");
		}

		const payload: Record<string, unknown> = {
			organizationId: this.organizationId,
			tenantId,
		};

		if (userId) payload.userId = userId;
		if (scopes?.length) payload.scopes = scopes;

		return await new SignJWT(payload)
			.setProtectedHeader({ alg: "RS256" })
			.setIssuedAt()
			.setExpirationTime("1h")
			.sign(this.cachedPrivateKey);
	}

	private buildSchemaRequest(
		databaseName: string,
		adapter: DatabaseAdapter,
		introspection: SchemaIntrospection,
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

		return {
			database: databaseName,
			dialect,
			tables,
		};
	}

	private hashSchemaRequest(payload: SchemaIngestRequest): string {
		const normalized = payload.tables.map((table) => ({
			name: table.table_name,
			columns: table.columns.map((column) => ({
				name: column.name,
				type: column.data_type,
				primary: column.is_primary_key,
			})),
		}));
		return createHash("sha256")
			.update(JSON.stringify(normalized))
			.digest("hex");
	}

	private mapGeneratedParams(
		params: Array<Record<string, unknown>>,
	): ParamRecord {
		const record: ParamRecord = {};

		params.forEach((param, index) => {
			const value = param.value as ParamValue | undefined;
			if (value === undefined) {
				return;
			}
			const nameCandidate =
				(typeof param.name === "string" && param.name.trim()) ||
				(typeof param.placeholder === "string" && param.placeholder.trim()) ||
				(typeof param.position === "number" && String(param.position)) ||
				String(index + 1);
			const key = nameCandidate.replace(/[{}:$]/g, "").trim();
			record[key] = value;
		});

		return record;
	}

	private ensureTenantIsolation(
		sql: string,
		params: ParamRecord,
		metadata: DatabaseMetadata,
		tenantId: string,
	): string {
		if (
			!metadata.tenantFieldName ||
			metadata.enforceTenantIsolation === false
		) {
			return sql;
		}

		const tenantField = metadata.tenantFieldName;
		const paramKey = tenantField;
		params[paramKey] = tenantId;

		const normalizedSql = sql.toLowerCase();
		if (normalizedSql.includes(tenantField.toLowerCase())) {
			return sql;
		}

		const tenantPredicate =
			metadata.dialect === "clickhouse"
				? `${tenantField} = {${tenantField}:${metadata.tenantFieldType ?? "String"}}`
				: `${tenantField} = '${tenantId}'`;

		if (/\bwhere\b/i.test(sql)) {
			return sql.replace(
				/\bwhere\b/i,
				(match) => `${match} ${tenantPredicate} AND `,
			);
		}

		return `${sql} WHERE ${tenantPredicate}`;
	}

	private async runSafeQueryOnClient(
		sql: string,
		database?: string,
		params?: ParamRecord,
	): Promise<Array<Record<string, unknown>>> {
		try {
			const adapter = this.getDatabase(database);
			const result = await adapter.execute(sql, params);
			return result.rows;
		} catch (error) {
			console.warn(
				`Failed to execute SQL locally for database '${database}':`,
				error,
			);
			return [];
		}
	}
}

export function anonymizeResults(
	rows: Array<Record<string, unknown>>,
): Array<Record<string, string>> {
	if (!rows?.length) return [];
	return rows.map((row) => {
		const masked: Record<string, string> = {};
		Object.entries(row).forEach(([key, value]) => {
			if (value === null) masked[key] = "null";
			else if (Array.isArray(value)) masked[key] = "array";
			else masked[key] = typeof value;
		});
		return masked;
	});
}
