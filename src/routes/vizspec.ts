import crypto from 'node:crypto';
import type { ApiClient } from "../core/client";
import type { IQueryPanelApi } from "../core/api-types";
import type { ChartType, EncodingHints, VizSpec } from "../types/vizspec";

export interface VizSpecGenerateInput {
  question: string;
  sql: string;
  rationale?: string;
  fields: string[];
  rows: Array<Record<string, unknown>>;
  max_retries?: number;
  query_id?: string;
  /**
   * Optional encoding hints for visualization modification.
   * When provided, these guide the LLM to generate specific visualization configurations.
   */
  encoding_hints?: EncodingHints;
  /** When set, VizSpec kind "chart" may only use these chart types (API: supported_chart_types). */
  supported_chart_types?: ChartType[];
}

export interface VizSpecGenerateOptions {
  tenantId?: string;
  userId?: string;
  scopes?: string[];
  maxRetries?: number;
}

export interface VizSpecResponse {
  spec: VizSpec;
  notes: string | null;
}

/**
 * Route module for VizSpec generation
 * Calls the /vizspec endpoint to generate visualization specifications
 */
export async function generateVizSpec(
  client: IQueryPanelApi,
  input: VizSpecGenerateInput,
  options?: VizSpecGenerateOptions,
  signal?: AbortSignal,
): Promise<VizSpecResponse> {
  const tenantId = resolveTenantId(client, options?.tenantId);
  const sessionId = crypto.randomUUID();

  const response = await client.post<VizSpecResponse>(
    "/vizspec",
    {
      question: input.question,
      sql: input.sql,
      rationale: input.rationale,
      fields: input.fields,
      rows: input.rows,
      max_retries: options?.maxRetries ?? input.max_retries ?? 3,
      query_id: input.query_id,
      encoding_hints: input.encoding_hints,
      ...(input.supported_chart_types?.length
        ? { supported_chart_types: input.supported_chart_types }
        : {}),
    },
    tenantId,
    options?.userId,
    options?.scopes,
    signal,
    sessionId,
  );

  return response;
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
