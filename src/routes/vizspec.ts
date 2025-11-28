import crypto from 'node:crypto';
import type { ApiClient } from "../core/client";
import type { VizSpec } from "../types/vizspec";

export interface VizSpecGenerateInput {
  question: string;
  sql: string;
  rationale?: string;
  fields: string[];
  rows: Array<Record<string, unknown>>;
  max_retries?: number;
  query_id?: string;
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
  client: ApiClient,
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
    },
    tenantId,
    options?.userId,
    options?.scopes,
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
