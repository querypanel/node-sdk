/**
 * Error codes for the query pipeline
 * These match the server-side error codes returned in API responses
 */
export const QueryErrorCode = {
	// Moderation errors
	MODERATION_FAILED: "MODERATION_FAILED",

	// Guardrail errors
	RELEVANCE_CHECK_FAILED: "RELEVANCE_CHECK_FAILED",
	SECURITY_CHECK_FAILED: "SECURITY_CHECK_FAILED",

	// SQL generation errors
	SQL_GENERATION_FAILED: "SQL_GENERATION_FAILED",

	// SQL validation errors
	SQL_VALIDATION_FAILED: "SQL_VALIDATION_FAILED",

	// Context retrieval errors
	CONTEXT_RETRIEVAL_FAILED: "CONTEXT_RETRIEVAL_FAILED",

	// Clarification errors (v2)
	CLARIFICATION_NEEDED: "CLARIFICATION_NEEDED",

	// Bring-your-own-key / provider errors
	AI_PROVIDER_AUTH_FAILED: "AI_PROVIDER_AUTH_FAILED",
	AI_PROVIDER_RATE_LIMITED: "AI_PROVIDER_RATE_LIMITED",
	AI_PROVIDER_UNAVAILABLE: "AI_PROVIDER_UNAVAILABLE",

	// General errors
	INTERNAL_ERROR: "INTERNAL_ERROR",
	AUTHENTICATION_REQUIRED: "AUTHENTICATION_REQUIRED",
	VALIDATION_ERROR: "VALIDATION_ERROR",
} as const;

export type QueryErrorCode =
	(typeof QueryErrorCode)[keyof typeof QueryErrorCode];

export function isQueryErrorCode(code: unknown): code is QueryErrorCode {
	return (
		typeof code === "string" &&
		(Object.values(QueryErrorCode) as string[]).includes(code)
	);
}

export function isAiProviderErrorCode(code: unknown): boolean {
	return (
		code === QueryErrorCode.AI_PROVIDER_AUTH_FAILED ||
		code === QueryErrorCode.AI_PROVIDER_RATE_LIMITED ||
		code === QueryErrorCode.AI_PROVIDER_UNAVAILABLE
	);
}

export function isAiProviderPipelineError(error: unknown): boolean {
	if (error instanceof QueryPipelineError) {
		return isAiProviderErrorCode(error.code);
	}
	if (error && typeof error === "object" && "code" in error) {
		return isAiProviderErrorCode((error as { code: unknown }).code);
	}
	return false;
}

export function throwIfGenerationErrorResponse(response: unknown): void {
	if (!response || typeof response !== "object") {
		return;
	}
	const record = response as Record<string, unknown>;
	if (typeof record.error !== "string") {
		return;
	}
	if (isQueryErrorCode(record.code)) {
		throw new QueryPipelineError(record.error, record.code);
	}
	throw new Error(record.error);
}

/**
 * Error thrown when the query pipeline fails
 */
export class QueryPipelineError extends Error {
	constructor(
		message: string,
		public readonly code: QueryErrorCode,
		public readonly details?: Record<string, unknown>,
		public readonly status?: number,
	) {
		super(message);
		this.name = "QueryPipelineError";
	}

	/**
	 * Check if this is a moderation error
	 */
	isModeration(): boolean {
		return this.code === QueryErrorCode.MODERATION_FAILED;
	}

	/**
	 * Check if this is a relevance error (question not related to database)
	 */
	isRelevanceError(): boolean {
		return this.code === QueryErrorCode.RELEVANCE_CHECK_FAILED;
	}

	/**
	 * Check if this is a security error (SQL injection, prompt injection, etc.)
	 */
	isSecurityError(): boolean {
		return this.code === QueryErrorCode.SECURITY_CHECK_FAILED;
	}

	/**
	 * Check if this is any guardrail error (relevance or security)
	 */
	isGuardrailError(): boolean {
		return this.isRelevanceError() || this.isSecurityError();
	}

	/**
	 * Check if this is a clarification needed error (v2 pipeline)
	 */
	isClarificationNeeded(): boolean {
		return this.code === QueryErrorCode.CLARIFICATION_NEEDED;
	}

	/**
	 * Check if this is a BYOK / provider credential error
	 */
	isAiProviderAuthError(): boolean {
		return this.code === QueryErrorCode.AI_PROVIDER_AUTH_FAILED;
	}

	/**
	 * Check if this is a BYOK / provider rate-limit error
	 */
	isAiProviderRateLimited(): boolean {
		return this.code === QueryErrorCode.AI_PROVIDER_RATE_LIMITED;
	}

	/**
	 * Check if this is any BYOK / provider error (auth, rate limit, or unavailable)
	 */
	isAiProviderError(): boolean {
		return isAiProviderErrorCode(this.code);
	}
}
