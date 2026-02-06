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

	// General errors
	INTERNAL_ERROR: "INTERNAL_ERROR",
	AUTHENTICATION_REQUIRED: "AUTHENTICATION_REQUIRED",
	VALIDATION_ERROR: "VALIDATION_ERROR",
} as const;

export type QueryErrorCode =
	(typeof QueryErrorCode)[keyof typeof QueryErrorCode];

/**
 * Error thrown when the query pipeline fails
 */
export class QueryPipelineError extends Error {
	constructor(
		message: string,
		public readonly code: QueryErrorCode,
		public readonly details?: Record<string, unknown>,
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
}
