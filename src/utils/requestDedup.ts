const inFlightRequests = new Map<string, Promise<unknown>>();

const resultCache = new Map<
  string,
  { value: unknown; expiry: number }
>();

export type RunDedupedRequestOptions = {
  /** Cache successful response for this many ms. Same key within TTL returns cached result without refetch. */
  cacheMs?: number;
};

/**
 * Deduplicate concurrent requests by key.
 * If the same request is triggered while one is still in flight, callers share the same promise.
 * Optionally cache the result so repeat requests within cacheMs return cached data without refetch.
 */
export function runDedupedRequest<T>(
  key: string,
  request: () => Promise<T>,
  options?: RunDedupedRequestOptions
): Promise<T> {
  const cacheMs = options?.cacheMs;
  if (cacheMs != null && cacheMs > 0) {
    const cached = resultCache.get(key);
    if (cached && cached.expiry > Date.now()) {
      return Promise.resolve(cached.value as T);
    }
  }

  const existing = inFlightRequests.get(key) as Promise<T> | undefined;
  if (existing) return existing;

  const promise = request().then(
    (value) => {
      if (cacheMs != null && cacheMs > 0) {
        resultCache.set(key, { value, expiry: Date.now() + cacheMs });
      }
      return value;
    },
    (err) => {
      throw err;
    }
  ).finally(() => {
    if (inFlightRequests.get(key) === promise) {
      inFlightRequests.delete(key);
    }
  }) as Promise<T>;

  inFlightRequests.set(key, promise as Promise<unknown>);
  return promise;
}
