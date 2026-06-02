/**
 * Wrap a known-permanent failure so the worker's failure path can route it
 * straight to `next_retry_at = NULL` (no auto-retry, manual restart only).
 * Use this when the cause won't clear on its own — e.g. a schema mismatch
 * between snapshot-server and avni-server's payload shape, or a missing
 * user/org the scheduler keeps trying to process.
 *
 * Default for un-wrapped Errors is transient — see isPermanentFailure().
 */
export class PermanentSnapshotError extends Error {
    constructor(message, {cause} = {}) {
        super(message);
        this.name = 'PermanentSnapshotError';
        if (cause) this.cause = cause;
    }
}

/**
 * Decide whether a caught error should skip auto-retry.
 *
 * Permanent (no auto-retry):
 *   - Explicit PermanentSnapshotError
 *   - HTTP 4xx from avni-server (with a few transient-coded exceptions
 *     below): 401/403 (auth misconfig), 404 (org/user gone), 400 (bad
 *     payload) — none clear on a quick retry
 *
 * Transient (auto-retry per retryPolicy):
 *   - Network errors (fetch failed, ECONNREFUSED, ETIMEDOUT)
 *   - HTTP 5xx (server overload / restart)
 *   - HTTP 408 / 425 / 429 (timeout, too-early, rate-limited — all advise retry)
 *   - SQLite / S3 errors not explicitly wrapped
 *   - Anything else we can't prove permanent
 */
export function isPermanentFailure(err) {
    if (err instanceof PermanentSnapshotError) return true;
    const status = typeof err?.status === 'number' ? err.status : null;
    if (status !== null && status >= 400 && status < 500) {
        if (status === 408 || status === 425 || status === 429) return false;
        return true;
    }
    return false;
}
