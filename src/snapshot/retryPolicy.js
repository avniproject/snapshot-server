/**
 * How long to wait before each retry, indexed by attempt count (1-based —
 * attempt 1 == first failure). Front-loaded so transient blips (network,
 * S3 timeouts) clear quickly; stretched out so later retries don't hammer
 * a persistent breakage. After 3 failures the schedule is exhausted and
 * the 4th failure is treated as permanent (next_retry_at = NULL).
 */
const RETRY_DELAY_SECONDS_BY_ATTEMPT = [
        0.5 * 3600,
        1 * 3600,
        2 * 3600,
];

// 1 initial attempt + N retries (== curve length). Derived from the curve so
// they can't decouple — extending the schedule means appending to the array
// above and MAX_FAILURE_ATTEMPTS tracks automatically.
export const MAX_FAILURE_ATTEMPTS = RETRY_DELAY_SECONDS_BY_ATTEMPT.length + 1;

export function retryDelayFor(attemptCount) {
    return RETRY_DELAY_SECONDS_BY_ATTEMPT[attemptCount - 1];
}

/**
 * Decide when (if ever) the scheduler may auto-retry a failed job.
 *
 *   - permanent === true              → null (no auto-retry, manual restart only)
 *   - attemptCount >= maxAttempts     → null (hit the ceiling)
 *   - otherwise                       → nowSec + retryDelayFor(attemptCount)
 */
export function computeNextRetryAt({attemptCount, permanent, maxAttempts, nowSec = Math.floor(Date.now() / 1000)}) {
    if (permanent) return null;
    if (attemptCount >= maxAttempts) return null;
    return nowSec + retryDelayFor(attemptCount);
}
