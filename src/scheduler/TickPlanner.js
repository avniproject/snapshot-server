/**
 * Per-tick decision-maker. For each user in an opted-in org, returns one of:
 *
 *   'enqueue-new'      — no record, latest is cancelled, or latest ready is
 *                        stale (older than freshness threshold, or SHA/schema
 *                        drift). Caller creates a fresh snapshot_request row.
 *   'restart-in-place' — there's an existing job to revive: a failed one
 *                        whose retry delay has elapsed, or an in_progress one
 *                        whose locked_at is past the crash timeout. Caller
 *                        bumps that same row's state back to queued so its
 *                        resume_cursor (if any) is re-used.
 *   'skip'             — user is fresh, already queued, in flight, awaiting
 *                        retry, or hit MAX_FAILURE_ATTEMPTS. The reason is
 *                        included in the action for logging.
 *
 * Strict per-org ordering note: restart-in-place reuses the existing job_id,
 * which is older than any enqueue-new ids the same tick creates. Across
 * multiple orgs the workers therefore drain all restart-in-place jobs first
 * (in their original creation order) before any newly-enqueued user. We
 * accept this as "finish in-flight work before starting fresh work," which
 * mildly relaxes "drain org A entirely before org B" but keeps the resume
 * path intact.
 */
export class TickPlanner {
    constructor({snapshotRequestRepository, currentSha, currentSchemaVersion, freshnessThresholdSeconds, crashTimeoutSeconds}) {
        this.repo = snapshotRequestRepository;
        this.currentSha = currentSha;
        this.currentSchemaVersion = currentSchemaVersion;
        this.freshnessThresholdSeconds = freshnessThresholdSeconds;
        this.crashTimeoutSeconds = crashTimeoutSeconds;
    }

    planActions({dbUser, usernames}) {
        const latest = this.repo.getLatestJobsByUser(dbUser);
        const latestReady = this.repo.getLatestReadyJobsByUser(dbUser);
        const nowSec = Math.floor(Date.now() / 1000);
        const actions = new Map();

        for (const username of usernames) {
            actions.set(username, this._planForUser({username, latest, latestReady, nowSec}));
        }
        return actions;
    }

    _planForUser({username, latest, latestReady, nowSec}) {
        const j = latest.get(username);

        if (!j) return {action: 'enqueue-new', reason: 'no-record'};

        switch (j.state) {
            case 'queued':
                return {action: 'skip', reason: 'already-queued'};

            case 'in_progress': {
                const lockedFor = j.locked_at == null ? 0 : (nowSec - j.locked_at);
                if (lockedFor > this.crashTimeoutSeconds) {
                    return {action: 'restart-in-place', jobId: j.id, reason: 'crashed', lockedForSec: lockedFor};
                }
                return {action: 'skip', reason: 'in-flight'};
            }

            case 'failed':
                if (j.next_retry_at == null) {
                    return {action: 'skip', reason: 'permanent-or-max-attempts'};
                }
                if (nowSec < j.next_retry_at) {
                    return {action: 'skip', reason: 'awaiting-retry', retryInSec: j.next_retry_at - nowSec};
                }
                return {action: 'restart-in-place', jobId: j.id, reason: 'retry-due', attempt: j.attempt_count};

            case 'ready':
            case 'cancelled': {
                const ready = latestReady.get(username);
                if (this._isFresh(ready, nowSec)) {
                    return {action: 'skip', reason: 'fresh'};
                }
                return {action: 'enqueue-new', reason: this._stalenessReason(ready, nowSec)};
            }

            default:
                // Future-proofing: any state we don't recognise falls through
                // to enqueue-new so a new state addition doesn't silently
                // strand users.
                return {action: 'enqueue-new', reason: `unknown-state:${j.state}`};
        }
    }

    _isFresh(ready, nowSec) {
        if (!ready) return false;
        if (ready.generated_by_sha !== this.currentSha) return false;
        if (ready.generated_for_schema !== this.currentSchemaVersion) return false;
        if (ready.finished_at == null) return false;
        return (nowSec - ready.finished_at) <= this.freshnessThresholdSeconds;
    }

    _stalenessReason(ready, nowSec) {
        if (!ready) return 'no-ready-snapshot';
        if (ready.generated_by_sha !== this.currentSha) return 'sha-drift';
        if (ready.generated_for_schema !== this.currentSchemaVersion) return 'schema-drift';
        if (ready.finished_at == null) return 'no-finished-at';
        return `aged-out:${nowSec - ready.finished_at}s`;
    }
}
