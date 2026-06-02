import {getStateDb} from './db.js';

const DEFAULT_WORKER_ID = `${process.pid}-${Math.random().toString(36).slice(2, 8)}`;

/** Composite request key: `<dbUser>-<orgSeq>` (e.g. `apfodisha-1`). */
function _buildRequestKey(dbUser, orgSeq) {
    return `${dbUser}-${orgSeq}`;
}

function _parseRequestKey(key) {
    if (typeof key !== 'string') return null;
    const lastDash = key.lastIndexOf('-');
    if (lastDash === -1) return null;
    const orgSeq = parseInt(key.slice(lastDash + 1), 10);
    if (Number.isNaN(orgSeq)) return null;
    const dbUser = key.slice(0, lastDash);
    if (dbUser.length === 0) return null;
    return {dbUser, orgSeq};
}

export class SnapshotRequestRepository {
    constructor(db = getStateDb()) {
        this.db = db;
    }

    createSnapshotRequestAndUserJobs({dbUser, mediaDirectory, mode = 'normal', requestedBy = null, usernames}) {
        const tx = this.db.transaction(() => {
            const maxRow = this.db
                .prepare('SELECT COALESCE(MAX(org_seq), 0) AS m FROM snapshot_request WHERE db_user = ?')
                .get(dbUser);
            const orgSeq = maxRow.m + 1;

            const info = this.db
                .prepare(`
                    INSERT INTO snapshot_request (db_user, media_directory, org_seq, mode, requested_by)
                    VALUES (?, ?, ?, ?, ?)
                `)
                .run(dbUser, mediaDirectory, orgSeq, mode, requestedBy);
            const id = Number(info.lastInsertRowid);

            const insertJob = this.db.prepare(`
                INSERT INTO snapshot_user_job (request_id, username)
                VALUES (?, ?)
            `);
            for (const username of usernames) insertJob.run(id, username);

            return {id, dbUser, orgSeq, key: _buildRequestKey(dbUser, orgSeq)};
        });
        return tx();
    }

    getSnapshotRequestById(id) {
        return this.db.prepare('SELECT * FROM snapshot_request WHERE id = ?').get(id);
    }

    _getSnapshotRequestByKey(key) {
        const parsed = _parseRequestKey(key);
        if (!parsed) return null;
        return this.db
            .prepare('SELECT * FROM snapshot_request WHERE db_user = ? AND org_seq = ?')
            .get(parsed.dbUser, parsed.orgSeq);
    }

    getSnapshotRequestSummaryByKey(key) {
        const request = this._getSnapshotRequestByKey(key);
        if (!request) return null;
        return this._summaryFor(request);
    }

    listSnapshotUserJobsByKey(key) {
        const request = this._getSnapshotRequestByKey(key);
        if (!request) return null;
        return this.db
            .prepare('SELECT * FROM snapshot_user_job WHERE request_id = ? ORDER BY id')
            .all(request.id);
    }

    // Latest snapshot_user_job per username across all of this org's requests,
    // regardless of state. Used by the scheduler's planner to route each user
    // to skip / restart-in-place / enqueue-new on every tick. ROW_NUMBER over
    // id desc (id is monotonic, so id desc == most-recently-inserted).
    getLatestJobsByUser(dbUser) {
        const rows = this.db.prepare(`
            SELECT id, username, state, attempt_count, finished_at, locked_at,
                   next_retry_at, generated_by_sha, generated_for_schema
            FROM (
                SELECT j.*,
                       ROW_NUMBER() OVER (PARTITION BY j.username ORDER BY j.id DESC) AS rn
                FROM snapshot_user_job j
                JOIN snapshot_request r ON r.id = j.request_id
                WHERE r.db_user = ?
            ) WHERE rn = 1
        `).all(dbUser);
        return new Map(rows.map(r => [r.username, r]));
    }

    // Latest *successful* snapshot per username — used by the freshness gate
    // to decide whether to enqueue a new run. The latest job overall may not
    // be ready (could be failed, cancelled, in-flight), so this is a separate
    // query from getLatestJobsByUser. ORDER tiebreaks on monotonic id DESC
    // because finished_at is second-resolution; two ready snapshots finishing
    // in the same epoch second would otherwise be picked arbitrarily.
    getLatestReadyJobsByUser(dbUser) {
        const rows = this.db.prepare(`
            SELECT username, finished_at, generated_by_sha, generated_for_schema
            FROM (
                SELECT j.username,
                       j.finished_at,
                       j.generated_by_sha,
                       j.generated_for_schema,
                       ROW_NUMBER() OVER (PARTITION BY j.username ORDER BY j.finished_at DESC, j.id DESC) AS rn
                FROM snapshot_user_job j
                JOIN snapshot_request r ON r.id = j.request_id
                WHERE r.db_user = ? AND j.state = 'ready'
            ) WHERE rn = 1
        `).all(dbUser);
        return new Map(rows.map(r => [r.username, r]));
    }

    _summaryFor(request) {
        const counts = this.db
            .prepare('SELECT state, COUNT(*) AS n FROM snapshot_user_job WHERE request_id = ? GROUP BY state')
            .all(request.id);
        return {
            request: {...request, key: _buildRequestKey(request.db_user, request.org_seq)},
            counts: Object.fromEntries(counts.map(r => [r.state, r.n])),
        };
    }

    /**
     * Claim the next queued job for this worker — flips state to 'in_progress'
     * and returns the row. Returns null if the queue is empty. Concurrent
     * callers can't double-claim: the conditional `AND state = 'queued'` in
     * the UPDATE means the second caller's UPDATE affects 0 rows; that branch
     * returns null and the caller loops back for the next id.
     */
    claimNextSnapshotUserJob(workerId = DEFAULT_WORKER_ID) {
        const tx = this.db.transaction(() => {
            const row = this.db
                .prepare(`SELECT * FROM snapshot_user_job WHERE state = 'queued' ORDER BY id LIMIT 1`)
                .get();
            if (!row) return null;
            const result = this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state         = 'in_progress',
                        worker_id     = ?,
                        locked_at     = unixepoch(),
                        started_at    = COALESCE(started_at, unixepoch()),
                        attempt_count = attempt_count + 1
                    WHERE id = ? AND state = 'queued'
                `)
                .run(workerId, row.id);
            if (result.changes !== 1) return null;
            this._recomputeRequestState(row.request_id);
            // attempt_count was incremented inside the UPDATE; surface the
            // post-increment value so callers (the Worker's failure path) see
            // the current count, not the pre-claim one from the SELECT.
            return {...row, state: 'in_progress', worker_id: workerId, attempt_count: row.attempt_count + 1};
        });
        return tx();
    }

    // expectedWorkerId: the worker_id this caller claimed. The UPDATE fences
    // on it so that if the planner re-queued the row as "crashed" (worker_id
    // cleared, then re-claimed by another worker), this stale terminal write
    // becomes a no-op instead of clobbering the new owner's progress. The
    // caller can detect the no-op via the returned changes count.
    markSnapshotUserJobReady(jobId, {s3Key, sha256, sizeBytes, generatedBySha, generatedForSchema, expectedWorkerId}) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return {changes: 0};
            const result = this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state                = 'ready',
                        finished_at          = unixepoch(),
                        s3_key               = ?,
                        sha256               = ?,
                        size_bytes           = ?,
                        generated_by_sha     = ?,
                        generated_for_schema = ?,
                        last_error           = NULL,
                        resume_cursor        = NULL,
                        locked_at            = NULL
                    WHERE id = ? AND worker_id = ?
                `)
                .run(s3Key, sha256, sizeBytes, generatedBySha, generatedForSchema, jobId, expectedWorkerId);
            if (result.changes > 0) this._recomputeRequestState(row.request_id);
            return {changes: result.changes};
        });
        return tx();
    }

    // nextRetryAt: epoch seconds when the scheduler may auto-restart this job
    //   in place (reusing resume_cursor). Pass null to mark a permanent failure
    //   — the scheduler will leave the row alone until ops manually restarts it.
    // expectedWorkerId: worker_id fence (see markSnapshotUserJobReady comment).
    //   A stale failure write from a worker whose claim has since been
    //   recycled by the crash-timeout path becomes a no-op.
    // last_error stores the stack (when present) so a row inspection in the
    // state DB tells ops where the failure was thrown without cross-referencing
    // the snapshot-server log stream. Falls back to message → toString.
    markSnapshotUserJobFailed(jobId, error, {nextRetryAt = null, expectedWorkerId} = {}) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return {changes: 0};
            const lastError = error?.stack ?? String(error?.message ?? error);
            const result = this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state         = 'failed',
                        finished_at   = unixepoch(),
                        last_error    = ?,
                        next_retry_at = ?,
                        locked_at     = NULL
                    WHERE id = ? AND worker_id = ?
                `)
                .run(lastError, nextRetryAt, jobId, expectedWorkerId);
            if (result.changes > 0) this._recomputeRequestState(row.request_id);
            return {changes: result.changes};
        });
        return tx();
    }

    saveSnapshotUserJobResumeCursor(jobId, cursor) {
        // Resume cursor doesn't change job state, so no request recompute needed.
        this.db
            .prepare(`UPDATE snapshot_user_job SET resume_cursor = ? WHERE id = ?`)
            .run(JSON.stringify(cursor), jobId);
    }

    // Re-queue a single user job for retry. Used by the ops restart endpoint
    // AND by the scheduler's planner for auto-retry of failed/crashed jobs.
    //
    //   - expectedFromState fences the update so a TOCTOU race (e.g. the
    //     original worker writing 'ready' between the planner's decision and
    //     this UPDATE) doesn't clobber the row. UPDATE affects 0 rows when
    //     state has moved on; the call becomes a safe no-op.
    //   - attempt_count resets to 0 so the next claim's increment makes it 1
    //     and the full retry schedule applies. Without this, restarting a
    //     maxed-out job (count == MAX_FAILURE_ATTEMPTS) would give zero auto-
    //     retries: first failure immediately re-goes-permanent.
    //   - resume_cursor is intentionally preserved so the next worker picks
    //     up from the last committed page boundary.
    //   - next_retry_at clears so the retry schedule starts fresh.
    //
    // Returns {fromState, username, dbUser} on success; null if the job_id
    // doesn't exist OR the state fence blocked the update.
    restartSnapshotUserJob(jobId, {expectedFromState}) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare(`
                SELECT j.request_id, j.state, j.username, r.db_user
                FROM snapshot_user_job j JOIN snapshot_request r ON r.id = j.request_id
                WHERE j.id = ?
            `).get(jobId);
            if (!row) return null;
            const result = this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state         = 'queued',
                        worker_id     = NULL,
                        locked_at     = NULL,
                        last_error    = NULL,
                        next_retry_at = NULL,
                        attempt_count = 0
                    WHERE id = ? AND state = ?
                `)
                .run(jobId, expectedFromState);
            if (result.changes === 0) return null;
            this._recomputeRequestState(row.request_id);
            return {fromState: row.state, username: row.username, dbUser: row.db_user};
        });
        return tx();
    }

    /** Cancel a queued job. In-flight or finished jobs are left alone. */
    cancelSnapshotUserJob(jobId) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return 0;
            const changes = this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state       = 'cancelled',
                        finished_at = unixepoch()
                    WHERE id = ? AND state = 'queued'
                `)
                .run(jobId).changes;
            if (changes > 0) this._recomputeRequestState(row.request_id);
            return changes;
        });
        return tx();
    }

    /**
     * Recompute the parent request's aggregate state from its user_jobs.
     *
     *   requested   → all jobs queued (initial state, no claims yet)
     *   in_progress → some jobs in-progress / awaiting retry / mix
     *   ready       → all jobs ready
     *   failed      → all (non-cancelled) terminal jobs failed permanently;
     *                 no ready
     *   cancelled   → all jobs cancelled
     *   partial     → mixed terminal states with at least one ready
     *
     * A failed row with next_retry_at set is "awaiting retry," logically still
     * in flight even though its current state is 'failed' — counted as
     * in-progress-like below. Only failed rows with next_retry_at = NULL
     * (permanent / hit cap) count as terminal.
     *
     * `started_at` is set on first transition out of 'requested' (COALESCE so
     * subsequent claims don't overwrite). `finished_at` is set on terminal
     * states and cleared (NULL) when work resumes.
     */
    _recomputeRequestState(requestId) {
        const c = this.db.prepare(`
            SELECT
                SUM(CASE WHEN state = 'queued'                                    THEN 1 ELSE 0 END) AS queued,
                SUM(CASE WHEN state = 'in_progress'                               THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN state = 'ready'                                     THEN 1 ELSE 0 END) AS ready,
                SUM(CASE WHEN state = 'failed' AND next_retry_at IS NULL          THEN 1 ELSE 0 END) AS failed_permanent,
                SUM(CASE WHEN state = 'failed' AND next_retry_at IS NOT NULL      THEN 1 ELSE 0 END) AS failed_retrying,
                SUM(CASE WHEN state = 'cancelled'                                 THEN 1 ELSE 0 END) AS cancelled,
                COUNT(*) AS total
            FROM snapshot_user_job WHERE request_id = ?
        `).get(requestId);

        if (!c || c.total === 0) return;

        const terminal = c.ready + c.failed_permanent + c.cancelled;

        let newState;
        if (c.queued === c.total) {
            newState = 'requested';
        } else if (terminal === c.total) {
            if (c.ready === c.total) newState = 'ready';
            else if (c.cancelled === c.total) newState = 'cancelled';
            else if (c.ready > 0) newState = 'partial';
            else newState = 'failed';
        } else {
            // queued + in_progress + failed_retrying present → still live
            newState = 'in_progress';
        }

        const isTerminal = terminal === c.total;
        const isRequested = newState === 'requested';
        this.db
            .prepare(`
                UPDATE snapshot_request
                SET state        = ?,
                    started_at   = ${isRequested ? 'started_at' : 'COALESCE(started_at, unixepoch())'},
                    finished_at  = ${isTerminal ? 'unixepoch()' : 'NULL'}
                WHERE id = ?
            `)
            .run(newState, requestId);
    }
}
