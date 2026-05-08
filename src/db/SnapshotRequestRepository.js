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

    createSnapshotRequestAndUserJobs({dbUser, mode = 'normal', requestedBy = null, usernames}) {
        const tx = this.db.transaction(() => {
            const maxRow = this.db
                .prepare('SELECT COALESCE(MAX(org_seq), 0) AS m FROM snapshot_request WHERE db_user = ?')
                .get(dbUser);
            const orgSeq = maxRow.m + 1;

            const info = this.db
                .prepare(`
                    INSERT INTO snapshot_request (db_user, org_seq, mode, requested_by)
                    VALUES (?, ?, ?, ?)
                `)
                .run(dbUser, orgSeq, mode, requestedBy);
            const id = Number(info.lastInsertRowid);

            const insertJob = this.db.prepare(`
                INSERT INTO snapshot_user_job (request_id, username, db_user)
                VALUES (?, ?, ?)
            `);
            for (const username of usernames) insertJob.run(id, username, dbUser);

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
            return {...row, state: 'in_progress', worker_id: workerId};
        });
        return tx();
    }

    markSnapshotUserJobReady(jobId, {s3Key, sha256, sizeBytes}) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return;
            this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state         = 'ready',
                        finished_at   = unixepoch(),
                        s3_key        = ?,
                        sha256        = ?,
                        size_bytes    = ?,
                        last_error    = NULL,
                        resume_cursor = NULL,
                        locked_at     = NULL
                    WHERE id = ?
                `)
                .run(s3Key, sha256, sizeBytes, jobId);
            this._recomputeRequestState(row.request_id);
        });
        tx();
    }

    markSnapshotUserJobFailed(jobId, error) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return;
            this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state       = 'failed',
                        finished_at = unixepoch(),
                        last_error  = ?,
                        locked_at   = NULL
                    WHERE id = ?
                `)
                .run(String(error?.message ?? error), jobId);
            this._recomputeRequestState(row.request_id);
        });
        tx();
    }

    saveSnapshotUserJobResumeCursor(jobId, cursor) {
        // Resume cursor doesn't change job state, so no request recompute needed.
        this.db
            .prepare(`UPDATE snapshot_user_job SET resume_cursor = ? WHERE id = ?`)
            .run(JSON.stringify(cursor), jobId);
    }

    /** Re-queue a single user job for retry. Used by the restart endpoint. */
    restartSnapshotUserJob(jobId) {
        const tx = this.db.transaction(() => {
            const row = this.db.prepare('SELECT request_id FROM snapshot_user_job WHERE id = ?').get(jobId);
            if (!row) return;
            this.db
                .prepare(`
                    UPDATE snapshot_user_job
                    SET state      = 'queued',
                        worker_id  = NULL,
                        locked_at  = NULL,
                        last_error = NULL
                    WHERE id = ?
                `)
                .run(jobId);
            this._recomputeRequestState(row.request_id);
        });
        tx();
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
     *   requested  → all jobs queued (initial state, no claims yet)
     *   in_progress → some jobs in-progress, or mix of queued + terminal
     *   ready       → all jobs ready
     *   failed      → all (non-cancelled) terminal jobs failed; no ready
     *   cancelled   → all jobs cancelled
     *   partial     → mixed terminal states with at least one ready
     *
     * `started_at` is set on first transition out of 'requested' (COALESCE so
     * subsequent claims don't overwrite). `finished_at` is set on terminal
     * states and cleared (NULL) when a restart bumps things back to
     * in_progress.
     */
    _recomputeRequestState(requestId) {
        const counts = this.db
            .prepare(`SELECT state, COUNT(*) AS n FROM snapshot_user_job WHERE request_id = ? GROUP BY state`)
            .all(requestId);
        const c = Object.fromEntries(counts.map(r => [r.state, r.n]));
        const queued = c.queued ?? 0;
        const inProgress = c.in_progress ?? 0;
        const ready = c.ready ?? 0;
        const failed = c.failed ?? 0;
        const cancelled = c.cancelled ?? 0;
        const total = queued + inProgress + ready + failed + cancelled;
        const terminal = ready + failed + cancelled;

        let newState;
        if (total === 0) return; // shouldn't happen — request always has ≥1 job
        if (queued === total) {
            newState = 'requested';
        } else if (terminal === total) {
            if (ready === total) newState = 'ready';
            else if (cancelled === total) newState = 'cancelled';
            else if (ready > 0) newState = 'partial';
            else newState = 'failed';
        } else {
            newState = 'in_progress';
        }

        const isTerminal = terminal === total;
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
