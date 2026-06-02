import {getStateDb} from '../db/db.js';

/**
 * Local-DB freshness gate for the scheduler. For each user in a given org,
 * decides whether their last successful snapshot is "fresh enough" to skip
 * regenerating this tick. A user is stale when ANY of:
 *
 *   - no successful ('ready') snapshot_user_job exists for them in this org
 *   - the latest ready job's finished_at is older than thresholdSeconds
 *   - the latest ready job's generated_by_sha != current commitSha
 *   - the latest ready job's generated_for_schema != current schema version
 *
 * The "freshness" signal is read from the local state DB rather than S3 by
 * design: we trust our own bookkeeping and avoid N HEAD calls per tick across
 * all opted-in orgs. The trade-off is that if someone manually deletes or
 * modifies the S3 objects, the local DB will still consider those users fresh
 * and skip regenerating until the threshold expires.
 */
export class FreshnessChecker {
    constructor({db = getStateDb(), currentSha, currentSchemaVersion, thresholdSeconds}) {
        this.db = db;
        this.currentSha = currentSha;
        this.currentSchemaVersion = currentSchemaVersion;
        this.thresholdSeconds = thresholdSeconds;
    }

    findStaleUsers({dbUser, usernames}) {
        if (!Array.isArray(usernames) || usernames.length === 0) return [];
        const latestByUser = this._latestReadyJobsByUser(dbUser);
        const activeUsers = this._usersWithActiveJobs(dbUser);
        const nowSec = Math.floor(Date.now() / 1000);
        const stale = [];
        for (const username of usernames) {
            // Already queued or in-flight from a prior tick — let it finish.
            // Without this guard the scheduler would pile up duplicate jobs
            // every tick when an org has more stale users than workers can
            // drain in one interval.
            if (activeUsers.has(username)) continue;
            if (this._isStale(latestByUser.get(username), nowSec)) stale.push(username);
        }
        return stale;
    }

    _isStale(latest, nowSec) {
        if (!latest) return true;
        if (latest.generated_by_sha !== this.currentSha) return true;
        if (latest.generated_for_schema !== this.currentSchemaVersion) return true;
        if (latest.finished_at == null) return true;
        return (nowSec - latest.finished_at) > this.thresholdSeconds;
    }

    // One latest-per-user row across all of this dbUser's ready jobs. The
    // ROW_NUMBER window partitions by username and orders by finished_at desc;
    // rn=1 picks the most recent successful snapshot per user. Bounded by org
    // size (typically <500 users), runs locally — cheap enough to do each tick.
    _latestReadyJobsByUser(dbUser) {
        const rows = this.db.prepare(`
            SELECT username, finished_at, generated_by_sha, generated_for_schema FROM (
                SELECT j.username,
                       j.finished_at,
                       j.generated_by_sha,
                       j.generated_for_schema,
                       ROW_NUMBER() OVER (PARTITION BY j.username ORDER BY j.finished_at DESC) AS rn
                FROM snapshot_user_job j
                JOIN snapshot_request r ON r.id = j.request_id
                WHERE r.db_user = ? AND j.state = 'ready'
            ) WHERE rn = 1
        `).all(dbUser);
        return new Map(rows.map(r => [r.username, r]));
    }

    _usersWithActiveJobs(dbUser) {
        const rows = this.db.prepare(`
            SELECT DISTINCT j.username
            FROM snapshot_user_job j
            JOIN snapshot_request r ON r.id = j.request_id
            WHERE r.db_user = ? AND j.state IN ('queued', 'in_progress')
        `).all(dbUser);
        return new Set(rows.map(r => r.username));
    }
}
