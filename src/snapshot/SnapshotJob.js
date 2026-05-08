import path from 'node:path';
import fs from 'node:fs';

import {createSqliteDb} from '../snapshotdb/createSqliteDb.js';
import {Persister} from '../snapshotdb/Persister.js';
import {SyncRunner} from './SyncRunner.js';
import {requestContext} from '../rest/requestContext.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

/**
 * Runs one snapshot end-to-end for a single user.
 *
 * Workers call run(job, requestRow) per claimed job. Output goes to
 * `snapshots/<dbUser>/<username>/<isoTimestamp>.db`. `mode = 'clean'` deletes
 * the user's prior local backup files before running so re-runs don't
 * accumulate stale snapshots.
 */
export class SnapshotJob {
    async run(job, requestRow) {
        return requestContext.run({username: job.username}, async () => {
            const out = path.resolve(
                config.snapshotsDir,
                fsSafe(job.db_user),
                fsSafe(job.username),
                `${timestampForFilename()}.db`
            );

            if (requestRow?.mode === 'clean') {
                this._cleanUserBackupDir(path.dirname(out));
            }

            logger.info(
                {jobId: job.id, user: job.username, dbUser: job.db_user, output: out, mode: requestRow?.mode},
                'snapshot job starting'
            );

            const start = Date.now();
            const db = createSqliteDb(out);
            try {
                const persister = new Persister(db);
                const runner = new SyncRunner({db, persister});
                await runner.run();
            } finally {
                db.close();
            }

            const stats = fs.statSync(out);
            logger.info(
                {jobId: job.id, user: job.username, output: out, bytes: stats.size, elapsedMs: Date.now() - start},
                'snapshot job done'
            );
            return {outputPath: out, sizeBytes: stats.size};
        });
    }

    _cleanUserBackupDir(dir) {
        if (!fs.existsSync(dir)) return;
        for (const f of fs.readdirSync(dir)) {
            const full = path.join(dir, f);
            try { fs.rmSync(full, {force: true}); } catch (e) {
                logger.warn({path: full, err: e.message}, 'failed to remove prior snapshot');
            }
        }
    }
}

function fsSafe(s) {
    return String(s).replace(/[^a-zA-Z0-9._@+-]/g, '_');
}

function timestampForFilename() {
    return new Date().toISOString().slice(0, 19).replace(/:/g, '-') + 'Z';
}
