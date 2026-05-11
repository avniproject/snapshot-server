import path from 'node:path';
import fs from 'node:fs';

import {createSqliteDb} from '../snapshotdb/createSqliteDb.js';
import {Persister} from '../snapshotdb/Persister.js';
import {SyncRunner} from './SyncRunner.js';
import {requestContext} from '../rest/requestContext.js';
import {S3Uploader} from '../s3/S3Uploader.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

/**
 * Runs one snapshot end-to-end for a single user.
 *
 * Workers call run(job, requestRow) per claimed job. The snapshot is written
 * to a local SQLite file, uploaded to S3 at
 *   <bucket>/<media_directory>/snapshots/<username>/<isoTimestamp>.db
 * (when S3 is configured), then the local file is deleted. The S3 key + sha256
 * + size go back to the worker so it can record them on the user_job row.
 *
 * `mode = 'clean'` deletes the user's prior local snapshots before running
 * AND, when S3 is enabled, deletes the user's S3 prefix so old uploads are
 * cleared too.
 */
export class SnapshotJob {
    constructor({s3Uploader = new S3Uploader()} = {}) {
        this.s3Uploader = s3Uploader;
    }

    async run(job, requestRow) {
        return requestContext.run({username: job.username}, async () => {
            const localPath = path.resolve(
                config.snapshotsDir,
                fsSafe(requestRow.db_user),
                fsSafe(job.username),
                `${timestampForFilename()}.db`
            );
            const s3Key = `${requestRow.media_directory}/snapshots/${job.username}/${path.basename(localPath)}`;

            if (requestRow?.mode === 'clean') {
                this._cleanUserSnapshotDir(path.dirname(localPath));
                if (this.s3Uploader.isEnabled()) {
                    const userS3Prefix = `${requestRow.media_directory}/snapshots/${job.username}/`;
                    await this.s3Uploader.deletePrefix(userS3Prefix);
                }
            }

            logger.info(
                {jobId: job.id, user: job.username, dbUser: requestRow.db_user, local: localPath, s3Key, mode: requestRow?.mode},
                'snapshot job starting'
            );

            const start = Date.now();
            const db = createSqliteDb(localPath);
            try {
                const persister = new Persister(db);
                const runner = new SyncRunner({db, persister});
                await runner.run();
            } finally {
                db.close();
            }

            // Upload (when configured) → returns the canonical s3Key + sha256 + size.
            // When S3 is disabled we keep the local file and return its path.
            let result;
            if (this.s3Uploader.isEnabled()) {
                const uploaded = await this.s3Uploader.uploadFile(localPath, s3Key);
                this._removeLocal(localPath);
                result = {
                    s3Key: uploaded.s3Key,
                    sha256: uploaded.sha256,
                    sizeBytes: uploaded.sizeBytes,
                };
            } else {
                const stats = fs.statSync(localPath);
                result = {
                    s3Key: localPath,
                    sha256: null,
                    sizeBytes: stats.size,
                };
            }

            logger.info(
                {jobId: job.id, user: job.username, ...result, elapsedMs: Date.now() - start, uploaded: this.s3Uploader.isEnabled()},
                'snapshot job done'
            );
            return result;
        });
    }

    _cleanUserSnapshotDir(dir) {
        if (!fs.existsSync(dir)) return;
        for (const f of fs.readdirSync(dir)) {
            const full = path.join(dir, f);
            try { fs.rmSync(full, {force: true}); } catch (e) {
                logger.warn({path: full, err: e.message}, 'failed to remove prior snapshot');
            }
        }
    }

    _removeLocal(localPath) {
        for (const suffix of ['', '-wal', '-shm']) {
            const p = localPath + suffix;
            try { if (fs.existsSync(p)) fs.rmSync(p); } catch (e) {
                logger.warn({path: p, err: e.message}, 'failed to remove local snapshot after upload');
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
