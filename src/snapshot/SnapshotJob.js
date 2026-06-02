import path from 'node:path';
import fs from 'node:fs';

import {createSqliteDb} from '../snapshotdb/createSqliteDb.js';
import {Persister} from '../snapshotdb/Persister.js';
import {SyncRunner} from './SyncRunner.js';
import {requestContext} from '../rest/requestContext.js';
import {S3Uploader} from '../s3/S3Uploader.js';
import {journal as drizzleJournal} from '../snapshotdb/drizzleMigrations.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

// Highest applied drizzle migration index — the client schema version.
// Computed once at module load; the migrations are bundled at build time.
const CLIENT_SCHEMA_VERSION = String(
    Math.max(...(drizzleJournal.entries ?? []).map(e => e.idx))
);

/**
 * Runs one snapshot end-to-end for a single user.
 *
 * Workers call run(job, requestRow) per claimed job. The snapshot is written
 * to a local SQLite file at a stable path, uploaded to S3 at the same stable
 * key shape
 *   <bucket>/<media_directory>/snapshots/<username>/snapshot.db
 * (when S3 is configured), then the local file is deleted. At most one
 * snapshot file per user exists on disk at any time. When a resume_cursor
 * is present the existing partial file is reused; otherwise any stale
 * orphan from a previous failed run is wiped first so we don't inherit
 * half-written state. S3 versioning preserves history server-side.
 *
 * `mode = 'clean'` deletes the user's prior local snapshots before running
 * AND, when S3 is enabled, deletes the user's S3 prefix so old uploads are
 * cleared too.
 */
export class SnapshotJob {
    constructor({s3Uploader = new S3Uploader()} = {}) {
        this.s3Uploader = s3Uploader;
    }

    async run(job, requestRow, {onCursorReady = null} = {}) {
        return requestContext.run({username: job.username}, async () => {
            // mode=clean ignores any prior cursor: we're explicitly blowing
            // away local + S3 state so resume would be meaningless.
            const isClean = requestRow?.mode === 'clean';
            const cursor = (!isClean && job.resume_cursor) ? this._parseCursor(job.resume_cursor) : null;

            // Stable local path — one snapshot per user at any time.
            const localPath = path.resolve(
                config.snapshotsDir,
                fsSafe(requestRow.db_user),
                fsSafe(job.username),
                'snapshot.db'
            );
            // Stable S3 key. avni-server's resolver signs it directly — no
            // list+sort. S3 versioning preserves history of past generations.
            // Extensionless `.db` (not `.db.zip`) even though the body is
            // zipped: avni-server signs via generateMediaUploadUrl which binds
            // Content-Type into the signature from the extension, and `.zip`
            // would require the device to send Content-Type: application/zip
            // on the GET (it doesn't, and S3 returns 403). The device's
            // BackupRestoreSqliteService unzips by content, not by extension.
            const s3Key = `${requestRow.media_directory}/snapshots/${job.username}/snapshot.db`;

            if (isClean) {
                this._cleanUserSnapshotDir(path.dirname(localPath));
                if (this.s3Uploader.isEnabled()) {
                    const userS3Prefix = `${requestRow.media_directory}/snapshots/${job.username}/`;
                    await this.s3Uploader.deletePrefix(userS3Prefix);
                }
            } else if (!cursor && fs.existsSync(localPath)) {
                // Not resuming and not cleaning, but a file is sitting at the
                // stable path — that's a stale orphan from a previous failure
                // that didn't write a cursor. Wipe it so this fresh run starts
                // on an empty schema rather than inheriting half-written
                // entity_sync_status rows.
                this._removeLocal(localPath);
            }

            logger.info(
                {jobId: job.id, user: job.username, dbUser: requestRow.db_user, local: localPath, s3Key, mode: requestRow?.mode, resuming: Boolean(cursor)},
                'snapshot job starting'
            );

            const start = Date.now();
            // createSqliteDb is idempotent — opens the existing file and skips
            // migrations that have already been applied (schema_version check).
            const db = createSqliteDb(localPath);
            try {
                const persister = new Persister(db);
                // Fresh run: persist the cursor as soon as /v2/syncDetails
                // returns so any subsequent crash can resume. Resume run: the
                // cursor is already on the row; nothing new to write.
                const onSyncDetailsFetched = cursor
                    ? null
                    : (async ({endDateTime, syncDetails}) => {
                        if (onCursorReady) {
                            await onCursorReady({localPath, endDateTime, syncDetails});
                        }
                    });
                const runner = new SyncRunner({
                    db,
                    persister,
                    resumeContext: cursor ? {endDateTime: cursor.endDateTime, syncDetails: cursor.syncDetails} : null,
                    onSyncDetailsFetched,
                });
                await runner.run();
            } finally {
                db.close();
            }

            // Upload (when configured) → returns the canonical s3Key + sha256 + size.
            // When S3 is disabled we keep the local file and return its path.
            // generatedBySha + generatedForSchema are persisted on the job row
            // (regardless of S3) so the scheduler's local-DB freshness check
            // can detect when either has drifted and re-enqueue the user.
            let result;
            if (this.s3Uploader.isEnabled()) {
                // Object metadata mirrors the persisted columns. All values
                // must be strings (S3 user-metadata is x-amz-meta-* on the wire).
                const metadata = {
                    'generated-at': new Date().toISOString(),
                    'snapshot-server-sha': config.commitSha,
                    'client-schema-version': CLIENT_SCHEMA_VERSION,
                };
                const uploaded = await this.s3Uploader.uploadFile(localPath, s3Key, metadata);
                this._removeLocal(localPath);
                result = {
                    s3Key: uploaded.s3Key,
                    sha256: uploaded.sha256,
                    sizeBytes: uploaded.sizeBytes,
                    generatedBySha: config.commitSha,
                    generatedForSchema: CLIENT_SCHEMA_VERSION,
                };
            } else {
                const stats = fs.statSync(localPath);
                result = {
                    s3Key: localPath,
                    sha256: null,
                    sizeBytes: stats.size,
                    generatedBySha: config.commitSha,
                    generatedForSchema: CLIENT_SCHEMA_VERSION,
                };
            }

            logger.info(
                {jobId: job.id, user: job.username, ...result, elapsedMs: Date.now() - start, uploaded: this.s3Uploader.isEnabled()},
                'snapshot job done'
            );
            return result;
        });
    }

    _parseCursor(cursorJson) {
        try {
            const parsed = JSON.parse(cursorJson);
            if (!parsed?.localPath || !parsed?.endDateTime || !Array.isArray(parsed?.syncDetails)) {
                logger.warn({cursor: cursorJson}, 'resume cursor missing required fields; starting fresh');
                return null;
            }
            if (!fs.existsSync(parsed.localPath)) {
                logger.warn({localPath: parsed.localPath}, 'resume cursor file gone; starting fresh');
                return null;
            }
            return parsed;
        } catch (e) {
            logger.warn({err: e.message, cursor: cursorJson}, 'resume cursor unparseable; starting fresh');
            return null;
        }
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
