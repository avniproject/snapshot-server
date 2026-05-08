import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import {journal, sqlFiles} from './drizzleMigrations.js';
import {logger} from '../util/logger.js';

/**
 * Open or create a per-user snapshot SQLite file and apply the bundled
 * drizzle migrations. Uses the same migration journal the device runs, so
 * the resulting schema is identical to a freshly-installed device DB.
 *
 * Foreign keys are ON by default; disable explicitly during sync writes
 * (mirrors SyncService._disableForeignKeysIfSqlite) and re-enable before
 * close so consumers see a FK-validated snapshot.
 */
export function createSqliteDb(filePath, {failIfExists = false} = {}) {
    const dir = path.dirname(filePath);
    fs.mkdirSync(dir, {recursive: true});
    if (failIfExists && fs.existsSync(filePath)) {
        throw new Error(`Snapshot DB already exists: ${filePath}`);
    }
    const db = new Database(filePath);
    db.pragma('journal_mode = WAL');
    db.pragma('foreign_keys = ON');
    const result = runMigrations(db);
    logger.info({path: filePath, ...result}, 'snapshot DB schema applied');
    return db;
}

function runMigrations(db) {
    db.exec(
        `CREATE TABLE IF NOT EXISTS schema_version (
            version    INTEGER NOT NULL,
            applied_at INTEGER NOT NULL,
            tag        TEXT
        )`
    );
    const currentRow = db.prepare('SELECT MAX(version) AS version FROM schema_version').get();
    const currentVersion = currentRow?.version ?? -1;

    const pending = (journal.entries ?? [])
        .filter(e => e.idx > currentVersion)
        .sort((a, b) => a.idx - b.idx);

    const insertVersion = db.prepare(
        'INSERT INTO schema_version (version, tag, applied_at) VALUES (?, ?, ?)'
    );

    for (const entry of pending) {
        const sql = sqlFiles[entry.tag];
        if (!sql) throw new Error(`Migration SQL not found for tag: ${entry.tag}`);

        const statements = entry.breakpoints
            ? sql.split('--> statement-breakpoint').map(s => s.trim()).filter(Boolean)
            : [sql.trim()];

        const tx = db.transaction(() => {
            for (const stmt of statements) if (stmt) db.exec(stmt);
            insertVersion.run(entry.idx, entry.tag, Date.now());
        });
        tx();
        logger.debug({tag: entry.tag, idx: entry.idx}, 'migration applied');
    }

    return {
        from: currentVersion,
        to: pending.length > 0 ? pending.at(-1).idx : currentVersion,
        applied: pending.length,
    };
}
