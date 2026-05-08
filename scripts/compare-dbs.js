#!/usr/bin/env node
/**
 * Compare a device-side SQLite DB (pulled from the running app after a sync)
 * against a snapshot-server-generated DB for the same user.
 *
 * Usage:
 *   node scripts/compare-dbs.js --device <path> --snapshot <path>
 *
 * Optional:
 *   --ignore-tables a,b,c    skip these tables in addition to the built-in list
 *   --show 5                 max mismatch examples per table (default 5)
 *   --json                   emit a JSON report to stdout instead of pretty text
 *
 * The script walks every user table that exists in the snapshot DB, compares
 * row counts, and (for tables with a `uuid` column) does per-row column-by-
 * column comparison. JSON columns are normalised so key order doesn't cause
 * spurious diffs. A few device-only / timing-sensitive tables are skipped by
 * default — see DEFAULT_IGNORE / TOLERANT_TABLES below.
 */
import {Command} from 'commander';
import Database from 'better-sqlite3';

// Tables that are device-only state or otherwise expected to not match.
// Comparing them just produces noise.
const DEFAULT_IGNORE = new Set([
    'media_queue',          // device-only outbox of media pending upload
    'settings',             // device-only configuration row (excluded from getEntitiesToBePulled)
    'locale_mapping',       // local-only language list — SettingsService.initLanguages()
                            //   writes these from a hardcoded AvailableLocales constant in
                            //   avni-client; not synced from server, so snapshot-server has none
    'beneficiary_mode_pin', // device-only auth state
    'draft_subject',        // device-only in-progress drafts
    'draft_encounter',
    'sync_telemetry',       // collected on device during sync, snapshot-server skips
    'rule_failure_telemetry',
    'entity_queue',         // device's outbox of pending pushes
    'schema_version',       // migration metadata
    'sqlite_sequence',      // SQLite-internal AUTOINCREMENT counter
    'custom_dashboard_cache', 'dashboard_cache', // device-only UI caches
]);

// Epoch ms of EntitySyncStatus.REALLY_OLD_DATE = 1900-01-01.
// Used to filter out pre-seeded "never synced" entity_sync_status rows below.
const REALLY_OLD_DATE_MS = -2208988800000;

// Tables where some columns legitimately differ (sync timing, random uuids).
const TOLERANT_TABLES = {
    // entity_sync_status uuids are randomly generated per process; the
    // (entity_name, entity_type_uuid) tuples should match but the row uuids
    // and loaded_since timestamps will differ between two runs.
    //
    // Device's setup() pre-seeds rows with loaded_since = REALLY_OLD_DATE for
    // every entity in getEntitiesToBePulled(), even those with no data this
    // user can pull. snapshot-server only persists rows for entities that
    // actually had pages of data. The pre-seeded "never synced" rows on
    // device aren't load-bearing — device's setup() re-seeds them on first
    // launch after restore — so we drop them from the comparison entirely.
    entity_sync_status: {
        keyColumns: ['entity_name', 'entity_type_uuid'],
        ignoreColumns: new Set(['uuid', 'loaded_since']),
        skipRow: (row) =>
            row.loaded_since == null || Number(row.loaded_since) <= REALLY_OLD_DATE_MS,
    },
};

// JSON-shaped string columns — normalise key order before comparing.
// Optional `stripKeys` removes specific inner fields before normalisation
// (e.g., the per-media-item `uuid` is generated client-side and won't match
// across two independent runs).
const JSON_COLUMNS = new Map([
    ['observations', {}],
    ['decisions', {}],
    ['cancel_observations', {}],
    ['settings', {}],
    ['sync_settings', {}],
    ['key_values', {}],
    ['registration_location', {}],
    ['subject_location', {}],
    ['location', {}],
    ['configurations', {}],
    ['translation_json', {}],
    ['address_translations', {}],
    ['media', {stripKeys: new Set(['uuid'])}],
    ['answers', {}],
    ['sync_attribute_values', {}],
]);

const program = new Command()
    .requiredOption('--device <path>', 'device-side SQLite DB')
    .requiredOption('--snapshot <path>', 'snapshot-server-generated SQLite DB')
    .option('--ignore-tables <list>', 'extra tables to skip (comma-separated)', '')
    .option('--show <n>', 'max mismatch examples per table', '5')
    .option('--json', 'emit JSON report instead of pretty text', false)
    .parse();

const opts = program.opts();
const showLimit = parseInt(opts.show, 10);
const extraIgnore = opts.ignoreTables.split(',').map(s => s.trim()).filter(Boolean);
const ignore = new Set([...DEFAULT_IGNORE, ...extraIgnore]);

const dev = new Database(opts.device, {readonly: true});
const snap = new Database(opts.snapshot, {readonly: true});

const snapTables = listUserTables(snap);
const devTables = new Set(listUserTables(dev));

const results = [];
const summary = {match: 0, diff: 0, skipped: 0, missingOnDevice: 0, errors: 0};

for (const table of snapTables) {
    if (ignore.has(table)) {
        results.push({table, status: 'skipped'});
        summary.skipped++;
        continue;
    }
    if (!devTables.has(table)) {
        results.push({table, status: 'missing-on-device'});
        summary.missingOnDevice++;
        continue;
    }
    try {
        const r = compareTable(table);
        results.push({table, ...r});
        if (r.status === 'match') summary.match++;
        else summary.diff++;
    } catch (e) {
        results.push({table, status: 'error', error: e.message});
        summary.errors++;
    }
}

if (opts.json) {
    console.log(JSON.stringify({summary, results}, null, 2));
} else {
    printPretty(results, summary);
}

dev.close();
snap.close();
process.exit(summary.diff + summary.errors > 0 ? 1 : 0);

// ──────────────────────────────────────────────────────────────────────

function listUserTables(db) {
    return db
        .prepare(`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name`)
        .all()
        .map(r => r.name);
}

function compareTable(table) {
    const cols = snap
        .prepare(`PRAGMA table_info(${table})`)
        .all()
        .map(c => c.name);

    const tolerant = TOLERANT_TABLES[table];
    const keyColumns = tolerant?.keyColumns ?? (cols.includes('uuid') ? ['uuid'] : null);
    const ignoreCols = tolerant?.ignoreColumns ?? new Set();
    const skipRow = tolerant?.skipRow ?? (() => false);

    const rawDevRows = dev.prepare(`SELECT * FROM ${table}`).all();
    const rawSnapRows = snap.prepare(`SELECT * FROM ${table}`).all();
    const devRows = rawDevRows.filter(r => !skipRow(r));
    const snapRows = rawSnapRows.filter(r => !skipRow(r));
    const devCount = devRows.length;
    const snapCount = snapRows.length;

    if (!keyColumns) {
        // No usable key — only row-count comparison
        return {
            status: devCount === snapCount ? 'match' : 'diff',
            note: `no uuid/key columns — only row counts compared (dev=${devCount} snap=${snapCount})`,
            device: devCount,
            snapshot: snapCount,
        };
    }

    const devByKey = new Map(devRows.map(r => [keyOf(r, keyColumns), r]));
    const snapByKey = new Map(snapRows.map(r => [keyOf(r, keyColumns), r]));

    const onlyDev = [];
    const onlySnap = [];
    const mismatches = [];

    for (const [key, snapRow] of snapByKey) {
        const devRow = devByKey.get(key);
        if (!devRow) {
            onlySnap.push(key);
            continue;
        }
        for (const col of cols) {
            if (ignoreCols.has(col)) continue;
            if (!valuesEqual(snapRow[col], devRow[col], col)) {
                mismatches.push({
                    key,
                    col,
                    snap: snapRow[col],
                    dev: devRow[col],
                });
            }
        }
    }
    for (const [key] of devByKey) {
        if (!snapByKey.has(key)) onlyDev.push(key);
    }

    const ok =
        onlyDev.length === 0 && onlySnap.length === 0 && mismatches.length === 0;

    return {
        status: ok ? 'match' : 'diff',
        device: devCount,
        snapshot: snapCount,
        onlyDev: onlyDev.length,
        onlySnap: onlySnap.length,
        mismatchCount: mismatches.length,
        mismatchSample: mismatches.slice(0, showLimit),
        onlyDevSample: onlyDev.slice(0, showLimit),
        onlySnapSample: onlySnap.slice(0, showLimit),
        keyColumns,
    };
}

function keyOf(row, keyColumns) {
    return keyColumns.map(k => row[k] ?? '').join('|');
}

function valuesEqual(a, b, columnName) {
    if (a === b) return true;
    if (a == null && b == null) return true;
    if (a == null || b == null) return false;

    // JSON normalisation for known JSON columns
    const colSpec = JSON_COLUMNS.get(columnName);
    if (colSpec || looksLikeJson(a) || looksLikeJson(b)) {
        const an = normaliseJson(a, colSpec);
        const bn = normaliseJson(b, colSpec);
        if (an === bn) return true;
    }

    // Numeric coercion: 0 vs '0', 1.0 vs 1
    if (Number.isFinite(+a) && Number.isFinite(+b) && +a === +b) return true;

    return false;
}

function looksLikeJson(v) {
    if (typeof v !== 'string') return false;
    const t = v.trim();
    return (
        (t.startsWith('{') && t.endsWith('}')) ||
        (t.startsWith('[') && t.endsWith(']'))
    );
}

function normaliseJson(v, spec = {}) {
    if (typeof v !== 'string') return v;
    try {
        return JSON.stringify(canonicalise(JSON.parse(v), spec));
    } catch {
        return v;
    }
}

function canonicalise(v, spec = {}) {
    if (Array.isArray(v)) return v.map(item => canonicalise(item, spec));
    if (v && typeof v === 'object') {
        const out = {};
        for (const k of Object.keys(v).sort()) {
            if (spec.stripKeys?.has(k)) continue;
            out[k] = canonicalise(v[k], spec);
        }
        return out;
    }
    return v;
}

function printPretty(results, summary) {
    console.log('=== TABLE-BY-TABLE COMPARISON ===');
    for (const r of results) {
        const t = r.table.padEnd(38);
        if (r.status === 'match') {
            console.log(`✓ ${t} match (${r.snapshot} rows)`);
        } else if (r.status === 'skipped') {
            console.log(`- ${t} skipped`);
        } else if (r.status === 'missing-on-device') {
            console.log(`! ${t} missing on device`);
        } else if (r.status === 'error') {
            console.log(`E ${t} error: ${r.error}`);
        } else {
            const detail = r.note
                ? r.note
                : `dev=${r.device} snap=${r.snapshot} onlyDev=${r.onlyDev} onlySnap=${r.onlySnap} mismatches=${r.mismatchCount}`;
            console.log(`✗ ${t} ${detail}`);
            for (const m of r.mismatchSample ?? []) {
                console.log(
                    `     ${m.key} [${m.col}]: dev=${truncate(m.dev)}  snap=${truncate(m.snap)}`
                );
            }
            if (r.onlyDevSample?.length) {
                console.log(`     onlyDev sample: ${r.onlyDevSample.join(', ')}`);
            }
            if (r.onlySnapSample?.length) {
                console.log(`     onlySnap sample: ${r.onlySnapSample.join(', ')}`);
            }
        }
    }
    console.log('\n=== SUMMARY ===');
    console.log(
        `match: ${summary.match}, diff: ${summary.diff}, skipped: ${summary.skipped}, missing-on-device: ${summary.missingOnDevice}, errors: ${summary.errors}`
    );
}

function truncate(v) {
    if (v == null) return 'null';
    const s = String(v);
    return s.length > 80 ? s.slice(0, 77) + '...' : s;
}
