#!/usr/bin/env node
// Reads the snapshot-server state DB and prints per-job timings, grouped by
// state. Run from the snapshot-server repo root: `npm run stats`.

import Database from 'better-sqlite3';
import {config} from '../src/config.js';

const db = new Database(config.stateDbPath, {readonly: true});

const counts = db
    .prepare(`SELECT state, COUNT(*) AS n FROM snapshot_user_job GROUP BY state ORDER BY state`)
    .all();

console.log('=== SUMMARY ===');
if (counts.length === 0) {
    console.log('  (no jobs in state DB)');
} else {
    for (const c of counts) console.log(`  ${c.state.padEnd(12)} ${c.n}`);
}
console.log();

printRequests();
printReady();
printFailed();
printInProgress();
printQueuedCount();

db.close();

// ────────────────────────────────────────────────────────────────────────

function printRequests() {
    const now = Math.floor(Date.now() / 1000);
    const rows = db.prepare(`
        SELECT
            sr.id,
            sr.db_user,
            sr.org_seq,
            sr.state AS request_state,
            MIN(sj.started_at)  AS first_started,
            MAX(sj.finished_at) AS last_finished,
            COUNT(*)                                                   AS total,
            SUM(CASE WHEN sj.state = 'ready'       THEN 1 ELSE 0 END)  AS ready,
            SUM(CASE WHEN sj.state = 'failed'      THEN 1 ELSE 0 END)  AS failed,
            SUM(CASE WHEN sj.state = 'in_progress' THEN 1 ELSE 0 END)  AS in_progress,
            SUM(CASE WHEN sj.state = 'queued'      THEN 1 ELSE 0 END)  AS queued,
            SUM(CASE WHEN sj.state = 'cancelled'   THEN 1 ELSE 0 END)  AS cancelled
        FROM snapshot_request sr
        LEFT JOIN snapshot_user_job sj ON sj.request_id = sr.id
        GROUP BY sr.id
        ORDER BY sr.id
    `).all();
    if (rows.length === 0) return;

    console.log(`=== REQUESTS (${rows.length}) ===`);
    console.log(
        `  ${'key'.padEnd(20)} ${'start'.padEnd(19)} ${'end'.padEnd(19)} ${'wall'.padEnd(10)} `
        + `total ready fail prog queue cancel`,
    );
    for (const r of rows) {
        const stillRunning = (r.in_progress + r.queued) > 0;
        const endEpoch = stillRunning ? now : r.last_finished;
        const wall = (r.first_started != null && endEpoch != null) ? endEpoch - r.first_started : null;
        const key = `${r.db_user}-${r.org_seq}`;
        const endLabel = stillRunning ? formatTimestamp(now) + '*' : formatTimestamp(r.last_finished);
        console.log(
            `  ${truncate(key, 20).padEnd(20)} `
            + `${formatTimestamp(r.first_started).padEnd(19)} `
            + `${endLabel.padEnd(19)} `
            + `${formatDuration(wall).padEnd(10)} `
            + `${String(r.total).padEnd(5)} ${String(r.ready).padEnd(5)} `
            + `${String(r.failed).padEnd(4)} ${String(r.in_progress).padEnd(4)} `
            + `${String(r.queued).padEnd(5)} ${String(r.cancelled).padEnd(6)}`,
        );
    }
    console.log(`  (* = end time is "now" because request is still running)`);
    console.log();
}

function formatTimestamp(epochSeconds) {
    if (epochSeconds == null) return '-';
    const d = new Date(epochSeconds * 1000);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} `
        + `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function printReady() {
    const rows = db.prepare(`
        SELECT id, username, started_at, finished_at, size_bytes,
               finished_at - started_at AS duration
        FROM snapshot_user_job
        WHERE state = 'ready' AND started_at IS NOT NULL AND finished_at IS NOT NULL
        ORDER BY duration DESC
    `).all();
    if (rows.length === 0) return;

    console.log(`=== READY (${rows.length}) — sorted by duration desc ===`);
    console.log(`  ${'id'.padEnd(4)} ${'username'.padEnd(35)} ${'duration'.padEnd(10)} size`);
    for (const r of rows) {
        console.log(
            `  ${String(r.id).padEnd(4)} ${truncate(r.username, 35).padEnd(35)} `
            + `${formatDuration(r.duration).padEnd(10)} ${formatBytes(r.size_bytes)}`,
        );
    }

    const durations = rows.map(r => r.duration);
    const sorted = [...durations].sort((a, b) => a - b);
    const sum = durations.reduce((a, b) => a + b, 0);
    const avg = sum / durations.length;
    const median = sorted[Math.floor(sorted.length / 2)];
    const totalSize = rows.reduce((a, r) => a + (r.size_bytes ?? 0), 0);

    console.log();
    console.log(`  average:  ${formatDuration(Math.round(avg))}`);
    console.log(`  median:   ${formatDuration(median)}`);
    console.log(`  min:      ${formatDuration(sorted[0])}`);
    console.log(`  max:      ${formatDuration(sorted.at(-1))}`);
    console.log(`  total:    ${formatDuration(sum)} of work across ${rows.length} jobs`);
    console.log(`  size sum: ${formatBytes(totalSize)} uploaded / on disk`);
    console.log();
}

function printFailed() {
    const rows = db.prepare(`
        SELECT id, username, attempt_count, started_at, finished_at, last_error,
               finished_at - started_at AS duration
        FROM snapshot_user_job
        WHERE state = 'failed'
        ORDER BY id
    `).all();
    if (rows.length === 0) return;

    console.log(`=== FAILED (${rows.length}) ===`);
    console.log(`  ${'id'.padEnd(4)} ${'username'.padEnd(35)} ${'attempts'.padEnd(9)} ${'duration'.padEnd(10)} error`);
    for (const r of rows) {
        const err = truncate(r.last_error ?? '', 70);
        console.log(
            `  ${String(r.id).padEnd(4)} ${truncate(r.username, 35).padEnd(35)} `
            + `${String(r.attempt_count).padEnd(9)} ${formatDuration(r.duration).padEnd(10)} ${err}`,
        );
    }
    console.log();
}

function printInProgress() {
    const now = Math.floor(Date.now() / 1000);
    const rows = db.prepare(`
        SELECT id, username, started_at, worker_id, attempt_count,
               ${now} - started_at AS so_far
        FROM snapshot_user_job
        WHERE state = 'in_progress' AND started_at IS NOT NULL
        ORDER BY started_at
    `).all();
    if (rows.length === 0) return;

    console.log(`=== IN PROGRESS (${rows.length}) — running right now ===`);
    console.log(`  ${'id'.padEnd(4)} ${'username'.padEnd(35)} ${'worker'.padEnd(20)} so far`);
    for (const r of rows) {
        console.log(
            `  ${String(r.id).padEnd(4)} ${truncate(r.username, 35).padEnd(35)} `
            + `${(r.worker_id ?? '-').padEnd(20)} ${formatDuration(r.so_far)}`,
        );
    }
    console.log();
}

function printQueuedCount() {
    const queued = db
        .prepare(`SELECT COUNT(*) AS n FROM snapshot_user_job WHERE state = 'queued'`)
        .get().n;
    if (queued > 0) {
        console.log(`=== QUEUED ===`);
        console.log(`  ${queued} waiting for a worker to pick up`);
        console.log();
    }
}

function formatDuration(seconds) {
    if (seconds == null || Number.isNaN(seconds)) return '-';
    const sign = seconds < 0 ? '-' : '';
    const abs = Math.abs(seconds);
    const h = Math.floor(abs / 3600);
    const m = Math.floor((abs % 3600) / 60);
    const s = abs % 60;
    return `${sign}${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function formatBytes(bytes) {
    if (bytes == null) return '-';
    if (bytes >= 1024 * 1024) return `${(bytes / 1024 / 1024).toFixed(1)} MB`;
    if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${bytes} B`;
}

function truncate(s, n) {
    const str = String(s ?? '');
    return str.length > n ? str.slice(0, n - 1) + '…' : str;
}
