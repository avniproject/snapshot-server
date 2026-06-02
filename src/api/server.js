import express from 'express';
import {logger} from '../util/logger.js';

/**
 * HTTP API for snapshot-server.
 *
 *   GET    /requests/:key                                  request summary + per-state counts
 *   GET    /requests/:key/users                            per-user job rows
 *   POST   /requests/:key/restart                          re-queue all failed/cancelled jobs
 *   POST   /requests/:key/users/:username/restart          re-queue one job
 *   DELETE /requests/:key                                  cancel queued jobs (in-flight unaffected)
 *   GET    /health                                         liveness check
 *
 * `:key` is the composite `<dbUser>-<orgSeq>` (e.g. `apfodisha-1`).
 *
 * Snapshot generation is no longer initiated via HTTP — the scheduler enqueues
 * runs based on per-org OrganisationConfig.enableSqliteSnapshotGeneration.
 * The endpoints below remain as ops tools for observation, retry, and cancel.
 */
export function createApi({snapshotRequestRepository}) {
    const app = express();
    app.use(express.json({limit: '1mb'}));

    app.use((req, _res, next) => {
        logger.info({method: req.method, path: req.path}, 'http');
        next();
    });

    app.get('/health', (_req, res) => res.json({ok: true}));

    app.get('/requests/:key', (req, res) => {
        const summary = snapshotRequestRepository.getSnapshotRequestSummaryByKey(req.params.key);
        if (!summary) return res.status(404).end();
        res.json(summary);
    });

    app.get('/requests/:key/users', (req, res) => {
        const jobs = snapshotRequestRepository.listSnapshotUserJobsByKey(req.params.key);
        if (jobs === null) return res.status(404).end();
        res.json(jobs);
    });

    app.post('/requests/:key/restart', (req, res) => {
        const jobs = snapshotRequestRepository.listSnapshotUserJobsByKey(req.params.key);
        if (jobs === null) return res.status(404).end();
        const restarted = [];
        for (const job of jobs) {
            if (job.state === 'failed' || job.state === 'cancelled') {
                snapshotRequestRepository.restartSnapshotUserJob(job.id);
                restarted.push(job.username);
            }
        }
        res.json({restarted});
    });

    app.post('/requests/:key/users/:username/restart', (req, res) => {
        const jobs = snapshotRequestRepository.listSnapshotUserJobsByKey(req.params.key);
        if (jobs === null) return res.status(404).end();
        const job = jobs.find(j => j.username === req.params.username);
        if (!job) return res.status(404).json({error: 'no such user job in this request'});
        snapshotRequestRepository.restartSnapshotUserJob(job.id);
        res.json({ok: true, jobId: job.id});
    });

    app.delete('/requests/:key', (req, res) => {
        const jobs = snapshotRequestRepository.listSnapshotUserJobsByKey(req.params.key);
        if (jobs === null) return res.status(404).end();
        const cancelled = [];
        for (const job of jobs) {
            if (job.state === 'queued' && snapshotRequestRepository.cancelSnapshotUserJob(job.id) === 1) {
                cancelled.push(job.username);
            }
        }
        res.json({cancelled});
    });

    app.use((err, _req, res, _next) => {
        logger.error({err: err.message, status: err.status, body: err.body, stack: err.stack}, 'unhandled api error');
        const status = err.status >= 400 && err.status < 600 ? err.status : 500;
        res.status(status).json({error: err.message, status: err.status, body: err.body});
    });

    return app;
}
