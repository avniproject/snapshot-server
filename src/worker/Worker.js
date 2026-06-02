import {logger} from '../util/logger.js';
import {isPermanentFailure} from '../snapshot/errors.js';
import {computeNextRetryAt, MAX_FAILURE_ATTEMPTS} from '../snapshot/retryPolicy.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Single worker poll loop. Claims the next queued job from SnapshotRequestRepository via
 * its atomic transaction (no two workers claim the same row), runs it,
 * marks the result. Sleeps when the queue is empty, exits when stop() is
 * called and the in-flight job (if any) finishes.
 */
export class Worker {
    constructor({id, snapshotRequestRepository, snapshotJob, idlePollMs = 2000}) {
        this.id = id;
        this.snapshotRequestRepository = snapshotRequestRepository;
        this.snapshotJob = snapshotJob;
        this.idlePollMs = idlePollMs;
        this._running = false;
        this._loopPromise = null;
    }

    start() {
        if (this._running) return this._loopPromise;
        this._running = true;
        this._loopPromise = this._loop().catch((e) =>
            logger.error({worker: this.id, err: e.message, stack: e.stack}, 'worker loop crashed'));
        return this._loopPromise;
    }

    async stop() {
        this._running = false;
        if (this._loopPromise) await this._loopPromise;
    }

    async _loop() {
        logger.info({worker: this.id}, 'worker started');
        while (this._running) {
            const job = this.snapshotRequestRepository.claimNextSnapshotUserJob(this.id);
            if (!job) {
                await sleep(this.idlePollMs);
                continue;
            }

            try {
                // Everything inside the try so any unexpected error here
                // (including a DB read on getSnapshotRequestById) marks the
                // job failed instead of crashing the whole worker loop.
                const requestRow = this.snapshotRequestRepository.getSnapshotRequestById(job.request_id);
                const org = requestRow?.db_user;
                logger.info(
                    {worker: this.id, jobId: job.id, user: job.username, org, attempt: job.attempt_count, fromState: 'queued', toState: 'in_progress'},
                    'snapshot job state transition'
                );
                const onCursorReady = (cursor) =>
                    this.snapshotRequestRepository.saveSnapshotUserJobResumeCursor(job.id, cursor);
                const result = await this.snapshotJob.run(job, requestRow, {onCursorReady});
                this.snapshotRequestRepository.markSnapshotUserJobReady(job.id, {
                    s3Key: result.s3Key,
                    sha256: result.sha256,
                    sizeBytes: result.sizeBytes,
                    generatedBySha: result.generatedBySha,
                    generatedForSchema: result.generatedForSchema,
                    expectedWorkerId: job.worker_id,
                });
                logger.info(
                    {worker: this.id, jobId: job.id, user: job.username, org, attempt: job.attempt_count, fromState: 'in_progress', toState: 'ready'},
                    'snapshot job state transition'
                );
            } catch (e) {
                const permanent = isPermanentFailure(e);
                const nextRetryAt = computeNextRetryAt({
                    attemptCount: job.attempt_count,
                    permanent,
                    maxAttempts: MAX_FAILURE_ATTEMPTS,
                });
                logger.error(
                    {worker: this.id, jobId: job.id, user: job.username, attempt: job.attempt_count, permanent, nextRetryAt, err: e.message, stack: e.stack},
                    'snapshot job failed'
                );
                this.snapshotRequestRepository.markSnapshotUserJobFailed(job.id, e, {nextRetryAt, expectedWorkerId: job.worker_id});
                logger.info(
                    {worker: this.id, jobId: job.id, user: job.username, attempt: job.attempt_count, fromState: 'in_progress', toState: 'failed', nextRetryAt, permanent},
                    'snapshot job state transition'
                );
            }
        }
        logger.info({worker: this.id}, 'worker stopped');
    }
}
