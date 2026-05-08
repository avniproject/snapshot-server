import {logger} from '../util/logger.js';

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
                // Inside the try so any unexpected error here marks the job
                // failed instead of crashing the whole worker loop.
                const requestRow = this.snapshotRequestRepository.getSnapshotRequestById(job.request_id);
                const result = await this.snapshotJob.run(job, requestRow);
                this.snapshotRequestRepository.markSnapshotUserJobReady(job.id, {
                    s3Key: result.outputPath,  // local path for now; S3 key when uploader lands
                    sha256: null,
                    sizeBytes: result.sizeBytes,
                });
            } catch (e) {
                logger.error(
                    {worker: this.id, jobId: job.id, user: job.username, err: e.message, stack: e.stack},
                    'snapshot job failed'
                );
                this.snapshotRequestRepository.markSnapshotUserJobFailed(job.id, e);
            }
        }
        logger.info({worker: this.id}, 'worker stopped');
    }
}
