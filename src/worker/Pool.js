import {Worker} from './Worker.js';
import {logger} from '../util/logger.js';

export class Pool {
    constructor({size, snapshotRequestRepository, snapshotJob}) {
        this.size = size;
        this.workers = Array.from({length: size}, (_, i) =>
            new Worker({
                id: `w-${process.pid}-${i + 1}`,
                snapshotRequestRepository,
                snapshotJob,
            }));
    }

    start() {
        logger.info({size: this.size}, 'starting worker pool');
        for (const w of this.workers) w.start();
    }

    async stop() {
        logger.info('stopping worker pool');
        await Promise.all(this.workers.map(w => w.stop()));
    }
}
