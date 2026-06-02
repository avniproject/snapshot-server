import {logger} from '../util/logger.js';

/**
 * setInterval-driven tick runner with concurrent-run protection.
 *
 * Fires the supplied tick fn immediately on start(), then every intervalMs.
 * If a tick is still running when the timer fires, the next firing no-ops
 * with a log — late ticks never queue up behind a slow one. start() is
 * idempotent; stop() clears the timer and awaits the in-flight tick (if any)
 * before resolving, so shutdown is clean.
 *
 * Errors from the tick fn are caught and logged; one bad tick doesn't break
 * the loop.
 */
export class Scheduler {
    constructor({tick, intervalMs}) {
        this.tick = tick;
        this.intervalMs = intervalMs;
        this._timer = null;
        this._tickInFlight = false;
        this._stopped = false;
        this._lastTickPromise = Promise.resolve();
    }

    start() {
        if (this._timer || this._stopped) return;
        logger.info({intervalMs: this.intervalMs}, 'scheduler starting');
        this._fireTick();
        this._timer = setInterval(() => this._fireTick(), this.intervalMs);
    }

    async stop() {
        this._stopped = true;
        if (this._timer) {
            clearInterval(this._timer);
            this._timer = null;
        }
        await this._lastTickPromise;
        logger.info('scheduler stopped');
    }

    _fireTick() {
        if (this._stopped) return;
        if (this._tickInFlight) {
            logger.info('scheduler tick skipped: previous tick still running');
            return;
        }
        this._tickInFlight = true;
        this._lastTickPromise = (async () => {
            try {
                await this.tick();
            } catch (e) {
                logger.error({err: e.message, stack: e.stack}, 'scheduler tick threw');
            } finally {
                this._tickInFlight = false;
            }
        })();
    }
}
