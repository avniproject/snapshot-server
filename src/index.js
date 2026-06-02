import {initStateDb, closeStateDb} from './db/db.js';
import {SnapshotRequestRepository} from './db/SnapshotRequestRepository.js';
import {SnapshotJob} from './snapshot/SnapshotJob.js';
import {Pool} from './worker/Pool.js';
import {createApi} from './api/server.js';
import {AvniSuperAdminClient} from './avni/AvniSuperAdminClient.js';
import {Scheduler} from './scheduler/Scheduler.js';
import {FreshnessChecker} from './scheduler/FreshnessChecker.js';
import {runTick} from './scheduler/tick.js';
import {journal as drizzleJournal} from './snapshotdb/drizzleMigrations.js';
import {logger} from './util/logger.js';
import {config} from './config.js';

initStateDb();

const snapshotRequestRepository = new SnapshotRequestRepository();
const snapshotJob = new SnapshotJob();
const avniSuperAdminClient = new AvniSuperAdminClient();
const pool = new Pool({size: config.maxConcurrency, snapshotRequestRepository, snapshotJob});
pool.start();

const currentSchemaVersion = String(
    Math.max(...(drizzleJournal.entries ?? []).map(e => e.idx))
);
const freshnessChecker = new FreshnessChecker({
    currentSha: config.commitSha,
    currentSchemaVersion,
    thresholdSeconds: config.freshnessThresholdHours * 3600,
});
const scheduler = new Scheduler({
    intervalMs: config.schedulerTickIntervalMs,
    tick: () => runTick({avniSuperAdminClient, snapshotRequestRepository, freshnessChecker}),
});
scheduler.start();

const app = createApi({snapshotRequestRepository});
const server = app.listen(config.httpPort, () =>
    logger.info(
        {
            port: config.httpPort,
            workers: config.maxConcurrency,
            server: config.avniServerUrl,
            adminUser: config.adminUser,
            tickIntervalMs: config.schedulerTickIntervalMs,
            freshnessThresholdHours: config.freshnessThresholdHours,
        },
        'snapshot-server listening'
    ));

let shuttingDown = false;
async function shutdown(signal) {
    if (shuttingDown) return;
    shuttingDown = true;
    logger.info({signal}, 'shutdown initiated — finishing in-flight jobs');
    await scheduler.stop();
    await pool.stop();
    server.close(() => {
        closeStateDb();
        logger.info('shutdown complete');
        process.exit(0);
    });
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
