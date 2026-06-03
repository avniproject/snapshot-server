import 'dotenv/config';

function required(name) {
    const v = process.env[name];
    if (!v) throw new Error(`Missing required env var: ${name}`);
    return v;
}

function optional(name, def) {
    const v = process.env[name];
    return v === undefined || v === '' ? def : v;
}

function intOpt(name, def) {
    return parseInt(optional(name, String(def)), 10);
}

export const config = {
    avniServerUrl: required('AVNI_SERVER_URL').replace(/\/$/, ''),

    // Required when avni-server runs with AVNI_IDP_TYPE=cognito (staging).
    // Empty in production where the dedicated avni-server instance runs with
    // AVNI_IDP_TYPE=none and reads USER-NAME instead.
    authToken: optional('AUTH_TOKEN', ''),

    // Username used for super-admin-only lookups (org search, user list).
    // `admin` is the account-admin user seeded by avni-server's V1_142.1
    // migration in every standard install — hardcoded since there's no
    // realistic case for overriding it.
    adminUser: 'admin',
    logLevel: optional('LOG_LEVEL', 'info'),
    stateDbPath: optional('STATE_DB_PATH', './snapshot-db/snapshot-server.db'),
    snapshotsDir: optional('SNAPSHOTS_DIR', './snapshots'),
    pageSize: intOpt('PAGE_SIZE', 1000),
    replicaLagMinutes: intOpt('REPLICA_LAG_MINUTES', 10),
    maxConcurrency: intOpt('MAX_CONCURRENCY', 1),
    httpPort: intOpt('HTTP_PORT', 3000),
    // Per-page fetch timeout (ms).
    fetchTimeoutMs: intOpt('FETCH_TIMEOUT_MS', 300000),

    s3Bucket:          optional('OPENCHS_BUCKET_NAME',             ''),
    awsRegion:         optional('OPENCHS_AWS_REGION',              'ap-south-1'),
    awsAccessKeyId:    optional('OPENCHS_IAM_USER_ACCESS_KEY',     ''),
    awsSecretAccessKey: optional('OPENCHS_IAM_USER_SECRET_ACCESS_KEY', ''),

    // Recorded on uploaded snapshots as x-amz-meta-snapshot-server-sha so the
    // scheduler's freshness check (sub-issue #1942/3) can detect that the
    // generating code changed and re-run a user. Set on deploy; falls back to
    // 'dev' when unset.
    commitSha: optional('SNAPSHOT_SERVER_COMMIT_SHA', 'dev'),

    // Scheduler — drives all snapshot generation as of sub-issue #1942/3.
    schedulerTickIntervalMs: intOpt('SCHEDULER_TICK_INTERVAL_MS', 60000),
    // A user's last successful snapshot is considered fresh for this long.
    // Shorter values regenerate more often (cost: more S3 traffic); longer
    // values mean devices see staler "fast sync" baselines.
    freshnessThresholdHours: intOpt('FRESHNESS_THRESHOLD_HOURS', 24),
    // How long an in_progress row can sit before the planner treats it as
    // crashed (worker died mid-job) and restarts it in place. Should be
    // comfortably larger than the longest legitimate single-user snapshot.
    crashTimeoutHours: intOpt('CRASH_TIMEOUT_HOURS', 4),
};
