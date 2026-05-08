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

    s3Bucket: optional('S3_BUCKET', ''),
    s3Prefix: optional('S3_PREFIX', 'snapshot-v2/sqlite'),
    awsRegion: optional('AWS_REGION', 'ap-south-1'),
};
