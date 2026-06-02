import {logger} from '../util/logger.js';

/**
 * One pass of the scheduler. Reads opted-in orgs from avni-server, evaluates
 * per-user freshness against the local state DB, and enqueues fresh
 * snapshot_request rows for stale users only.
 *
 * Org ordering = the order avni-server returns (enabledAt asc, oldest first).
 * Within an org users are ordered by lastSyncedAt desc with uuid asc as a
 * deterministic tiebreak. The worker pool FIFO-claims by snapshot_user_job.id,
 * so the order users are inserted here = the order they're processed —
 * org-first across orgs, recent-activity-first within an org.
 *
 * Per-org failures are logged and swallowed so one bad org (network blip,
 * 500 from avni-server) doesn't poison the rest of the tick.
 */
export async function runTick({
    avniSuperAdminClient,
    snapshotRequestRepository,
    freshnessChecker,
    requestedBy = 'scheduler',
}) {
    const start = Date.now();
    let orgs;
    try {
        orgs = await avniSuperAdminClient.listSqliteSnapshotEnabledOrgs();
    } catch (e) {
        logger.error({err: e.message}, 'tick: failed to list opted-in orgs');
        return {orgs: 0, enqueuedUsers: 0, requests: 0};
    }
    logger.info({orgCount: orgs.length}, 'tick: opted-in orgs');

    let totalEnqueued = 0;
    let totalRequests = 0;
    for (const org of orgs) {
        try {
            const enq = await _processOrg({
                org,
                avniSuperAdminClient,
                snapshotRequestRepository,
                freshnessChecker,
                requestedBy,
            });
            totalEnqueued += enq;
            if (enq > 0) totalRequests += 1;
        } catch (e) {
            logger.error(
                {org: org.dbUser, err: e.message, stack: e.stack},
                'tick: org failed'
            );
        }
    }
    logger.info(
        {elapsedMs: Date.now() - start, orgs: orgs.length, requests: totalRequests, enqueuedUsers: totalEnqueued},
        'tick done'
    );
    return {orgs: orgs.length, enqueuedUsers: totalEnqueued, requests: totalRequests};
}

async function _processOrg({org, avniSuperAdminClient, snapshotRequestRepository, freshnessChecker, requestedBy}) {
    const activities = await avniSuperAdminClient.listUserActivitiesForOrg(org.id);
    if (activities.length === 0) {
        logger.info({org: org.dbUser}, 'tick: org has no users');
        return 0;
    }

    activities.sort(_byRecentActivity);
    const orderedUsernames = activities.map(a => a.username);

    const staleUsernames = freshnessChecker.findStaleUsers({
        dbUser: org.dbUser,
        usernames: orderedUsernames,
    });
    if (staleUsernames.length === 0) {
        logger.info({org: org.dbUser, users: activities.length}, 'tick: all users fresh');
        return 0;
    }

    const req = snapshotRequestRepository.createSnapshotRequestAndUserJobs({
        dbUser: org.dbUser,
        mediaDirectory: org.mediaDirectory,
        mode: 'normal',
        requestedBy,
        usernames: staleUsernames,
    });
    logger.info(
        {org: org.dbUser, requestKey: req.key, stale: staleUsernames.length, total: activities.length},
        'tick: enqueued snapshot request'
    );
    return staleUsernames.length;
}

// Recent-sync first. Users with no sync_telemetry row (lastSyncedAt = null)
// sort last — they're the least likely to be active. uuid asc breaks ties.
function _byRecentActivity(a, b) {
    const aSynced = a.lastSyncedAt;
    const bSynced = b.lastSyncedAt;
    if (aSynced === bSynced) return (a.uuid ?? '').localeCompare(b.uuid ?? '');
    if (aSynced == null) return 1;
    if (bSynced == null) return -1;
    if (aSynced > bSynced) return -1;
    if (aSynced < bSynced) return 1;
    return (a.uuid ?? '').localeCompare(b.uuid ?? '');
}
