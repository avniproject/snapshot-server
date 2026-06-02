import {logger} from '../util/logger.js';

/**
 * One pass of the scheduler. Reads opted-in orgs from avni-server, asks the
 * planner what to do per user, and acts:
 *
 *   - 'enqueue-new'      → batched into one new snapshot_request per org
 *   - 'restart-in-place' → per-row state transition back to queued (re-uses
 *                          the existing resume_cursor and job_id)
 *   - 'skip'             → no-op, log only at debug verbosity
 *
 * Org ordering = the order avni-server returns (enabledAt asc, oldest first).
 * Within an org users are ordered by lastSyncedAt desc with uuid asc as a
 * deterministic tiebreak. Per-org failures are logged and swallowed so one
 * bad org doesn't poison the rest of the tick.
 */
export async function runTick({
    avniSuperAdminClient,
    snapshotRequestRepository,
    tickPlanner,
    requestedBy = 'scheduler',
}) {
    const start = Date.now();
    let orgs;
    try {
        orgs = await avniSuperAdminClient.listSqliteSnapshotEnabledOrgs();
    } catch (e) {
        logger.error({err: e.message}, 'tick: failed to list opted-in orgs');
        return {orgs: 0, enqueuedUsers: 0, restartedUsers: 0, requests: 0};
    }
    logger.info({orgCount: orgs.length}, 'tick: opted-in orgs');

    let totalEnqueued = 0;
    let totalRestarted = 0;
    let totalRequests = 0;
    for (const org of orgs) {
        try {
            const summary = await _processOrg({
                org,
                avniSuperAdminClient,
                snapshotRequestRepository,
                tickPlanner,
                requestedBy,
            });
            totalEnqueued += summary.enqueued;
            totalRestarted += summary.restarted;
            if (summary.enqueued > 0) totalRequests += 1;
        } catch (e) {
            logger.error(
                {org: org.dbUser, err: e.message, stack: e.stack},
                'tick: org failed'
            );
        }
    }
    logger.info(
        {elapsedMs: Date.now() - start, orgs: orgs.length, requests: totalRequests, enqueuedUsers: totalEnqueued, restartedUsers: totalRestarted},
        'tick done'
    );
    return {orgs: orgs.length, enqueuedUsers: totalEnqueued, restartedUsers: totalRestarted, requests: totalRequests};
}

async function _processOrg({org, avniSuperAdminClient, snapshotRequestRepository, tickPlanner, requestedBy}) {
    const activities = await avniSuperAdminClient.listUserActivitiesForOrg(org.id);
    if (activities.length === 0) {
        logger.info({org: org.dbUser}, 'tick: org has no users');
        return {enqueued: 0, restarted: 0};
    }

    activities.sort(_byRecentActivity);
    const orderedUsernames = activities.map(a => a.username);

    const actions = tickPlanner.planActions({dbUser: org.dbUser, usernames: orderedUsernames});

    const toEnqueue = [];
    let restarted = 0;
    for (const username of orderedUsernames) {
        const decision = actions.get(username);
        if (decision.action === 'enqueue-new') {
            toEnqueue.push(username);
        } else if (decision.action === 'restart-in-place') {
            // expectedFromState fences against TOCTOU: the original worker
            // might race to terminal state between the planner's read and
            // this UPDATE. crashed → was in_progress; retry-due → was failed.
            const expectedFromState = decision.reason === 'crashed' ? 'in_progress' : 'failed';
            const r = snapshotRequestRepository.restartSnapshotUserJob(decision.jobId, {expectedFromState});
            if (r) {
                restarted += 1;
                logger.info(
                    {org: org.dbUser, user: username, jobId: decision.jobId, reason: decision.reason, fromState: r.fromState, toState: 'queued', restartedBy: 'scheduler'},
                    'snapshot job state transition'
                );
            } else {
                logger.info(
                    {org: org.dbUser, user: username, jobId: decision.jobId, reason: decision.reason, expectedFromState},
                    'tick: restart skipped (state moved on)'
                );
            }
        }
        // 'skip' is logged at debug to keep the info channel clean.
        else logger.debug({org: org.dbUser, user: username, reason: decision.reason}, 'tick: skip user');
    }

    if (toEnqueue.length === 0 && restarted === 0) {
        logger.info({org: org.dbUser, users: activities.length}, 'tick: org clean, nothing to do');
        return {enqueued: 0, restarted};
    }

    if (toEnqueue.length > 0) {
        const req = snapshotRequestRepository.createSnapshotRequestAndUserJobs({
            dbUser: org.dbUser,
            mediaDirectory: org.mediaDirectory,
            mode: 'normal',
            requestedBy,
            usernames: toEnqueue,
        });
        logger.info(
            {org: org.dbUser, requestKey: req.key, enqueued: toEnqueue.length, restarted, total: activities.length},
            'tick: enqueued snapshot request'
        );
    } else {
        logger.info(
            {org: org.dbUser, restarted, total: activities.length},
            'tick: in-place restarts only'
        );
    }

    return {enqueued: toEnqueue.length, restarted};
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
