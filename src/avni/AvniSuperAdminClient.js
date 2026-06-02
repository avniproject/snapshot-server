import {requestContext} from '../rest/requestContext.js';
import {getJSON} from '../rest/requests.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

/**
 * Talks to avni-server's super-admin endpoints to drive the scheduler:
 *   1. enumerate orgs opted into snapshot generation
 *   2. fetch each org's users with their last sync_telemetry timestamp
 *
 * Both endpoints require super-admin privileges (the account-admin `admin`
 * user seeded by V1_142.1 has them by default). All calls run inside a
 * requestContext.run with username = config.adminUser so the avni-server
 * `IdpType=none` filter authenticates as that super-admin.
 */
export class AvniSuperAdminClient {
    constructor({adminUser = config.adminUser, baseUrl = config.avniServerUrl} = {}) {
        this.adminUser = adminUser;
        this.baseUrl = baseUrl.replace(/\/$/, '');
    }

    // Scheduler entrypoint: orgs that have opted into snapshot generation via
    // OrganisationConfig.enableSqliteSnapshotGeneration. avni-server returns
    // them already sorted by enabledAt asc (oldest-opted-in first).
    async listSqliteSnapshotEnabledOrgs() {
        return this._asAdmin(async () => {
            const url = `${this.baseUrl}/organisation/sqliteSnapshotEnabled`;
            const resp = await getJSON(url);
            return Array.isArray(resp) ? resp : [];
        });
    }

    // Per-user lastSyncedAt (max sync_telemetry.sync_end_time). Used by the
    // scheduler to order users within an org recent-activity-first. Returns
    // [{id, uuid, username, lastSyncedAt: string|null}, ...].
    async listUserActivitiesForOrg(organisationId) {
        return this._asAdmin(async () => {
            const url = `${this.baseUrl}/user/activities?organisationId=${organisationId}`;
            const resp = await getJSON(url);
            return Array.isArray(resp) ? resp : [];
        });
    }

    async _asAdmin(fn) {
        // Wrap in a requestContext so requests.js auth header reads adminUser
        // for these calls regardless of any outer context the API handler ran in.
        return requestContext.run({username: this.adminUser}, async () => {
            try {
                return await fn();
            } catch (e) {
                logger.error(
                    {adminUser: this.adminUser, status: e.status, body: e.body, err: e.message},
                    'avni admin call failed'
                );
                throw e;
            }
        });
    }
}
