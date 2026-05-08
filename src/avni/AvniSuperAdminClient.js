import {requestContext} from '../rest/requestContext.js';
import {getJSON} from '../rest/requests.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

/**
 * Talks to avni-server's super-admin endpoints to:
 *   1. fetch an organisation by its dbUser
 *   2. enumerate the active users of that organisation
 *
 * Both endpoints require super-admin privileges (the account-admin `admin`
 * user seeded by V1_142.1 has them by default). All calls run inside a
 * requestContext.run with username = config.adminUser so the avni-server
 * `IdpType=none` filter authenticates as that super-admin.
 *
 * Endpoints used:
 *   GET /organisation/search/find?dbUser=<dbUser>&size=1   (assertIsSuperAdmin)
 *   GET /user/search/findByOrganisation?organisationId=<id>&page=&size=
 *       (PrivilegeType.EditUserConfiguration)
 */
export class AvniSuperAdminClient {
    constructor({adminUser = config.adminUser, baseUrl = config.avniServerUrl, pageSize = 200} = {}) {
        this.adminUser = adminUser;
        this.baseUrl = baseUrl.replace(/\/$/, '');
        this.pageSize = pageSize;
    }

    async getOrgByDbUser(dbUser) {
        return this._asAdmin(async () => {
            const url = `${this.baseUrl}/organisation/search/find?dbUser=${encodeURIComponent(dbUser)}&size=1`;
            const page = await getJSON(url);
            const content = page?.content ?? page?._embedded?.organisations ?? [];
            return content.length === 0 ? null : content[0];
        });
    }

    async listUsersForOrg(organisationId) {
        return this._asAdmin(async () => {
            const users = [];
            let page = 0;
            while (true) {
                const url = `${this.baseUrl}/user/search/findByOrganisation`
                    + `?organisationId=${organisationId}`
                    + `&page=${page}`
                    + `&size=${this.pageSize}`;
                const resp = await getJSON(url);
                const content = resp?.content ?? resp?._embedded?.users ?? [];
                for (const u of content) {
                    if (u?.username && !u.voided && !u.isVoided) users.push(u);
                }
                const totalPages = resp?.totalPages ?? resp?.page?.totalPages ?? 1;
                page += 1;
                if (page >= totalPages) break;
            }
            return users;
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
