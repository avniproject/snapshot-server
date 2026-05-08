/**
 * Port of avni-client's framework/http/requests.js, adapted for snapshot-server.
 *
 * Kept upstream-shaped so this stays a vendor with localized edits, not a
 * parallel reimplementation. Edits relative to upstream are bracketed by
 * "// <SS …>" comments below; nothing else is intentionally divergent.
 *
 * Edits:
 *   - Auth comes from snapshot-server's requestContext (AsyncLocalStorage)
 *     and config.authToken (env), not from GlobalContext.beanRegistry.
 *   - XSRF cookie handling dropped — server-to-server calls don't need it,
 *     and the @react-native-cookies/cookies module is RN-only.
 *   - 401/403 throws a plain annotated Error instead of avni-client's
 *     AuthenticationError / ServerError (no React UI to discriminate).
 *   - IDP_PROVIDERS switch dropped: snapshot-server unconditionally sends
 *     both USER-NAME and AUTH-TOKEN; avni-server's IdpType decides which to
 *     read.
 *   - Uses Node 20's built-in fetch (matches upstream's fetch usage).
 */
import _ from 'lodash';
import {config} from '../config.js';
import {currentUsername} from './requestContext.js';
import General from '../snapshotdb/General.js';

const ACCEPTABLE_RESPONSE_STATUSES = [200, 201];

// <SS> avni-client reads auth from GlobalContext.beanRegistry; we read from
// our own context — username via AsyncLocalStorage, AUTH-TOKEN from env.
const getAuthToken = () => config.authToken;
const getActiveUsername = () => currentUsername();
// </SS>

export function isHttpRequestSuccessful(responseCode) {
    return ACCEPTABLE_RESPONSE_STATUSES.indexOf(responseCode) > -1;
}

const fetchFactory = (endpoint, method = "GET", params, fetchWithoutTimeout) => {
    const processResponse = async (response) => {
        const responseCode = parseInt(response.status);
        if (isHttpRequestSuccessful(responseCode)) {
            return response;
        }
        // <SS> upstream throws AuthenticationError / ServerError; we throw a
        // plain Error annotated with status/body — no React UI to branch on.
        const body = await response.text().catch(() => '');
        General.logError("requests", `${method} ${endpoint} → ${responseCode}: ${body}`);
        const err = new Error(`HTTP ${responseCode} on ${method} ${endpoint}`);
        err.status = responseCode;
        err.body = body;
        throw err;
        // </SS>
    };
    const requestInit = {"method": method, ...params};
    // <SS> XSRF cookie injection (getXSRFPromise) dropped — RN-only and not
    // required for server-to-server calls into avni-server.
    return (fetchWithoutTimeout
        ? fetch(endpoint, requestInit)
        : fetchWithTimeOut(endpoint, requestInit)
    ).then(processResponse);
    // </SS>
};

const fetchWithTimeOut = (url, options, timeout = 60000) => {
    return Promise.race([
        fetch(url, options),
        new Promise((_resolve, reject) =>
            setTimeout(() => reject(new Error("syncTimeoutError")), timeout)
        )
    ]);
};

const makeHeader = function (type) {
    const jsonRequestHeader = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    };
    const textRequestHeader = {'Accept': 'text/plain', 'Content-Type': 'text/plain'};
    return new Map([['json', {headers: jsonRequestHeader}],
        ['text', {headers: textRequestHeader}]]).get(type);
}

const makeRequest = (type, opts = {}) => _.assignIn({...makeHeader(type), ...opts});

// <SS> upstream switches between USER-NAME and AUTH-TOKEN based on
// IDP_PROVIDERS. snapshot-server sends both unconditionally — whichever the
// avni-server instance honours (USER-NAME for IdpType=none, AUTH-TOKEN for
// IdpType=cognito) is what authenticates the request.
const _addAuthIfRequired = (request, bypassAuth) => {
    if (bypassAuth) return request;
    const username = getActiveUsername();
    const authToken = getAuthToken();
    const headers = {'USER-NAME': username};
    if (authToken) headers['AUTH-TOKEN'] = authToken;
    return _.merge({}, request, {headers});
};
// </SS>

const _get = (endpoint, bypassAuth) => {
    General.logDebug('Requests', `GET: ${endpoint}`);
    return fetchFactory(endpoint, "GET", _addAuthIfRequired(makeHeader("json"), bypassAuth))
        .then((response) => response.json());
};

const _getText = (endpoint, bypassAuth, fetchWithoutTimeout) => {
    General.logDebug('Requests', `Calling getText: ${endpoint}`);
    return fetchFactory(endpoint, "GET", _addAuthIfRequired(makeHeader("text"), bypassAuth), fetchWithoutTimeout)
        .then((response) => response.text());
};

const _post = (endpoint, file, fetchWithoutTimeout, bypassAuth = false) => {
    General.logDebug('Requests', `POST: ${endpoint}`);
    return fetchFactory(endpoint, "POST",
        _addAuthIfRequired(makeRequest("json", {body: JSON.stringify(file)}), bypassAuth),
        fetchWithoutTimeout);
};

const _put = (endpoint, body, fetchWithoutTimeout, bypassAuth = false) => {
    General.logDebug('Requests', `PUT: ${endpoint}`);
    return fetchFactory(endpoint, "PUT",
        _addAuthIfRequired(makeRequest("json", {body: JSON.stringify(body)}), bypassAuth),
        fetchWithoutTimeout);
};

export const post = _post;

export const get = (endpoint, bypassAuth = false, fetchWithoutTimeout = true) => {
    return _getText(endpoint, bypassAuth, fetchWithoutTimeout);
};

export const getJSON = (endpoint, bypassAuth = false) => {
    return _get(endpoint, bypassAuth);
};

export const putJSON = (endpoint, body, fetchWithoutTimeout = false, bypassAuth = false) => {
    return _put(endpoint, body, fetchWithoutTimeout, bypassAuth);
};
