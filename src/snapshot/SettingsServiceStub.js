import {config} from '../config.js';

/**
 * Tiny stub of avni-client's SettingsService — only getSettings() is read by
 * the vendored ConventionalRestClient (for serverURL + pageSize). Reads from
 * the process config so the call site stays a no-arg constructor.
 */
export class SettingsServiceStub {
    getSettings() {
        return {serverURL: config.avniServerUrl, pageSize: config.pageSize};
    }
}
