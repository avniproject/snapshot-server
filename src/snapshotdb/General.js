/**
 * Tiny shim mirroring avni-client's `General` log API by delegating to
 * openchs-models' `ModelGeneral`. Lets us vendor avni-client's db modules
 * (EntityHydrator, SqliteProxy bits) byte-for-byte — they import this as
 * `./General` instead of `../../utility/General`.
 */
import {ModelGeneral} from 'openchs-models';

class General {
    static logInfo(source, ...messages)  { ModelGeneral.log('info',  source, ...messages); }
    static logDebug(source, ...messages) { ModelGeneral.log('debug', source, ...messages); }
    static logWarn(source, ...messages)  { ModelGeneral.log('warn',  source, ...messages); }
    static logError(source, error)       { ModelGeneral.log('error', source, error?.message ?? error); }
}

export default General;
