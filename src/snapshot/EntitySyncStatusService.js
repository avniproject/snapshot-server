import {EntitySyncStatus} from 'openchs-models';

/**
 * Snapshot-server's stand-in for avni-client's EntitySyncStatusService.
 *
 * On the device this class is backed by Realm/SQLite and the rows are read
 * back during the next sync. For snapshot generation we keep the rows in
 * memory and also persist each one to the snapshot DB so the device, after
 * restoring our snapshot, can resume sync from the right loadedSince
 * instead of re-pulling everything from REALLY_OLD_DATE.
 *
 * Methods named after the device equivalents so the copied SyncService code
 * doesn't need to be rewritten.
 */
export class EntitySyncStatusService {
    constructor(persister) {
        this.persister = persister;
        this._rows = new Map(); // key = `${entityName}|${entityTypeUuid}`
    }

    _key(entityName, entityTypeUuid) {
        return `${entityName}|${entityTypeUuid ?? ''}`;
    }

    /**
     * Used by SyncService._persistAllBatch to fetch the existing row's uuid
     * so the new row upserts in place rather than creating a duplicate.
     * Mirrors device behaviour where setup() pre-seeded a row with a uuid.
     */
    get(entityName, entityTypeUuid = '') {
        const key = this._key(entityName, entityTypeUuid);
        let row = this._rows.get(key);
        if (!row) {
            row = EntitySyncStatus.create(
                entityName,
                EntitySyncStatus.REALLY_OLD_DATE,
                globalThis.crypto.randomUUID(),
                entityTypeUuid ?? ''
            );
            this._rows.set(key, row);
        }
        return row;
    }

    /**
     * Persist one row to the snapshot DB and update the in-memory copy.
     * SyncService._persistAllBatch calls bulkSaveOrUpdate(...) on the device;
     * SyncRunner replaces that single line with a direct call here.
     */
    saveOrUpdate(entitySyncStatus) {
        const key = this._key(entitySyncStatus.entityName, entitySyncStatus.entityTypeUuid);
        this._rows.set(key, entitySyncStatus);
        this.persister.bulkCreate('EntitySyncStatus', [entitySyncStatus]);
    }

    /**
     * Mirrors device updateAsPerSyncDetails — install rows for each
     * (entityName, entityTypeUuid) returned by /v2/syncDetails. We persist
     * them so the snapshot has a complete EntitySyncStatus index even for
     * entity types that ended up with zero pages of data this run.
     */
    updateAsPerSyncDetails(entitySyncStatuses) {
        for (const sd of entitySyncStatuses) {
            const row = EntitySyncStatus.create(
                sd.entityName,
                sd.loadedSince ?? EntitySyncStatus.REALLY_OLD_DATE,
                sd.uuid ?? globalThis.crypto.randomUUID(),
                sd.entityTypeUuid ?? ''
            );
            this.saveOrUpdate(row);
        }
    }

    /**
     * Privilege filtering happens server-side. Returning syncDetails as-is
     * matches the contract SyncService.updateAsPerNewPrivilege expects.
     */
    removeRevokedPrivileges(_allEntitiesMetaData, syncDetails) {
        return syncDetails;
    }

    /**
     * SyncService.getSyncDetails reads this to build the POST body for
     * /v2/syncDetails. For a fresh snapshot run the server is happy with an
     * empty list — but to mirror the device, we seed one row per "to be
     * pulled" entity at REALLY_OLD_DATE.
     */
    findAll() {
        return Array.from(this._rows.values());
    }
}
