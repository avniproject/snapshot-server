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

    /**
     * Resume support. A fresh process has an empty _rows map; without this,
     * the first `get()` call on resume would create a fresh REALLY_OLD_DATE
     * row with a new random UUID, and avni-server's /v2/syncDetails would
     * echo that new UUID back (it passes the client's contracts through —
     * see SyncController.getChangedEntities). saveOrUpdate would then INSERT
     * a duplicate row beside the original, leaving an orphan in the snapshot
     * DB. Loading every existing row first — including those still at
     * REALLY_OLD_DATE because they were seeded by updateAsPerSyncDetails but
     * never pulled — preserves their UUIDs so subsequent upserts hit in
     * place. Returns the number of rows rehydrated.
     */
    rehydrateFromDb(db) {
        const rows = db.prepare('SELECT uuid, entity_name, loaded_since, entity_type_uuid FROM entity_sync_status').all();
        for (const r of rows) {
            const loadedSince = r.loaded_since != null ? new Date(r.loaded_since) : EntitySyncStatus.REALLY_OLD_DATE;
            const row = EntitySyncStatus.create(r.entity_name, loadedSince, r.uuid, r.entity_type_uuid ?? '');
            this._rows.set(this._key(row.entityName, row.entityTypeUuid), row);
        }
        return rows.length;
    }
}
