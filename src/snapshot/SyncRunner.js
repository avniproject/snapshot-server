import _ from 'lodash';
import {EntityMetaData, EntitySyncStatus, IgnorableSyncError} from 'openchs-models';

import ConventionalRestClient from '../rest/ConventionalRestClient.js';
import {post} from '../rest/requests.js';
import General from '../snapshotdb/General.js';
import {SqliteFacade} from '../snapshotdb/SqliteFacade.js';
import {SettingsServiceStub} from './SettingsServiceStub.js';
import {EntityServiceStub} from './EntityServiceStub.js';
import {EntitySyncStatusService} from './EntitySyncStatusService.js';
import {logger} from '../util/logger.js';
import {config} from '../config.js';

/**
 * Server-side port of avni-client's SyncService — pull path only.
 *
 * Method bodies are copied as close to verbatim as possible from
 * packages/openchs-android/src/service/SyncService.js so this stays a
 * mechanical port rather than a parallel reimplementation. Edits are
 * limited to:
 *
 *   1. Stripping branches that don't apply on the server: media sync,
 *      telemetry POST, news/extension/icon downloads, mid-sync backend
 *      switch, push, redux dispatch, post-sync reset, encryption,
 *      reference-cache build, shallow-hydration toggle, name-translated
 *      message-service writes, TaskUnAssignment / UserSubjectAssignment
 *      deletes (DB starts empty so there's nothing to delete).
 *   2. Replacing `this.context.getRepositoryFactory().*` and the
 *      bulkSaveOrUpdate / getCreateEntityFunctions BaseService helpers
 *      with direct calls — see inline comments where these appear.
 *
 * Adapter dependencies (constructed in the constructor):
 *   - this.db                       → SqliteFacade (isSqlite/bulkCreate/pragma)
 *   - this.entityService            → EntityServiceStub (cache + {uuid} fallback)
 *   - this.entitySyncStatusService  → in-memory + persists rows
 *   - this.conventionalRestClient   → vendored ConventionalRestClient (fetch via requests.js)
 *
 * Auth comes from requestContext (AsyncLocalStorage); the CLI wraps the
 * top-level run() in requestContext.run({username}, ...).
 */

// Copied verbatim from SyncService.js (top-level helper used by persistAll).
function transformResourceToEntity(entityMetaData, entityResources) {
    return (acc, resource) => {
        try {
            return acc.concat([entityMetaData.entityClass.fromResource(resource, this.entityService, entityResources)]);
        } catch (error) {
            if (error instanceof IgnorableSyncError) {
                resource.excludeFromPersist = true;
                General.logError("SyncService", error);
            } else {
                throw error;
            }
        }
        return acc; // since error is IgnorableSyncError, return accumulator as is
    }
}

export class SyncRunner {
    constructor({db, persister}) {
        this.deviceId = 'snapshot-server';

        // Adapter wiring — replaces SyncService.init() and BaseService deps.
        this.db = new SqliteFacade(db, persister);
        this.entityService = new EntityServiceStub();
        this.entitySyncStatusService = new EntitySyncStatusService(persister);
        // ConventionalRestClient takes (settingsService, privilegeService) but
        // never reads privilegeService — it's a dead param in upstream too.
        // Passing null matches the contract without wiring up an unused stub.
        this.conventionalRestClient = new ConventionalRestClient(new SettingsServiceStub(), null);

        // Match SyncService.constructor — persistAll is called as a passed
        // function in conventionalRestClient.getAll, so it must be bound.
        this.persistAll = this.persistAll.bind(this);
    }

    /**
     * Top-level entry point — replaces the on-device SyncService.sync(...)
     * which handles the full UX (progress, lock, telemetry). For snapshot
     * generation we just seed entitySyncStatuses, then run dataServerSync.
     * Username comes from the surrounding requestContext.run({username}, ...).
     */
    async run() {
        const allEntitiesMetaData = EntityMetaData.model();

        // Seed entitySyncStatusService the same way EntitySyncStatusService.setup
        // does on first install (one row per to-be-pulled entity, REALLY_OLD_DATE,
        // blank entityTypeUuid). The snapshot DB will then carry these rows so
        // the device can resume sync after restoring.
        this._seedEntitySyncStatuses();

        // No-op callbacks for the parameters dataServerSync uses for UI
        // progress reporting on the device.
        const noop = () => {};
        const noopReturning = () => undefined;

        return this.dataServerSync(
            allEntitiesMetaData,
            noop,           // statusMessageCallBack
            noop,           // onProgressPerEntity
            noop,           // onAfterMediaPush
            noop,           // updateProgressSteps
            false,          // isSyncResetRequired
            noopReturning,  // userConfirmation
            false,          // isOnlyUploadRequired
        );
    }

    _seedEntitySyncStatuses() {
        const pulled = EntityMetaData.getEntitiesToBePulled();
        for (const entity of pulled) {
            if (!_.isEmpty(entity.privilegeParam)) continue;
            // Triggers create-and-store of the seed row.
            this.entitySyncStatusService.get(entity.entityName, '');
        }
    }

    // ─────────── Methods copied from SyncService.js (nearly verbatim) ───────────

    getMetadataByType(entityMetadata, type) {
        return entityMetadata.filter((entityMetaData) => entityMetaData.type === type);
    }

    async getSyncDetails() {
        const url = config.avniServerUrl;
        const requestParams = `includeUserSubjectType=true&deviceId=${this.deviceId}`;
        const entitySyncStatuses = this.entitySyncStatusService.findAll().map(_.identity);
        return post(`${url}/v2/syncDetails?${requestParams}`, entitySyncStatuses, true)
            .then(res => res.json())
            .then(({syncDetails, nowMinus10Seconds, now}) => {
                // Replica-lag guard: override the server's 10-second buffer
                // with `now − replicaLagMinutes`. The server-side query param
                // for this lands later; until then we adjust client-side.
                const adjustedEnd = new Date(
                    new Date(now).getTime() - config.replicaLagMinutes * 60 * 1000
                ).toISOString();
                return {syncDetails, now, endDateTime: adjustedEnd, serverEndDateTime: nowMinus10Seconds};
            });
    }

    retainEntitiesPresentInCurrentVersion(syncDetails, allEntitiesMetaData) {
        const entityMetadataEntityNames = _.map(allEntitiesMetaData, 'entityName');
        return _.filter(syncDetails, (syncDetail) =>
            entityMetadataEntityNames.includes(syncDetail.entityName)
        )
    }

    /*
     * Copy of SyncService.dataServerSync's pull path. Stripped: push, media,
     * reset-sync prompt, mid-sync backend switch, encryption, subject-migration
     * deletes, asset downloads, post-sync reset, shallow-hydration toggle.
     * Flow preserved verbatim where it applies.
     */
    async dataServerSync(allEntitiesMetaData, statusMessageCallBack, onProgressPerEntity, onAfterMediaPush, updateProgressSteps, isSyncResetRequired, userConfirmation, isOnlyUploadRequired) {
        // Push path skipped — snapshot-server is a read-only generator.
        if (isOnlyUploadRequired) return;

        let {syncDetails, endDateTime, now} = await this.getSyncDetails();

        const entitiesWithoutSubjectMigrationAndResetSync = _.filter(allEntitiesMetaData, ({entityName}) => !_.includes(['ResetSync', 'SubjectMigration'], entityName));
        const filteredMetadata = _.filter(entitiesWithoutSubjectMigrationAndResetSync, ({entityName}) => _.find(syncDetails, sd => sd.entityName === entityName));
        const referenceEntityMetadata = this.getMetadataByType(filteredMetadata, "reference");
        const filteredTxData = this.getMetadataByType(filteredMetadata, "tx");
        const userInfoData = _.filter(filteredMetadata, ({entityName}) => entityName === "UserInfo");
        const subjectMigrationMetadata = _.filter(allEntitiesMetaData, ({entityName}) => entityName === "SubjectMigration");
        const currentVersionEntitySyncDetails = this.retainEntitiesPresentInCurrentVersion(syncDetails, allEntitiesMetaData);
        General.logDebug("SyncService", `Entities to sync ${_.map(currentVersionEntitySyncDetails, ({entityName, entityTypeUuid}) => [entityName, entityTypeUuid])}`);
        this.entitySyncStatusService.updateAsPerSyncDetails(currentVersionEntitySyncDetails);

        let syncDetailsWithPrivileges;
        let syncSucceeded = false;
        this._disableForeignKeysIfSqlite();
        // Shallow-hydration toggle is irrelevant on snapshot-server (we never
        // read back during sync — EntityServiceStub already returns lightweight
        // cached instances), so the matching toggle calls are dropped.
        return Promise.resolve(statusMessageCallBack("downloadForms"))
            .then(() => this.getTxData(userInfoData, onProgressPerEntity, syncDetails, endDateTime))
            .then(() => this.getRefData(referenceEntityMetadata, onProgressPerEntity, now, endDateTime))
            // Mid-sync backend switch / reference cache / encryption blocks dropped.
            .then(() => syncDetailsWithPrivileges = this.updateAsPerNewPrivilege(allEntitiesMetaData, updateProgressSteps, currentVersionEntitySyncDetails))
            .then(() => statusMessageCallBack("downloadNewDataFromServer"))
            .then(() => this.getTxData(subjectMigrationMetadata, onProgressPerEntity, syncDetailsWithPrivileges, endDateTime))
            // SubjectMigrationService.migrateSubjects dropped — we don't have a
            // local subject tree to delete from; the SubjectMigration rows
            // persisted above are enough for the device to act on at restore.
            .then(() => this.getTxData(filteredTxData, onProgressPerEntity, syncDetailsWithPrivileges, endDateTime))
            // News images / extensions / custom card HTML / icons / migration
            // finalisation all dropped — handled by the device after restore.
            .then(() => { syncSucceeded = true; })
            .finally(() => {
                this._enableForeignKeysIfSqlite();
                if (syncSucceeded) this._checkForeignKeyIntegrityIfSqlite();
            })
    }

    updateAsPerNewPrivilege(allEntitiesMetaData, updateProgressSteps, syncDetails) {
        let syncDetailsWithPrivileges = this.entitySyncStatusService.removeRevokedPrivileges(allEntitiesMetaData, syncDetails);
        updateProgressSteps(allEntitiesMetaData, syncDetails);
        return syncDetailsWithPrivileges;
    }

    getRefData(entitiesMetadata, afterEachPagePulled, now) {
        const entitiesMetaDataWithSyncStatus = entitiesMetadata
            .reverse()
            .map((entityMetadata) => _.assignIn({
                syncStatus: this.entitySyncStatusService.get(entityMetadata.entityName),
            }, entityMetadata));
        return this.getData(entitiesMetaDataWithSyncStatus, afterEachPagePulled, now);
    }

    getResetSyncData(entitiesMetadata, afterEachPagePulled) {
        const entitiesMetaDataWithSyncStatus = entitiesMetadata
            .map((entityMetadata) => _.assignIn({
                syncStatus: this.entitySyncStatusService.get(entityMetadata.entityName),
            }, entityMetadata));
        return this.getData(entitiesMetaDataWithSyncStatus, afterEachPagePulled, new Date().toISOString());
    }

    getTxData(entitiesMetadata, afterEachPagePulled, syncDetails, now) {
        const entitiesMetaDataWithSyncStatus = entitiesMetadata
            .reverse()
            .map((entityMetadata) => {
                const entitiesToSync = _.filter(syncDetails, ({entityName}) => entityMetadata.entityName === entityName);
                return _.reduce(entitiesToSync, (acc, m) => {
                    acc.push(_.assignIn({syncStatus: m}, entityMetadata));
                    return acc;
                }, [])
            }).flat(1);
        return this.getData(entitiesMetaDataWithSyncStatus, afterEachPagePulled, now);
    }

    getData(entitiesMetaDataWithSyncStatus, afterEachPagePulled, now) {
        // dispatchAction(RECORD_FIRST_PAGE_OF_PULL) dropped — telemetry isn't
        // posted by snapshot-server, but we keep the callback to log progress.
        const onGetOfFirstPage = (entityName, page) => {
            logger.debug({entity: entityName, totalElements: page?.totalElements}, 'first page');
        };

        return this.conventionalRestClient.getAll(entitiesMetaDataWithSyncStatus, this.persistAll, onGetOfFirstPage, afterEachPagePulled, now, this.deviceId);
    }

    async persistAll(entityMetaData, entityResources) {
        if (_.isEmpty(entityResources)) return;
        entityResources = _.sortBy(entityResources, 'lastModifiedDateTime');
        const loadedSince = _.last(entityResources).lastModifiedDateTime;

        const entities = entityResources.reduce(transformResourceToEntity.call(this, entityMetaData, entityResources), []);
        const initialLength = entityResources.length;
        entityResources = _.filter(entityResources, (resource) => !resource.excludeFromPersist);
        General.logDebug("SyncService", `Before filter entityResources length: ${initialLength}, after filter entityResources length: ${entityResources.length}, entities length  ${entities.length}`);

        // nameTranslated → messageService.addTranslation dropped (translations
        // regenerate on the device after restore).

        // TaskUnAssignment / UserSubjectAssignment delete branches dropped —
        // snapshot DB starts empty so there's nothing to delete.

        General.logDebug("SyncService", `Syncing - ${entityMetaData.entityName} with subType: ${entityMetaData.syncStatus.entityTypeUuid}`);

        if (this.db.isSqlite && typeof this.db.bulkCreate === 'function') {
            await this._persistAllBatch(entityMetaData, entityResources, entities, loadedSince);
        } else {
            // No Realm path on snapshot-server; SQLite is the only backend.
            throw new Error('snapshot-server expects this.db.isSqlite=true');
        }

        // Cache the just-persisted instances so subsequent fromResource calls
        // (observations referencing this Concept, etc.) get a real instance
        // back instead of the {uuid} fallback. Mirrors what on-device shallow
        // hydration provides via SqliteResultsProxy.
        this.entityService.register(entityMetaData.schemaName, entities);

        // dispatchAction(ENTITY_PULL_COMPLETED) dropped (no redux server-side).
        logger.info(
            {entity: entityMetaData.entityName, count: entities.length, entityTypeUuid: entityMetaData.syncStatus?.entityTypeUuid},
            'persisted page'
        );
    }

    async _persistAllBatch(entityMetaData, entityResources, entities, loadedSince) {
        await this.db.bulkCreate(entityMetaData.schemaName, entities);

        const currentEntitySyncStatus = this.entitySyncStatusService.get(entityMetaData.entityName, entityMetaData.syncStatus.entityTypeUuid);
        const entitySyncStatus = new EntitySyncStatus();
        entitySyncStatus.entityName = entityMetaData.entityName;
        entitySyncStatus.entityTypeUuid = entityMetaData.syncStatus.entityTypeUuid;
        entitySyncStatus.uuid = currentEntitySyncStatus.uuid;
        entitySyncStatus.loadedSince = new Date(loadedSince);
        // Device version: this.bulkSaveOrUpdate(this.getCreateEntityFunctions(EntitySyncStatus.schema.name, [entitySyncStatus]))
        // Snapshot-server: write directly through the EntitySyncStatusService,
        // which both updates its in-memory map and persists the row.
        this.entitySyncStatusService.saveOrUpdate(entitySyncStatus);
    }

    _disableForeignKeysIfSqlite() {
        // Device version: this.context.getRepositoryFactory().setForeignKeysEnabled(false)
        this.db.pragma('foreign_keys = OFF');
        General.logDebug("SyncService", "SQLite foreign keys disabled for sync");
    }

    _enableForeignKeysIfSqlite() {
        // Device version: this.context.getRepositoryFactory().setForeignKeysEnabled(true)
        this.db.pragma('foreign_keys = ON');
        General.logDebug("SyncService", "SQLite foreign keys re-enabled after sync");
    }

    _checkForeignKeyIntegrityIfSqlite() {
        try {
            // Device version: this.context.getRepositoryFactory().runForeignKeyCheck()
            const violations = this.db.pragma('foreign_key_check');
            if (violations && violations.length > 0) {
                const summary = violations.slice(0, 10).map(v =>
                    `${v.table}.rowid=${v.rowid}→${v.parent}`
                ).join(', ');
                const message = `${violations.length} FK violation(s) after sync: ${summary}`;
                General.logError("SyncService", message);
                // Device additionally calls ErrorUtil.notifyBugsnag — we just log.
            }
        } catch (e) {
            General.logWarn("SyncService", `FK integrity check failed: ${e.message}`);
        }
    }
}
