import {EntityMetaData} from 'openchs-models';

import EntityHydrator from '../snapshotdb/EntityHydrator.js';
import {getTableMetaMap, getRealmSchemaMap} from '../snapshotdb/schemaCache.js';
import {logger} from '../util/logger.js';

/**
 * Minimal EntityService for openchs-models' fromResource.
 *
 * Most callers of findByKey only consume `.uuid` — for them a {uuid: value}
 * stub is enough. But some (e.g., General.assignObsFields → concept.isQuestionGroup)
 * call methods on the returned entity, so we need real instances.
 *
 * Strategy:
 *   1. register() — entities just persisted via _persistAllBatch land in the
 *      in-memory cache so subsequent references within the same run resolve
 *      to a real instance (this covers the fresh sync path).
 *   2. read-through — on cache miss, if a snapshot DB is wired in, hydrate
 *      the row at shallow depth and return a proper openchs-models instance.
 *      This is what makes resume work: entities written in a previous attempt
 *      are still in the snapshot DB but not in this process's _cache; the
 *      read-through fetches and caches them on demand.
 *   3. {uuid} fallback — if the schema isn't in the table-meta map (e.g.,
 *      reference data not persisted yet) or no DB is wired, return a stub.
 *      Downstream code that only reads .uuid keeps working.
 */
export class EntityServiceStub {
    constructor({db = null} = {}) {
        this._cache = new Map(); // schemaName → Map<uuid, entityInstance>
        this._db = db;
        this._hydrator = null;
        this._entityClassCache = new Map();
    }

    register(schemaName, entities) {
        if (!entities || entities.length === 0) return;
        let bucket = this._cache.get(schemaName);
        if (!bucket) {
            bucket = new Map();
            this._cache.set(schemaName, bucket);
        }
        for (const e of entities) {
            const uuid = e?.uuid ?? e?.that?.uuid;
            if (uuid) bucket.set(uuid, e);
        }
    }

    findByKey(key, value, schemaName) {
        if (key !== 'uuid' || !value) return null;
        const cached = schemaName && this._cache.get(schemaName)?.get(value);
        if (cached) return cached;

        if (this._db && schemaName) {
            const instance = this._lookupInDb(schemaName, value);
            if (instance) {
                this.register(schemaName, [instance]);
                return instance;
            }
        }

        return {uuid: value};
    }

    /** Mirrors BaseService.findByUUID — delegates to findByKey('uuid', ...). */
    findByUUID(uuid, schemaName) {
        return this.findByKey('uuid', uuid, schemaName);
    }

    findOnly() { return null; }

    findAll() { return []; }

    _lookupInDb(schemaName, uuid) {
        try {
            const tableMeta = getTableMetaMap().get(schemaName);
            if (!tableMeta) return null;

            const row = this._db
                .prepare(`SELECT * FROM ${tableMeta.tableName} WHERE uuid = ?`)
                .get(uuid);
            if (!row) return null;

            const hydrator = this._ensureHydrator();
            if (!hydrator) return null;

            // Shallow hydration: scalars + embedded JSON + {uuid} stubs for
            // FK refs, lists empty. Sufficient for the documented motivating
            // case (Concept.isQuestionGroup reads .datatype) and keeps the
            // miss-path cheap (no recursive child queries).
            const hydrated = hydrator.hydrate(schemaName, row, {depth: 0, skipLists: true});
            const EntityClass = this._getEntityClass(schemaName);
            if (!EntityClass) return null;

            return new EntityClass(hydrated);
        } catch (e) {
            logger.warn(
                {schemaName, uuid, err: e.message},
                'EntityServiceStub read-through hydration failed; falling back to {uuid} stub'
            );
            return null;
        }
    }

    _ensureHydrator() {
        if (this._hydrator) return this._hydrator;
        if (!this._db) return null;
        const executeQuery = (sql, params = []) => this._db.prepare(sql).all(...params);
        this._hydrator = new EntityHydrator(getTableMetaMap(), getRealmSchemaMap(), executeQuery, {});
        return this._hydrator;
    }

    _getEntityClass(schemaName) {
        if (this._entityClassCache.has(schemaName)) return this._entityClassCache.get(schemaName);
        const md = EntityMetaData.model().find(e => e.entityName === schemaName);
        const cls = md?.entityClass ?? null;
        this._entityClassCache.set(schemaName, cls);
        return cls;
    }
}
