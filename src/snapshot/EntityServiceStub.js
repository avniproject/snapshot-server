/**
 * Minimal EntityService for openchs-models' fromResource.
 *
 * Most callers of findByKey only consume `.uuid` — for them a {uuid: value}
 * stub is enough. But some (e.g., General.assignObsFields → concept.isQuestionGroup)
 * call methods on the returned entity, so we need real instances.
 *
 * Strategy: register entity instances as they get persisted (Concepts,
 * SubjectTypes, etc. land before their references are hydrated thanks to
 * SyncService's "reference data first" ordering). findByKey checks the
 * registered cache first, falls back to {uuid} for everything else.
 *
 * This mirrors what on-device shallow-hydration mode does — return a Concept
 * with at least its `datatype` populated so isQuestionGroup() works — but
 * sourced from the in-flight sync rather than from a DB read.
 */
export class EntityServiceStub {
    constructor() {
        this._cache = new Map(); // schemaName → Map<uuid, entityInstance>
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
        return {uuid: value};
    }

    /** Mirrors BaseService.findByUUID — delegates to findByKey('uuid', ...). */
    findByUUID(uuid, schemaName) {
        return this.findByKey('uuid', uuid, schemaName);
    }

    findOnly() { return null; }

    findAll() { return []; }
}
