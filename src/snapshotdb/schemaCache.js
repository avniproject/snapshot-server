import {EntityMappingConfig} from 'openchs-models';
import SchemaGenerator from './SchemaGenerator.js';

// Process-wide memoised schemas derived from openchs-models. Built lazily on
// first access; reused thereafter.

let _tableMetaMap;
let _realmSchemaMap;

function _build() {
    if (_tableMetaMap) return;
    const cfg = EntityMappingConfig.getInstance();
    _tableMetaMap = SchemaGenerator.generateAll(cfg);
    _realmSchemaMap = SchemaGenerator.buildRealmSchemaMap(cfg);
}

export function getTableMetaMap() {
    _build();
    return _tableMetaMap;
}

export function getRealmSchemaMap() {
    _build();
    return _realmSchemaMap;
}
