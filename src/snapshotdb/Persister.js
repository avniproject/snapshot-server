import EntityHydrator from './EntityHydrator.js';
import General from './General.js';
import {getTableMetaMap, getRealmSchemaMap} from './schemaCache.js';

/**
 * Snapshot-server's write-only persister. Mirrors the bulkCreate contract from
 * SqliteProxy.js — flattens entities via EntityHydrator and upserts them into
 * the snapshot DB. Read-side machinery (objects/query/results-proxy) is not
 * vendored because snapshot generation never reads back what it just wrote.
 *
 * Adapter difference from on-device: op-sqlite's executeBatch (one native
 * call for many statements) becomes better-sqlite3's prepared statement +
 * synchronous transaction, which is the closest equivalent on Node.
 *
 * Schemas (tableMetaMap, realmSchemaMap) are memoised in schemaCache.js and
 * reused across every Persister instance — they don't change within a process.
 */
export class Persister {
    constructor(db, {tableMetaMap, realmSchemaMap, slowQueryThresholdMs = 100} = {}) {
        this.db = db;
        this.tableMetaMap = tableMetaMap ?? getTableMetaMap();
        this.slowQueryThresholdMs = slowQueryThresholdMs;
        this.hydrator = new EntityHydrator(this.tableMetaMap, realmSchemaMap ?? getRealmSchemaMap(), () => [], {});
    }

    /**
     * Build a reusable UPSERT SQL template for a schema. Same SQL for all
     * entities; copied verbatim from SqliteProxy._buildUpsertTemplate.
     */
    _buildUpsertTemplate(schemaName) {
        const tableMeta = this.tableMetaMap.get(schemaName);
        if (!tableMeta) throw new Error(`No table metadata for schema "${schemaName}"`);

        const columnNames = tableMeta.getColumnNames();
        const colList = columnNames.map(c => `"${c}"`).join(', ');
        const placeholders = columnNames.map(() => '?').join(', ');
        const pk = tableMeta.primaryKey || 'uuid';
        const updateCols = columnNames
            .filter(c => c !== pk)
            .map(c => `"${c}" = COALESCE(excluded."${c}", "${c}")`)
            .join(', ');

        const sql = `INSERT INTO ${tableMeta.tableName} (${colList}) VALUES (${placeholders})` +
            ` ON CONFLICT("${pk}") DO UPDATE SET ${updateCols}`;

        return {sql, columnNames};
    }

    _extractParams(flatRow, columnNames) {
        return columnNames.map(col => Object.prototype.hasOwnProperty.call(flatRow, col) ? flatRow[col] : null);
    }

    /**
     * Upsert a batch of entities for a schema in a single transaction.
     */
    bulkCreate(schemaName, entities) {
        if (!entities || entities.length === 0) return;

        const {sql, columnNames} = this._buildUpsertTemplate(schemaName);
        const stmt = this.db.prepare(sql);

        const start = Date.now();
        const tx = this.db.transaction((rows) => {
            for (const entity of rows) {
                const rawObject = (entity && entity.that) ? entity.that : entity;
                const flatRow = this.hydrator.flatten(schemaName, {that: rawObject});
                stmt.run(this._extractParams(flatRow, columnNames));
            }
        });
        tx(entities);
        const elapsed = Date.now() - start;

        if (elapsed > this.slowQueryThresholdMs) {
            General.logWarn('Persister', `bulkCreate ${schemaName}: ${entities.length} entities in ${elapsed}ms (${(elapsed / entities.length).toFixed(1)}ms/entity)`);
        }
    }
}
