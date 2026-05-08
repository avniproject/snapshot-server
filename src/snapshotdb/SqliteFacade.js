/**
 * Duck-types the slice of avni-client's SqliteProxy that SyncService uses on
 * the device. By exposing the same surface (isSqlite, bulkCreate, pragma)
 * we can copy SyncService method bodies into snapshot-server's SyncRunner
 * verbatim — nothing in the copied code knows that this.db is actually a
 * thin shim over better-sqlite3 + Persister.
 */
export class SqliteFacade {
    constructor(db, persister) {
        this.db = db;
        this.persister = persister;
        this.isSqlite = true;
    }

    /**
     * Matches SqliteProxy.bulkCreate's signature/return type (Promise<void>).
     * Persister.bulkCreate is synchronous under better-sqlite3, but we keep
     * the async return so copied `await this.db.bulkCreate(...)` calls work
     * without any tweak.
     */
    async bulkCreate(schemaName, entities) {
        this.persister.bulkCreate(schemaName, entities);
    }

    pragma(stmt) {
        return this.db.pragma(stmt);
    }

    close() {
        this.db.close();
    }
}
