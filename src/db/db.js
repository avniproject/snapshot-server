import Database from 'better-sqlite3';
import fs from 'node:fs';
import path from 'node:path';
import {fileURLToPath} from 'node:url';
import {config} from '../config.js';

let _db;

export function getStateDb() {
    if (_db) return _db;
    const dir = path.dirname(config.stateDbPath);
    fs.mkdirSync(dir, {recursive: true});
    _db = new Database(config.stateDbPath);
    _db.pragma('journal_mode = WAL');
    _db.pragma('foreign_keys = ON');
    return _db;
}

export function initStateDb() {
    const db = getStateDb();
    const schemaPath = fileURLToPath(new URL('./schema.sql', import.meta.url));
    db.exec(fs.readFileSync(schemaPath, 'utf-8'));
    return db;
}

export function closeStateDb() {
    if (_db) {
        _db.close();
        _db = undefined;
    }
}
