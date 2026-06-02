# snapshot-server

Server-side process that generates user-scoped SQLite fast-sync snapshots for the Avni Android app, by replaying the device's sync flow against avni-server.

## Setup

```bash
nvm use 20
npm install
cp .env.example .env
# Fill in AVNI_SERVER_URL and (for cognito-mode testing) AUTH_TOKEN
```

## Running a snapshot

Snapshot generation is scheduler-driven. To trigger runs for an org:

1. In the avni-webapp admin UI, flip **Enable SQLite snapshot generation** on the target organisation's settings (writes `OrganisationConfig.enableSqliteSnapshotGeneration = true`).
2. snapshot-server picks the org up on its next scheduler tick (default 1 hour, see `SCHEDULER_TICK_INTERVAL_MS`). Per tick it lists opted-in orgs, evaluates per-user freshness against the local state DB, and enqueues only stale users.
3. Workers FIFO-claim from the queue and upload each snapshot to S3.

There is no HTTP initiation endpoint — `POST /requests` was removed in favour of the scheduler. See `src/api/server.js` for the remaining ops endpoints (status, restart, cancel).

### Scheduler config

| Env | Default | Effect |
|---|---|---|
| `SCHEDULER_TICK_INTERVAL_MS` | `3600000` (1h) | How often the scheduler wakes. A long-running tick blocks the next firing (no overlap). |
| `FRESHNESS_THRESHOLD_HOURS` | `24` | A user's last successful snapshot is considered fresh for this long. After it expires the user is re-enqueued. |
| `SNAPSHOT_SERVER_COMMIT_SHA` | `dev` | Recorded on every snapshot. When the deployed SHA changes, all users go stale and are regenerated on the next tick. |

### QA flow

To smoke-test against a staging org:

1. Toggle the org's `enableSqliteSnapshotGeneration` from the webapp admin UI.
2. Watch snapshot-server logs (`make start`) for `tick: enqueued snapshot request` lines.
3. Inspect progress with `GET /requests/<dbUser>-<orgSeq>` or `make stats`.

To force regeneration of a user that's still within the freshness window, you can either bump `SNAPSHOT_SERVER_COMMIT_SHA` (forces every user stale) or update their `snapshot_user_job.finished_at` in the state DB to a past time outside the window.

## Design notes

### Why `createSnapshotRequestAndUserJobs` writes both tables in one transaction

The scheduler's tick calls `createSnapshotRequestAndUserJobs`, which inserts one `snapshot_request` row + N `snapshot_user_job` rows inside a single SQLite transaction. We considered splitting the write — request insert first, jobs streamed in after — but kept the synchronous design. Reasons:

1. **Atomicity.** The transaction guarantees: either the request and all its jobs commit together, or none does. Splitting it leaves orphan requests on any crash mid-write — visible to ops with `counts: {}` and no jobs to claim. We'd then need a startup sweeper to detect and repair that state.
2. **No worker race window.** Workers poll `WHERE state = 'queued' LIMIT 1`. If we inserted jobs after the request, a worker that wakes up between the two writes would see no work, idle for `idlePollMs`, and add latency for no win.
3. **Consistent observability.** `GET /requests/:key` returns a complete view as soon as the tick commits.
4. **Errors stay visible.** A FK / constraint failure surfaces synchronously inside the tick. With background writes, errors would only show in logs.

## Vendored modules

These files are copy-pasted from `avni-client` so snapshot-server's sync logic stays identical to the device's. Edits per file are kept minimal and well-defined:

| File | Origin in `avni-client` | Edits applied |
|---|---|---|
| `src/snapshotdb/drizzleMigrations.js` | `packages/openchs-android/src/framework/db/migrations/drizzleMigrations.js` | None |
| `src/snapshotdb/SqliteUtils.js` | `packages/openchs-android/src/framework/db/SqliteUtils.js` | None |
| `src/snapshotdb/SchemaGenerator.js` | `packages/openchs-android/src/framework/db/SchemaGenerator.js` | Relative imports get `.js` extensions (Node ESM) |
| `src/snapshotdb/EntityHydrator.js` | `packages/openchs-android/src/framework/db/EntityHydrator.js` | Relative imports get `.js` extensions; `General` import points to local shim |
| `src/rest/ConventionalRestClient.js` | `packages/openchs-android/src/service/rest/ConventionalRestClient.js` | Import paths updated. Constructor takes `(settingsService, privilegeService)`; `privilegeService` is dead in upstream too (assigned to `this`, never read), so we pass `null` at the call site in `SyncRunner`. |
| `src/rest/ChainedRequests.js` | `packages/openchs-android/src/framework/http/ChainedRequests.js` | Import paths updated |
| `src/rest/requests.js` | `packages/openchs-android/src/framework/http/requests.js` | RN-only blocks dropped (XSRF cookies, GlobalContext, IDP_PROVIDERS, AuthenticationError/ServerError); auth pulled from `requestContext` + env. Edits bracketed by `<SS>…</SS>` markers in the file. |

These files are committed to this repo and don't need to be regenerated on every build. If avni-client's schema or sync logic changes, manually copy the new versions over and re-apply the edits documented above.

## Stubs

Some avni-client classes are too deeply wired into the React Native app to vendor cleanly — they extend `BaseService`, depend on Realm transactions, AsyncStorage, the bean-injection framework, etc. For each, we provide a minimal implementation that satisfies *only* the surface the vendored code actually calls:

| File | Avni-client equivalent | Why a stub instead of a vendor |
|---|---|---|
| `src/snapshotdb/General.js` | `packages/openchs-android/src/utility/General.js` | 16-line shim mapping `logInfo / logDebug / logWarn / logError` to openchs-models' `ModelGeneral.log`. Vendoring the original would pull in `randomUUID`, JSON-stringify wrappers, stack-trace utilities, and a console-flag system the vendored db modules never call. |
| `src/snapshot/SettingsServiceStub.js` | `packages/openchs-android/src/service/SettingsService.js` | The vendored `ConventionalRestClient` only calls `getSettings()` for `serverURL` + `pageSize`. Vendoring the real `SettingsService` would transitively pull in `BaseService`, `AsyncStorage`, `RNRestart`, the bean-injection framework, and `initLanguages` — none of which apply server-side. |
| `src/snapshot/EntityServiceStub.js` | `packages/openchs-android/src/service/EntityService.js` (+ inherited `BaseService.js`) | openchs-models' `fromResource` calls `findByKey('uuid', …)` / `findByUUID(…)` to extract parent uuids for FK columns. We register entity instances as we persist them and return them on lookup (or `{uuid: value}` as a fallback). Vendoring `BaseService` would require shimming the read-side machinery (`SqliteResultsProxy`, `RealmQueryParser`, `JsFallbackFilterEvaluator`) — ~2000 more lines for a method we'd hand-write anyway. |

Rule used to decide vendor vs. stub: **does the file's RN-only dependency tree exceed the actually-useful logic?** If no → vendor verbatim with import shims (the table above). If yes → stub the interface, not the implementation (this table).

## Snapshot-server modules

Files written for snapshot-server itself. Some adapt vendored code to a non-RN environment; some port avni-client logic with snapshot-server-specific storage; one is a runner with no direct upstream equivalent.

| File | Avni-client equivalent | What it does |
|---|---|---|
| `src/snapshotdb/SqliteFacade.js` | `SqliteProxy.js` (op-sqlite wrapper) | Duck-types the slice of `SqliteProxy` that the vendored `SyncRunner` calls — `isSqlite`, `bulkCreate`, `pragma`. Lets the copied `SyncService` method bodies invoke `this.db.bulkCreate(...)` without knowing the engine underneath is `better-sqlite3` instead of op-sqlite. |
| `src/snapshotdb/schemaCache.js` | (no direct equivalent) | Process-wide memoised cache of `tableMetaMap` + `realmSchemaMap` derived from openchs-models. Built lazily on first call to `Persister`; reused across every snapshot job in the process. |
| `src/snapshotdb/Persister.js` | extracted from `SqliteProxy.bulkCreate` + `_buildUpsertTemplate` + `_extractParams` | Write-only persister: flattens entities via `EntityHydrator`, builds the `INSERT … ON CONFLICT DO UPDATE` template once per schema, runs all rows in a single `better-sqlite3` transaction. Swaps op-sqlite's `executeBatch` (one native call for many statements) for prepared-statement reuse inside a sync transaction — the closest equivalent under Node. |
| `src/snapshot/EntitySyncStatusService.js` | avni-client's `EntitySyncStatusService` (Realm/SQLite-backed) | Mirrors the device class's contract (`get`, `saveOrUpdate`, `updateAsPerSyncDetails`, `findAll`, `removeRevokedPrivileges`) but holds rows in memory *and* writes them through to the snapshot DB so a device restoring our snapshot can resume sync from the right `loaded_since` cursor. |
| `src/snapshot/SyncRunner.js` | `SyncService.dataServerSync` + `persistAll` + `_persistAllBatch` + `getRefData` / `getTxData` / `getData` (port) | Port of the device's pull-only sync flow. Method bodies are near-verbatim copies; the branches that don't apply server-side are stripped (push, telemetry POST, media/news/extension/icon downloads, mid-sync backend switch, post-sync reset, encryption, shallow-hydration toggle). Inline comments call out each strip. |
| `src/snapshot/SnapshotJob.js` | (no direct equivalent) | One-user runner. For each claimed `snapshot_user_job`: builds the output path, opens a fresh per-user SQLite file (with bundled migrations applied), instantiates `Persister` + `SyncRunner`, awaits `runner.run(username)`, closes the DB. The thing a `Worker` calls per loop iteration. |

## File naming

| File exports … | File name | Examples |
|---|---|---|
| A class as the primary thing | **PascalCase** matching the class | `Persister.js` → `class Persister`, `SqliteFacade.js` → `class SqliteFacade`, `SnapshotJob.js` → `class SnapshotJob` |
| Functions / utilities (no owning class) | **camelCase** | `schemaCache.js` → `getTableMetaMap`, `getRealmSchemaMap`; `requestContext.js` → `requestContext`, `currentUsername` |
| Generic single-purpose module | **lowercase** | `db.js`, `logger.js`, `config.js`, `requests.js`, `server.js`, `index.js` |

Vendored files keep their upstream names even when they don't fit the rule — e.g., `SqliteUtils.js` exports plain functions but stays PascalCase to match `avni-client`'s file name, so a `cp` refresh stays clean. Vendor consistency wins over local consistency for those.

## snapshot-server DB

snapshot-server tracks request metadata (queue, history, status) in `snapshot-db/snapshot-server.db`. Inspect with:

```bash
sqlite3 snapshot-db/snapshot-server.db
.tables
SELECT * FROM snapshot_request ORDER BY id DESC LIMIT 5;
SELECT * FROM snapshot_user_job ORDER BY id DESC LIMIT 10;
```

## Snapshot output

**Stable paths, one snapshot per user.** Both local and S3 use a stable filename — `snapshot.db` — so at most one file per user exists at any time:

- **Local**: `snapshots/<dbUser>/<username>/snapshot.db`. Successful runs delete the file post-upload; failed runs leave it on disk for the next attempt to resume (gated by the resume_cursor — no cursor means we wipe the orphan and start fresh).
- **S3**: `s3://<bucket>/<mediaDirectory>/snapshots/<username>/snapshot.db`. Each new generation overwrites the same key; history is preserved by S3 versioning. avni-server's `/media/mobileDatabaseSqliteSnapshotUrl/download` signs that key directly — no listing, no sorting.

Each upload sets S3 object metadata used by the scheduler's freshness check:
- `x-amz-meta-generated-at` — ISO timestamp of upload
- `x-amz-meta-snapshot-server-sha` — `SNAPSHOT_SERVER_COMMIT_SHA` env var (falls back to `dev`)
- `x-amz-meta-client-schema-version` — max drizzle migration index bundled at build time

Inspect a local file (the upload deletes it post-success — present only mid-run or for a failed user):
```bash
sqlite3 snapshots/apfodisha/omzz@apfodisha/snapshot.db
.tables
SELECT count(*) FROM individual;
SELECT entity_name, entity_type_uuid, datetime(loaded_since/1000, 'unixepoch')
  FROM entity_sync_status WHERE loaded_since > 0;
```

### Cleaning up legacy timestamped S3 keys

If the bucket still holds old `<isoTimestamp>.db` objects from before the switch to a stable key, run the one-shot cleanup script per organisation:

```bash
# preview
npm run cleanup-legacy-snapshots -- --media-directory apfodisha
# actually delete
npm run cleanup-legacy-snapshots -- --media-directory apfodisha --apply
```

It deletes everything under `<mediaDirectory>/snapshots/<username>/` whose basename isn't `snapshot.db`. Defaults to dry-run.

## Excluded entities

`src/snapshot/excludedEntities.js` is the single source of truth for entity types kept out of a snapshot. Today the only actively-filtered entity is `IdentifierAssignment` (pulling it makes avni-server `UPDATE` the identifier pool, which fails on a read-only replica). The file also documents the entities already handled upstream — `syncPullRequired: false` ones (telemetry, the `EntityApprovalStatus` base) dropped by `EntityMetaData.getEntitiesToBePulled()`, and device-local ones not in `EntityMetaData.model()` at all — plus the ones intentionally kept (`MyGroups`, `UserSubjectAssignment`, `UserInfo`, `News`).

**When avni-client's `EntityMetaData` gains a new synced type, evaluate it against this list before the next rollout wave** — decide whether it's user/device-private, telemetry, device-local, or genuine shared data, and add it to `SNAPSHOT_EXCLUDED_ENTITIES` (or leave it) accordingly.

`npm test` covers the filter logic (`test/excludedEntities.test.js`).

## Auth modes

`avni-server` reads either `USER-NAME` (when `AVNI_IDP_TYPE=none`) or validates `AUTH-TOKEN` (when `AVNI_IDP_TYPE=cognito`). snapshot-server always sends both headers so the same client code works against either deployment.

- **Production**: dedicated avni-server instance with `AVNI_IDP_TYPE=none`, network-isolated. snapshot-server sets `USER-NAME: <target>` per request; `AUTH_TOKEN` env var is unset.
- **Functional testing**: against staging (`AVNI_IDP_TYPE=cognito`). Paste a real user's JWT into `AUTH_TOKEN`; every request runs as that JWT's owner regardless of `USER-NAME`.
