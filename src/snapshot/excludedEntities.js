/**
 * Single source of truth for entity types snapshot-server must NOT pull into
 * a snapshot (card #1942, sub-issue 1).
 *
 * Only the entries in SNAPSHOT_EXCLUDED_ENTITIES are actively filtered — they
 * are entities that EntityMetaData would otherwise pull (syncPullRequired:
 * true) but that we deliberately keep out. Everything else worth excluding is
 * already handled upstream and is documented here only so the full rationale
 * lives in one place.
 *
 * Already excluded by EntityMetaData.getEntitiesToBePulled()
 * (= _.filter(model(), e => e.syncPullRequired)) — no active filtering needed:
 *   SyncTelemetry         - upload-only (syncPullRequired: false)
 *   VideoTelemetric       - upload-only
 *   RuleFailureTelemetry  - upload-only (syncPullRequired: false as of
 *                           openchs-models 1.33.62; pulled before that due to
 *                           a `syncPullFRequired` typo in the model)
 *   EntityApprovalStatus  - upload-only client-side approval queue (base
 *                           type). Its per-type subtypes
 *                           (SubjectEntityApprovalStatus, …) ARE pulled and
 *                           are intentionally kept — they carry real data.
 *
 * Never reach snapshot-server (not in EntityMetaData.model()) — documented,
 * not filtered:
 *   DraftSubject, DraftEncounter, DraftEnrolment, DraftProgramEncounter,
 *   EntityQueue, MediaQueue, Settings, LocaleMapping  - device-local state
 *
 * Intentionally KEPT in the snapshot (do NOT add these):
 *   MyGroups, UserSubjectAssignment - the Realm fast-sync flow deletes these
 *     post-restore only because its backup is catchment-level (multi-user).
 *     The SQLite snapshot is user-scoped, so they're already correct for the
 *     one user; excluding would force a needless re-pull.
 *   UserInfo - required for the device's identity check (BackupRestoreSqliteService).
 *   News
 */
export const SNAPSHOT_EXCLUDED_ENTITIES = new Set([
    'IdentifierAssignment',
]);

export function isExcludedFromSnapshot(entityName) {
    return SNAPSHOT_EXCLUDED_ENTITIES.has(entityName);
}
