import {test} from 'node:test';
import assert from 'node:assert/strict';
import {EntityMetaData} from 'openchs-models';
import {SNAPSHOT_EXCLUDED_ENTITIES, isExcludedFromSnapshot} from '../src/snapshot/excludedEntities.js';

// The filter is deterministic — it depends only on EntityMetaData + the
// constant, not on any user's data. So verifying it once here covers every
// snapshot; there's no need to assert per generated snapshot at runtime.
const pullSetNames = () =>
    EntityMetaData.model()
        .filter(e => !isExcludedFromSnapshot(e.entityName))
        .map(e => e.entityName);

test('IdentifierAssignment is the active exclusion (read-replica blocker)', () => {
    assert.ok(SNAPSHOT_EXCLUDED_ENTITIES.has('IdentifierAssignment'));
    assert.ok(isExcludedFromSnapshot('IdentifierAssignment'));
});

test('excluded entities are dropped from the pull set', () => {
    const names = pullSetNames();
    for (const excluded of SNAPSHOT_EXCLUDED_ENTITIES) {
        assert.ok(!names.includes(excluded), `${excluded} should be filtered out of the pull set`);
    }
});

test('entities we intentionally keep stay in the pull set', () => {
    const names = pullSetNames();
    // UserInfo is required for the device identity check; MyGroups /
    // UserSubjectAssignment are user-scoped and already correct in the snapshot.
    for (const kept of ['UserInfo', 'MyGroups', 'UserSubjectAssignment', 'News']) {
        assert.ok(names.includes(kept), `${kept} should remain in the pull set`);
    }
});

test('syncPullRequired:false entities are excluded by the model, not by our constant', () => {
    // These must NOT be in our constant — getEntitiesToBePulled() already drops
    // them. (RuleFailureTelemetry only became false in openchs-models 1.33.62.)
    for (const upstreamHandled of ['SyncTelemetry', 'VideoTelemetric', 'RuleFailureTelemetry', 'EntityApprovalStatus']) {
        assert.ok(!SNAPSHOT_EXCLUDED_ENTITIES.has(upstreamHandled),
            `${upstreamHandled} should not be in SNAPSHOT_EXCLUDED_ENTITIES — it's handled by getEntitiesToBePulled()`);
        const md = EntityMetaData.model().find(e => e.entityName === upstreamHandled);
        if (md) assert.equal(md.syncPullRequired, false, `${upstreamHandled} should be syncPullRequired:false`);
    }
});
