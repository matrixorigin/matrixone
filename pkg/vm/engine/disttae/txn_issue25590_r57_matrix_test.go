// Copyright 2026 Matrix One
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

// Round-57 unit matrix (issue #25590 review of PR #30988 head
// 9df36413c2): the production-path regression in
// txn_issue25590_r57_test.go drives the reviewer's shared-engine
// topology end to end; this matrix pins the ledger's claim mechanics
// directly, without the workspace fixture:
//
//   - every membership-creating path registers the CREATING workspace's
//     claim, idempotent per (workspace, name) - a duplicate record of
//     the same object never adds a second claim on one workspace and
//     never spends a second capacity unit, whatever the claimant count;
//   - the published-class release retires exactly the caller's claims -
//     one workspace's terminal never removes another live
//     claimant's claim;
//   - the membership and its capacity unit retire exactly when the
//     name's FINAL claim retires, so no completed workspace can leak
//     either;
//   - the two claim-insensitive retire classes keep whole-membership
//     semantics: an enqueue hands the obligation to the queue (one
//     slot, whatever the claims were) and a storage deletion ends the
//     obligation for every claimant.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func r57LedgerSnapshot(e *Engine, name string) (live int64, claims int) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	return e.unpublishedPending.live, len(e.unpublishedPending.ownedNames[name])
}

func TestIssue25590LedgerPublishedReleaseRetiresPerWorkspaceClaim(t *testing.T) {
	e := &Engine{}
	recorder := &Transaction{} // workspaces only key claims; identity is
	fragment := &Transaction{} // the pointer, exactly the recordedOwners rule

	// Creation claims its workspace's membership: two distinct names
	// spend two capacity units.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		fragment, []string{"r57-sole", "r57-shared"}))
	live, soleClaims := r57LedgerSnapshot(e, "r57-sole")
	require.Equal(t, int64(2), live,
		"two distinct recorded objects count two capacity units")
	require.Equal(t, 1, soleClaims,
		"the fragment's record registered exactly its own claim")

	// The recording workspace's re-record of the shared object: no
	// second capacity unit, a second claim in the set.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		recorder, []string{"r57-shared"}))
	live, sharedClaims := r57LedgerSnapshot(e, "r57-shared")
	require.Equal(t, int64(2), live,
		"the same-CN re-record spends no second capacity unit")
	require.Equal(t, 2, sharedClaims,
		"the shared object now carries one claim per workspace")

	// Idempotence per (workspace, name): neither a repeat record nor a
	// repeat release may move the counters.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		recorder, []string{"r57-shared"}))
	live, sharedClaims = r57LedgerSnapshot(e, "r57-shared")
	require.Equal(t, int64(2), live,
		"a duplicate record of the same object spends no capacity")
	require.Equal(t, 2, sharedClaims,
		"a duplicate record adds no second claim to its own workspace")

	// A names token the caller never claimed releases nothing: the
	// release is conditional on THE CALLER's claim, so one workspace's
	// terminal cannot raid another's.
	e.releaseUnpublishedPendingPublished(recorder, []string{"r57-sole"})
	live, soleClaims = r57LedgerSnapshot(e, "r57-sole")
	require.Equal(t, int64(2), live,
		"a workspace cannot release a name it never claimed")
	require.Equal(t, 1, soleClaims,
		"the fragment's sole-claim membership is untouched")

	// The published-class release retires exactly the caller's claims:
	// the fragment's release leaves the recorder's claim and the
	// capacity it still needs.
	e.releaseUnpublishedPendingPublished(
		fragment, []string{"r57-sole", "r57-shared"})
	live, soleClaims = r57LedgerSnapshot(e, "r57-sole")
	_, sharedClaims = r57LedgerSnapshot(e, "r57-shared")
	require.Equal(t, int64(1), live,
		"the fragment's release retired its sole claim's capacity and kept the shared object's")
	require.Zero(t, soleClaims,
		"the sole-claim membership retired with its only claim")
	require.Equal(t, 1, sharedClaims,
		"the recorder's claim survives the fragment's release")

	// Idempotent duplicate release: no double retire, no leak.
	e.releaseUnpublishedPendingPublished(fragment, []string{"r57-shared"})
	live, sharedClaims = r57LedgerSnapshot(e, "r57-shared")
	require.Equal(t, int64(1), live,
		"a duplicate release of a retired claim changes nothing")
	require.Equal(t, 1, sharedClaims)

	// The FINAL claim retires the membership and the capacity with it.
	e.releaseUnpublishedPendingPublished(recorder, []string{"r57-shared"})
	live, sharedClaims = r57LedgerSnapshot(e, "r57-shared")
	require.Zero(t, live,
		"the final claim's release retired the ledger's capacity")
	require.Zero(t, sharedClaims,
		"no completed workspace stays behind the claim map")
	require.Empty(t, e.unpublishedPending.ownedNames,
		"the ledger holds no membership after every claim retired")

	// Claim-insensitive retire class 1 - the enqueue: a queued name
	// takes the bound's space whole and clears the claim set with the
	// membership, whatever the claims were.
	writerA, writerB := &Transaction{}, &Transaction{}
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writerA, []string{"r57-queued"}))
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writerB, []string{"r57-queued"}))
	live, _ = r57LedgerSnapshot(e, "r57-queued")
	require.Equal(t, int64(1), live,
		"the queued probe starts as one dual-claimed unit")
	e.unpublishedPending.mu.Lock()
	queuedBefore := len(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	e.enqueueUnpublishedPendingFrom(nil, []string{"r57-queued"})
	e.unpublishedPending.mu.Lock()
	_, stillMember := e.unpublishedPending.ownedNames["r57-queued"]
	queuedAfter := len(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	require.False(t, stillMember,
		"the queue entry retires the whole membership - one slot, whatever the claims")
	require.Equal(t, queuedBefore+1, queuedAfter,
		"the name is queued exactly once")
	e.unpublishedPending.mu.Lock()
	ownedAfterEnqueue := len(e.unpublishedPending.ownedNames)
	liveAfterEnqueue := e.unpublishedPending.live
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, liveAfterEnqueue,
		"the queued name holds the bound through the queue's own slot, not the owned ledger")
	require.Zero(t, ownedAfterEnqueue,
		"no owned membership survived the queue entry")

	// A claimant terminal after the enqueue finds nothing to release -
	// the queue owns the obligation now.
	e.releaseUnpublishedPendingPublished(writerA, []string{"r57-queued"})
	e.releaseUnpublishedPendingPublished(writerB, []string{"r57-queued"})
	e.unpublishedPending.mu.Lock()
	live, inFlight := e.unpublishedPending.live, e.unpublishedPending.inFlight
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, live, "post-enqueue releases retire no owned capacity")
	require.Zero(t, inFlight)

	// Claim-insensitive retire class 2 - the storage deletion: the
	// settle retires the whole membership even at multi-claim, so no
	// later release may resurrect capacity behind a deleted file.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writerA, []string{"r57-deleted"}))
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writerB, []string{"r57-deleted"}))
	e.settleUnpublishedPendingDeleted([]string{"r57-deleted"})
	e.unpublishedPending.mu.Lock()
	_, deletedMember := e.unpublishedPending.ownedNames["r57-deleted"]
	e.unpublishedPending.mu.Unlock()
	require.False(t, deletedMember,
		"a storage deletion retires the whole membership for every claimant")
	e.releaseUnpublishedPendingPublished(writerA, []string{"r57-deleted"})
	e.releaseUnpublishedPendingPublished(writerB, []string{"r57-deleted"})
	e.unpublishedPending.mu.Lock()
	live, inFlight = e.unpublishedPending.live, e.unpublishedPending.inFlight
	e.unpublishedPending.mu.Unlock()
	require.Zero(t, live,
		"no claimant release resurrects capacity behind a deleted file")
	require.Zero(t, inFlight)
}
