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

// Round-58 unit matrix (issue #25590 review of PR #30988 head
// 7c75f3e07b): the production-path regressions in txn_issue25590_r58_
// test.go drive the reviewer's lifetime end to end through WriteFile,
// GCObjsByStats and the terminals; this matrix pins the engine-owned
// GC capacity claim's mechanics directly, without the workspace
// fixture:
//
//   - the published-class release always retires exactly the caller's
//     claim, but a name whose delete batch the pool accepted keeps its
//     membership and its capacity unit while its refcount is positive —
//     for EVERY claimant count, including the final-claim release the
//     round's finding priced;
//   - the settle alone retires the refcount with the whole membership
//     (the storage-deletion claim-insensitive class round-57 pinned),
//     so the stored claim cannot outlive the delete job or leak behind
//     a settled batch — a settle of an unregistered name (the queue's
//     own drain class) is the documented no-op;
//   - the refcount itself is exact under duplicate accepted batches:
//     one name in several in-flight batches retires one reference per
//     settle;
//   - the submit-failure unwind retires the failed batch's references
//     before the lossless queue tail takes the names over, so the
//     queue's own slot is the only unit left and nothing
//     double-counts.

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func r58LedgerSnapshot(e *Engine, name string) (live int64, claims int, gcRefs int) {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	return e.unpublishedPending.live,
		len(e.unpublishedPending.ownedNames[name]),
		e.unpublishedPending.gcAccepted[name]
}

func TestIssue25590EngineOwnedGCClaimOutlivesFinalClaimRelease(t *testing.T) {
	e := &Engine{}
	submitter := &Transaction{} // workspaces only key claims; identity is
	claimer := &Transaction{}   // the pointer, exactly the r57 rule

	// The recorded object and a second claimant of the same name: one
	// capacity unit, two workspace claims — the r57 base state.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		submitter, []string{"r58-held"}))
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		claimer, []string{"r58-held"}))
	live, claims, _ := r58LedgerSnapshot(e, "r58-held")
	require.Equal(t, int64(1), live)
	require.Equal(t, 2, claims)

	// The pool ACCEPTS the name's delete batch: the engine-owned GC
	// claim registers, capacity is still exactly one unit.
	e.retainUnpublishedPendingGCNames([]string{"r58-held"})
	live, _, gcRefs := r58LedgerSnapshot(e, "r58-held")
	require.Equal(t, int64(1), live,
		"the accepted batch adds no capacity unit — the membership already counts it")
	require.Equal(t, int64(1), int64(gcRefs),
		"the accepted batch registers exactly one name-keyed reference")

	// The submitting workspace's terminal releases its claim: the
	// claimant's claim keeps the membership, exactly the r57 rule.
	e.releaseUnpublishedPendingPublished(submitter, []string{"r58-held"})
	live, claims, _ = r58LedgerSnapshot(e, "r58-held")
	require.Equal(t, int64(1), live)
	require.Equal(t, 1, claims,
		"the submitter's release retired exactly its own claim")

	// THE FINAL-CLAIM RELEASE (the review's classification): with no
	// workspace claiming the name anymore, the retained obligation —
	// not any terminal — owns the capacity until the delete settles.
	e.releaseUnpublishedPendingPublished(claimer, []string{"r58-held"})
	live, claims, gcRefs = r58LedgerSnapshot(e, "r58-held")
	require.Equal(t, int64(1), live,
		"the final claim's release kept the accepted delete job's engine-owned capacity counted (I4 admitted-in-flight)")
	require.Zero(t, claims,
		"the completed workspaces hold no claim behind the obligation")
	require.Equal(t, 1, gcRefs)

	// Idempotent per (workspace, name): a repeated release moves
	// nothing, and a settle is the only retirement left.
	e.releaseUnpublishedPendingPublished(claimer, []string{"r58-held"})
	live, _, gcRefs = r58LedgerSnapshot(e, "r58-held")
	require.Equal(t, int64(1), live)
	require.Equal(t, 1, gcRefs)

	// The settle ends the obligation whole: the membership, the capacity
	// and the refcount all retire together.
	e.settleUnpublishedPendingDeleted([]string{"r58-held"})
	live, claims, gcRefs = r58LedgerSnapshot(e, "r58-held")
	require.Zero(t, live,
		"the worker's settle alone returned the stalled name's capacity")
	require.Zero(t, claims)
	require.Zero(t, gcRefs,
		"no accepted-batch reference outlives its delete job")

	// No terminal may resurrect either: after the settle the release
	// is a no-op and the ledger closes empty.
	e.releaseUnpublishedPendingPublished(submitter, []string{"r58-held"})
	e.releaseUnpublishedPendingPublished(claimer, []string{"r58-held"})
	live, claims, gcRefs = r58LedgerSnapshot(e, "r58-held")
	require.Zero(t, live)
	require.Zero(t, claims)
	require.Zero(t, gcRefs)
	require.Empty(t, e.unpublishedPending.ownedNames,
		"the ledger holds no membership after the settled obligation")
	require.Empty(t, e.unpublishedPending.gcAccepted,
		"the engine-owned claim map holds no batch the workers finished")
}

func TestIssue25590EngineOwnedGCClaimRefCountAndUnwind(t *testing.T) {
	e := &Engine{}
	writer := &Transaction{}

	// One name in TWO in-flight accepted batches: the refcount is two
	// and the capacity still exactly one unit.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writer, []string{"r58-batched"}))
	e.retainUnpublishedPendingGCNames([]string{"r58-batched"})
	e.retainUnpublishedPendingGCNames([]string{"r58-batched"})
	live, _, gcRefs := r58LedgerSnapshot(e, "r58-batched")
	require.Equal(t, int64(1), live)
	require.Equal(t, 2, gcRefs)

	// The FIRST settle is the file's death: the storage-deletion class
	// retires the WHOLE membership — the round-57 doctrine, the deleted
	// file frees the claimant-agnostic unit — and the submitter
	// workspace's own terminal may release its claim meanwhile without
	// anything to release. The second batch owes only its own settle's
	// refcount retire over an empty membership.
	e.settleUnpublishedPendingDeleted([]string{"r58-batched"})
	e.releaseUnpublishedPendingPublished(writer, []string{"r58-batched"})
	live, _, gcRefs = r58LedgerSnapshot(e, "r58-batched")
	require.Zero(t, live,
		"the deleted file returned the capacity whole — the settle class frees the unit for every claimant")
	require.Equal(t, 1, gcRefs,
		"the unsettled second batch keeps exactly its own name-keyed reference")

	// The LAST batch's settle retires the final reference and the
	// claim map closes empty; a settle of a name no batch ever
	// registered — the queue's own drain class — is the documented no-op.
	e.settleUnpublishedPendingDeleted([]string{"r58-batched"})
	_, _, gcRefs = r58LedgerSnapshot(e, "r58-batched")
	require.Zero(t, gcRefs,
		"the final settled batch left no reference in the claim map")
	require.Empty(t, e.unpublishedPending.ownedNames,
		"no completed membership survived the settled obligation")
	e.settleUnpublishedPendingDeleted([]string{"r58-unregistered"})
	_, _, gcRefs = r58LedgerSnapshot(e, "r58-unregistered")
	require.Zero(t, gcRefs)
	require.Empty(t, e.unpublishedPending.gcAccepted)

	// The submit-failure unwind: a retained batch whose submission
	// fails releases its references BEFORE the lossless queue tail
	// takes the names, so the enqueue's own whole-membership class
	// leaves exactly the queue's one slot — capacity moved, not added.
	require.NoError(t, e.admitUnpublishedPendingRecordedNames(
		writer, []string{"r58-unwound"}))
	e.retainUnpublishedPendingGCNames([]string{"r58-unwound"})
	e.releaseUnpublishedPendingGCNames([]string{"r58-unwound"})
	e.enqueueUnpublishedPendingFrom(nil, []string{"r58-unwound"})
	e.unpublishedPending.mu.Lock()
	_, stillMember := e.unpublishedPending.ownedNames["r58-unwound"]
	queued := len(e.unpublishedPending.names)
	e.unpublishedPending.mu.Unlock()
	require.False(t, stillMember,
		"the queue entry owns the obligation whole — the claim classes hold nothing")
	require.Equal(t, 1, queued)
	live, _, gcRefs = r58LedgerSnapshot(e, "r58-unwound")
	require.Zero(t, live,
		"the unwound batch left no engine-owned claim behind the queue's slot")
	require.Zero(t, gcRefs)
	require.Empty(t, e.unpublishedPending.gcAccepted,
		"the unwind left no accepted-batch reference in the claim map")
}
