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

// Round-60 (issue #25590 review of PR #30988 head a52b906996, request-
// changes review of 2026-10-29, the GC ownership path): an accepted GC
// worker treated an UNSUCCESSFUL DeleteUnpublishedObjects call as a
// completed deletion — it logged the error, then unconditionally settled
// the whole ledger batch (returning the admission capacity) and retired
// the recorded-name copy — although the helper stops at its own first
// failing storage sub-batch and proves nothing about the rest, so a
// storage outage could leave the objects on storage with the last
// enumerable owner dropped and the queue never retrying the lost names.
// The PR's ownership ledger promises lossless ownership until physical
// deletion (design I2), and the pre-existing log-and-drop loss class is
// exactly what the ledger removed from every other ownership move.
//
// The regressions below compile against the pre-fix head and assert only
// behavioral surfaces — the queue snapshot, the live/in-flight counters,
// the reservation gate's verdicts, the recorded-copy state and the
// physical file's fate — so the RED run fails exactly at the review's
// lossless contract:
//
//   - the whole-batch failure regression drives the reviewer's exact
//     scenario: an accepted worker whose whole delete fails requeues the
//     batch to the engine's bounded retry owner, keeps every name's
//     capacity through the outage, keeps the recorded copies tracking
//     the live obligations, and the recovery drain deletes, settles and
//     retires everything per name at the proof point;
//   - the mid-batch failure regression arms the LocalFS deleteSingle
//     shape: the first file of the failed call is already physically
//     gone, so the requeue must still take the WHOLE batch (an unknown
//     prefix proved nothing about the remainder), and the recovery retry
//     must tolerate the absent prefix exactly — deletion of an absent
//     file is the documented success — while settling the rest;
//   - the terminal-lifetime regression commits the transaction while the
//     failed batch sits queued: the queued obligation survives the
//     terminal, the completed transaction is not retained behind the
//     queue (its recorded copy dropped and owner entries detached), and
//     the emptied holder settles nothing while the recovery drain still
//     proves the deletions.

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// r60NamesOf unfolds the stats list into its object names, the exact
// strings the GC front doors submit and the queue records.
func r60NamesOf(stats []objectio.ObjectStats) []string {
	names := make([]string, 0, len(stats))
	for _, stats := range stats {
		names = append(names, stats.ObjectName().String())
	}
	return names
}

// r60RecordObsoleteNames drives the round-58 production shape the
// review's own reproduction starts from: the statement records its S3
// objects and compaction then retires the workspace entries that carried
// them, so every name is recorded WITHOUT belonging to the commit
// payload — the obsolete-object state a GCObjsByStats submission owns.
func r60RecordObsoleteNames(
	t *testing.T, txn *Transaction, ctx context.Context,
) []objectio.ObjectStats {
	t.Helper()
	stats := mockStatsList(t, 2)
	r58RecordStats(t, txn, ctx, stats)
	for _, s := range stats {
		name := s.ObjectName().String()
		txn.Lock()
		require.True(t, txn.hasRecordedUnpublishedObjectName(name),
			"the obsolete object's record is what the accepted worker's settle may retire")
		txn.Unlock()
		r58RetireRecordedEntry(t, txn, name)
	}
	return stats
}

// r60RequireFailedBatchRequeued waits for the accepted worker's failed
// delete to hand its whole batch back to the engine's queue and asserts
// the lossless state the review requires: every name queued under the
// engine's bounded retry owner, no owned membership or in-flight unit
// double-counting, and the record copies still tracking the live
// obligations for the drain's settle.
func r60RequireFailedBatchRequeued(
	t *testing.T, txn *Transaction, names []string,
) {
	t.Helper()
	e := txn.engine
	require.Eventually(t, func() bool {
		return len(issue25590QueuedNames(e)) == len(names)
	}, 10*time.Second, 10*time.Millisecond,
		"the accepted worker's failed delete must hand the whole batch to the lossless queue")
	require.ElementsMatch(t, names, issue25590QueuedNames(e),
		"the failed batch's names joined the bounded retry owner - the storage failure may not drop any of them")
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Len(t, queued, len(names))
	require.Zero(t, live,
		"the membership moved whole into the queue's own slots, never freed at the failed delete")
	require.Zero(t, inFlight)
	txn.Lock()
	defer txn.Unlock()
	for _, name := range names {
		require.True(t, txn.hasRecordedUnpublishedObjectName(name),
			"the recorded copy keeps tracking the queued obligation until a settle retires it")
	}
}

// TestIssue25590AcceptedGCWorkerFailureRequeuesObligationUntilProof
// pins the reviewer's whole-batch scenario on the production surfaces:
// the pool ACCEPTS the obsolete objects' delete, the storage outage
// makes the whole DeleteUnpublishedObjects call fail, and the worker
// must NOT settle on that unproven state. The failed batch transfers to
// the engine's queue with the capacity held (limit two: no further
// reservation fits beside the two queued obligations), the recorded
// copies keep tracking, and after recovery the queue's own drain
// deletes both objects, retires the ledger membership and the recorded
// copies per name, and returns the full bound.
func TestIssue25590AcceptedGCWorkerFailureRequeuesObligationUntilProof(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	r58NewAcceptedPool(t, e, 2)
	r58PinAdmissionLimit(t, 2)
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	stats := r60RecordObsoleteNames(t, txn, ctx)
	names := r60NamesOf(stats)
	require.Empty(t, issue25590QueuedNames(e))
	_, live, _ := e.InspectUnpublishedPendingForTest()
	require.Equal(t, 2, live,
		"the record choke booked both obsolete objects' ledger memberships")

	// THE OUTAGE: every storage delete fails, so the accepted worker's
	// delete call is unproven for the whole batch.
	failFS.armDeleteFailures(false, 1000)
	require.NoError(t, txn.GCObjsByStats(stats...),
		"the pool accepts the delete submission — the failure is the storage's, not the caller's")

	r60RequireFailedBatchRequeued(t, txn, names)

	// The failed delete freed NO capacity: the queued obligations keep
	// their units, so with the bound pinned at two nothing else fits.
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-further-1"),
		"the failed batch's two obligations still hold the bound — an unproven delete may not return capacity")
	for _, name := range names {
		issue25590RequireKept(t, failFS, name,
			"the unproven delete leaves the object on storage")
	}

	// RECOVERY: the outage ends, and the queue's bounded retry owner
	// proves the deletions. The per-name settles retire the recorded
	// copies and return the whole bound.
	failFS.armDeleteFailures(false, 0)
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the recovery drain settles the requeued batch")
	for _, name := range names {
		issue25590RequireGone(t, failFS, name,
			"the recovered queue deleted the failed batch's object")
	}
	require.Empty(t, issue25590QueuedNames(e))
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"the queue's settle closed the recorded-owner registrations with the queue")

	txn.Lock()
	for _, name := range names {
		require.False(t, txn.hasRecordedUnpublishedObjectName(name),
			"the drain's per-name settle retired the recorded copy at the proof point")
	}
	txn.Unlock()

	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-cap-1"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-cap-2"))
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-cap-3"),
		"both settled obligations returned their units — the bound is exactly two again")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r60-cap-1", "issue25590-r60-cap-2"})
}

// TestIssue25590AcceptedGCWorkerMidBatchFailureRequeuesWholeBatch pins
// the partial-batch order: LocalFS deletes one file at a time, so a
// storage error can strike after the batch's first file is already
// physically gone. The failed call still proves nothing about the
// remainder — the worker cannot observe which prefix died — so the
// WHOLE batch requeues, capacity stays held for every name through the
// outage, and the recovery retry tolerates the absent first file
// exactly (deleting an absent file is the documented success) while
// deleting the rest and settling every name per file at its proof.
func TestIssue25590AcceptedGCWorkerMidBatchFailureRequeuesWholeBatch(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	r58NewAcceptedPool(t, e, 2)
	r58PinAdmissionLimit(t, 3)
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	stats := r60RecordObsoleteNames(t, txn, ctx)
	// The partial-batch record class needs a third obsolete object so
	// the failed call owns file names on both sides of its strike.
	third := mockStatsList(t, 1)
	r58RecordStats(t, txn, ctx, third)
	r58RetireRecordedEntry(t, txn, third[0].ObjectName().String())
	stats = append(stats, third...)
	names := r60NamesOf(stats)

	require.Empty(t, issue25590QueuedNames(e))
	_, live, _ := e.InspectUnpublishedPendingForTest()
	require.Equal(t, 3, live,
		"the three obsolete objects hold three ledger memberships")

	// THE MID-BATCH OUTAGE: the accepted worker's delete applies the
	// FIRST file and then fails the whole call.
	failFS.armDeleteFailAfter(1)
	require.NoError(t, txn.GCObjsByStats(stats...),
		"the pool accepts the three-object delete submission")

	r60RequireFailedBatchRequeued(t, txn, names)

	// The prefix is physically gone, that proof covers ONLY it, and the
	// worker has no way to see even that — the whole batch requeued.
	issue25590RequireGone(t, failFS, names[0],
		"the failed call's applied prefix deleted the first file")
	issue25590RequireKept(t, failFS, names[1],
		"the failed call proved nothing about the objects after its strike")
	issue25590RequireKept(t, failFS, names[2],
		"the failed call proved nothing about the objects after its strike")
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-mid-1"),
		"the requeued batch's three obligations still hold the bound of three")

	// RECOVERY: one drain retry deletes what remains and tolerates the
	// first file's absence — the whole sub-batch returns success — so
	// every name settles per file at its proof point.
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the recovery drain settles the mid-batch-failed batch")
	for _, name := range names {
		issue25590RequireGone(t, failFS, name,
			"the recovered retry deleted the object or found it already deleted")
	}
	require.Empty(t, issue25590QueuedNames(e))
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live)
	require.Zero(t, inFlight)
	txn.Lock()
	for _, name := range names {
		require.False(t, txn.hasRecordedUnpublishedObjectName(name),
			"every name's settle retired its recorded copy — the already-absent prefix included")
	}
	txn.Unlock()

	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-mid-2"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-mid-3"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-mid-4"))
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-mid-5"),
		"the three settled obligations returned their units — the bound is exactly three again")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r60-mid-2", "issue25590-r60-mid-3", "issue25590-r60-mid-4"})
}

// TestIssue25590FailedGCBatchObligationSurvivesTerminalWithoutRetainingTxn
// drives the terminal lifetime the failed-delete transfer adds to the
// round-40/41 doctrine: the accepted worker's delete fails, the batch
// requeues, and the recording transaction then COMMITS while its
// obsolete objects sit queued. The commit's published-class release
// drops the recorded copies and detaches the owner entries (the
// completed transaction's graph may not stay reachable behind the
// queue), but the queued obligations themselves survive the terminal
// unchanged — the files' fates are the recovery drain's to prove —
// and the emptied holder settles nothing while the drain still deletes
// and closes the ledger whole.
func TestIssue25590FailedGCBatchObligationSurvivesTerminalWithoutRetainingTxn(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	r58NewAcceptedPool(t, e, 2)
	r58PinAdmissionLimit(t, 2)
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	stats := r60RecordObsoleteNames(t, txn, ctx)
	names := r60NamesOf(stats)

	// THE OUTAGE and the accepted worker's failed delete: the batch
	// requeues with the holder registered as its recorded-copy owner.
	failFS.armDeleteFailures(false, 1000)
	require.NoError(t, txn.GCObjsByStats(stats...))
	r60RequireFailedBatchRequeued(t, txn, names)

	// COMMIT while the failed names stay queued.
	txn.FinalizeCommit(ctx)

	// The terminal published nothing of the queued obligations: every
	// name keeps its queue membership and its capacity unit.
	require.ElementsMatch(t, names, issue25590QueuedNames(e),
		"the commit terminal leaves the requeued batch's obligations queued")
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-terminal-1"),
		"the queued obligations still hold the bound of two after the terminal")
	for _, name := range names {
		issue25590RequireKept(t, failFS, name,
			"the outage keeps the queued objects on storage through the terminal")
	}

	// The completed transaction is not retained: its recorded copies
	// dropped at the release and the queue's owner entries detached with
	// them — only the bounded recorded-name holder stayed behind.
	txn.Lock()
	for _, name := range names {
		require.False(t, txn.hasRecordedUnpublishedObjectName(name),
			"the commit's published-class release dropped the recorded copies")
	}
	txn.Unlock()
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"the terminal detached the queued names' owner entries — no completed Transaction stays reachable behind the queue")

	// RECOVERY: with the copies empty, the drain settles the backlog
	// alone: deletes both objects, closes the queue and the ledger,
	// and returns the full bound. The emptied holder's retire no-ops.
	failFS.armDeleteFailures(false, 0)
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the recovery drain settles the committed transaction's requeued batch")
	for _, name := range names {
		issue25590RequireGone(t, failFS, name,
			"the recovered drain proved the queued objects' deletion")
	}
	require.Empty(t, issue25590QueuedNames(e))
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-terminal-2"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r60-terminal-3"))
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r60-terminal-4"),
		"the settled obligations returned the bound's full capacity")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r60-terminal-2", "issue25590-r60-terminal-3"})
}
