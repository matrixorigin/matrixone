// Copyright 2026 Matrix Origin
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

// Round-40 (issue #25590 review of 2026-10-24, finding 1): the
// recorded-owner registry the round-38 settle added maps a QUEUED name
// to the transaction whose recordedUnpublishedObjectNames copy still
// tracks it, and nothing but a successful drain delete ever popped the
// entry - not the terminal releases, not delTransaction. A transaction
// that ends while a storage outage parks its handed-off names in the
// queue therefore stays strongly reachable FROM the engine through
// every queued name: each surviving entry retains the whole completed
// Transaction - statement-offset history, the writes backing slice and
// the process reference - while the only work the entry could ever
// still do, retiring a recorded-name copy, ended the moment the
// terminal release dropped that copy. Repeated terminal transactions
// during one storage outage accumulate the full retained graphs, and
// the admitted-name limit bounds the NAME count, never the retained
// transaction graphs.
//
// The two regressions below pin the reviewer's two terminal lifetimes -
// the full Rollback and the commit after a statement rollback already
// queued recorded objects - on one shared contract:
//
//   - while the transaction stays OPEN and its file still exists, the
//     registered owner entry is exactly the round-38 settle contract -
//     the drain's settle retires the copy through it;
//   - the terminal that drops the recorded-name copy ALSO detaches the
//     expired owner alongside it - the cleanup obligation (the queued
//     name and the file's deletion) survives untouched under the
//     engine's ownership alone;
//   - after the outage ends, the drain settles the backlog with no
//     expired-transaction entry involved, and the queue, the registry
//     and the ledger all return to empty.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// issue25590QueuedOwnerEntriesFor counts the queue's recorded-owner
// registry entries still pointing at owner's recorded-name holder - the
// exact retention the round-40 detach must end at every terminal. The
// registry is holder-keyed (round-60), so the probe reads through the
// transaction's stable holder pointer.
func issue25590QueuedOwnerEntriesFor(e *Engine, owner *Transaction) int {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	holder := owner.recordedUnpublishedObjects
	if holder == nil || e.unpublishedPending.recordedOwners == nil {
		return 0
	}
	n := 0
	for _, o := range e.unpublishedPending.recordedOwners {
		if o == holder {
			n++
		}
	}
	return n
}

// issue25590RecordOwnerOf returns the registered recorded-name holder
// for a queued name, or nil when the name carries no recorded-copy
// entry (round-60 keys the registry by the holder).
func issue25590RecordOwnerOf(e *Engine, name string) *txnRecordedUnpublished {
	e.unpublishedPending.mu.Lock()
	defer e.unpublishedPending.mu.Unlock()
	return e.unpublishedPending.recordedOwners[name]
}

// TestIssue25590RollbackTerminalDetachesQueuedRecordedOwners drives
// the reviewer's first terminal lifetime: an explicit transaction
// records an S3 object, the storage outage makes every delete fail
// persistently, and the transaction's FULL Rollback - the production
// path through gcObjsByIdxRangeToOwner's owner-carrying handoff, the
// terminal release and delTransaction - ends it with the obligation
// still queued. The rollback's terminal release drops the recorded-name
// copy; the same step must detach the registry entry that points at
// the now-expired transaction, or the queue keeps the whole completed
// Transaction reachable through the queued name until the outage ends.
// The recovery leg proves the detachment kept the cleanup obligation
// lossless: the drain alone deletes the file, retires the queue and
// closes the ledger with no entry pointing at the dead transaction.
func TestIssue25590RollbackTerminalDetachesQueuedRecordedOwners(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	record := func(name string, stats objectio.ObjectStats) error {
		recBat := colexec.AllocCNS3ResultBat(false)
		defer recBat.Clean(mp)
		if err := colexec.ExpandObjectStatsToBatch(
			mp, false, recBat, false, stats); err != nil {
			return err
		}
		return txn.WriteFile(
			INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
			name, recBat, txn.tnStores[0])
	}

	// The transaction's one statement records its S3 object exactly as
	// production does, so the full rollback's handed-off name is a
	// recorded member with a live ownership lifecycle.
	issue25590AdmitBoundary(t, txn, ctx)
	stats := mockStatsList(t, 1)[0]
	name := stats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, name))
	require.NoError(t, record(name, stats),
		"the statement records its S3 object before the transaction ends")
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()
	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(name),
		"the recorded write is a member of the recorded-name set before terminal")
	txn.Unlock()
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"nothing is queued yet - the member still owns the name in the workspace")

	// The storage outage: every delete fails persistently, so nothing
	// can settle the rollout backlog while the transaction ends.
	failFS.armDeleteFailures(false, 1000)

	// FULL Terminal: the production Rollback path.
	require.NoError(t, txn.Rollback(ctx))

	// The cleanup obligation survived the terminal exactly as designed:
	// the name is queued under the engine's ownership alone, the file
	// still exists, and nothing is live or in flight.
	require.Equal(t, []string{name}, issue25590QueuedNames(e),
		"the full rollback's owner-carrying handoff moved the recorded object losslessly to the queue")
	issue25590RequireKept(t, failFS, name,
		"the failed delete outage leaves the queued object's file on storage")
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live,
		"the queued name left the owned ledger exactly once at the enqueue")
	require.Zero(t, inFlight)

	// THE ROUND-40 CONTRACT: the terminal that dropped the recorded-name
	// copy also detached the registry entry, so no queued name keeps the
	// expired transaction - its whole completed object graph - strongly
	// reachable from the engine (issue #25590 round-40 finding).
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"the rolled-back transaction's terminal release must detach its recorded-owner entries - a queued cleanup obligation may not retain the completed Transaction's statement history, writes and proc reference")

	// Recovery: the outage ends and the drain settles the backlog with
	// no expired-transaction entry involved.
	failFS.armDeleteFailures(false, 0)
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the recovered drain settles the terminal transaction's backlog alone")
	issue25590RequireGone(t, failFS, name,
		"the recovered drain physically deleted the handed-off object")
	require.Empty(t, issue25590QueuedNames(e))
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn))
	_, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Zero(t, live)
	require.Zero(t, inFlight)
}

// TestIssue25590CommitTerminalDetachesQueuedRecordedOwners drives the
// reviewer's second terminal lifetime: the failed statement's
// RollbackLastStatement queues its recorded object with the OPEN
// transaction as the recorded-copy owner (the round-38 contract while
// the file exists), the storage outage parks the file, and the
// transaction then COMMITS - the FinalizeCommit terminal whose
// successful classification releases the recorded members as published.
// That release must detach the queued name's owner entry too: the file
// was never part of the committed payload, so its deletion obligation
// stays queued - but the entry pointing at the committed transaction
// has nothing left to retire and must not retain it. The recovery leg
// settles the obligation through the drain alone.
func TestIssue25590CommitTerminalDetachesQueuedRecordedOwners(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	record := func(name string, stats objectio.ObjectStats) error {
		recBat := colexec.AllocCNS3ResultBat(false)
		defer recBat.Clean(mp)
		if err := colexec.ExpandObjectStatsToBatch(
			mp, false, recBat, false, stats); err != nil {
			return err
		}
		return txn.WriteFile(
			INSERT, 1, tbl.db.databaseId, tbl.tableId, "db", tbl.tableName,
			name, recBat, txn.tnStores[0])
	}

	// The standing first statement carves the statement boundary the
	// failed second statement rolls back to.
	issue25590AdmitBoundary(t, txn, ctx)
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	stats := mockStatsList(t, 1)[0]
	name := stats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, name))

	// THE failed statement: it records its S3 object, and the frontend
	// rolls the whole statement back - the exact round-38 handoff.
	issue25590AdmitBoundary(t, txn, ctx)
	require.NoError(t, record(name, stats),
		"the failed statement records its S3 object before its rollback")
	require.NoError(t, txn.RollbackLastStatement(ctx),
		"the statement rollback succeeds")
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	// The OPEN-transaction window: the round-38 contract holds - the
	// handoff moved the recorded object to the queue with the OPEN
	// transaction registered as its recorded-copy owner, and the file
	// still exists for the drain's later settle to retire.
	require.Equal(t, []string{name}, issue25590QueuedNames(e),
		"the statement rollback's owner-carrying handoff queued the recorded object")
	require.Equal(t, 1, issue25590QueuedOwnerEntriesFor(e, txn),
		"the OPEN transaction is the queued name's recorded-copy owner - the entry the drain's settle needs while the file exists")
	require.Same(t, txn.recordedUnpublishedObjects, issue25590RecordOwnerOf(e, name))
	issue25590RequireKept(t, failFS, name,
		"the handed-off object's file exists while the outage parks the settle")
	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(name),
		"the recorded member still tracks the queued name until a settle retires it")
	txn.Unlock()

	// The storage outage: the drain cannot settle before the commit.
	failFS.armDeleteFailures(false, 1000)

	// COMMIT Terminal: the production success classification.
	txn.FinalizeCommit(ctx)

	// The rolled-back statement's object was never part of the commit's
	// payload: its deletion obligation stays queued under the engine's
	// ownership, exactly as before the terminal.
	require.Equal(t, []string{name}, issue25590QueuedNames(e),
		"the commit published nothing of the rolled-back statement - its object keeps its queued deletion obligation")
	issue25590RequireKept(t, failFS, name,
		"the outage keeps the file until the drain settles it")

	// THE ROUND-40 CONTRACT: the successful commit's release of the
	// recorded members (the queued one included - its record never
	// lands) also detached the registry entry, so the completed
	// transaction's graph is unreachable long before the drain runs.
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"the commit terminal that dropped the recorded-name copy must detach its recorded-owner entries - the queued obligation may not retain the completed Transaction until the outage ends (issue #25590 round-40 finding)")

	// Recovery after the commit: the drain settles the backlog alone.
	failFS.armDeleteFailures(false, 0)
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the recovered drain settles the rolled-back statement's object")
	issue25590RequireGone(t, failFS, name,
		"the recovered drain physically deleted the failed statement's object")
	require.Empty(t, issue25590QueuedNames(e))
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn))
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live)
	require.Zero(t, inFlight)
}
