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

// Round-38 (issue #25590 review of 2026-10-24, finding 1): the
// recorded-name retirement the round-37 settle added covered ONLY the
// gcFiles pool worker. The queue-side settle — the drain's successful
// batch delete that retires a name's queue membership — never retired
// the recording transaction's recordedUnpublishedObjectNames copy, so
// the two handoff paths that move RECORDED names to the engine's
// lossless queue left the copy growing with failed-statement history
// while the ledger, the queue and the physical files were all empty:
//
//   - the statement rollback (RollbackLastStatement ->
//     gcObjsByIdxRangeToOwner), the exact shape the reviewer's probe
//     drove through production WriteFile and real file-service state:
//     an OPEN explicit transaction that keeps recording S3 objects and
//     failing the statement afterwards — 32 rounds under admission
//     limit 2 left queued=0, live=1, inFlight=0, recorded_names=33;
//   - the gcFiles pool-submit-failure tail (a released pool refuses the
//     submission and the whole unsent batch transfers to the queue),
//     the round-13 lossless handoff whose recorded members the pool
//     worker never got to settle.
//
// The retire must fire exactly where the round-37 line put it: when the
// drain's batch delete proves the file gone, never earlier — the member
// still protects the still-existing file against late producer
// handoffs (the retain funnels, the quiesced handoff and the remote
// adoption keep skipping it) — and the settle retires the copy of the
// transaction that recorded it, leaving every OTHER live recorded
// member untouched.

import (
	"context"
	"fmt"
	"testing"

	"github.com/panjf2000/ants/v2"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

// TestIssue25590RollbackHandoffRetiresRecordedNamesAtQueueSettle drives
// the reviewer's loop on one OPEN transaction with a prior RECORDED
// write standing before every failed statement: record an S3 object in
// the admitted statement, end the statement through the frontend's
// RollbackLastStatement, let the boundary's queue handoff move the
// recorded object to the engine's lossless queue, then drain. The
// drain's settle must retire the transaction's recorded-name copy in
// the same worker step — the recorded set returns to the live prior
// member every round — while the file still exists the member keeps
// its late-handoff protection, and after the settle a fresh producer
// event on the same name is bookable again exactly like any fresh
// name.
func TestIssue25590RollbackHandoffRetiresRecordedNamesAtQueueSettle(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()

	const rounds = 6
	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	unpublishedPendingAdmissionLimit = 2
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
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

	// The standing prior recorded write — the LIVE-record class the
	// settle must never touch: its member, its file and its owned
	// ledger membership all survive every rolled-back statement and
	// every queue retire below.
	admit()
	priorStats := mockStatsList(t, 1)[0]
	priorName := priorStats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, priorName))
	require.NoError(t, record(priorName, priorStats))
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	var settledName string
	for round := 0; round < rounds; round++ {
		roundStats := mockStatsList(t, 1)[0]
		roundName := roundStats.ObjectName().String()
		require.NoError(t, writeObjectToFS(ctx, failFS, roundName))
		settledName = roundName

		// The failed statement: it records its S3 object first, runs
		// into the later statement error, and the frontend rolls the
		// whole statement back through the production entry point.
		admit()
		require.NoError(t, record(roundName, roundStats),
			"round %d: the statement records its S3 object before its failure", round)
		writesBeforeRollback := liveEntryCount(txn)
		require.NoError(t, txn.RollbackLastStatement(ctx),
			"round %d: the statement rollback succeeds", round)
		txn.EndStatement()
		txn.UpdateSnapshotWriteOffset()

		// The rollback's queue handoff moved exactly the recorded
		// object of the rolled-back statement; the prior statements'
		// records stay in the workspace exactly as before.
		require.Equal(t, []string{roundName}, issue25590QueuedNames(e),
			"round %d: the rolled-back statement's recorded object reached the engine's lossless queue", round)
		require.Equal(t, writesBeforeRollback-1, liveEntryCount(txn),
			"round %d: the rollback restored the workspace exactly around the prior statements' records", round)
		issue25590RequireKept(t, failFS, roundName,
			fmt.Sprintf("round %d: the handed-off object's file still exists before the drain settles it", round))

		// Late-handoff safety while the file exists: the recorded
		// member still owns the name's lifecycle, so every funnel and
		// the remote adoption keep skipping it and nothing new books
		// capacity or joins the pending cleanup list.
		txn.Lock()
		memberKept := txn.hasRecordedUnpublishedObjectName(roundName)
		txn.Unlock()
		require.True(t, memberKept,
			"round %d: the recorded member outlives the handoff exactly as long as its FILE — the still-existing object keeps its late-handoff protection", round)
		require.NoError(t, txn.AdoptRemoteTransferredObjectNames([]string{roundName}),
			"round %d: the adoption probe itself must succeed", round)
		_, liveLedger, inFlight := e.InspectUnpublishedPendingForTest()
		require.Zero(t, inFlight,
			"round %d: the recorded member's skip means the late adoption books NO in-flight unit", round)
		require.Equal(t, 1, liveLedger,
			"round %d: the ledger holds exactly the prior recorded write's owned membership mid-window", round)
		txn.RetainUnrecordedS3Objects(
			[]string{roundName},
			fmt.Sprintf("round %d: the late retain-funnel probe on the still-recorded queued member", round))
		pending, reserved := txn.UnsettledCleanupOwnershipForTest()
		require.Empty(t, reserved,
			"round %d: no reservation can exist for a recorded member's name — its record converted the hook at creation", round)
		require.Empty(t, pending,
			"round %d: the retain funnel skipped the recorded member — the record's lifecycle, not the orphan machinery, owns the queued object", round)

		// THE SETTLE: the queue's drain deletes the physical file, so
		// the retain/handoff/adoption protection has nothing left to
		// protect and the drain retires the transaction's recorded-name
		// copy of the same name in the same worker step.
		require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
			"round %d: the drain settles the handed-off object", round)
		issue25590RequireGone(t, failFS, roundName,
			fmt.Sprintf("round %d: the queued object's file is physically deleted", round))
		require.Empty(t, issue25590QueuedNames(e),
			"round %d: the queue is empty after the settle", round)
		require.Equal(t, map[string]struct{}{priorName: {}},
			issue25590RecordedSnapshot(txn),
			"round %d: the queue settle retired the transaction's recorded-name copy — an OPEN transaction must keep the set at its LIVE obligations, never its failed-statement history (issue #25590 round-38 finding)", round)

		queuedAfter, liveAfter, inFlightAfter := e.InspectUnpublishedPendingForTest()
		require.Empty(t, queuedAfter)
		require.Equal(t, 1, liveAfter,
			"round %d: the prior recorded write's owned membership survived the settle untouched", round)
		require.Zero(t, inFlightAfter)
	}

	// A fresh producer event on the just-settled name is bookable
	// again exactly like any fresh name — the retirement ended the
	// name's recorded lifecycle, and the tolerated no-op delete retires
	// the re-taken name's cleanup back to empty.
	txn.RetainUnrecordedS3Objects(
		[]string{settledName},
		"the fresh producer-event probe on the settled name")
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Equal(t, []string{settledName}, pending,
		"the settled name re-entered the pending cleanup list on its fresh producer event")
	require.Empty(t, reserved)
	txn.Lock()
	txn.handoffPendingObjectCleanup()
	txn.Unlock()
	require.Equal(t, []string{settledName}, issue25590QueuedNames(e),
		"the fresh producer event's name moved to the engine's queue at the quiesced boundary")
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the drain tolerates the already-deleted file exactly as production")
	require.Empty(t, issue25590QueuedNames(e))
	require.Equal(t, map[string]struct{}{priorName: {}},
		issue25590RecordedSnapshot(txn),
		"the recorded set still carries exactly the LIVE prior obligation")

	// The standing prior recorded write ends fully alive: member, file
	// and owned ledger membership all intact.
	issue25590RequireKept(t, failFS, priorName,
		"the prior recorded write's file survived every rolled-back statement and every queue retire")
	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(priorName),
		"the prior recorded write keeps its recorded membership")
	txn.Unlock()
	_, liveFinal, inFlightFinal := e.InspectUnpublishedPendingForTest()
	require.Equal(t, 1, liveFinal,
		"the ledger ends at exactly the prior recorded write's owned membership")
	require.Zero(t, inFlightFinal)
	require.Empty(t, issue25590QueuedNames(e))
}

// TestIssue25590PoolHandoffTailRetiresRecordedNamesAtQueueSettle pins
// the same settle retirement on the second recorded-name queue path: a
// released GC pool refuses the gcFiles submission, so the whole unsent
// batch — names the recording transaction still records — transfers to
// the engine's lossless queue and the pool worker's own round-37 retire
// never runs. The drain's settle must retire the recorded-name copy for
// exactly that handoff too, while the prior recorded member stays
// untouched and the pre-settle window keeps its late-handoff skips.
func TestIssue25590PoolHandoffTailRetiresRecordedNamesAtQueueSettle(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()
	restoreScheduling := issue25590FreezeUnpublishedDrainScheduling(t, e)
	defer restoreScheduling()

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }
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

	// The standing prior recorded write, outside the handoff.
	admit()
	priorStats := mockStatsList(t, 1)[0]
	priorName := priorStats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, priorName))
	require.NoError(t, record(priorName, priorStats))
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	// The GC'd object: recorded by this OPEN transaction, then handed
	// to the compaction-style GC front door with the pool already
	// released.
	gcStats := mockStatsList(t, 1)[0]
	gcName := gcStats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, gcName))
	admit()
	require.NoError(t, record(gcName, gcStats),
		"the transaction records the object whose GC will hit the released pool")
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	released, err := ants.NewPool(1)
	require.NoError(t, err)
	released.Release()
	e.gcMu.Lock()
	require.Nil(t, e.gcPool)
	e.gcPool = released
	e.gcMu.Unlock()
	t.Cleanup(func() {
		e.gcMu.Lock()
		e.gcPool = nil
		e.gcMu.Unlock()
	})

	require.NoError(t, txn.GCObjsByStats(gcStats),
		"the released pool refuses the submission and the batch tail hands off losslessly to the queue")
	require.Equal(t, []string{gcName}, issue25590QueuedNames(e),
		"the refused submission's whole batch tail transferred to the engine's lossless queue")
	issue25590RequireKept(t, failFS, gcName,
		"the handed-off object's file still exists before the drain settles it")

	// Late-handoff safety in the pre-settle window: the recorded member
	// still owns the name's lifecycle.
	txn.Lock()
	memberKept := txn.hasRecordedUnpublishedObjectName(gcName)
	txn.Unlock()
	require.True(t, memberKept,
		"the recorded member outlives the pool-failure handoff exactly as long as its FILE")
	require.NoError(t, txn.AdoptRemoteTransferredObjectNames([]string{gcName}),
		"the adoption probe itself must succeed")
	_, _, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, inFlight,
		"the recorded member's skip means the late adoption books NO in-flight unit")
	txn.RetainUnrecordedS3Objects(
		[]string{gcName},
		"the late retain-funnel probe on the still-recorded queued member")
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, reserved)
	require.Empty(t, pending,
		"the retain funnel skipped the recorded member — the orphan machinery must not take a still-recorded name")

	// THE SETTLE retires the recorded-name copy the pool worker never
	// got to retire.
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the drain settles the handed-off tail")
	issue25590RequireGone(t, failFS, gcName,
		"the handed-off object's file is physically deleted")
	require.Empty(t, issue25590QueuedNames(e))
	require.Equal(t, map[string]struct{}{priorName: {}},
		issue25590RecordedSnapshot(txn),
		"the queue settle also retired the recorded-name copy of the pool-failure tail — the submit-refused handoff must not convert a recorded member into failed history (issue #25590 round-38 finding)")
	_, liveAfter, inFlightAfter := e.InspectUnpublishedPendingForTest()
	require.Equal(t, 1, liveAfter,
		"the prior recorded write's owned membership survived the settle untouched")
	require.Zero(t, inFlightAfter)

	// The prior recorded write ends fully alive.
	issue25590RequireKept(t, failFS, priorName,
		"the prior recorded write's file survived the pool-failure handoff and the queue retire")
	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(priorName),
		"the prior recorded write keeps its recorded membership")
	txn.Unlock()
}
