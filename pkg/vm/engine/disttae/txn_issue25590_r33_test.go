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

// TestIssue25590RollbackRecyclesUnrecordedReservations pins the
// round-33 pre-PR review's finding 2 against design r11's decision 3
// (the statement's failure path transfers ownership with NO storage
// call, collection deferred to the boundary's queue handoff and the
// drain). Adoption (txn.go AdoptRemoteTransferredObjectNames) reserves
// a per-object in-flight unit, and a record-preparation failure
// deliberately leaves the reservation outstanding - but the pre-fix
// STATEMENT rollback (RollbackLastStatement) neither drained those
// reservations nor transferred them to pending cleanup: reservation
// draining ran only at transaction/fragment terminals, so every failed
// statement of an OPEN transaction left its adopted name's in-flight
// unit booked forever. Repeated failed statements therefore accumulated
// in-flight units until the ENGINE-WIDE admission bound rejected other
// writers' spills - with healthy storage and an empty cleanup queue
// (the round-31 composed regression masked exactly this by ending with
// a full transaction Rollback).
//
// The regression drives the reviewer's loop under a SMALL admission
// bound, with a PRIOR RECORDED write standing before every failed
// statement: adopt a name, fail before recording, roll the statement
// back, drain cleanup, and retry - successfully - inside the SAME open
// transaction, round after round. The quiesced statement boundary must
// transfer the failed statement's unrecorded reservation to cleanup
// ownership exactly (reservation surface empty again, the name pending
// cleanup, its in-flight unit released) WITHOUT touching the prior
// recorded writes: their recorded membership, their files and their
// staged records all survive every rollback, and the queue's drain
// deletes exactly each round's orphan file.

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestIssue25590RollbackRecyclesUnrecordedReservations(t *testing.T) {
	txn, e, failFS, tbl := newR32MaterializingFixture(t)
	ctx := context.Background()
	mp := txn.proc.Mp()

	const bound = 5
	const rounds = 3

	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	unpublishedPendingAdmissionLimit = bound

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

	// The PRIOR RECORDED write: a statement before every failed round
	// records one object - the exact class a statement rollback must
	// never touch.
	admit()
	probeStats := mockStatsList(t, 1)[0]
	probeName := probeStats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, probeName))
	require.NoError(t, record(probeName, probeStats))
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()
	recordedPrior := []string{probeName}

	for round := 0; round < rounds; round++ {
		// The adopted, never-recorded name - a real file, so the
		// queue's drain proves its physical deletion.
		orphanStats := mockStatsList(t, 1)[0]
		orphanName := orphanStats.ObjectName().String()
		require.NoError(t, writeObjectToFS(ctx, failFS, orphanName))

		// The failed statement: it adopts the relayed name at its
		// intake (the reservation books the in-flight unit), then
		// fails before its record can land; the frontend's
		// RollbackLastStatement ends it.
		writesBefore := liveEntryCount(txn)
		admit()
		require.NoError(t, txn.AdoptRemoteTransferredObjectNames(
			[]string{orphanName}),
			"round %d: the boundary's admission must accept the adopted name", round)
		_, _, inFlightAdopted := e.InspectUnpublishedPendingForTest()
		require.Equal(t, 1, inFlightAdopted,
			"round %d sanity: the adoption holds exactly one in-flight unit", round)
		require.NoError(t, txn.RollbackLastStatement(ctx))
		txn.EndStatement()
		txn.UpdateSnapshotWriteOffset()

		// THE RECYCLE (round-33 finding 2): the quiesced statement
		// boundary transferred the failed statement's unrecorded
		// reservation to cleanup ownership - the reservation surface
		// is empty again and the adopted name is pending cleanup with
		// its in-flight unit released, so a retry inside the SAME open
		// transaction keeps its full admission budget.
		pending, reserved := txn.UnsettledCleanupOwnershipForTest()
		require.Empty(t, reserved,
			"round %d: the rolled-back statement's unrecorded reservations must recycle at the statement boundary - an open transaction that keeps failing statements must not accumulate in-flight units (issue #25590 round-33 finding 2)",
			round)
		require.Equal(t, []string{orphanName}, pending,
			"round %d: the recycled reservation transferred to cleanup ownership", round)
		_, live, inFlight := e.InspectUnpublishedPendingForTest()
		require.Zero(t, inFlight,
			"round %d: the failed statement's in-flight unit is released at the boundary", round)
		require.Len(t, recordedPrior, live-1,
			"round %d: the ledger holds the prior recorded writes plus exactly the recycled orphan's owned membership - the ownership transfer is capacity-neutral until the queue's drain deletes it",
			round)
		require.Contains(t, txn.recordedUnpublishedObjectNames, probeName,
			"round %d: the prior recorded write keeps its recorded membership", round)
		require.Equal(t, writesBefore, liveEntryCount(txn),
			"round %d: the rollback restored the workspace exactly around the prior statements' entries", round)

		// The boundary's queue handoff and the engine's bounded drain
		// retire the round's orphan: collection is deferred exactly the
		// design's decision 3 shapes, and the file is physically gone
		// while every prior recorded write's file stays.
		txn.Lock()
		txn.handoffPendingObjectCleanup()
		txn.Unlock()
		require.Equal(t, []string{orphanName}, issue25590QueuedNames(e),
			"round %d: exactly the recycled orphan reached the engine's queue", round)
		require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
			"round %d: the drain retires the recycled orphan", round)
		issue25590RequireGone(t, failFS, orphanName,
			fmt.Sprintf("round %d: the recycled reservation's file is deleted once the queue owns it", round))
		for _, prior := range recordedPrior {
			issue25590RequireKept(t, failFS, prior,
				fmt.Sprintf("round %d: the prior recorded write's file survives the same drain", round))
		}
		_, liveAfterDrain, _ := e.InspectUnpublishedPendingForTest()
		require.Len(t, recordedPrior, liveAfterDrain,
			"round %d: the drain retired the recycled orphan, leaving exactly the prior recorded writes' owned memberships",
			round)

		// THE RETRY succeeds inside the same open transaction: a fresh
		// statement adopts a fresh name and records it - on a saturated
		// pre-fix ledger this admission was the refusal the accumulated
		// in-flight units produced; after the recycle it must pass.
		retryStats := mockStatsList(t, 1)[0]
		retryName := retryStats.ObjectName().String()
		require.NoError(t, writeObjectToFS(ctx, failFS, retryName))
		admit()
		require.NoError(t, txn.AdoptRemoteTransferredObjectNames(
			[]string{retryName}),
			"round %d: the retry's admission must succeed - the recycled bound accepts the retry inside the same OPEN transaction",
			round)
		require.NoError(t, record(retryName, retryStats),
			"round %d: the retried statement records successfully", round)
		txn.EndStatement()
		txn.UpdateSnapshotWriteOffset()
		recordedPrior = append(recordedPrior, retryName)
	}

	// Three adopt-fail-rollback-drain-retry rounds later, the ledger is
	// exactly the transaction's RECORDED members: nothing of any failed
	// statement survived, and the transaction is still OPEN and
	// functional.
	pending, reserved := txn.UnsettledCleanupOwnershipForTest()
	require.Empty(t, pending, "no orphan remained anywhere in the transaction")
	require.Empty(t, reserved, "no reservation outlived any rolled-back statement")
	_, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Zero(t, inFlight)
	require.Len(t, recordedPrior, live,
		"the ledger holds exactly the recorded members' owned memberships")
	for _, prior := range recordedPrior {
		require.Contains(t, txn.recordedUnpublishedObjectNames, prior)
	}
}
