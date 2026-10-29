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

// Round-58 (issue #25590 review of PR #30988 head 7c75f3e07b, P2 at
// pkg/vm/engine/disttae/txn.go:3906): the commit terminal's
// published-class release classified EVERY still-recorded name as
// published, but intermediate-object compaction (the
// compactDeletionOnObjsLocked tail's GCObjsByStats at txn.go:5274, after
// releaseWorkspaceEntryBatchLocked removed the originals' entries) and
// dropped-table cleanup (mergeTxnWorkspaceLocked's GCObjsByIdxRange)
// submit the delete of names the commit payload no longer carries, on an
// OPEN transaction's behalf: the submitter's recorded-name copy keeps
// the obsolete name until the accepted worker settles, so a transaction
// that commits while its parked worker stalls inside
// DeleteUnpublishedObjects releases the obsolete object's final
// workspace claim — the membership, live and its capacity — although the
// object's cleanup obligation is still outstanding in the pool. With the
// limit pinned at two, ONE stalled obsolete object then coexisted with
// TWO further admitted objects — three retained obligations behind a
// two-object bound, exactly the overshoot the r57 distinct-engine
// control shape refuses. I4 and §5.5 of the design (retained obligation
// population = queued + owned + admitted-in-flight alike) require every
// retained obligation to count, and the round-41 record already made the
// accepted worker's settle state independently owned, so the capacity
// claim of a name whose delete a pool worker accepted must live the
// same way: engine-owned, keyed by name alone, and retired exactly at
// the worker's settle — never by the completed transaction's terminal
// release, which may not retain the Transaction either (round-40/41).
//
// The regressions below compile against the pre-fix head: every
// assertion is behavioral (the live counter, the reservation gate's
// verdicts, the physical file's fate), so the RED run fails exactly at
// the review's capacity contract and the fixed tree passes the same
// assertions.
//
//   - the production-path repro drives the reviewer's exact lifetime:
//     a recorded object the workspace entry no longer carries, a GC
//     submission the pool ACCEPTS, a storage stall parking the accepted
//     worker inside its delete, and the production FinalizeCommit
//     terminal — the stalled obligation must stay counted the whole
//     outage and the bound must refuse the overshoot;
//   - the settle-first order proves the settlement itself — never a
//     terminal — returns the capacity, so the retained claim cannot
//     leak past the delete's completion;
//   - the pool-failure order proves the refused submission's queue
//     handoff moves the obligation without double-counting it, so the
//     retained class owns nothing the queue already owns.

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// newIssue25590R58ProbeTransaction builds a production-shaped probe the
// r58 regressions drive through WriteFile, GCObjsByStats and
// FinalizeCommit — the same shape newIssue25590R41ProbeTransaction
// pins for the retention probes, with this group's own txn id so the
// ledger's per-workspace claims never cross the r41 recordings.
func newIssue25590R58ProbeTransaction(t *testing.T, pkName string) *Transaction {
	t.Helper()
	proc := testutil.NewProc(t)
	txn := &Transaction{
		proc:        proc,
		engine:      &Engine{},
		tableOps:    newTableOps(),
		databaseOps: newDbOps(),
	}
	ctrl := gomock.NewController(t)
	op := mock_frontend.NewMockTxnOperator(ctrl)
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("r58-probe")}).AnyTimes()
	op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
	op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{}).AnyTimes()
	op.EXPECT().IsSnapOp().Return(false).AnyTimes()
	txn.op = op

	db := &txnDatabase{
		op:           op,
		databaseId:   7,
		databaseName: "db",
	}
	txn.databaseOps.addCreateDatabase(genDatabaseKey(1, "db"), 0, db)
	txn.tableOps.addCreateTable(
		genTableKey(1, "tbl", 7, "db"),
		0,
		&txnTable{
			accountId: 1,
			tableId:   42,
			tableName: "tbl",
			db:        db,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: pkName},
				},
				Pkey: &pbplan.PrimaryKeyDef{
					PkeyColName: pkName,
				},
			},
		},
	)
	return txn
}

// r58RecordStats drives the production statement that records the given
// S3 objects — the same WriteFile choke path the r41 probes use — with
// each object's file made durable before its record.
func r58RecordStats(
	t *testing.T, txn *Transaction, ctx context.Context,
	statsList []objectio.ObjectStats,
) {
	t.Helper()
	issue25590AdmitBoundary(t, txn, ctx)
	mp := txn.proc.Mp()
	for _, stats := range statsList {
		name := stats.ObjectName().String()
		require.NoError(t, writeObjectToFS(ctx, txn.engine.fs, name))
		recBat := colexec.AllocCNS3ResultBat(false)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(
			mp, false, recBat, false, stats))
		require.NoError(t, txn.WriteFile(
			INSERT, 1, 7, 42, "db", "tbl",
			name, recBat, txn.tnStores[0]),
			"the statement records its S3 object before its GC is scheduled")
		recBat.Clean(mp)
	}
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()
}

// r58RetireRecordedEntry mirrors compaction's payload drop: the
// workspace entry that carried the recorded object is retired exactly
// the way the compactDeletionOnObjsLocked tail retires the originals'
// batches (releaseWorkspaceEntryBatchLocked), so the object's name is
// recorded WITHOUT belonging to the commit payload — the finding's
// obsolete-object state.
func r58RetireRecordedEntry(t *testing.T, txn *Transaction, name string) {
	t.Helper()
	txn.Lock()
	defer txn.Unlock()
	for i := range txn.writes {
		if txn.writes[i].bat != nil && txn.writes[i].fileName == name {
			txn.releaseWorkspaceEntryBatchLocked(i)
			return
		}
	}
	t.Fatalf("no workspace entry carries the recorded object %q", name)
}

// r58RequireAcceptedParkedGC submits the transaction's object delete
// through the production GCObjsByStats front door, verifies the pool
// ACCEPTED the job, and parks the worker inside the stalled delete — the
// exact accepted-but-unsettled window this round closes. The stall's
// release is returned; unarmDeleteBlock already lifted the park for the
// transaction's own terminal, whose tail this probe never wants joined
// to the stall.
func r58RequireAcceptedParkedGC(
	t *testing.T, txn *Transaction, failFS *issue25590PostPersistFailFS,
	statsList []objectio.ObjectStats,
) (releaseStall func()) {
	t.Helper()
	release := failFS.armDeleteBlock()
	require.NoError(t, txn.GCObjsByStats(statsList...),
		"the pool accepts the obsolete object's GC submission")
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() >= 1
	}, 10*time.Second, 10*time.Millisecond,
		"the accepted worker must be parked inside the stalled delete")
	return release
}

// r58PinAdmissionLimit pins the retained-obligation bound for one probe
// and schedules its restore.
func r58PinAdmissionLimit(t *testing.T, limit int) {
	t.Helper()
	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	unpublishedPendingAdmissionLimit = limit
}

// r58NewAcceptedPool arms the engine with a REAL GC pool so the gcFiles
// submission is ACCEPTED — the probes measure what an accepted worker's
// obligation retains, not the refused submission's queue handoff.
func r58NewAcceptedPool(t *testing.T, e *Engine, size int) *ants.Pool {
	t.Helper()
	pool, err := ants.NewPool(size)
	require.NoError(t, err)
	e.gcMu.Lock()
	e.gcPool = pool
	e.gcMu.Unlock()
	t.Cleanup(pool.Release)
	return pool
}

// TestIssue25590AcceptedGCDeletionObligationKeepsCapacityThroughCommit
// drives the reviewer's exact lifetime on the production surfaces: the
// statement records its S3 object, compaction retires the entry that
// carried it (the payload drops the name), the pool ACCEPTS the object's
// delete and stalls inside it, and the transaction commits through the
// production FinalizeCommit terminal while the worker stays parked. The
// retained obligation must stay counted the whole outage — the obsolete
// object's capacity belongs to the accepted delete until the worker
// settles it — so with the bound pinned at two, exactly ONE further
// reservation fits beside the stalled obligation and the second is
// refused fail-closed. On the fixed tree the recovered worker's settle
// alone returns the capacity with the ledger and queue balanced.
func TestIssue25590AcceptedGCDeletionObligationKeepsCapacityThroughCommit(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	r58NewAcceptedPool(t, e, 2)
	r58PinAdmissionLimit(t, 2)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	r58RecordStats(t, txn, ctx, stats)
	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(name),
		"the obsolete object's record is what the accepted worker may settle")
	txn.Unlock()
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Equal(t, 1, live,
		"the record choke booked the recorded object's ledger membership")
	require.Zero(t, inFlight)

	// Compaction's payload drop: the entry carrying the object retires,
	// so the commit payload no longer contains the name.
	r58RetireRecordedEntry(t, txn, name)

	// THE STALL: the deletion parks, the pool ACCEPTS the GC job, and
	// the transaction then commits while the worker stays parked.
	releaseStall := r58RequireAcceptedParkedGC(t, txn, failFS, stats)
	failFS.unarmDeleteBlock()

	txn.FinalizeCommit(ctx)

	txn.Lock()
	memberKept := txn.hasRecordedUnpublishedObjectName(name)
	txn.Unlock()
	require.False(t, memberKept,
		"the commit terminal released the recorded-name copy while the worker stayed parked")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued,
		"the stalled obligation holds no queue entry")
	require.Equal(t, 1, live,
		"the commit terminal kept the stalled delete obligation counted — an accepted GC job holds the obsolete object's capacity until its settle completes the deletion (I4 admitted-in-flight)")
	require.Zero(t, inFlight)

	// THE OVERSHOOT COUNTEREXAMPLE the review priced: at limit two, the
	// stalled obsolete object plus TWO further reservations — exactly
	// what the pre-fix terminal's capacity release admitted.
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-further-1"),
		"exactly one further reservation fits beside the stalled obligation")
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r58-further-2"),
		"the second further reservation exceeds the bound — the accepted GC job's obligation still occupies capacity")
	e.abandonUnpublishedPendingReservations([]string{"issue25590-r58-further-1"})

	// RECOVERY: the outage ends, and the worker's settle alone ends the
	// obligation — the file goes, the settle's storage-deletion class
	// retires the retained membership, and the bound re-opens.
	releaseStall()
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, name)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the recovered worker completed the stalled batch's file deletion")
	issue25590RequireGone(t, failFS, name,
		"the recovered worker deleted the stalled object's file")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, inFlight)
	require.Zero(t, live,
		"the accepted worker's settle retired the retained obligation's ledger membership")
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-further-3"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-further-4"),
		"the bound's full capacity returned with the settled obligation's retirement")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r58-further-3", "issue25590-r58-further-4"})
}

// TestIssue25590GCDeletionSettleBeforeCommitReturnsCapacityToBound pins
// the ALTERNATE order: when the accepted worker finishes its delete
// before the transaction ends, the settle — the storage-deletion
// whole-membership class — alone returns the obsolete object's capacity,
// and the later commit terminal must find nothing left to release and
// release nothing behind a settled obligation.
func TestIssue25590GCDeletionSettleBeforeCommitReturnsCapacityToBound(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	r58NewAcceptedPool(t, e, 2)
	r58PinAdmissionLimit(t, 2)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	r58RecordStats(t, txn, ctx, stats)
	r58RetireRecordedEntry(t, txn, name)

	// No stall: the accepted worker deletes the file and settles the
	// ledger before the transaction's own terminal runs. The settle's
	// whole-membership retire follows the delete's return, so the probe
	// waits for the LEDGER to reach zero — the same sync point r41's
	// recovery leg uses.
	require.NoError(t, txn.GCObjsByStats(stats...))
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, name)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the accepted worker completes the file deletion")
	require.Eventually(t, func() bool {
		_, l, _ := e.InspectUnpublishedPendingForTest()
		return l == 0
	}, 10*time.Second, 10*time.Millisecond,
		"the settle class alone returns the deleted object's capacity")
	_, live, _ := e.InspectUnpublishedPendingForTest()
	require.Zero(t, live,
		"the settle class alone returns the deleted object's capacity")
	txn.Lock()
	memberKept := txn.hasRecordedUnpublishedObjectName(name)
	txn.Unlock()
	require.False(t, memberKept,
		"round-37's rule holds: the settle retired the transaction's recorded-name copy too")

	// The terminal after the settle releases exactly nothing, and the
	// bound's full capacity is already back.
	txn.FinalizeCommit(ctx)
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, live,
		"a terminal after a settled obligation frees no further capacity")
	require.Zero(t, inFlight)
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-settled-1"))
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-settled-2"),
		"the bound's full capacity returned with the delete's settlement")
	e.abandonUnpublishedPendingReservations([]string{
		"issue25590-r58-settled-1", "issue25590-r58-settled-2"})
}

// TestIssue25590GCPoolSubmitFailureQueuesObligationWithoutDoubleCount
// pins the refused submission's order: a pool released between the
// gcFiles admission and its batch submit must hand the WHOLE unsent tail
// to the engine's lossless queue (round-13's own contract) without the
// accepted-job obligation class retaining anything the queue now owns —
// the queued name alone holds the bound's space, capacity is moved, not
// added.
func TestIssue25590GCPoolSubmitFailureQueuesObligationWithoutDoubleCount(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R58ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS
	// Released BEFORE the submission: admission passes (the engine is
	// open, the pool pointer snapshotted) but every batch submit fails,
	// so the whole name set transfers to the queue — deterministically,
	// because a released pool cannot start the scheduled drain either.
	pool := r58NewAcceptedPool(t, e, 2)
	pool.Release()

	r58PinAdmissionLimit(t, 2)

	stats := mockStatsList(t, 1)
	name := stats[0].ObjectName().String()
	r58RecordStats(t, txn, ctx, stats)
	r58RetireRecordedEntry(t, txn, name)

	require.NoError(t, txn.GCObjsByStats(stats...),
		"the refused submission's lossless queue handoff is not an error")
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Equal(t, []string{name}, queued,
		"the refused submission transferred the names to the lossless queue")
	require.Zero(t, live,
		"the queue owns the obligation whole — no owned membership and no accepted-job claim may double-count it")
	require.Zero(t, inFlight)

	// The queued obligation still prices exactly one bound unit.
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-poolfail-1"),
		"one further reservation fits beside the queued obligation")
	require.Error(t, e.reserveUnpublishedPendingObject("issue25590-r58-poolfail-2"),
		"the second further reservation exceeds the bound — the queued name holds its unit")
	e.abandonUnpublishedPendingReservations([]string{"issue25590-r58-poolfail-1"})

	// The queue's own drain discharges the obligation: the file goes, the
	// queue entry retires, and the bound re-opens.
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx))
	issue25590RequireGone(t, failFS, name,
		"the queue's drain deleted the refused submission's object file")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
	require.NoError(t, e.reserveUnpublishedPendingObject("issue25590-r58-poolfail-3"),
		"the drained obligation returned its capacity to the bound")
	e.abandonUnpublishedPendingReservations([]string{"issue25590-r58-poolfail-3"})
}
