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

// Round-41 (issue #25590 review of 2026-10-25, finding 1): the round-40
// terminal detach ended the QUEUE's recorded-owner registrations, but a
// GC job the pool ALREADY accepted keeps a much stronger reference - the
// settle callback the gcFiles submission installs captured the whole
// *Transaction for its two post-delete steps (the ledger settle and the
// recorded-name retire) instead of capturing the state those steps
// actually need. Intermediate-object compaction (compactDeletionOnObjsLocked
// -> GCObjsByStats) and dropped-table cleanup (GCObjsByIdxRange) submit
// such jobs on an OPEN transaction's behalf; when stalled storage parks
// the accepted worker's delete - DeleteUnpublishedObjects bounds each
// attempt but the stall window can outlive that bound by retrying -
// and the transaction then completes (FinalizeCommit, which never joins
// these jobs), the parked callback retains the completed transaction's
// entire graph: the statement-offset history, the writes backing slice
// and the process reference. The five-worker default pool bounds the
// NUMBER of such parked jobs, never the size of the graphs they keep
// alive, so repeated failed compaction during one storage outage
// accumulates unbounded completed-transaction memory.
//
// The regression below drives the reviewer's exact lifetime on the
// production front door: a recorded S3 object, the GCObjsByStats
// submission the pool accepts, the storage stall parking the worker
// inside its delete, then the production FinalizeCommit terminal. The
// weak reference the probe drops proves the only root left after the
// terminal is the worker's own settle callback - the parent tree keeps
// the completed transaction alive through it, the fixed tree collects
// the transaction while the stalling worker stays parked - and the
// release leg proves the settled cleanup obligation still completes
// losslessly with nothing left of the transaction to retire.

import (
	"context"
	"runtime"
	"testing"
	"time"
	"weak"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

// newIssue25590R41ProbeTransaction builds the retention probe's
// transaction with the same shape newTransactionWithActivePKTableForTest
// produces, with ONE difference that the probe's weak check depends on:
// its operator records NO expectation whose return value is the
// transaction itself. The gomock controller outlives the whole test
// (NewController registers its Finish on t.Cleanup), and every
// expectation's receiver and return values stay strongly reachable
// through it until the test ends - an operator returning the workspace
// would therefore root the probe's transaction forever and hide exactly
// the retention this regression measures. Every return value here is a
// value type, so after the probe drops its own reference the only root
// left on the parent tree is the accepted GC worker's settle callback.
func newIssue25590R41ProbeTransaction(t *testing.T, pkName string) *Transaction {
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
	op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("r41-probe")}).AnyTimes()
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

// unarmDeleteBlock lifts the deletion stall for FUTURE deletion calls:
// every call already parked inside armDeleteBlock's barrier keeps
// waiting on the channel it captured at entry (only the barrier's
// release ends that stall), while the transaction's own terminal - whose
// tail deletions this probe never wants parked - can no longer join the
// stall and deadlock against it.
func (fs *issue25590PostPersistFailFS) unarmDeleteBlock() {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.deleteBlock = nil
}

// issue25590CollectedEventually gives the garbage collector bounded
// full-cycle opportunities to observe that nothing reachable holds the
// weakly referenced transaction. The probe drops every strong reference
// first, so a weak pointer that stays live across the whole budget is
// exactly the retention this regression rejects.
func issue25590CollectedEventually(ref weak.Pointer[Transaction]) bool {
	for i := 0; i < 24; i++ {
		runtime.GC()
		if ref.Value() == nil {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// TestIssue25590AcceptedGCWorkerStallDoesNotRetainCompletedTransaction
// drives the reviewer's lifetime end to end: the pool ACCEPTS the GC
// job of a recorded intermediate object, the storage stall parks the
// worker inside its delete, and the OPEN transaction then completes
// through the production FinalizeCommit terminal while the worker stays
// parked. The settle state the accepted callback keeps must stay
// INDEPENDENTLY OWNED - the admission-bounded recorded-name holder plus
// the engine - so the completed transaction's graph stays collected for
// the rest of the outage, while the stall's own cleanup obligation
// survives untouched: after the outage ends, the recovered worker deletes
// the file, settles the ledger and finds nothing of the transaction
// left to retire, and the queue, the registry and the ledger all close
// empty.
func TestIssue25590AcceptedGCWorkerStallDoesNotRetainCompletedTransaction(t *testing.T) {
	colexec.NewServer("")
	txn := newIssue25590R41ProbeTransaction(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	ctx := context.Background()
	mp := txn.proc.Mp()

	e := txn.engine
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	// A REAL pool: the gcFiles submission must be accepted - this probe
	// measures what an ACCEPTED worker retains, not the refused
	// submission's queue handoff the round-38 regression already pins.
	pool, err := ants.NewPool(2)
	require.NoError(t, err)
	e.gcMu.Lock()
	e.gcPool = pool
	e.gcMu.Unlock()
	t.Cleanup(pool.Release)

	// The single statement records its S3 object exactly as production
	// does, so the accepted GC settles a real recorded member with real
	// ledger membership.
	issue25590AdmitBoundary(t, txn, ctx)
	stats := mockStatsList(t, 1)[0]
	name := stats.ObjectName().String()
	require.NoError(t, writeObjectToFS(ctx, failFS, name),
		"the recorded object's file exists before its GC is scheduled")
	recBat := colexec.AllocCNS3ResultBat(false)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(
		mp, false, recBat, false, stats))
	require.NoError(t, txn.WriteFile(
		INSERT, 1, 7, 42, "db", "tbl", name, recBat, txn.tnStores[0]),
		"the statement records its S3 object before the GC is scheduled")
	recBat.Clean(mp)
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	txn.Lock()
	require.True(t, txn.hasRecordedUnpublishedObjectName(name),
		"the recorded write is a member of the recorded-name set before its GC")
	txn.Unlock()
	queued, live, inFlight := e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Equal(t, 1, live,
		"the record choke booked the member's owned ledger membership")
	require.Zero(t, inFlight)

	// THE STALL: every deletion parks, and the pool ACCEPTS the GC job -
	// the exact accepted-worker window this round closes.
	releaseStall := failFS.armDeleteBlock()
	require.NoError(t, txn.GCObjsByStats(stats),
		"the pool accepts the intermediate object's GC submission")
	require.Eventually(t, func() bool {
		return failFS.blockedDeleteCalls() >= 1
	}, 10*time.Second, 10*time.Millisecond,
		"the accepted worker must be parked inside the stalled delete")
	// The transaction's own terminal must never join the stall.
	failFS.unarmDeleteBlock()

	// The pre-settle window keeps every ownership exactly as it was: the
	// member still owns the name, the file still exists and the ledger
	// still counts the member live.
	txn.Lock()
	memberKept := txn.hasRecordedUnpublishedObjectName(name)
	txn.Unlock()
	require.True(t, memberKept,
		"the parked worker settled nothing yet - the recorded member keeps the name's lifecycle")
	issue25590RequireKept(t, failFS, name,
		"the stalled delete touched no file before the transaction ends")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Equal(t, 1, live)
	require.Zero(t, inFlight)

	// COMMIT terminal - the production FinalizeCommit, which never joins
	// the in-flight GC jobs: the release classifies the recorded member
	// as published and drops the copy while the worker stays parked.
	txn.FinalizeCommit(ctx)

	txn.Lock()
	memberKept = txn.hasRecordedUnpublishedObjectName(name)
	txn.Unlock()
	require.False(t, memberKept,
		"the commit terminal released the recorded-name copy while the worker stayed parked")
	require.Zero(t, issue25590QueuedOwnerEntriesFor(e, txn),
		"a committed transaction owns no queued recorded-name entry")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued,
		"the committed object's settle obligation lives only in the parked worker")
	require.Zero(t, live,
		"the terminal release retired the member's owned ledger membership as published")
	require.Zero(t, inFlight)

	// THE ROUND-41 CONTRACT: the probe drops its own reference and the
	// parked worker's settle state must keep NOTHING of the completed
	// transaction. On the fixed tree the weak reference dies while the
	// worker stays parked; on the parent tree the accepted callback keeps
	// the whole completed graph reachable through the stall
	// (issue #25590 round-41 finding).
	deadTxn := weak.Make(txn)
	txn = nil

	collected := issue25590CollectedEventually(deadTxn)
	require.True(t, collected,
		"the accepted GC worker's settle state must be independently owned - a cleanup job parked on stalled storage may not retain the completed transaction's statement history, writes backing slice and process reference (issue #25590 round-41 finding)")

	// RECOVERY: the outage ends, and the recovered worker alone finishes
	// the obligation - the file deletion, the ledger settle and the
	// recorded-name retire (which finds the terminal already emptied the
	// holder) - with the transaction long gone.
	releaseStall()
	// The failFS records a delete call the moment it passes the parking
	// barrier, BEFORE the underlying unlink runs, so a recorded name is
	// an in-flight attempt, not a completed deletion: the completion is
	// the file's disappearance itself (CI run 40176540982 attempt 3
	// stat'ed the file alive inside exactly this window), and only that
	// terminal state grounds the gone-check and the empty-ledger
	// assertions below.
	require.Eventually(t, func() bool {
		_, statErr := failFS.StatFile(ctx, name)
		return moerr.IsMoErrCode(statErr, moerr.ErrFileNotFound)
	}, 10*time.Second, 10*time.Millisecond,
		"the recovered worker completes the stalled batch's file deletion")
	issue25590RequireGone(t, failFS, name,
		"the recovered worker deleted the stalled object's file")
	queued, live, inFlight = e.InspectUnpublishedPendingForTest()
	require.Empty(t, queued)
	require.Zero(t, live)
	require.Zero(t, inFlight)
}
