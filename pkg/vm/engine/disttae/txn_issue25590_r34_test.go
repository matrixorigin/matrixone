// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

// TestIssue25590FailedMergeStillHandsOffForInTxnRecovery pins the
// round-34 pre-PR review's finding against design r11's decision set:
// the admission boundary's pending-cleanup handoff rides inside the
// dump (txn.go dumpBatchLocked's opening call), and IncrStatementID
// only runs that dump when the MERGE succeeded - so a failing merge
// returned before the boundary's handoff, leaving the failed pass's
// retained replacements transaction-owned in the pending list. The
// engine's GC tick collects only its own queue, and a list full of
// retained replacements pins the admission bound's owned members: the
// retried merges could not reserve replacement capacity and failed
// again BEFORE the handoff, so a recovered storage service could never
// restore admission inside the still-open transaction - the prior
// regression manually handed the list off and lifted the bound to make
// its retry pass, masking exactly this gap.
//
// This regression drives the REAL statement admission/rollback
// ceremony end to end with NO manual handoff and NO bound lift: a
// record-stage fault fails the first pass after both replacements
// persisted (the failure retains them and preserves the marks), the
// failed admission's own boundary must hand the orphans to the
// engine's queue, removing ONLY that fault plus the engine's
// scheduled delete pass (no fault injection stays armed, no limit
// change) must let the SAME transaction's next admission complete the
// retried pass and then COMMIT.

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

func TestIssue25590FailedMergeStillHandsOffForInTxnRecovery(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	issue25590InitDumpableFixture(t, txn)
	// The round-21 binding: the process carries the transaction operator
	// (the structural workspace resolution the commit ceremony drives
	// performs), plus the commit-path operator surface.
	issue25590BindRound21Txn(t, txn)
	if mockOp, ok := txn.op.(*mock_frontend.MockTxnOperator); ok {
		mockOp.EXPECT().TxnOptions().Return(txnpb.TxnOptions{}).AnyTimes()
	}
	defer func() {
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
	}()

	ctx := context.Background()
	e := txn.engine
	e.pClient = PushClient{}
	failFS := issue25590SwapProcSharedFS(t, txn)
	e.fs = failFS

	compactPool, err := ants.NewPool(1)
	require.NoError(t, err)
	t.Cleanup(func() { compactPool.Release() })
	txn.compactWorker = compactPool
	gcPool, err := ants.NewPool(2)
	require.NoError(t, err)
	t.Cleanup(func() { gcPool.Release() })
	require.NoError(t, e.ResetGCWorkerPool(gcPool))

	dbv := txn.databaseOps.existAndActive(genDatabaseKey(1, "db"))
	require.NotNil(t, dbv)
	const (
		ctaID = uint64(44)
		ctbID = uint64(45)
	)
	newCtbl := func(id uint64, name string) *txnTable {
		return &txnTable{
			accountId: 1,
			tableId:   id,
			tableName: name,
			db:        dbv,
			tableDef: &pbplan.TableDef{
				Cols: []*pbplan.ColDef{
					{Name: "a", Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
					{Name: "v", Typ: pbplan.Type{Id: int32(types.T_int64)}},
					{Name: catalog.Row_ID,
						Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
				},
				Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "a"},
			},
		}
	}
	txn.tableOps.addCreateTable(genTableKey(1, "cta", 7, "db"), 0, newCtbl(ctaID, "cta"))
	txn.tableOps.addCreateTable(genTableKey(1, "ctb", 7, "db"), 0, newCtbl(ctbID, "ctb"))

	admit := func() { issue25590AdmitBoundary(t, txn, ctx) }

	// Statement 1: two raw entries, one per table; ONE forced dump
	// publishes both metadata batches in workspace order.
	const rowsPerTable = 100000
	admit()
	for _, tt := range []struct {
		id   uint64
		name string
	}{{ctaID, "cta"}, {ctbID, "ctb"}} {
		pks := make([]int64, rowsPerTable)
		vals := make([]int64, rowsPerTable)
		for i := range pks {
			pks[i] = int64(i + 1)
			vals[i] = int64(i + 1)
		}
		rowidVec, aerr := txn.batchAllocNewRowIds(rowsPerTable)
		require.NoError(t, aerr)
		defer rowidVec.Free(txn.proc.Mp())
		rowids := vector.MustFixedColNoTypeCheck[types.Rowid](rowidVec)
		bat := batch.NewWithSize(3)
		bat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a", "v"})
		rowidCol := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(rowidCol, rowids, nil, txn.proc.Mp()))
		aCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(aCol, pks, nil, txn.proc.Mp()))
		vCol := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(vCol, vals, nil, txn.proc.Mp()))
		bat.Vecs[0] = rowidCol
		bat.Vecs[1] = aCol
		bat.Vecs[2] = vCol
		bat.SetRowCount(rowsPerTable)
		txn.appendWorkspaceEntryLocked(Entry{
			typ:          INSERT,
			accountId:    1,
			databaseId:   issue25590DBID,
			tableId:      tt.id,
			databaseName: "db",
			tableName:    tt.name,
			bat:          bat,
		})
	}
	smallObjsDisarm := issue25590ArmKeyedFault(t, objectio.FJ_CNFlushSmallObjs, 20, "")
	txn.Lock()
	var pkCount int
	err = txn.dumpInsertBatchLocked(ctx, failFS, txn.GetSnapshotWriteOffset(), &pkCount)
	txn.Unlock()
	smallObjsDisarm()
	require.NoError(t, err)
	require.Equal(t, 2*rowsPerTable, pkCount)
	txn.EndStatement()

	type metaBatInfo struct {
		idx  int
		id   uint64
		name string
	}
	metas := []metaBatInfo{}
	for i := range txn.writes {
		entry := txn.writes[i]
		if entry.bat == nil || entry.bat.IsEmpty() ||
			entry.typ != INSERT || entry.bat.Attrs[0] != catalog.BlockMeta_BlockInfo {
			continue
		}
		metas = append(metas, metaBatInfo{idx: i, id: entry.tableId, name: entry.tableName})
	}
	require.Len(t, metas, 2,
		"the forced dump published exactly two metadata batches")
	require.NotEqual(t, metas[0].name, metas[1].name,
		"the fixture's two tables published distinct metadata batches")

	objCountOf := func(i int) (objectio.ObjectStats, objectio.ObjectStats, int, types.Blockid) {
		entry := txn.writes[metas[i].idx]
		statsCol, statsArea := vector.MustVarlenaRawData(entry.bat.Vecs[1])
		infoCol, infoArea := vector.MustVarlenaRawData(entry.bat.Vecs[0])
		n := len(statsCol)
		require.GreaterOrEqual(t, n, 2,
			"table %s's dump batch holds a clean object to re-publish", metas[i].name)
		objs := make([]objectio.ObjectStats, n)
		for j := 0; j < n; j++ {
			objs[j] = objectio.ObjectStats(statsCol[j].GetByteSlice(statsArea))
		}
		tailOffset := 0
		for j := 0; j < n-1; j++ {
			tailOffset += int(objs[j].BlkCnt())
		}
		blk := objectio.DecodeBlockInfo(
			infoCol[tailOffset].GetByteSlice(infoArea)).BlockID
		return objs[0], objs[n-1], n, blk
	}
	_, _, ctaCount, firstTailBlock := objCountOf(0)
	_, _, ctbCount, secondTailBlock := objCountOf(1)
	totalObjs := ctaCount + ctbCount

	// Statement 2: mark each table's TAIL object dirty through its final
	// block's first row.
	admit()
	for _, mt := range []struct {
		id   uint64
		blk  types.Blockid
		name string
	}{{metas[0].id, firstTailBlock, metas[0].name}, {metas[1].id, secondTailBlock, metas[1].name}} {
		delBat := batch.NewWithSize(2)
		delBat.SetAttributes([]string{objectio.PhysicalAddr_Attr, "a"})
		rowid := vector.NewVec(types.T_Rowid.ToType())
		require.NoError(t, vector.AppendFixedList(
			rowid, []types.Rowid{types.NewRowid(&mt.blk, 0)}, nil, txn.proc.Mp()))
		pkOne := vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(pkOne, []int64{1}, nil, txn.proc.Mp()))
		delBat.Vecs[0] = rowid
		delBat.Vecs[1] = pkOne
		delBat.SetRowCount(1)
		out := txn.deleteBatch(delBat, issue25590DBID, mt.id)
		t.Cleanup(func() { out.Clean(txn.proc.Mp()) })
	}
	require.Equal(t, 2, txn.deletedBlocks.size(),
		"each table's tail object carries one pending deletion")
	txn.EndStatement()

	allOriginalNames := make(map[string]bool, totalObjs)
	for _, i := range []int{metas[0].idx, metas[1].idx} {
		for _, name := range issue25590StatsNamesOfMeta(t, txn, i) {
			allOriginalNames[name] = true
		}
	}

	// The bound is pinned to EXACTLY the originals plus the two
	// replacements, and NEVER changes again: the failed pass's retained
	// replacements therefore fill it, and the recovery below must fit
	// back into the same limit once the scheduled cleanup deleted them.
	const recoveryLimit = 2
	recoveryAdmissionLimit := totalObjs + recoveryLimit
	knobSaved := unpublishedPendingAdmissionLimit
	t.Cleanup(func() { unpublishedPendingAdmissionLimit = knobSaved })
	unpublishedPendingAdmissionLimit = recoveryAdmissionLimit

	// Statement 3: the keyed record-stage fault refuses the SECOND-ordered
	// record after the first staged fully - the failure-atomic pass
	// retains both persisted replacements and preserves the marks. The
	// admission fails INSIDE the real ceremony; the frontend's statement
	// rollback follows it.
	secondTable := metas[1].name
	stageDisarm := issue25590ArmKeyedFault(
		t, objectio.FJ_CNWorkspaceRecordStageFailed, 0, secondTable)
	txn.StartStatement()
	require.ErrorContains(t, txn.IncrStatementID(ctx, false),
		"fault injection: workspace record staging failed",
		"the keyed record-stage failure fails the admission's merge")
	require.NoError(t, txn.RollbackLastStatement(ctx),
		"the frontend's statement rollback for the failed admission")
	txn.EndStatement()
	txn.UpdateSnapshotWriteOffset()

	require.Equal(t, 2, txn.deletedBlocks.size(),
		"the interrupted-admission rollback preserved the marks for the retried pass")

	// THE ROUND-34 PROOF, with ZERO manual handoff calls: the failed
	// admission's OWN boundary moved the retained replacements to the
	// engine's lossless queue, so nothing of the previous failure stays
	// transaction-owned behind the merge.
	orphanNames := issue25590QueuedNames(e)
	require.Len(t, orphanNames, recoveryLimit,
		"the failed admission's boundary handed BOTH orphaned replacements to the engine's queue WITHOUT any manual handoff - a merge-gated handoff strands them behind the failing merge forever (issue #25590 round-34 finding)")
	for _, name := range orphanNames {
		require.False(t, allOriginalNames[name],
			"the queued names are the FRESH orphaned replacements, not originals")
	}
	require.Empty(t, txn.pendingObjectCleanupNames(),
		"nothing stays transaction-owned where the engine's GC tick cannot collect it")
	queued, live, inFlight := issue25590LedgerNow(e)
	require.Equal(t, recoveryLimit, queued,
		"exactly the two orphaned replacements reached the engine queue")
	require.Equal(t, int64(totalObjs), live,
		"the enqueue retired the orphans' owned counts - the lossless queue carries their admission weight as queued members now")
	require.Zero(t, inFlight,
		"the retained replacements hold no in-flight reservation")
	require.Equal(t, recoveryAdmissionLimit, queued+int(live)+int(inFlight),
		"the transfer stays capacity-neutral: the admission total pins exactly the originals plus the two orphans at the unchanged bound")

	// Remove ONLY the record-stage fault: the storage failure is gone
	// but the bound is still full of the orphaned replacements, proving
	// the recovery runs inside the SAME capacity limit.
	stageDisarm()
	require.Equal(t, recoveryAdmissionLimit, unpublishedPendingAdmissionLimit,
		"the capacity limit is retained exactly")

	// The engine's scheduled delete pass retires the queued orphans
	// (the ONLY cleanup drive: no transaction-side call anywhere), which
	// is what restores admission capacity.
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the engine's scheduled pass deletes the orphaned replacements")
	for _, name := range orphanNames {
		issue25590RequireGone(t, failFS, name,
			"the orphaned replacement the failed pass left")
	}
	for name := range allOriginalNames {
		issue25590RequireKept(t, failFS, name,
			"an original object the retried pass still needs")
	}
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Equal(t, int64(totalObjs), live,
		"the drain retired the orphans' owned memberships - the bound has room again")
	require.Zero(t, inFlight)

	// The retried admission INSIDE the same open transaction, under the
	// SAME limit: the merge reapplies the marks, the pass rewrites both
	// dirty objects fresh, records them, and the admission completes.
	txn.StartStatement()
	require.NoError(t, txn.IncrStatementID(ctx, false),
		"the retried admission completes with the fault removed and the SAME capacity limit - the recovered storage can finally serve the transaction again")
	txn.UpdateSnapshotWriteOffset()
	txn.EndStatement()

	require.Zero(t, txn.deletedBlocks.size(),
		"the retried pass consumed the marks")
	// The ceremony's post-merge compactNilWorkspaceSlotsLocked compacts
	// the retired originals' nil slots out of the writes slice - the
	// raw-call reference test still finds nil batches at the old slot
	// indexes, but a real statement boundary shifts the slice. The
	// per-object record census carries the same discriminators: no
	// original per-table dump batch can survive beside its per-object
	// re-publications, no duplicate from the aborted attempt can ride
	// with them, and exactly the two superseded dirty originals' names
	// are gone.
	idxes := issue25590AllMetadataEntries(t, txn)
	require.Len(t, idxes, totalObjs,
		"one record per object after the retried pass - the re-publications and the fresh replacements")
	namesNow := map[string]bool{}
	originalsRepublished := 0
	freshNames := map[string]bool{}
	for _, idx := range idxes {
		for _, name := range issue25590StatsNamesOfMeta(t, txn, idx) {
			if !namesNow[name] {
				namesNow[name] = true
				if allOriginalNames[name] {
					originalsRepublished++
				} else {
					freshNames[name] = true
				}
			}
		}
	}
	require.Len(t, freshNames, 2,
		"exactly two fresh replacement records - one per dirty object")
	require.Equal(t, totalObjs-recoveryLimit, originalsRepublished,
		"only the six unaffected originals re-published - the two superseded dirty originals' names left the workspace with the retired dump batches")
	for _, name := range orphanNames {
		require.False(t, namesNow[name],
			"an orphaned replacement the scheduled pass deleted never re-entered the workspace")
	}
	for name := range freshNames {
		require.NotContains(t, orphanNames, name,
			"the retry rewrote the objects freshly, not the orphaned names")
		require.Contains(t, txn.recordedUnpublishedObjectNames, name,
			"the fresh replacements are recorded members")
	}
	queued, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, queued)
	require.Zero(t, inFlight)
	require.Equal(t, int64(totalObjs+recoveryLimit), live,
		"the completed pass's ledger equals the originals' re-publications plus the fresh replacements")
	require.Empty(t, txn.pendingObjectCleanupNames())
	require.Equal(t, recoveryAdmissionLimit, unpublishedPendingAdmissionLimit,
		"the capacity limit never changed through the whole recovery")

	// COMMIT in the SAME transaction: the commit admission's merge has
	// nothing failed left to run, the commit-prepare handoff queues
	// nothing recorded, and the success classification retires the
	// recorded members as published, leaving the published files intact.
	require.NoError(t, txn.checkWorkspaceAccountingLocked())
	require.NoError(t, func() error {
		_, cerr := txn.Commit(ctx)
		return cerr
	}(), "the recovered transaction commits - the failed admission and its orphaned replacements cost it nothing but the scheduled deletions")
	require.Zero(t, len(issue25590QueuedNames(e)),
		"the commit-prepare handoff queues nothing recorded")
	txn.FinalizeCommit(ctx)
	_, live, inFlight = issue25590LedgerNow(e)
	require.Zero(t, live,
		"the successful commit released every recorded member as published")
	require.Zero(t, inFlight)
	require.NoError(t, e.DrainUnpublishedPendingForTest(ctx),
		"the final scheduled pass has no obligation left")
	for name := range freshNames {
		issue25590RequireKept(t, failFS, name,
			"a published replacement file survives the terminal")
	}
}
