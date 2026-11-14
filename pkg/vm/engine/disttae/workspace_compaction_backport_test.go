// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package disttae

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/require"
)

type workspaceCopyRefusal struct{ calls, failAfter int }

func (c *workspaceCopyRefusal) AcquireAllocationCapacity(uint64) error {
	c.calls++
	if c.failAfter > 0 && c.calls > c.failAfter {
		return mpool.ErrAllocationAccountCapacity
	}
	return nil
}
func (c *workspaceCopyRefusal) ReleaseAllocationCapacity(uint64) {}

// Two real two-row objects rewrite down to one row each. Admission refuses the
// second replacement after the first record has staged, not before persistence.
func TestBackportWorkspaceCompactionLateRecordRefusal(t *testing.T) {
	ctx := context.Background()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	txn.tnStores = []DNStore{{}}
	txn.tableCache = new(sync.Map)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.tablesInVain = make(map[uint64]int)
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	tbl.tableDef = &pbplan.TableDef{Cols: []*pbplan.ColDef{
		{Name: "pk", Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
		{Name: catalog.Row_ID, Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
	}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
	tbl.proc.Store(txn.proc)
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	txn.engine.fs = fs
	var originals []objectio.ObjectStats
	for i := range 2 {
		func() {
			writer := colexec.NewCNS3DataWriter(txn.proc.Mp(), fs, tbl.tableDef, -1, false)
			defer writer.Close()
			data := batch.NewWithSize(1)
			data.Attrs = []string{"pk"}
			data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			defer data.Clean(txn.proc.Mp())
			require.NoError(t, vector.AppendFixedList(data.Vecs[0], []int64{int64(i*2 + 1), int64(i*2 + 2)}, nil, txn.proc.Mp()))
			data.SetRowCount(2)
			require.NoError(t, writer.Write(ctx, data))
			stats, err := writer.Sync(ctx)
			require.NoError(t, err)
			require.Len(t, stats, 1)
			originals = append(originals, stats[0])
		}()
	}
	metadata := colexec.AllocCNS3ResultBat(false)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(txn.proc.Mp(), false, metadata, true, originals...))
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", fileName: originals[0].ObjectName().String(), bat: metadata})
	for i := range originals {
		stats := &originals[i]
		txn.registerCNObjects(*stats.ObjectName().ObjectId(), 1, metadata, "db", "tbl", 0, false)
		info := objectio.DecodeBlockInfo(metadata.Vecs[0].GetBytesAt(i))
		txn.deletedBlocks.addDeletedBlocks(&info.BlockID, []int64{0})
	}
	require.NoError(t, txn.registerUnpublishedObjectRecordLocked(INSERT, metadata))
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	controller := &workspaceCopyRefusal{}
	account, err := registry.OpenWithController(1<<20, controller)
	require.NoError(t, err)
	txn.workspaceSelection, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.retireCompactionBatchesLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	}()
	// Measure one real metadata copy's allocation count, then refuse the following
	// copy's first backing allocation. This remains independent of allocator sizes.
	one := colexec.AllocCNS3ResultBat(false)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(txn.proc.Mp(), false, one, true, originals[0]))
	copy, err := txn.copyWorkspaceBatch(one)
	require.NoError(t, err)
	copy.Clean(txn.proc.Mp())
	one.Clean(txn.proc.Mp())
	controller.failAfter, controller.calls = controller.calls, 0
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	txn.compactWorker = pool
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Len(t, txn.writes, 1)
	require.Same(t, metadata, txn.writes[0].bat)
	require.Equal(t, 2, txn.deletedBlocks.size())
	require.Len(t, txn.cnObjsSummary, 2)
	require.Zero(t, account.Snapshot().Used)
	require.Len(t, txn.pendingObjectCleanupNames(), 2)
	require.Len(t, txn.recordedUnpublishedObjects.names, 2)
	// The originals retain their record claims; only replacement admissions retract.
	// Inspect the canonical names through actual file-service reads.
	for _, stats := range originals {
		require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()))
	}
	require.NoError(t, txn.handoffPendingObjectCleanupLocked(true))
	require.NoError(t, txn.engine.deleteQueuedUnpublishedPendingOnce(ctx))
	controller.failAfter = 0
	txn.Lock()
	err = txn.compactDeletionOnObjsLocked(ctx)
	txn.Unlock()
	require.NoError(t, err)
	txn.Lock()
	require.NoError(t, txn.finishCompactionLocked())
	txn.Unlock()
	require.Len(t, txn.writes, 1)
	require.NotNil(t, txn.writes[0].bat)
	require.NotSame(t, metadata, txn.writes[0].bat)
	require.Zero(t, txn.deletedBlocks.size())
	rows := 0
	for _, e := range txn.writes {
		if e.bat != nil {
			for _, stats := range collectObjectStatsFromEntry(e) {
				rows += int(stats.Rows())
			}
		}
	}
	require.Equal(t, 2, rows)
}

func TestBackportWorkspaceDumpDeleteResolveRollback(t *testing.T) {
	for _, reuseSlot := range []bool{false, true} {
		t.Run(map[bool]string{false: "truncate", true: "reuse slot"}[reuseSlot], func(t *testing.T) {
			ctx := context.Background()
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			// This test exercises rollback during the resolver unlock, not host
			// memory pressure. Preserve real accounting with a fixed CN budget.
			budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
			t.Cleanup(parent.Close)
			colexec.NewServer("")
			txn.tableCache = new(sync.Map)
			txn.batchSelectList = make(map[*batch.Batch][]int64)
			txn.tablesInVain = make(map[uint64]int)
			txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
			txn.cnObjsSummary = make(map[types.Objectid]Summary)
			txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
			txn.tnStores = []DNStore{{}}
			txn.engine.config.insertEntryMaxCount = 1
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			txn.engine.fs = fs
			tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			tbl.tableDef = &pbplan.TableDef{Cols: []*pbplan.ColDef{
				{Name: "pk", Seqnum: 0, Primary: true, Typ: pbplan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.Row_ID, Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
			}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
			tbl.proc.Store(txn.proc)
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			op.EXPECT().EnterRollbackStmt()
			op.EXPECT().ExitRollbackStmt()
			txn.op = op
			prior := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{111})
			aborted := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{222})
			deleted := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{333})
			entry := func(typ int, b *batch.Batch) Entry {
				return Entry{typ: typ, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", bat: b}
			}
			txn.appendWorkspaceEntryLocked(entry(INSERT, prior))
			txn.appendWorkspaceEntryLocked(entry(INSERT, aborted))
			txn.appendWorkspaceEntryLocked(entry(DELETE, deleted))
			txn.statementID = 1
			txn.offsets = []int{1}
			txn.snapshotWriteOffset.Store(1)
			entered, resume := make(chan struct{}), make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(resume) }) }
			oldHook := dumpResolveBeforeRelock
			dumpResolveBeforeRelock = func(kind int) {
				if kind == DELETE {
					close(entered)
					<-resume
				}
			}
			done := make(chan error, 1)
			exited := make(chan struct{})
			go func() { defer close(exited); txn.Lock(); defer txn.Unlock(); done <- txn.dumpBatchLocked(ctx, -1) }()
			t.Cleanup(func() {
				release()
				<-exited
				dumpResolveBeforeRelock = oldHook
				for i := range txn.writes {
					txn.releaseWorkspaceEntryBatchLocked(i)
				}
				txn.closeWorkspaceAllocation()
			})
			<-entered
			require.NoError(t, txn.RollbackLastStatement(ctx))
			require.Len(t, txn.writes, 1)
			require.Same(t, prior, txn.writes[0].bat)
			require.Nil(t, aborted.Vecs)
			require.Nil(t, deleted.Vecs)
			var replacement *batch.Batch
			if reuseSlot {
				replacement = newInt64BatchForTest(t, txn.proc, []string{"replacement"}, []int64{999})
				txn.Lock()
				txn.appendWorkspaceEntryLocked(entry(ALTER, replacement))
				txn.Unlock()
			}
			release()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Fatal("dump did not finish after rollback resolution window")
			}
			rows := 0
			replacementFound := false
			for _, e := range txn.writes {
				if e.typ == ALTER {
					replacementFound = true
					require.Same(t, replacement, e.bat)
					require.Equal(t, int64(999), vector.GetFixedAtNoTypeCheck[int64](e.bat.Vecs[0], 0))
				}
				for _, stats := range collectObjectStatsFromEntry(e) {
					rows += int(stats.Rows())
				}
			}
			require.Equal(t, 1, rows, "the aborted insert must never be resurrected by staged metadata")
			require.Equal(t, reuseSlot, replacementFound)
			require.NoError(t, txn.checkWorkspaceAccountingLocked())
		})
	}
}

// A prefix captured before a later statement must retain exactly its old rows
// after a dump spanning both regions, and after rollback of the newer region.
func TestBackportWorkspaceDumpPreservesCapturedPrefixAndRollbackRegion(t *testing.T) {
	for _, tc := range []struct {
		name              string
		statementBoundary bool
		snapshot          int
	}{
		{"statement_and_snapshot", true, 1},
		{"snapshot_inside_statement_region", false, 1},
		{"statement_boundary_inside_snapshot_prefix", true, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			statementBoundary := tc.statementBoundary
			ctx := context.Background()
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			// This oracle tests publication positions, not host pressure. Keep real
			// capacity accounting while isolating it from the host's live sampler.
			budget := process.MustNewHashBuildBudget(16<<20, 8<<20)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
			t.Cleanup(parent.Close)

			colexec.NewServer("")
			txn.tableCache = new(sync.Map)
			txn.tablesInVain = make(map[uint64]int)
			txn.batchSelectList = make(map[*batch.Batch][]int64)
			txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
			txn.cnObjsSummary = make(map[types.Objectid]Summary)
			txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
			txn.tnStores = []DNStore{{}}
			fs, err := colexec.GetSharedFSFromProc(txn.proc)
			require.NoError(t, err)
			txn.engine.fs = fs
			tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
			tbl.tableDef = &pbplan.TableDef{Cols: []*pbplan.ColDef{
				{Name: "pk", Seqnum: 0, Primary: true, Typ: pbplan.Type{Id: int32(types.T_int64)}},
				{Name: catalog.Row_ID, Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
			}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
			tbl.proc.Store(txn.proc)
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			txn.op = op
			if statementBoundary {
				op.EXPECT().EnterRollbackStmt()
				op.EXPECT().ExitRollbackStmt()
			}
			for _, value := range []int64{111, 222} {
				b := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{value})
				txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", bat: b})
			}
			const capturedPrefix = 1
			if statementBoundary {
				txn.offsets = []int{capturedPrefix}
				txn.statementID = 1
			}
			txn.snapshotWriteOffset.Store(int64(tc.snapshot))
			t.Cleanup(func() {
				for i := range txn.writes {
					txn.releaseWorkspaceEntryBatchLocked(i)
				}
				txn.closeWorkspaceAllocation()
			})
			readPrefix := func(offset int) []int64 {
				var values []int64
				txn.ForEachTableWrites(7, 42, offset, func(e Entry) {
					require.Equal(t, catalog.BlockMeta_BlockInfo, e.bat.Attrs[0])
					info := objectio.DecodeBlockInfo(e.bat.Vecs[0].GetBytesAt(0))
					rows := batch.NewWithSize(1)
					defer rows.Clean(txn.proc.Mp())
					require.NoError(t, blockio.CopyBlockData(ctx, info.MetaLoc[:], nil, []uint16{0}, []types.Type{types.T_int64.ToType()}, rows, fs, txn.proc.Mp()))
					values = append(values, vector.MustFixedColNoTypeCheck[int64](rows.Vecs[0])...)
					require.Same(t, e.bat, txn.cnObjsSummary[*info.BlockID.Object()].objBat)
				})
				return values
			}
			txn.Lock()
			var rows int
			err = txn.dumpInsertBatchLocked(ctx, fs, 0, &rows)
			txn.Unlock()
			require.NoError(t, err)
			require.Equal(t, 2, rows)
			require.Len(t, txn.writes, 2, "dump replaces source anchors without moving the captured boundary")
			require.Equal(t, []int64{111}, readPrefix(capturedPrefix))
			require.Equal(t, []int64{111, 222}, readPrefix(int(txn.WriteOffset())))
			require.NoError(t, txn.checkWorkspaceAccountingLocked())
			if !statementBoundary {
				return
			}
			require.NoError(t, txn.RollbackLastStatement(ctx))
			require.Len(t, txn.writes, 1)
			require.Equal(t, []int64{111}, readPrefix(capturedPrefix))
			require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
			require.Equal(t, []int64{111}, readPrefix(capturedPrefix), "GC of the rolled-back region must not delete the earlier object's file")
			require.NoError(t, txn.checkWorkspaceAccountingLocked())
		})
	}
}

type compactionPrefixCase struct {
	onlyFirstDirty, rollback, maintenanceFailure, snapshotFailure bool
	concurrentRollback, transferFailure, allDeleted, multiAnchor  bool
	successfulBoundary, unknownCommit, unsortedDeletes            bool
}

func TestReview32536CompactionCapturedPrefix(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts compactionPrefixCase
	}{
		{"both_rewritten", compactionPrefixCase{}},
		{"clean_and_rewritten", compactionPrefixCase{onlyFirstDirty: true}},
		{"all_deleted", compactionPrefixCase{allDeleted: true}},
		{"all_deleted_unsorted", compactionPrefixCase{allDeleted: true, unsortedDeletes: true}},
		{"multiple_anchors", compactionPrefixCase{multiAnchor: true}},
		{"multiple_anchors_rollback", compactionPrefixCase{multiAnchor: true, rollback: true}},
		{"rollback_after_compaction", compactionPrefixCase{rollback: true}},
		{"dump_failure_in_statement_maintenance", compactionPrefixCase{rollback: true, maintenanceFailure: true}},
		{"snapshot_failure_in_statement_maintenance", compactionPrefixCase{snapshotFailure: true}},
		{"rollback_during_dump_resolution", compactionPrefixCase{rollback: true, concurrentRollback: true}},
		{"transfer_failure_in_statement_maintenance", compactionPrefixCase{transferFailure: true}},
		{"successful_boundary_then_statement_rollback", compactionPrefixCase{rollback: true, successfulBoundary: true}},
		{"unknown_commit_preserves_objects", compactionPrefixCase{unknownCommit: true}},
	} {
		t.Run(tc.name, func(t *testing.T) { testCompactionCapturedPrefix(t, tc.opts) })
	}
}

func testCompactionCapturedPrefix(t *testing.T, opts compactionPrefixCase) {
	onlyFirstDirty, rollback, maintenanceFailure, snapshotFailure := opts.onlyFirstDirty, opts.rollback, opts.maintenanceFailure, opts.snapshotFailure
	concurrentRollback, transferFailure, allDeleted, multiAnchor := opts.concurrentRollback, opts.transferFailure, opts.allDeleted, opts.multiAnchor
	ctx := context.Background()
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "pk")
	snapshotErr := errors.New("injected snapshot failure")
	if snapshotFailure || transferFailure {
		op := mock_frontend.NewMockTxnOperator(gomock.NewController(t))
		op.EXPECT().Txn().Return(txnpb.TxnMeta{ID: []byte("txn-test"), Isolation: txnpb.TxnIsolation_RC}).AnyTimes()
		op.EXPECT().NextSequence().Return(uint64(1)).AnyTimes()
		op.EXPECT().Status().Return(txnpb.TxnStatus_Active).AnyTimes()
		op.EXPECT().GetWorkspace().Return(txn).AnyTimes()
		op.EXPECT().SnapshotTS().Return(timestamp.Timestamp{PhysicalTime: 10}).AnyTimes()
		if snapshotFailure {
			op.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).Return(snapshotErr)
		} else {
			op.EXPECT().UpdateSnapshot(gomock.Any(), gomock.Any()).Return(nil)
		}
		op.EXPECT().EnterIncrStmt()
		op.EXPECT().ExitIncrStmt()
		op.EXPECT().EnterRollbackStmt().Times(2)
		op.EXPECT().ExitRollbackStmt().Times(2)
		txn.op = op
	}
	if rollback {
		op := newTxnOperatorForTestWithWorkspace(t, txn)
		rollbackCalls := 1
		if concurrentRollback || maintenanceFailure {
			rollbackCalls = 2
		}
		op.EXPECT().EnterRollbackStmt().Times(rollbackCalls)
		op.EXPECT().ExitRollbackStmt().Times(rollbackCalls)
		if maintenanceFailure {
			op.EXPECT().EnterIncrStmt().Times(2)
			op.EXPECT().ExitIncrStmt().Times(2)
		} else if concurrentRollback || opts.successfulBoundary {
			op.EXPECT().EnterIncrStmt()
			op.EXPECT().ExitIncrStmt()
		}
		txn.op = op
	}
	txn.tnStores = []DNStore{{}}
	txn.tableCache = new(sync.Map)
	txn.batchSelectList = make(map[*batch.Batch][]int64)
	txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
	txn.cnObjsSummary = make(map[types.Objectid]Summary)
	txn.cn_flushed_s3_tombstone_object_stats_list = new(sync.Map)
	txn.tablesInVain = make(map[uint64]int)
	tbl := txn.tableOps.existAndActive(genTableKey(1, "tbl", 7, "db"))
	tbl.tableDef = &pbplan.TableDef{Cols: []*pbplan.ColDef{
		{Name: "pk", Seqnum: 0, Typ: pbplan.Type{Id: int32(types.T_int64)}, Primary: true},
		{Name: catalog.Row_ID, Seqnum: 1, Typ: pbplan.Type{Id: int32(types.T_Rowid)}},
	}, Pkey: &pbplan.PrimaryKeyDef{PkeyColName: "pk"}}
	tbl.proc.Store(txn.proc)
	fs, err := colexec.GetSharedFSFromProc(txn.proc)
	require.NoError(t, err)
	txn.engine.fs = fs
	var originals []objectio.ObjectStats
	for i := range 2 {
		func() {
			writer := colexec.NewCNS3DataWriter(txn.proc.Mp(), fs, tbl.tableDef, -1, false)
			defer writer.Close()
			data := batch.NewWithSize(1)
			data.Attrs = []string{"pk"}
			data.Vecs[0] = vector.NewVec(types.T_int64.ToType())
			defer data.Clean(txn.proc.Mp())
			require.NoError(t, vector.AppendFixedList(data.Vecs[0], []int64{int64(i*2 + 1), int64(i*2 + 2)}, nil, txn.proc.Mp()))
			data.SetRowCount(2)
			require.NoError(t, writer.Write(ctx, data))
			stats, err := writer.Sync(ctx)
			require.NoError(t, err)
			require.Len(t, stats, 1)
			originals = append(originals, stats[0])
		}()
	}
	metadata := colexec.AllocCNS3ResultBat(false)
	statsForFirst := originals
	if multiAnchor {
		statsForFirst = originals[:1]
	}
	require.NoError(t, colexec.ExpandObjectStatsToBatch(txn.proc.Mp(), false, metadata, true, statsForFirst...))
	txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", fileName: originals[0].ObjectName().String(), bat: metadata})
	metadataBatches := []*batch.Batch{metadata}
	if multiAnchor {
		second := colexec.AllocCNS3ResultBat(false)
		require.NoError(t, colexec.ExpandObjectStatsToBatch(txn.proc.Mp(), false, second, true, originals[1]))
		txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", fileName: originals[1].ObjectName().String(), bat: second})
		metadataBatches = append(metadataBatches, second)
	}
	for i := range originals {
		stats := &originals[i]
		batchIndex, rowIndex := 0, i
		if multiAnchor {
			batchIndex, rowIndex = i, 0
		}
		bat := metadataBatches[batchIndex]
		txn.registerCNObjects(*stats.ObjectName().ObjectId(), 1, bat, "db", "tbl", 0, false)
		info := objectio.DecodeBlockInfo(bat.Vecs[0].GetBytesAt(rowIndex))
		if !onlyFirstDirty || i == 0 {
			offsets := []int64{0}
			if allDeleted {
				offsets = []int64{0, 1}
				if opts.unsortedDeletes {
					offsets = []int64{1, 0}
				}
			}
			txn.deletedBlocks.addDeletedBlocks(&info.BlockID, offsets)
		}
	}
	for _, bat := range metadataBatches {
		require.NoError(t, txn.registerUnpublishedObjectRecordLocked(INSERT, bat))
	}
	registry, err := mpool.NewAllocationAccountRegistry(1, 1024)
	require.NoError(t, err)
	controller := &workspaceCopyRefusal{}
	account, err := registry.OpenWithController(1<<20, controller)
	require.NoError(t, err)
	txn.workspaceSelection, err = vector.NewAllocationAccountSelection(account, mpool.AllocationOwnerTxnWorkspace, 1, 2, 3, 4)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, txn.finishCompactionLocked())
		require.NoError(t, txn.retireCompactionBatchesLocked())
		for i := range txn.writes {
			txn.releaseWorkspaceEntryBatchLocked(i)
		}
		_, _, err := registry.CompleteTerminal(account)
		require.NoError(t, err)
	}()
	// Measure one real metadata copy's allocation count, then refuse the following
	// copy's first backing allocation. This remains independent of allocator sizes.
	one := colexec.AllocCNS3ResultBat(false)
	require.NoError(t, colexec.ExpandObjectStatsToBatch(txn.proc.Mp(), false, one, true, originals[0]))
	copy, err := txn.copyWorkspaceBatch(one)
	require.NoError(t, err)
	copy.Clean(txn.proc.Mp())
	one.Clean(txn.proc.Mp())
	controller.failAfter, controller.calls = 0, 0
	pool, err := ants.NewPool(1)
	require.NoError(t, err)
	defer pool.Release()
	txn.compactWorker = pool
	txn.offsets = []int{len(metadataBatches)}
	if rollback || snapshotFailure || transferFailure {
		txn.statementID = 1
	}
	txn.snapshotWriteOffset.Store(int64(len(metadataBatches)))
	if snapshotFailure || transferFailure {
		// These deletes are from the preceding successful statement. Failed
		// snapshot advancement must restore the physical source and its masks.
		txn.transfer.statements = []statementTransferState{{}}
		txn.writeWorkspaceThreshold = 1 << 40
		if transferFailure {
			// Real tombstone transfer must resolve this table after snapshot
			// advancement. Its database was removed by an earlier statement.
			txn.databaseOps.addDeleteDatabase(genDatabaseKey(1, "missing"), 0, 99)
			deletes := newInt64BatchForTest(t, txn.proc, []string{"pk"}, []int64{7})
			txn.appendWorkspaceEntryLocked(Entry{typ: DELETE, accountId: 1, databaseId: 99, tableId: 99, databaseName: "missing", tableName: "tbl", bat: deletes})
		}
		txn.StartStatement()
		err = txn.IncrStatementID(ctx, false)
		if snapshotFailure {
			require.ErrorIs(t, err, snapshotErr)
		} else {
			require.True(t, moerr.IsMoErrCode(err, moerr.OkExpectedEOB), "transfer must return its own database-resolution error")
		}
		require.Same(t, metadata, txn.writes[0].bat, "snapshot failure must undo the installed replacement")
		require.Equal(t, 2, txn.deletedBlocks.size(), "the preceding successful deletes must remain represented")
		require.NoError(t, txn.RollbackLastStatement(ctx))
		require.NoError(t, txn.RollbackLastStatement(ctx))
		require.Same(t, metadata, txn.writes[0].bat)
		require.Equal(t, 2, txn.deletedBlocks.size(), "rolling back the failed boundary must preserve earlier deletes")
		require.Equal(t, 1, txn.statementID)
		require.NoError(t, txn.checkWorkspaceAccountingLocked())
		txn.EndStatement()
		return
	} else if opts.successfulBoundary {
		// The deletions precede this SQL statement. Once its maintenance
		// succeeds, rolling back its new write must not undo those deletions.
		txn.writeWorkspaceThreshold = 1 << 40
		txn.StartStatement()
		require.NoError(t, txn.IncrStatementID(ctx, false))
		txn.UpdateSnapshotWriteOffset()
		b := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{99})
		txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", bat: b})
	} else if maintenanceFailure || concurrentRollback {
		// This insert and the deleted-block marks belong to the previously
		// successful statement: they exist before StartStatement. Force the
		// subsequent maintenance dump to refuse its first workspace copy.
		extra := newInsertBatchWithRowIDForTest(t, txn.proc, []int64{99})
		txn.appendWorkspaceEntryLocked(Entry{typ: INSERT, accountId: 1, databaseId: 7, tableId: 42, databaseName: "db", tableName: "tbl", bat: extra})
		txn.writeWorkspaceThreshold = 1
		txn.engine.config.extraWorkspaceThreshold = 1
		txn.engine.config.insertEntryMaxCount = 1
		oldHook := dumpResolveBeforeRelock
		t.Cleanup(func() { dumpResolveBeforeRelock = oldHook })
		dumpResolved := false
		if concurrentRollback {
			entered, resume := make(chan struct{}), make(chan struct{})
			done := make(chan error, 1)
			dumpResolveBeforeRelock = func(kind int) {
				if kind == INSERT {
					close(entered)
					<-resume
				}
			}
			txn.StartStatement()
			go func() { done <- txn.IncrStatementID(ctx, false) }()
			<-entered
			rollbackErr := txn.RollbackLastStatement(ctx)
			close(resume)
			incrErr := <-done
			require.NoError(t, rollbackErr)
			require.ErrorIs(t, incrErr, context.Canceled, "the resumed maintenance must not publish after rollback")
			require.NoError(t, txn.RollbackLastStatement(ctx), "the frontend may retry rollback after IncrStatementID returns")
			require.Len(t, txn.writes, 2, "the preceding statement's insert must survive")
			require.Same(t, extra, txn.writes[1].bat)
			require.Same(t, metadata, txn.writes[0].bat)
			require.Equal(t, 2, txn.deletedBlocks.size())
			require.NoError(t, txn.checkWorkspaceAccountingLocked())
			for _, stats := range originals {
				require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()))
			}
			txn.EndStatement()
			return
		}
		dumpResolveBeforeRelock = func(kind int) {
			if kind == INSERT {
				dumpResolved = true
				controller.failAfter = controller.calls
			}
		}
		txn.StartStatement()
		err = txn.IncrStatementID(ctx, false)
		require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		require.True(t, dumpResolved, "the refusal must occur after compaction, during dump")
		require.Equal(t, 1, txn.statementID, "dump failed before IncrStatementID advanced the boundary")
		require.Equal(t, []int{1}, txn.offsets)
		dumpResolveBeforeRelock = oldHook
	} else {
		txn.Lock()
		err = txn.compactDeletionOnObjsLocked(ctx)
		txn.Unlock()
		require.NoError(t, err)
	}
	if rollback {
		require.NoError(t, txn.RollbackLastStatement(ctx))
		if maintenanceFailure {
			require.NoError(t, txn.RollbackLastStatement(ctx))
		}
		if opts.successfulBoundary {
			txn.EndStatement()
			require.Len(t, txn.writes, 1)
		}
		if maintenanceFailure {
			txn.EndStatement()
			var priorValues []int64
			for _, entry := range txn.writes {
				if entry.bat != nil && entry.typ == INSERT && entry.fileName == "" {
					priorValues = append(priorValues, vector.MustFixedColNoTypeCheck[int64](entry.bat.Vecs[1])...)
				}
			}
			require.Equal(t, []int64{99}, priorValues, "a failed IncrStatementID must not roll back the preceding successful statement")
		}
	}
	rows := 0
	var values []int64
	txn.ForEachTableWrites(7, 42, len(metadataBatches), func(e Entry) {
		if e.bat == nil {
			return
		}
		for _, stats := range collectObjectStatsFromEntry(e) {
			require.Same(t, e.bat, txn.cnObjsSummary[*stats.ObjectName().ObjectId()].objBat)
		}
		for i := 0; i < e.bat.Vecs[0].Length(); i++ {
			info := objectio.DecodeBlockInfo(e.bat.Vecs[0].GetBytesAt(i))
			data := batch.NewWithSize(1)
			require.NoError(t, blockio.CopyBlockData(ctx, info.MetaLoc[:], nil, []uint16{0}, []types.Type{types.T_int64.ToType()}, data, fs, txn.proc.Mp()))
			txn.deletedBlocks.RLock()
			deleted := slices.Clone(txn.deletedBlocks.offsets[info.BlockID])
			txn.deletedBlocks.RUnlock()
			for row, value := range vector.MustFixedColNoTypeCheck[int64](data.Vecs[0]) {
				if maintenanceFailure && slices.Contains(deleted, int64(row)) {
					continue
				}
				values = append(values, value)
				rows++
			}
			data.Clean(txn.proc.Mp())
		}
	})
	wantRows := 2
	wantValues := []int64{2, 4}
	if onlyFirstDirty {
		wantRows = 3
		wantValues = []int64{2, 3, 4}
	}
	if allDeleted {
		wantRows, wantValues = 0, nil
	}
	if rollback && !maintenanceFailure && !opts.successfulBoundary {
		wantRows = 4
		wantValues = []int64{1, 2, 3, 4}
	}
	require.Equal(t, wantRows, rows, "captured prefix must still include the surviving rows")
	require.ElementsMatch(t, wantValues, values)
	if opts.unknownCommit {
		require.NoError(t, txn.finishCompactionLocked())
		replacements := slices.Clone(txn.retiredCompactions[0].newNames)
		require.NotEmpty(t, replacements)
		txn.FinalizeCommitWithUnknownResult(ctx)
		require.True(t, txn.removed)
		require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
		for _, stats := range originals {
			require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()), "unknown commit must not delete original objects")
		}
		for _, name := range replacements {
			require.True(t, objectExistsInFS(ctx, fs, name), "unknown commit must not delete replacement objects")
		}
		return
	}
	if multiAnchor {
		require.Len(t, txn.writes, 2)
		first := collectObjectStatsFromEntry(txn.writes[0])
		second := collectObjectStatsFromEntry(txn.writes[1])
		require.Len(t, first, 1)
		require.Len(t, second, 1)
		want := uint32(1)
		if rollback {
			want = 2
		}
		require.Equal(t, want, first[0].Rows())
		require.Equal(t, want, second[0].Rows())
	}
	if rollback && !maintenanceFailure && !concurrentRollback && !opts.successfulBoundary {
		// The original entries are live again, but discarded replacements
		// still have cleanup obligations until the GC handoff succeeds.
		require.Len(t, txn.retiredCompactions, 1)
		newNames := slices.Clone(txn.retiredCompactions[0].newNames)
		require.NotEmpty(t, newNames)
		t.Cleanup(func() { txn.engine.workspaceGCClosed.Store(false) })
		txn.engine.workspaceGCClosed.Store(true)
		require.Error(t, txn.retireCompactionBatchesLocked())
		require.Len(t, txn.retiredCompactions, 1)
		for _, stats := range originals {
			require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()))
		}
		txn.engine.workspaceGCClosed.Store(false)
		require.NoError(t, txn.retireCompactionBatchesLocked())
		require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
		for _, name := range newNames {
			require.False(t, objectExistsInFS(ctx, fs, name))
		}
		for _, stats := range originals {
			require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()))
		}
	}
	if !rollback && !snapshotFailure {
		// A reader can keep an old ObjectStats after releasing the workspace
		// lock. Settlement must not free its batch or delete its object until
		// transaction readers have drained.
		require.NoError(t, txn.finishCompactionLocked())
		require.Len(t, txn.retiredCompactions, 1)
		require.NotEmpty(t, metadata.Vecs)
		for _, stats := range originals {
			require.True(t, objectExistsInFS(ctx, fs, stats.ObjectName().String()))
		}
		info := objectio.DecodeBlockInfo(metadata.Vecs[0].GetBytesAt(0))
		oldData := batch.NewWithSize(1)
		require.NoError(t, blockio.CopyBlockData(ctx, info.MetaLoc[:], nil, []uint16{0}, []types.Type{types.T_int64.ToType()}, oldData, fs, txn.proc.Mp()))
		require.Equal(t, []int64{1, 2}, vector.MustFixedColNoTypeCheck[int64](oldData.Vecs[0]))
		oldData.Clean(txn.proc.Mp())
		// A failed GC admission must keep both the original backing and its
		// object obligation available for a later retry.
		t.Cleanup(func() { txn.engine.workspaceGCClosed.Store(false) })
		txn.engine.workspaceGCClosed.Store(true)
		require.Error(t, txn.retireCompactionBatchesLocked())
		require.Len(t, txn.retiredCompactions, 1)
		require.NotEmpty(t, metadata.Vecs)
		txn.engine.workspaceGCClosed.Store(false)
		require.NoError(t, txn.retireCompactionBatchesLocked())
		require.Empty(t, metadata.Vecs)
		require.NoError(t, txn.engine.DrainUnpublishedPendingForTest(ctx))
		require.False(t, objectExistsInFS(ctx, fs, originals[0].ObjectName().String()))
		if onlyFirstDirty {
			require.True(t, objectExistsInFS(ctx, fs, originals[1].ObjectName().String()), "clean republication still owns this object")
		} else {
			require.False(t, objectExistsInFS(ctx, fs, originals[1].ObjectName().String()))
		}
	}
	if maintenanceFailure {
		// The same transaction must remain usable after the refused boundary.
		controller.failAfter = 0
		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
		txn.writeWorkspaceThreshold = 1 << 40
		txn.StartStatement()
		require.NoError(t, txn.IncrStatementID(ctx, false))
		txn.UpdateSnapshotWriteOffset()
		src := batch.NewWithSize(1)
		src.Attrs = []string{"pk"}
		src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(100), false, txn.proc.Mp()))
		src.SetRowCount(1)
		copied, copyErr := txn.copyWorkspaceBatch(src)
		src.Clean(txn.proc.Mp())
		require.NoError(t, copyErr)
		_, writeErr := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", copied, DNStore{})
		if writeErr != nil {
			copied.Clean(txn.proc.Mp())
		}
		require.NoError(t, writeErr)
		txn.EndStatement()
		var remaining []int64
		for _, entry := range txn.writes {
			if entry.bat != nil && entry.typ == INSERT && entry.fileName == "" {
				remaining = append(remaining, vector.MustFixedColNoTypeCheck[int64](entry.bat.Vecs[1])...)
			}
		}
		require.Equal(t, []int64{99, 100}, remaining)
	}
}
