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
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
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
	require.Nil(t, txn.writes[0].bat)
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
