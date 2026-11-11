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
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestBackportWorkspaceAllocationIndependentClose(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetHashBuildBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	clone := txn.CloneSnapshotWS().(*Transaction)
	t.Cleanup(clone.closeWorkspaceAllocation)
	t.Cleanup(txn.closeWorkspaceAllocation)
	v, err := txn.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	t.Cleanup(func() { v.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(v, int64(42), false, proc.Mp()))
	w, err := clone.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	t.Cleanup(func() { w.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixed(w, int64(7), false, proc.Mp()))
	v.Free(proc.Mp())
	txn.closeWorkspaceAllocation()
	txn.closeWorkspaceAllocation()
	require.False(t, parent.Closed())
	require.NoError(t, vector.AppendFixed(w, int64(8), false, proc.Mp()))
	_, err = txn.workspaceAllocation()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
	w.Free(proc.Mp())
	clone.closeWorkspaceAllocation()
	require.Zero(t, budget.AggregateUsed())
}

func TestBackportWorkspaceAllocationConcurrentInitialization(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetHashBuildBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	defer txn.closeWorkspaceAllocation()
	const workers = 16
	selections := make([]*vector.AllocationAccountSelection, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func() { defer wg.Done(); <-start; selections[i], errs[i] = txn.workspaceAllocation() }()
	}
	close(start)
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, selections[0], selections[i])
	}
	require.Zero(t, budget.AggregateUsed())
}

// Both failures occur after a batch from the failing statement was published.
// Real statement rollback must release that batch without retiring the account.
func TestBackportWorkspaceAllocationStatementRollbackRetainsEarlierWrites(t *testing.T) {
	for _, stage := range []string{"copy", "rowid"} {
		t.Run(stage, func(t *testing.T) {
			colexec.NewServer("")
			txn := newTransactionWithActivePKTableForTest(t, "v")
			txn.tableCache = new(sync.Map)
			txn.tablesInVain = make(map[uint64]int)
			txn.batchSelectList = make(map[*batch.Batch][]int64)
			txn.deletedBlocks = &deletedBlocks{offsets: make(map[types.Blockid][]int64)}
			txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
			txn.writeWorkspaceThreshold, txn.commitWorkspaceThreshold = 1<<40, 1<<40
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			txn.op = op
			op.EXPECT().EnterIncrStmt().Times(3)
			op.EXPECT().ExitIncrStmt().Times(3)
			op.EXPECT().EnterRollbackStmt().Times(1)
			op.EXPECT().ExitRollbackStmt().Times(1)
			budget := process.MustNewHashBuildBudget(4096, 4096)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
			defer parent.Close()
			defer txn.delTransaction()
			ctx := context.Background()
			admit := func() {
				txn.StartStatement()
				require.NoError(t, txn.IncrStatementID(ctx, false))
				txn.UpdateSnapshotWriteOffset()
			}
			makeInput := func(rows int, wide bool, value int8) *batch.Batch {
				b := batch.NewWithSize(1)
				b.Attrs = []string{"v"}
				b.Vecs[0] = vector.NewVec(types.T_int8.ToType())
				t.Cleanup(func() { b.Clean(txn.proc.Mp()) })
				for range rows {
					require.NoError(t, vector.AppendFixed(b.Vecs[0], value, false, txn.proc.Mp()))
				}
				if wide {
					v := vector.NewVec(types.T_varchar.ToType())
					b.Vecs = append(b.Vecs, v)
					b.Attrs = append(b.Attrs, "payload")
					for range rows {
						require.NoError(t, vector.AppendBytes(v, []byte(strings.Repeat("x", 128)), false, txn.proc.Mp()))
					}
				}
				b.SetRowCount(rows)
				return b
			}
			copied := false
			write := func(src *batch.Batch) error {
				copied = false
				b, err := txn.copyWorkspaceBatch(src)
				if err != nil {
					return err
				}
				copied = true
				_, err = txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, DNStore{})
				if err != nil {
					b.Clean(txn.proc.Mp())
				}
				return err
			}
			admit()
			require.NoError(t, write(makeInput(1, false, 1)))
			txn.EndStatement()
			account, generation := txn.workspaceAccount, txn.workspaceGeneration
			priorUsed := account.Snapshot().Used
			require.Positive(t, priorUsed)
			admit()
			require.NoError(t, write(makeInput(1, false, 2)))
			beforeFailure := account.Snapshot().Used
			require.Greater(t, beforeFailure, priorUsed)
			// int8 data fits; its copied payload or the rowid buffer cannot fit.
			peakBefore := account.Snapshot().Peak
			require.ErrorIs(t, write(makeInput(1024, stage == "copy", 9)), mpool.ErrAllocationAccountCapacity)
			require.Equal(t, stage == "rowid", copied)
			require.Greater(t, account.Snapshot().Peak, peakBefore, "some destination storage was admitted before refusal")
			require.Equal(t, beforeFailure, account.Snapshot().Used)
			require.NoError(t, txn.RollbackLastStatement(ctx))
			txn.EndStatement()
			require.Equal(t, priorUsed, account.Snapshot().Used)
			require.Same(t, account, txn.workspaceAccount)
			require.Same(t, generation, txn.workspaceGeneration)
			require.False(t, generation.Closed())
			require.Len(t, txn.writes, 1)
			require.Equal(t, int8(1), vector.MustFixedColNoTypeCheck[int8](txn.writes[0].bat.Vecs[1])[0])
			admit()
			require.NoError(t, write(makeInput(1, false, 3)))
			txn.EndStatement()
			var values []int8
			for _, e := range txn.writes {
				if e.bat != nil {
					values = append(values, vector.MustFixedColNoTypeCheck[int8](e.bat.Vecs[1])...)
				}
			}
			require.Equal(t, []int8{1, 3}, values)
			txn.delTransaction()
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, budget.AggregateUsed())
			require.True(t, generation.Closed())
		})
	}
}

func TestBackportWorkspaceMergeAdoptsRegeneratedRowids(t *testing.T) {
	colexec.NewServer("")
	for _, limit := range []uint64{140000, 200000} {
		txn := newTransactionWithActivePKTableForTest(t, "v")
		txn.deletedBlocks = &deletedBlocks{}
		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
		budget := process.MustNewHashBuildBudget(limit, limit)
		parent, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		t.Cleanup(parent.Close)
		t.Cleanup(txn.delTransaction)
		require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
		src := batch.NewWithSize(1)
		t.Cleanup(func() { src.Clean(txn.proc.Mp()) })
		src.Attrs = []string{"v"}
		src.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		for range 64 {
			require.NoError(t, vector.AppendFixed(src.Vecs[0], int64(1), false, txn.proc.Mp()))
		}
		src.SetRowCount(64)
		for k := range 30 {
			for i := range 64 {
				require.NoError(t, vector.SetFixedAtWithTypeCheck(src.Vecs[0], i, int64(k*64+i)))
			}
			b, e := txn.copyWorkspaceBatch(src)
			require.NoError(t, e)
			_, e = txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, DNStore{})
			if e != nil {
				b.Clean(txn.proc.Mp())
			}
			require.NoError(t, e)
		}
		before := txn.workspaceAccount.Snapshot().Used
		if limit != 200000 {
			// Retain the complete real prefix, then deny any additional physical
			// backing. Release growth sizes differ from main's; the failure
			// oracle must not depend on either allocator's rounding policy.
			require.Equal(t, before, budget.AggregateUsed())
			require.NoError(t, budget.UpdateAggregateCap(before))
		}
		original := append([]Entry(nil), txn.writes...)
		txn.Lock()
		err = txn.mergeTxnWorkspaceLocked(context.Background())
		txn.Unlock()
		if limit == 200000 {
			require.NoError(t, err)
			require.LessOrEqual(t, txn.workspaceAccount.Snapshot().Peak, limit)
		} else {
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
		}
		t.Logf("limit=%d before=%d after=%d peak=%d", limit, before, txn.workspaceAccount.Snapshot().Used, txn.workspaceAccount.Snapshot().Peak)
		total, active := 0, 0
		seen := map[int64]bool{}
		for _, e := range txn.writes {
			if e.bat == nil {
				continue
			}
			active++
			total += e.bat.RowCount()
			ids := vector.MustFixedColNoTypeCheck[types.Rowid](e.bat.Vecs[0])
			values := vector.MustFixedColNoTypeCheck[int64](e.bat.Vecs[1])
			for i, id := range ids {
				require.Equal(t, uint32(i), id.GetRowOffset())
				require.False(t, seen[values[i]])
				seen[values[i]] = true
			}
		}
		require.Equal(t, 1920, total)
		require.Len(t, seen, 1920)
		if err == nil {
			require.Equal(t, 1, active)
		} else {
			require.Equal(t, 30, active)
		}
		if err != nil {
			require.Equal(t, before, txn.workspaceAccount.Snapshot().Used)
			require.Len(t, txn.writes, 30)
			for i := range original {
				require.Same(t, original[i].bat, txn.writes[i].bat)
				require.Equal(t, original[i].databaseId, txn.writes[i].databaseId)
				require.Equal(t, original[i].tableId, txn.writes[i].tableId)
				require.Equal(t, original[i].typ, txn.writes[i].typ)
			}
		}
		src.Clean(txn.proc.Mp())
		txn.delTransaction()
		require.Zero(t, txn.workspaceAccount.Snapshot().Used)
		require.Zero(t, budget.AggregateUsed())
		parent.Close()
	}
}

// A rowid-only entry has no payload vectors. Staging still has to retain every
// source row and regenerate rowids, without aliasing or freeing the old buffers.
func TestBackportWorkspaceMergeRowidOnlyStage(t *testing.T) {
	for _, refuse := range []bool{false, true} {
		name := "success"
		if refuse {
			name = "rowid_admission_failure"
		}
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			t.Cleanup(parent.Close)
			require.NoError(t, proc.SetHashBuildBudgetForTesting(parent))
			txn := &Transaction{proc: proc}
			t.Cleanup(txn.closeWorkspaceAllocation)
			txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
			indexes := make([]int64, 30)
			sources := make([]*batch.Batch, 0, 30)
			cleanSources := func() {
				for _, b := range sources {
					b.Clean(proc.Mp())
				}
				sources = nil
			}
			t.Cleanup(cleanSources)
			for i := range indexes {
				ids, err := txn.batchAllocNewRowIds(64)
				require.NoError(t, err)
				b := batch.NewOffHeapWithSize(1)
				b.Attrs = []string{"__mo_rowid"}
				b.SetVector(0, ids)
				b.SetRowCount(64)
				sources = append(sources, b)
				txn.writes = append(txn.writes, Entry{typ: INSERT, bat: b})
				indexes[i] = int64(i)
			}
			before := txn.workspaceAccount.Snapshot().Used
			originals := make([][]types.Rowid, len(sources))
			for i, b := range sources {
				originals[i] = append([]types.Rowid(nil), vector.MustFixedColNoTypeCheck[types.Rowid](b.Vecs[0])...)
			}
			if refuse {
				require.NoError(t, budget.UpdateAggregateCap(before))
			}
			staged, err := txn.stageWorkspaceMerge(context.Background(), indexes, true)
			if staged != nil {
				t.Cleanup(func() { staged.Clean(proc.Mp()) })
			}
			if refuse {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, staged)
			} else {
				require.NoError(t, err)
				require.NotNil(t, staged)
				require.Equal(t, 30*64, staged.RowCount())
				require.Len(t, staged.Vecs, 1)
				ids := vector.MustFixedColNoTypeCheck[types.Rowid](staged.Vecs[0])
				require.Len(t, ids, 30*64)
				for i, id := range ids {
					require.Equal(t, uint32(i), id.GetRowOffset())
				}
				staged.Clean(proc.Mp())
			}
			require.Equal(t, before, txn.workspaceAccount.Snapshot().Used)
			for i, b := range sources {
				require.Same(t, b, txn.writes[i].bat)
				require.Equal(t, 64, b.RowCount())
				require.Equal(t, originals[i], vector.MustFixedColNoTypeCheck[types.Rowid](b.Vecs[0]))
			}
			cleanSources()
			require.Zero(t, txn.workspaceAccount.Snapshot().Used)
			require.Zero(t, budget.AggregateUsed())
			txn.closeWorkspaceAllocation()
		})
	}
}
