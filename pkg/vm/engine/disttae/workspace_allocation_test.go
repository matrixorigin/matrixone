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
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestWorkspaceAllocationIndependentClose(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	clone := txn.CloneSnapshotWS().(*Transaction)
	v, vmp, err := txn.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(v, int64(42), false, vmp))
	w, wmp, err := clone.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(w, int64(7), false, wmp))
	v.Free(vmp)
	txn.closeWorkspaceAllocation()
	txn.closeWorkspaceAllocation()
	require.False(t, parent.Closed())
	require.NoError(t, vector.AppendFixed(w, int64(8), false, wmp))
	_, err = txn.workspaceAllocation()
	require.ErrorIs(t, err, mpool.ErrAllocationAccountSealed)
	w.Free(wmp)
	clone.closeWorkspaceAllocation()
	require.Zero(t, budget.AggregateUsed())
}

func TestWorkspaceAllocationConcurrentInitialization(t *testing.T) {
	proc := testutil.NewProcess(t)
	budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	require.NoError(t, proc.SetExecutionResourceBudgetForTesting(parent))
	defer parent.Close()
	txn := &Transaction{proc: proc}
	defer txn.closeWorkspaceAllocation()
	const workers = 16
	selections := make([]*mpool.MPool, workers)
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
func TestWorkspaceAllocationStatementRollbackRetainsEarlierWrites(t *testing.T) {
	for _, stage := range []string{"copy", "rowid"} {
		t.Run(stage, func(t *testing.T) {
			colexec.NewServer("")
			txn := newTransactionWithActivePKTableForTest(t, "v")
			txn.tableCache = new(sync.Map)
			txn.tablesInVain = make(map[uint64]int)
			txn.batchSelectList = make(map[*batch.Batch][]int64)
			txn.deletedBlocks = &deletedBlocks{offsets: make(map[uint64]map[types.Blockid][]int64)}
			txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
			txn.writeWorkspaceThreshold, txn.commitWorkspaceThreshold = 1<<40, 1<<40
			op := newTxnOperatorForTestWithWorkspace(t, txn)
			txn.op = op
			op.EXPECT().EnterIncrStmt().Times(3)
			op.EXPECT().ExitIncrStmt().Times(3)
			op.EXPECT().EnterRollbackStmt().Times(1)
			op.EXPECT().ExitRollbackStmt().Times(1)
			budget := process.MustNewExecutionResourceBudget(4096, 4096)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetExecutionResourceBudgetForTesting(parent))
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
				b.Vecs[0] = vector.NewVec(types.T_int8.ToType(), txn.proc.Mp())
				t.Cleanup(func() { b.Clean(txn.proc.Mp()) })
				for range rows {
					require.NoError(t, vector.AppendFixed(b.Vecs[0], value, false, txn.proc.Mp()))
				}
				if wide {
					v := vector.NewVec(types.T_varchar.ToType(), txn.proc.Mp())
					b.Vecs = append(b.Vecs, v)
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
				_, err = txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, []string{"v", "payload"}[:len(b.Vecs)], DNStore{})
				if err != nil {
					b.Clean(txn.workspaceMP())
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

func TestWorkspaceInsertAdoptsRowidsWithinCapacity(t *testing.T) {
	colexec.NewServer("")
	for _, limit := range []uint64{1500, 2500} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "v")
			budget := process.MustNewExecutionResourceBudget(limit, limit)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetExecutionResourceBudgetForTesting(parent))
			defer parent.Close()
			defer txn.delTransaction()
			txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
			src := batch.NewWithSize(1)
			src.Vecs[0] = vector.NewVec(types.T_int8.ToType(), txn.proc.Mp())
			defer src.Clean(txn.proc.Mp())
			for range 64 {
				require.NoError(t, vector.AppendFixed(src.Vecs[0], int8(1), false, txn.proc.Mp()))
			}
			src.SetRowCount(64)
			b, err := txn.copyWorkspaceBatch(src)
			require.NoError(t, err)
			defer b.Clean(txn.workspaceMP())
			before := txn.workspaceAccount.Snapshot().Used
			rowids, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, []string{"v", "payload"}[:len(b.Vecs)], DNStore{})
			if limit == 1500 {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Nil(t, rowids)
				require.Empty(t, txn.writes)
				require.Equal(t, before, txn.workspaceAccount.Snapshot().Used)
				b.Clean(txn.workspaceMP()) // the caller still owns an unpublished batch
			} else {
				require.NoError(t, err)
				require.Same(t, rowids, b.Vecs[0])
				require.Len(t, txn.writes, 1)
				require.Equal(t, 64, rowids.Length())
				ids := vector.MustFixedColNoTypeCheck[types.Rowid](rowids)
				for i, id := range ids {
					require.Equal(t, uint32(i), id.GetRowOffset())
				}
				require.LessOrEqual(t, txn.workspaceAccount.Snapshot().Peak, limit)
				require.Equal(t, txn.workspaceAccount.Snapshot().Used, txn.workspaceAccount.Snapshot().Peak,
					"only the final payload and rowid buffers should have been charged")
			}
			txn.delTransaction()
			require.Zero(t, txn.workspaceAccount.Snapshot().Used)
			require.Zero(t, budget.AggregateUsed())
		})
	}
}

func TestWorkspaceInsertPreservesRawBatchProvenance(t *testing.T) {
	colexec.NewServer("")
	for _, accounted := range []bool{false, true} {
		t.Run(fmt.Sprint(accounted), func(t *testing.T) {
			txn := newTransactionWithActivePKTableForTest(t, "v")
			budget := process.MustNewExecutionResourceBudget(1<<20, 1<<20)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetExecutionResourceBudgetForTesting(parent))
			defer parent.Close()
			defer txn.delTransaction()
			txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
			b := batch.NewWithSize(1)
			var account *mpool.AllocationAccount
			appendMP := txn.proc.Mp()
			registry, err := parent.AllocationAccountRegistry()
			require.NoError(t, err)
			if accounted {
				account, err = registry.OpenWithController(1<<20, parent)
				require.NoError(t, err)
				defer func() { _, _, err := registry.CompleteTerminal(account); require.NoError(t, err) }()
				appendMP, err = txn.proc.Mp().WithAccount(account, mpool.AllocationOwnerDML, 1, mpool.AllocationCapacityClassDefault)
				require.NoError(t, err)
				b = batch.NewWithSize(1)
				b.SetVector(0, vector.NewVec(types.T_int8.ToType(), appendMP))
			} else {
				b.SetVector(0, vector.NewVec(types.T_int8.ToType(), appendMP))
			}
			bAttrs := []string{"v"}
			require.NoError(t, vector.AppendFixed(b.Vecs[0], int8(1), false, appendMP))
			b.SetRowCount(1)
			rowids, err := txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, bAttrs, DNStore{})
			require.NoError(t, err)
			// The workspace copies a raw caller batch into its own pool and
			// never holds the caller's batch: the caller still owns and frees it.
			require.Len(t, txn.writes, 1)
			require.Same(t, rowids, txn.writes[0].bat.Vecs[0])
			require.Same(t, txn.workspaceMP(), rowids.MPool())
			require.Positive(t, txn.workspaceAccount.Snapshot().Used, "the copy is charged to the workspace")
			require.Same(t, appendMP, b.Vecs[0].MPool())
			b.Clean(appendMP)
			txn.delTransaction()
			if account != nil {
				require.Zero(t, account.Snapshot().Used, "the caller released its batch")
			}
			require.Zero(t, budget.AggregateUsed())
			if account != nil {
				require.Zero(t, account.Snapshot().Used)
			}
		})
	}
}

func TestWorkspaceMergeAdoptsRegeneratedRowids(t *testing.T) {
	colexec.NewServer("")
	for _, limit := range []uint64{140000, 200000} {
		txn := newTransactionWithActivePKTableForTest(t, "v")
		t.Cleanup(txn.delTransaction)
		txn.deletedBlocks = &deletedBlocks{}
		txn.currentRowId.SetSegment(colexec.TxnWorkspaceSegment)
		budget := process.MustNewExecutionResourceBudget(limit, limit)
		parent, err := budget.OpenGeneration(1)
		require.NoError(t, err)
		t.Cleanup(parent.Close)
		require.NoError(t, txn.proc.SetExecutionResourceBudgetForTesting(parent))
		src := batch.NewWithSize(1)
		t.Cleanup(func() { src.Clean(txn.proc.Mp()) })
		src.Vecs[0] = vector.NewVec(types.T_int64.ToType(), txn.proc.Mp())
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
			_, e = txn.WriteBatch(INSERT, "", 1, 7, 42, "db", "tbl", b, []string{"v", "payload"}[:len(b.Vecs)], DNStore{})
			if e != nil {
				b.Clean(txn.workspaceMP())
			}
			require.NoError(t, e)
		}
		before := txn.workspaceAccount.Snapshot().Used
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
		}
		src.Clean(txn.proc.Mp())
		txn.delTransaction()
		require.Zero(t, txn.workspaceAccount.Snapshot().Used)
		require.Zero(t, budget.AggregateUsed())
		parent.Close()
	}
}

func TestWorkspaceCatalogInsertOwnsSuccessAndFailure(t *testing.T) {
	for _, rows := range []int{1, 1024} {
		t.Run(fmt.Sprintf("rows=%d", rows), func(t *testing.T) {
			colexec.NewServer("")
			txn := newTransactionWithActivePKTableForTest(t, "pk")
			issue25590InitDumpableFixture(t, txn)
			budget := process.MustNewExecutionResourceBudget(4096, 4096)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			require.NoError(t, txn.proc.SetExecutionResourceBudgetForTesting(parent))
			defer parent.Close()
			defer txn.delTransaction()

			view, err := txn.workspaceAllocation()
			require.NoError(t, err)
			input := batch.NewWithSize(1)
			input.Attrs = []string{"payload"}
			input.Vecs[0] = vector.NewVec(types.T_int8.ToType(), view)
			for range rows {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], int8(42), false, view))
			}
			input.SetRowCount(rows)
			err = txn.adoptCatalogInsert("catalog insert", catalog.MO_COLUMNS_ID, catalog.MO_COLUMNS, input)
			if rows == 1024 {
				require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
				require.Empty(t, txn.writes)
				require.Zero(t, txn.workspaceAccount.Snapshot().Used)
			} else {
				require.NoError(t, err)
				require.Same(t, input, txn.writes[0].bat)
				require.Equal(t, int8(42), vector.GetFixedAtNoTypeCheck[int8](txn.writes[0].bat.Vecs[1], 0))
				require.NoError(t, txn.checkWorkspaceAccountingLocked())
			}
			require.Empty(t, txn.workspaceCopies)
		})
	}
}
