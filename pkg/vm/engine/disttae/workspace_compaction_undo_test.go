// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestCompactionRetiresOnlyCapturedDeletes(t *testing.T) {
	var a, b types.Blockid
	b[0] = 1
	txn := &Transaction{deletedBlocks: &deletedBlocks{offsets: map[types.Blockid][]int64{
		a: {1, 2},
		b: {4},
	}}}
	undo := &workspaceCompactionUndo{deleted: map[types.Blockid][]int64{
		a: {1, 2},
	}}
	require.NoError(t, txn.retireCompactionDeletionsLocked(undo))
	_, exists := txn.deletedBlocks.offsets[a]
	require.False(t, exists)
	require.Equal(t, []int64{4}, txn.deletedBlocks.offsets[b])
}

func TestCompactionTerminalFinalizationReleasesRetainedAccount(t *testing.T) {
	colexec.NewServer("")
	txn := newTransactionWithActivePKTableForTest(t, "v")
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, txn.proc.SetHashBuildBudgetForTesting(parent))
	v, err := txn.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(v, int64(42), false, txn.proc.Mp()))
	b := batch.NewWithSize(1)
	b.Vecs[0] = v
	b.SetRowCount(1)
	txn.retiredCompactions = []*workspaceCompactionUndo{{entries: []workspaceCompactionOldEntry{{entry: Entry{bat: b}}}}}
	require.Positive(t, txn.workspaceAccount.Snapshot().Used)
	txn.FinalizeCommit(context.Background())
	require.True(t, txn.removed)
	require.Zero(t, txn.workspaceAccount.Snapshot().Used)
	require.Empty(t, b.Vecs)
	txn.FinalizeCommit(context.Background())
	require.Zero(t, txn.workspaceAccount.Snapshot().Used)
}

func TestCompactionRetirementWaitsForRunningSQL(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	op, closeOp := client.NewTestTxnOperator(ctx)
	defer closeOp()
	colexec.NewServer("")
	proc := testutil.NewProcess(t)
	budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
	parent, err := budget.OpenGeneration(1)
	require.NoError(t, err)
	defer parent.Close()
	require.NoError(t, proc.SetHashBuildBudgetForTesting(parent))
	txn := &Transaction{op: op, proc: proc, engine: &Engine{}}
	op.AddWorkspace(txn)
	v, err := txn.newWorkspaceVector(types.T_int64.ToType())
	require.NoError(t, err)
	require.NoError(t, vector.AppendFixed(v, int64(42), false, proc.Mp()))
	b := batch.NewWithSize(1)
	b.Vecs[0] = v
	b.SetRowCount(1)
	txn.retiredCompactions = []*workspaceCompactionUndo{{entries: []workspaceCompactionOldEntry{{entry: Entry{bat: b}}}}}

	readerCanceled := make(chan struct{})
	var cancelOnce sync.Once
	token := op.EnterRunSqlWithTokenAndSQL(func() { cancelOnce.Do(func() { close(readerCanceled) }) }, "retained reader")
	require.NotZero(t, token)
	var exitOnce sync.Once
	exitReader := func() { exitOnce.Do(func() { op.ExitRunSqlWithToken(token) }) }
	defer exitReader()
	done := make(chan error, 1)
	go func() { done <- op.Rollback(ctx) }()
	select {
	case <-readerCanceled:
	case <-ctx.Done():
		t.Fatal("rollback did not cancel the running reader")
	}
	select {
	case err := <-done:
		t.Fatalf("rollback finished before reader exit: %v", err)
	default:
	}
	require.Positive(t, txn.workspaceAccount.Snapshot().Used)
	require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0))
	exitReader()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("rollback did not finish after reader exit")
	}
	require.True(t, txn.removed)
	require.Zero(t, txn.workspaceAccount.Snapshot().Used)
	require.Empty(t, b.Vecs)
}

func TestFailedBoundaryRollbackKeepsEarlierProducerReservation(t *testing.T) {
	txn := newTransactionWithActivePKTableForTest(t, "v")
	op := newTxnOperatorForTestWithWorkspace(t, txn)
	op.EXPECT().EnterRollbackStmt()
	op.EXPECT().ExitRollbackStmt()
	txn.op = op
	const name = "previous-statement-active-producer"
	require.NoError(t, txn.engine.reserveUnpublishedPendingObject(name))
	txn.noteUnpublishedDumpReservation(name)
	t.Cleanup(func() {
		txn.engine.unpublishedPending.mu.Lock()
		_, active := txn.engine.unpublishedPending.producerNames[name]
		txn.engine.unpublishedPending.mu.Unlock()
		if active {
			txn.engine.abandonUnpublishedPendingReservations([]string{name})
		}
	})
	txn.incrFailedBeforeBoundary = true
	require.NoError(t, txn.RollbackLastStatement(context.Background()))
	txn.unpublishedDumpReservationsMu.Lock()
	_, retained := txn.unpublishedDumpReservations[name]
	txn.unpublishedDumpReservationsMu.Unlock()
	txn.engine.unpublishedPending.mu.Lock()
	_, producerActive := txn.engine.unpublishedPending.producerNames[name]
	txn.engine.unpublishedPending.mu.Unlock()
	require.True(t, retained, "a failed new boundary cannot drain an earlier producer's reservation")
	require.True(t, producerActive, "the earlier producer has not exited")
}

func TestCompactionRetainedBatchRemainsChargedUntilRetirement(t *testing.T) {
	for _, restored := range []bool{false, true} {
		t.Run(map[bool]string{false: "old", true: "replacement"}[restored], func(t *testing.T) {
			proc := testutil.NewProcess(t)
			budget := process.MustNewHashBuildBudget(1<<20, 1<<20)
			parent, err := budget.OpenGeneration(1)
			require.NoError(t, err)
			defer parent.Close()
			require.NoError(t, proc.SetHashBuildBudgetForTesting(parent))
			txn := &Transaction{proc: proc}
			defer txn.closeWorkspaceAllocation()
			v, err := txn.newWorkspaceVector(types.T_int64.ToType())
			require.NoError(t, err)
			require.NoError(t, vector.AppendFixed(v, int64(42), false, proc.Mp()))
			b := batch.NewWithSize(1)
			b.Vecs[0] = v
			b.SetRowCount(1)
			held := workspaceCompactionOldEntry{}
			if restored {
				held.replacement = b
			} else {
				held.entry.bat = b
			}
			txn.retiredCompactions = []*workspaceCompactionUndo{{entries: []workspaceCompactionOldEntry{held}, restored: restored}}
			require.Positive(t, txn.workspaceAccount.Snapshot().Used)
			require.Equal(t, int64(42), vector.GetFixedAtNoTypeCheck[int64](b.Vecs[0], 0))
			require.NoError(t, txn.retireCompactionBatchesLocked())
			require.Zero(t, txn.workspaceAccount.Snapshot().Used)
			require.Empty(t, b.Vecs)
			require.NoError(t, txn.retireCompactionBatchesLocked(), "retirement is idempotent")
		})
	}
}

func TestCompactionRejectsConcurrentDeletesOnRewrittenBlock(t *testing.T) {
	var id types.Blockid
	txn := &Transaction{deletedBlocks: &deletedBlocks{offsets: map[types.Blockid][]int64{
		id: {1, 2},
	}}}
	undo := &workspaceCompactionUndo{deleted: map[types.Blockid][]int64{id: {1}}}
	require.True(t, moerr.IsMoErrCode(txn.retireCompactionDeletionsLocked(undo), moerr.ErrInvalidState))
	require.Equal(t, []int64{1, 2}, txn.deletedBlocks.offsets[id])
}

func TestCompactionDoesNotPartiallyRetireChangedSnapshot(t *testing.T) {
	var a, b types.Blockid
	b[0] = 1
	txn := &Transaction{deletedBlocks: &deletedBlocks{offsets: map[types.Blockid][]int64{
		a: {1},
		b: {9},
	}}}
	undo := &workspaceCompactionUndo{deleted: map[types.Blockid][]int64{
		a: {1},
		b: {4},
	}}
	require.True(t, moerr.IsMoErrCode(txn.retireCompactionDeletionsLocked(undo), moerr.ErrInvalidState))
	require.Equal(t, []int64{1}, txn.deletedBlocks.offsets[a])
	require.Equal(t, []int64{9}, txn.deletedBlocks.offsets[b])
}
