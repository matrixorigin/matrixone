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

package multi_update

import (
	"context"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func committedDeleteInput(t *testing.T, proc *process.Process, first, rows int) *batch.Batch {
	t.Helper()
	b := batch.NewWithSize(2)
	b.Attrs = DeleteBatchAttrs
	b.Vecs[0] = vector.NewOffHeapVecWithType(types.T_Rowid.ToType())
	b.Vecs[1] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	t.Cleanup(func() { b.Clean(proc.Mp()) })
	for id := first; id < first+rows; id++ {
		block := types.BuildTestBlockid(1, int64(id))
		require.NoError(t, vector.AppendFixed(b.Vecs[0], types.NewRowid(&block, 0), false, proc.Mp()))
		require.NoError(t, vector.AppendFixed(b.Vecs[1], int64(id), false, proc.Mp()))
	}
	b.SetRowCount(rows)
	return b
}

func deleteContractWriter(credits *admissionThrottler) *s3WriterDelegate {
	w := &s3WriterDelegate{
		updateCtxs:    []*MultiUpdateCtx{{TableDef: &plan.TableDef{Name: "delete_contract", TblId: 99}, DeleteCols: []int{0, 1}}},
		insertSinkers: make([]*colexec.CNS3Writer, 1), deleteBatches: make([]*batch.BatchSet, 1),
		deleteBlockMap: make([]map[types.Blockid]*deleteBlockData, 1), segmentMap: make(map[string]int32),
		flushThreshold: 64 << 20,
	}
	w.memController.throttler = credits
	return w
}

func TestCommittedDeleteFutureGrowthRemainsAdmitted(t *testing.T) {
	_, ctrl, proc := prepareTestCtx(t, true)
	t.Cleanup(proc.Free)
	t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
	credits := &admissionThrottler{capacity: 1 << 30}
	w := deleteContractWriter(credits)
	t.Cleanup(func() { require.NoError(t, w.free(proc)) })
	analyzer := process.NewAnalyzer(0, false, false, "delete-contract")
	for first := 1; first <= 5; first += 2 {
		require.NoError(t, w.append(proc, analyzer, committedDeleteInput(t, proc, first, 2)))
		require.Greater(t, w.chargedBytes(), w.retainedBytes(), "deferred materialization is an owner, not unused credit")
		require.Equal(t, w.chargedBytes(), credits.reserved)
	}
	var observed int
	txnOp := mock_frontend.NewMockTxnOperator(ctrl)
	txnOp.EXPECT().Txn().DoAndReturn(func() txn.TxnMeta {
		observed++
		require.LessOrEqual(t, w.retainedBytes(), credits.reserved, "observe real buffers and per-block bitmap growth before each next mutation")
		return txn.TxnMeta{ID: []byte{1}}
	}).AnyTimes()
	proc.Base.TxnOperator = txnOp
	acquires := credits.acquires
	result, err := w.prepareDeleteBatches(proc, 0, w.deleteBatches[0].TakeBatches(), true)
	defer func() {
		for _, b := range result {
			b.Clean(w.memoryPool(proc))
		}
	}()
	require.NoError(t, err)
	require.Equal(t, 6, observed)
	require.Equal(t, acquires, credits.acquires, "materialization cannot reacquire D while holding its own source")
	var rows int
	for _, b := range result {
		rows += b.RowCount()
	}
	require.Equal(t, 6, rows)
	require.LessOrEqual(t, w.retainedBytes(), credits.reserved)
	for _, b := range result {
		b.Clean(w.memoryPool(proc))
	}
	result = nil
	w.cleanDeleteBatches(w.memoryPool(proc))
	w.reconcileMemory()
	require.Zero(t, credits.reserved)
	require.NoError(t, w.reset(proc))
	require.NoError(t, w.append(proc, analyzer, committedDeleteInput(t, proc, 10, 2)))
	require.Positive(t, credits.reserved)
}

func TestCommittedDeletePreparationFailureReleasesOwners(t *testing.T) {
	for _, failure := range []string{"cancel", "allocation"} {
		t.Run(failure, func(t *testing.T) {
			_, _, proc := prepareTestCtx(t, true)
			t.Cleanup(proc.Free)
			t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
			credits := &admissionThrottler{capacity: 1 << 30}
			w := deleteContractWriter(credits)
			t.Cleanup(func() { require.NoError(t, w.free(proc)) })
			if failure == "allocation" {
				var err error
				w.ownedMP, err = mpool.NewMPool("delete-partial-failure", 1<<20, mpool.NoFixed)
				require.NoError(t, err)
			}
			input := committedDeleteInput(t, proc, 1, 2)
			if failure == "allocation" {
				input.Vecs[1].Free(proc.Mp())
				input.Vecs[1] = testutil.MakeVarcharVector([]string{strings.Repeat("x", 320<<10), strings.Repeat("y", 320<<10)}, nil, proc.Mp())
			}
			require.NoError(t, w.append(proc, process.NewAnalyzer(0, false, false, "delete-failure"), input))
			if failure == "cancel" {
				ctx, cancel := context.WithCancel(proc.Ctx)
				proc.Ctx = ctx
				cancel()
			}
			result, err := w.prepareDeleteBatches(proc, 0, w.deleteBatches[0].TakeBatches(), true)
			for _, b := range result {
				b.Clean(w.memoryPool(proc))
			}
			require.Error(t, err)
			if failure == "cancel" {
				require.ErrorIs(t, err, context.Canceled)
			}
			require.NoError(t, w.reset(proc))
			require.Zero(t, credits.reserved)
			require.Zero(t, w.retainedBytes())
			require.Empty(t, w.deleteBlockMap[0])
			require.Zero(t, w.chargedBytes())
			require.False(t, credits.invalidRelease)
		})
	}
}

func TestPartitionWriterDrainsAllMetadataFrames(t *testing.T) {
	for _, count := range []int{0, 1, 3, 5} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			proc := testutil.NewProc(t)
			t.Cleanup(proc.Free)
			w := &s3WriterDelegate{updateCtxs: []*MultiUpdateCtx{{TableDef: &plan.TableDef{TblId: 99}}, {TableDef: &plan.TableDef{TblId: 100}}}}
			t.Cleanup(func() { require.NoError(t, w.free(proc)) })
			src := batch.NewWithSize(1)
			t.Cleanup(func() { src.Clean(proc.Mp()) })
			src.Vecs[0] = testutil.MakeInt64Vector([]int64{7}, nil, proc.Mp())
			src.SetRowCount(1)
			var want []uint64
			for i := 0; i < count; i++ {
				// Alternate table and action; the spool must still emit deletes
				// before inserts, with each table's frame preserved exactly once.
				action := actionInsert
				if i%2 == 0 {
					action = actionDelete
				}
				require.NoError(t, w.spoolFrame(proc, action, i%2, uint64(i+1), "", src))
			}
			for _, parity := range []int{0, 1} {
				for i := parity; i < count; i += 2 {
					want = append(want, uint64(i+1))
				}
			}
			w.tailFlushed = true
			raw := NewArgument()
			t.Cleanup(raw.Release)
			raw.Action = UpdateWriteS3
			raw.OpAnalyzer = process.NewAnalyzer(0, false, false, "partition-contract")
			raw.ctr.state = vm.Build
			once := false
			raw.getFlushableS3WriterFunc = func() *s3WriterDelegate {
				if once {
					return nil
				}
				once = true
				return w
			}
			op := &PartitionMultiUpdate{raw: raw}
			child := colexec.NewMockOperator().WithBatchs(nil)
			t.Cleanup(func() { child.Free(proc, false, nil) })
			op.AppendChild(child)
			var got []uint64
			for calls := 0; ; calls++ {
				require.Less(t, calls, count+2)
				result, err := op.Call(proc)
				require.NoError(t, err)
				if result.Batch == nil {
					break
				}
				got = append(got, vector.MustFixedColNoTypeCheck[uint64](result.Batch.Vecs[2])...)
			}
			require.Equal(t, want, got)
		})
	}
}

func TestPartitionWriterCleanupCoversBuildAndPartialEval(t *testing.T) {
	for _, reset := range []bool{false, true} {
		_, _, proc := prepareTestCtx(t, true)
		t.Cleanup(proc.Free)
		t.Cleanup(func() { proc.GetFileService().Close(context.Background()) })
		credits := &admissionThrottler{capacity: 1 << 30}
		build, eval := deleteContractWriter(credits), deleteContractWriter(credits)
		for _, w := range []*s3WriterDelegate{build, eval} {
			require.NoError(t, w.append(proc, process.NewAnalyzer(0, false, false, "partition-cleanup"), committedDeleteInput(t, proc, 1, 2)))
		}
		raw := NewArgument()
		t.Cleanup(raw.Release)
		op := &PartitionMultiUpdate{raw: raw, writers: map[uint64]*s3WriterDelegate{99: build}, freeWriters: []*s3WriterDelegate{eval}, affectedRows: 3}
		if reset {
			op.Reset(proc, true, context.Canceled)
			require.Equal(t, uint64(3), op.affectedRows, "Compile reads the result after Scope.Run resets writers")
			require.Equal(t, vm.Build, raw.ctr.state)
		} else {
			op.Free(proc, true, context.Canceled)
		}
		require.Empty(t, op.writers)
		require.Empty(t, op.freeWriters)
		require.Zero(t, credits.reserved)
		require.Nil(t, build.ownedMP)
		require.Nil(t, eval.ownedMP)
		op.Free(proc, false, nil)
		require.False(t, credits.invalidRelease)
	}
}
