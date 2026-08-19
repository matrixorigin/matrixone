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

package mergeorder

import (
	"errors"
	"io"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/spillutil"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type mergeOrderTestAllocation struct {
	generation *process.ExecutionResourceGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
}

type shortMergeOrderDiskWriter struct{}

func (shortMergeOrderDiskWriter) Write(value []byte) (int, error) {
	return len(value) - 1, nil
}

func installMergeOrderTestAllocation(
	t testing.TB,
	op *MergeOrder,
	proc *process.Process,
	limit uint64,
) mergeOrderTestAllocation {
	t.Helper()
	proc.Base.Lim.Size = int64(limit)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	require.NoError(t, op.SetAllocationAccount(account))
	return mergeOrderTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
}

func finalizeMergeOrderTestAllocation(
	t testing.TB,
	op *MergeOrder,
	state mergeOrderTestAllocation,
) {
	t.Helper()
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(state.account))
	require.Zero(t, state.generation.Snapshot().Used)
	snapshot, first, err := state.registry.CompleteTerminal(state.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	for _, owner := range snapshot.Owners {
		require.Zero(t, owner.Current)
	}
}

func newAccountedMergeOrder() *MergeOrder {
	return &MergeOrder{
		OrderBySpecs: []*plan.OrderBySpec{{
			Expr: newExpression(0, types.T_int8),
		}},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

func TestAccountedMergeOrderResidentLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	batches := []*batch.Batch{
		newValuesBatch(proc, []int8{1, 4, 7}),
		newValuesBatch(proc, []int8{2, 5, 8}),
		newValuesBatch(proc, []int8{3, 6, 9}),
	}
	op.AppendChild(colexec.NewMockOperator().WithBatchs(batches))
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int8{1, 2, 3, 4, 5, 6, 7, 8, 9},
		collectInt8Results(t, op, proc, 0))

	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerOrder)
	require.True(t, ok)
	require.Positive(t, owner.Peak)
	require.Positive(t, owner.Current)

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderResetAndReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	first := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	resetChildren(op, []*batch.Batch{
		newValuesBatch(proc, []int8{2}),
		newValuesBatch(proc, []int8{1}),
	})
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int8{1, 2}, collectInt8Results(t, op, proc, 0))
	require.Positive(t, first.account.Snapshot().Used)

	op.Children[0].Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, first.account.Snapshot().Used)
	require.Nil(t, op.ctr.buf)
	require.Empty(t, op.ctr.executors)
	finalizeMergeOrderTestAllocation(t, op, first)

	second := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	resetChildren(op, []*batch.Batch{
		newValuesBatch(proc, []int8{4}),
		newValuesBatch(proc, []int8{3}),
	})
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int8{3, 4}, collectInt8Results(t, op, proc, 0))
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeOrderTestAllocation(t, op, second)

	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderSpillRunBoundAndResources(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	require.NoError(t, op.Prepare(proc))
	op.ctr.generateCompares(op.OrderBySpecs)
	analyzer := process.NewAnalyzer(0, false, false, "accounted-mergeorder-runs")

	for value := int8(40); value > 0; value-- {
		bat := newValuesBatch(proc, []int8{value})
		run, err := op.ctr.spillBatchToNewRun(proc, bat, nil, analyzer)
		bat.Clean(proc.Mp())
		require.NoError(t, err)
		op.ctr.spillRuns = append(op.ctr.spillRuns, run)
		require.LessOrEqual(t, len(op.ctr.spillRuns), spillMergeFanIn-1)
		require.LessOrEqual(t, state.generation.SpillFDUsed(), uint64(spillMergeFanIn))
	}
	require.Positive(t, state.generation.SpillDiskUsed())
	require.Equal(t, uint64(len(op.ctr.spillRuns)), state.generation.SpillFDUsed())
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerOrder)
	require.True(t, ok)
	require.Positive(t, owner.Peak,
		"the accounted coalescing writer must be visible under Order")

	op.Free(proc, false, nil)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderResidentMetadataBoundSpills(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	op.SpillThreshold = 1 << 30
	state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	batches := make([]*batch.Batch, maxResidentBatches+1)
	for i := range batches {
		batches[i] = newValuesBatch(proc, []int8{1})
	}
	op.AppendChild(colexec.NewMockOperator().WithBatchs(batches))
	require.NoError(t, op.Prepare(proc))
	require.Len(t, collectInt8Results(t, op, proc, 0), len(batches))
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderForcedSpillKeepsOrdering(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	op.SpillThreshold = 1
	state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	batches := make([]*batch.Batch, 0, 40)
	for value := int8(40); value > 0; value-- {
		batches = append(batches, newValuesBatch(proc, []int8{value}))
	}
	op.AppendChild(colexec.NewMockOperator().WithBatchs(batches))
	require.NoError(t, op.Prepare(proc))
	got := collectInt8Results(t, op, proc, 0)
	want := make([]int8, 40)
	for i := range want {
		want[i] = int8(i + 1)
	}
	require.Equal(t, want, got)
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderPhysicalPressureSpillsBelowPolicyHint(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	// Keep the logical policy ceiling well above this input while disabling
	// append-tail state. The only spill trigger below is real account pressure.
	op.SpillThreshold = 8 << 20
	state := installMergeOrderTestAllocation(t, op, proc, 16<<20)
	first := newValuesBatch(proc, []int8{1})
	second := newValuesBatch(proc, []int8{2})
	var external uint64
	child := colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			switch index {
			case 1:
				snapshot := state.generation.Snapshot()
				require.Less(t, snapshot.Used, snapshot.Cap)
				external = snapshot.Cap - snapshot.Used - 1
				require.NoError(t,
					state.generation.AcquireAllocationCapacity(external))
			case 2:
				state.generation.ReleaseAllocationCapacity(external)
				external = 0
			}
		})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	defer func() {
		if external != 0 {
			state.generation.ReleaseAllocationCapacity(external)
		}
	}()

	require.Equal(t, []int8{1, 2}, collectInt8Results(t, op, proc, 0))
	require.Positive(t, state.generation.RejectCount())
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderSpillResourceAdmissionCleans(t *testing.T) {
	tests := []struct {
		name      string
		component process.ExecutionResourceComponent
		reserve   func(*process.ExecutionResourceGeneration) (func(), error)
	}{
		{
			name:      "disk",
			component: process.ExecutionResourceComponentSpillDisk,
			reserve: func(generation *process.ExecutionResourceGeneration) (func(), error) {
				token, err := generation.ReserveSpillDisk(generation.SpillDiskCap())
				return func() {
					if token != nil {
						token.Release()
					}
				}, err
			},
		},
		{
			name:      "file-descriptor",
			component: process.ExecutionResourceComponentSpillFD,
			reserve: func(generation *process.ExecutionResourceGeneration) (func(), error) {
				token, err := generation.ReserveSpillFD(generation.SpillFDCap())
				return func() {
					if token != nil {
						token.Release()
					}
				}, err
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := newAccountedMergeOrder()
			state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
			releaseBlocker, err := tc.reserve(state.generation)
			require.NoError(t, err)
			released := false
			defer func() {
				if !released {
					releaseBlocker()
				}
			}()
			require.NoError(t, op.Prepare(proc))

			bat := newValuesBatch(proc, []int8{1, 2, 3})
			_, err = op.ctr.spillBatchToNewRun(
				proc,
				bat,
				nil,
				process.NewAnalyzer(0, false, false, "mergeorder-admission"),
			)
			bat.Clean(proc.Mp())
			var resourceErr *process.ExecutionResourceError
			require.True(t, errors.As(err, &resourceErr))
			require.Equal(t, tc.component, resourceErr.Component)

			releaseBlocker()
			released = true
			op.Free(proc, true, err)
			require.Zero(t, state.generation.SpillDiskUsed())
			require.Zero(t, state.generation.SpillFDUsed())
			finalizeMergeOrderTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestAccountedMergeOrderShortDiskWriteReconciles(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeOrder()
	state := installMergeOrderTestAllocation(t, op, proc, 64<<20)
	require.NoError(t, op.Prepare(proc))
	run, err := op.ctr.createSpillRun(proc)
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.generation.SpillFDUsed())

	writer := spillutil.NewDiskReservationWriter(
		shortMergeOrderDiskWriter{},
		run.diskToken,
	)
	written, err := writer.Write([]byte{1, 2, 3, 4})
	require.ErrorIs(t, err, io.ErrShortWrite)
	require.Equal(t, 3, written)
	require.Equal(t, uint64(3), run.diskToken.Size())
	require.Equal(t, uint64(3), state.generation.SpillDiskUsed())

	run.close()
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	op.Free(proc, true, err)
	finalizeMergeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeOrderOptionalTailFallsBackToCommittedRun(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.Lim.Size = 64 << 20
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(1, generation)
	require.NoError(t, err)
	op := newAccountedMergeOrder()
	require.NoError(t, op.SetAllocationAccount(account))
	require.NoError(t, op.Prepare(proc))
	op.ctr.generateCompares(op.OrderBySpecs)
	op.ctr.spillAppendEnabled = true
	op.ctr.spillAppendTarget = 1 << 20

	bat := newValuesBatch(proc, []int8{1})
	err = op.ctr.spillEvaluatedBatch(
		proc,
		bat,
		[]*vector.Vector{bat.Vecs[0]},
		process.NewAnalyzer(0, false, false, "mergeorder-tail-fallback"),
	)
	require.NoError(t, err)
	require.False(t, op.ctr.spillAppendEnabled)
	require.Nil(t, op.ctr.spillActiveRun)
	require.Len(t, op.ctr.spillRuns, 1)
	require.Empty(t, op.ctr.spillTailCols)
	bat.Clean(proc.Mp())

	op.Free(proc, false, nil)
	require.NoError(t, op.ClearAllocationAccount(account))
	snapshot, first, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	require.Zero(t, generation.SpillDiskUsed())
	require.Zero(t, generation.SpillFDUsed())
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeOrderAllocationBindingBoundaryMatrix(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 64)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)

	var nilOp *MergeOrder
	require.ErrorIs(t, nilOp.SetAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, nilOp.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	var nilCtr *container
	require.ErrorIs(t, nilCtr.setAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilCtr.clearAllocationAccount(first))

	op := newAccountedMergeOrder()
	op.ctr.batchList = []*batch.Batch{batch.NewWithSize(0)}
	require.ErrorIs(t, op.SetAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	op.ctr.batchList = nil
	require.ErrorIs(t, op.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, op.SetAllocationAccount(first))
	require.NoError(t, op.SetAllocationAccount(first))
	require.ErrorIs(t, op.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, op.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	op.ctr.orderCols = [][]*vector.Vector{{}}
	require.ErrorIs(t, op.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	op.ctr.orderCols = nil
	require.NoError(t, op.ClearAllocationAccount(first))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
}

func BenchmarkMergeOrderAccountedResident(b *testing.B) {
	benchmarkMergeOrderResident(b, true)
}

func BenchmarkMergeOrderUnaccountedResident(b *testing.B) {
	benchmarkMergeOrderResident(b, false)
}

func benchmarkMergeOrderResident(b *testing.B, accounted bool) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		op := newAccountedMergeOrder()
		var state mergeOrderTestAllocation
		if accounted {
			state = installMergeOrderTestAllocation(b, op, proc, 64<<20)
		}
		batches := []*batch.Batch{
			newRandomBatch([]types.Type{types.T_int8.ToType()}, proc, BenchmarkRows),
			batch.EmptyBatch,
			newRandomBatch([]types.Type{types.T_int8.ToType()}, proc, BenchmarkRows),
		}
		op.AppendChild(colexec.NewMockOperator().WithBatchs(batches))
		b.StartTimer()
		require.NoError(b, op.Prepare(proc))
		for {
			result, err := vm.Exec(op, proc)
			require.NoError(b, err)
			if result.Status == vm.ExecStop {
				break
			}
		}
		b.StopTimer()
		op.Children[0].Free(proc, false, nil)
		op.Free(proc, false, nil)
		if accounted {
			finalizeMergeOrderTestAllocation(b, op, state)
		}
		require.Zero(b, proc.Mp().CurrNB())
		b.StartTimer()
	}
}
