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

package top

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/topsites"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type topTestAllocation struct {
	generation *process.ExecutionResourceGeneration
	registry   *mpool.AllocationAccountRegistry
	account    *mpool.AllocationAccount
}

func installTopTestAllocation(
	t testing.TB,
	op *Top,
	proc *process.Process,
	limit uint64,
) topTestAllocation {
	t.Helper()
	proc.Base.Lim.Size = int64(limit)
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, generation)
	require.NoError(t, err)
	require.NoError(t, op.SetAllocationAccount(account))
	return topTestAllocation{
		generation: generation,
		registry:   registry,
		account:    account,
	}
}

func finalizeTopTestAllocation(
	t testing.TB,
	op *Top,
	state topTestAllocation,
) {
	t.Helper()
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(state.account))
	require.Zero(t, state.generation.Snapshot().Used)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	snapshot, first, err := state.registry.CompleteTerminal(state.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	for _, owner := range snapshot.Owners {
		require.Zero(t, owner.Current)
	}
}

func newAccountedTop(limit uint64) *Top {
	return &Top{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(limit),
		Fs: []*plan.OrderBySpec{{
			Expr: newExpression(0),
		}},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

func newInt64TopBatch(
	t testing.TB,
	proc *process.Process,
	values []int64,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], values, nil, proc.Mp()))
	bat.SetRowCount(len(values))
	return bat
}

func newNullableVarcharTopBatch(
	t testing.TB,
	proc *process.Process,
	key int64,
	value []byte,
	isNull bool,
) *batch.Batch {
	t.Helper()
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], key, false, proc.Mp()))
	bat.Vecs[1] = vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(bat.Vecs[1], value, isNull, proc.Mp()))
	bat.SetRowCount(1)
	return bat
}

func collectTopInt64(
	t testing.TB,
	op *Top,
	proc *process.Process,
) []int64 {
	t.Helper()
	var values []int64
	for {
		result, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if result.Batch == nil || result.Status == vm.ExecStop {
			return values
		}
		values = append(values,
			vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...)
	}
}

func TestAccountedTopResidentLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(3)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInt64TopBatch(t, proc, []int64{7, 1, 4}),
		newInt64TopBatch(t, proc, []int64{3, 2, 8}),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int64{1, 2, 3}, collectTopInt64(t, op, proc))

	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerTop)
	require.True(t, ok)
	require.Positive(t, owner.Peak)
	require.Positive(t, owner.Current)

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopCopiesLateNullVarchar(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(1)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newNullableVarcharTopBatch(t, proc, 2, []byte("seed"), false),
		newNullableVarcharTopBatch(t, proc, 1, nil, true),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))

	result, err := vm.Exec(op, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int64(1), vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])[0])
	require.True(t, result.Batch.Vecs[1].GetNulls().Contains(0))

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopSpillLifecycleAndStableOrder(t *testing.T) {
	const limit = topSpillThreshold + 1
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(limit)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	values := make([]int64, 3*8192)
	for i := range values {
		values[i] = int64(len(values) - i)
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInt64TopBatch(t, proc, values[:8192]),
		newInt64TopBatch(t, proc, values[8192:2*8192]),
		newInt64TopBatch(t, proc, values[2*8192:]),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	got := collectTopInt64(t, op, proc)
	require.Len(t, got, int(limit))
	for i, value := range got {
		require.Equal(t, int64(i+1), value)
	}
	require.Positive(t, op.OpAnalyzer.GetOpStats().SpillSize)
	require.Zero(t, state.generation.SpillDiskUsed())
	require.Zero(t, state.generation.SpillFDUsed())
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerTop)
	require.True(t, ok)
	require.Positive(t, owner.Peak)

	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopSpillMetadataIndependentOfInputBatchCount(t *testing.T) {
	const limit = topSpillThreshold + 1
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(limit)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	require.NoError(t, op.Prepare(proc))
	op.ctr.n = 1
	initialValues := make([]int64, int(limit))
	for i := range initialValues {
		initialValues[i] = int64(i)
	}
	initial := newInt64TopBatch(t, proc, initialValues)
	require.NoError(t, op.ctr.build(op, initial, proc, op.OpAnalyzer))
	initial.Clean(proc.Mp())
	usedAfterLimit := state.account.Snapshot().Used
	require.Positive(t, usedAfterLimit)

	for i := range 2000 {
		extra := newInt64TopBatch(t, proc, []int64{int64(limit) + int64(i)})
		require.NoError(t, op.ctr.build(op, extra, proc, op.OpAnalyzer))
		extra.Clean(proc.Mp())
	}
	require.Equal(t, usedAfterLimit, state.account.Snapshot().Used,
		"input batch count must grow disk bytes, not resident metadata")
	require.Positive(t, state.generation.SpillDiskUsed())
	require.Equal(t, uint64(1), state.generation.SpillFDUsed())

	op.Free(proc, false, nil)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopResetAndReuse(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(2)
	first := installTopTestAllocation(t, op, proc, 64<<20)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInt64TopBatch(t, proc, []int64{2, 1}),
	})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int64{1, 2}, collectTopInt64(t, op, proc))
	child.Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, first.account.Snapshot().Used)
	require.Nil(t, op.ctr.limitExecutor)
	require.Empty(t, op.ctr.executorsForOrderColumn)
	finalizeTopTestAllocation(t, op, first)

	second := installTopTestAllocation(t, op, proc, 64<<20)
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInt64TopBatch(t, proc, []int64{4, 3}),
	})
	op.Children = nil
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, []int64{3, 4}, collectTopInt64(t, op, proc))
	child.Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeTopTestAllocation(t, op, second)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopPrepareExactCapacityBoundary(t *testing.T) {
	prepare := func(t *testing.T, capacity uint64) (uint64, error) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		op := newAccountedTop(3)
		state := installTopTestAllocation(t, op, proc, capacity)
		err := op.Prepare(proc)
		used := state.account.Snapshot().Used
		op.Free(proc, err != nil, err)
		finalizeTopTestAllocation(t, op, state)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
		return used, err
	}

	required, err := prepare(t, 64<<20)
	require.NoError(t, err)
	require.Positive(t, required)
	used, err := prepare(t, required)
	require.NoError(t, err)
	require.Equal(t, required, used)
	_, err = prepare(t, required-1)
	require.Error(t, err)
	require.True(t, mpool.IsRetryableAllocationCapacity(err))
}

func TestAccountedTopRuntimeCapacityRejectionCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(3)
	state := installTopTestAllocation(t, op, proc, 64<<10)
	src := batch.NewWithSize(1)
	src.Vecs[0] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		src.Vecs[0],
		bytes.Repeat([]byte("x"), 1<<20),
		false,
		proc.Mp(),
	))
	src.SetRowCount(1)
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{src})
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	_, err := vm.Exec(op, proc)
	require.Error(t, err)
	require.True(t, mpool.IsRetryableAllocationCapacity(err))

	child.Free(proc, true, err)
	op.Free(proc, true, err)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopSpillResourceAdmissionCleans(t *testing.T) {
	tests := []struct {
		name      string
		component process.ExecutionResourceComponent
		reserve   func(*process.ExecutionResourceGeneration) (func(), error)
	}{
		{
			name:      "disk",
			component: process.ExecutionResourceComponentSpillDisk,
			reserve: func(g *process.ExecutionResourceGeneration) (func(), error) {
				token, err := g.ReserveSpillDisk(g.SpillDiskCap())
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
			reserve: func(g *process.ExecutionResourceGeneration) (func(), error) {
				token, err := g.ReserveSpillFD(g.SpillFDCap())
				return func() {
					if token != nil {
						token.Release()
					}
				}, err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
			op := newAccountedTop(topSpillThreshold + 1)
			state := installTopTestAllocation(t, op, proc, 64<<20)
			releaseBlocker, err := test.reserve(state.generation)
			require.NoError(t, err)
			released := false
			defer func() {
				if !released {
					releaseBlocker()
				}
			}()
			child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
				newInt64TopBatch(t, proc, []int64{1}),
			})
			op.AppendChild(child)
			require.NoError(t, op.Prepare(proc))
			_, err = vm.Exec(op, proc)
			var resourceErr *process.ExecutionResourceError
			require.ErrorAs(t, err, &resourceErr)
			require.Equal(t, test.component, resourceErr.Component)

			child.Free(proc, true, err)
			op.Free(proc, true, err)
			releaseBlocker()
			released = true
			finalizeTopTestAllocation(t, op, state)
			proc.Free()
			require.Zero(t, proc.Mp().CurrNB())
		})
	}
}

func TestAccountedTopCorruptSpillFailsClosed(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(topSpillThreshold + 1)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	require.NoError(t, op.Prepare(proc))
	op.ctr.n = 1
	src := newInt64TopBatch(t, proc, []int64{1})
	record, err := op.ctr.spillBatch(
		src,
		proc,
		process.NewAnalyzer(0, false, false, "top-corrupt-spill"),
	)
	require.NoError(t, err)
	require.NoError(t, op.ctr.flushSpillWriter())
	op.ctr.sels, err = growTopSlice(
		op.ctr.sels,
		1,
		proc,
		op.ctr.spillAllocation,
		topsites.TopSelections,
	)
	require.NoError(t, err)
	op.ctr.rowRefs, err = growTopSlice(
		op.ctr.rowRefs,
		1,
		proc,
		op.ctr.spillAllocation,
		topsites.TopRowReferences,
	)
	require.NoError(t, err)
	op.ctr.sels[0] = 0
	op.ctr.rowRefs[0] = rowRef{
		offset: record.offset,
		size:   record.size - 1,
		rowIdx: 0,
	}
	op.ctr.spillOrdered = true
	var result vm.CallResult
	done, err := op.ctr.evalSpill(topSpillThreshold+1, 1, proc, &result)
	require.Error(t, err)
	require.False(t, done)
	require.Nil(t, result.Batch)

	src.Clean(proc.Mp())
	op.Free(proc, true, err)
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopCancellationCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedTop(topSpillThreshold + 1)
	state := installTopTestAllocation(t, op, proc, 64<<20)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInt64TopBatch(t, proc, []int64{3, 2, 1}),
	}).WithEndOfDataCallback(cancel)
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := vm.Exec(op, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	child.Free(proc, true, err)
	op.Free(proc, true, err)
	proc.Ctx = baseCtx
	finalizeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedTopSetClearContract(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.Base.Lim.Size = 64 << 20
	generation, err := proc.GetExecutionResourceBudget()
	require.NoError(t, err)
	registry, err := mpool.NewAllocationAccountRegistry(2, 32)
	require.NoError(t, err)
	first, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)
	second, err := registry.OpenWithController(64<<20, generation)
	require.NoError(t, err)
	op := newAccountedTop(1)
	var nilOp *Top
	require.ErrorIs(t, nilOp.SetAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, nilOp.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	var nilCtr *container
	require.ErrorIs(t, nilCtr.setAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilCtr.clearAllocationAccount(first))
	require.ErrorIs(t, op.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, op.SetAllocationAccount(first))
	require.NoError(t, op.SetAllocationAccount(first))
	require.ErrorIs(t, op.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.ErrorIs(t, op.ClearAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.NoError(t, op.Prepare(proc))
	require.ErrorIs(t, op.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvariant)
	op.Free(proc, false, nil)
	require.NoError(t, op.ClearAllocationAccount(first))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
	proc.Free()
}

func BenchmarkTopAccountedResident(b *testing.B) {
	benchmarkTopResident(b, true)
}

func BenchmarkTopUnaccountedResident(b *testing.B) {
	benchmarkTopResident(b, false)
}

func benchmarkTopResident(b *testing.B, accounted bool) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		op := newAccountedTop(3)
		var state topTestAllocation
		if accounted {
			state = installTopTestAllocation(b, op, proc, 64<<20)
		}
		child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			newBatch([]types.Type{types.T_int8.ToType()}, proc, BenchmarkRows),
			newBatch([]types.Type{types.T_int8.ToType()}, proc, BenchmarkRows),
		})
		op.AppendChild(child)
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
		child.Free(proc, false, nil)
		op.Free(proc, false, nil)
		if accounted {
			finalizeTopTestAllocation(b, op, state)
		}
		require.Zero(b, proc.Mp().CurrNB())
		b.StartTimer()
	}
}
