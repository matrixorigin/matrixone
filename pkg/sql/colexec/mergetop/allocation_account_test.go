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

package mergetop

import (
	"context"
	"math"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type mergeTopTestAllocation struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
}

type mergeTopTestCapacityController struct {
	mu         sync.Mutex
	used       uint64
	limit      uint64
	rejectNext bool
}

func (c *mergeTopTestCapacityController) AcquireAllocationCapacity(capacity uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.rejectNext {
		c.rejectNext = false
		return mpool.ErrAllocationAccountCapacity
	}
	if c.used > c.limit || capacity > c.limit-c.used {
		return mpool.ErrAllocationAccountCapacity
	}
	c.used += capacity
	return nil
}

func (c *mergeTopTestCapacityController) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rejectNext = true
}

func (c *mergeTopTestCapacityController) ReleaseAllocationCapacity(capacity uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if capacity > c.used {
		panic("merge top test capacity underflow")
	}
	c.used -= capacity
}

func (c *mergeTopTestCapacityController) freeze() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limit = c.used
	return c.used
}

func (c *mergeTopTestCapacityController) current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func installMergeTopTestAllocation(
	t testing.TB,
	op *MergeTop,
	limit uint64,
	controller mpool.AllocationCapacityController,
) mergeTopTestAllocation {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, controller)
	require.NoError(t, err)
	require.NoError(t, op.SetAllocationAccount(account))
	return mergeTopTestAllocation{registry: registry, account: account}
}

func finalizeMergeTopTestAllocation(
	t testing.TB,
	op *MergeTop,
	state mergeTopTestAllocation,
) {
	t.Helper()
	require.Zero(t, state.account.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(state.account))
	snapshot, first, err := state.registry.CompleteTerminal(state.account)
	require.NoError(t, err)
	require.True(t, first)
	require.Zero(t, snapshot.Used)
	for _, owner := range snapshot.Owners {
		require.Zero(t, owner.Current)
	}
}

func newAccountedMergeTop(limit uint64) *MergeTop {
	return &MergeTop{
		Limit: plan2.MakePlan2Uint64ConstExprWithType(limit),
		Fs: []*plan.OrderBySpec{{
			Expr: newExpression(0),
		}},
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

func newMergeTopValuesBatch(
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

func runMergeTopInt64(
	t testing.TB,
	op *MergeTop,
	proc *process.Process,
) ([]int64, error) {
	t.Helper()
	result, err := vm.Exec(op, proc)
	if err != nil || result.Batch == nil {
		return nil, err
	}
	return append([]int64(nil),
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...), nil
}

func TestAccountedMergeTopResidentLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeTop(3)
	state := installMergeTopTestAllocation(t, op, 64<<20, nil)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{7, 1, 5}),
		newMergeTopValuesBatch(t, proc, []int64{3, 2, 9}),
	}))
	require.NoError(t, op.Prepare(proc))
	values, err := runMergeTopInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, values)

	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerTop)
	require.True(t, ok)
	require.Positive(t, owner.Current)
	require.Positive(t, owner.Peak)
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeTopComputedOrderExpression(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	orderExpr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"+",
		[]*plan.Expr{newExpression(0), plan2.MakePlan2Int64ConstExprWithType(0)},
	)
	require.NoError(t, err)
	op := newAccountedMergeTop(2)
	op.Fs[0].Expr = orderExpr
	state := installMergeTopTestAllocation(t, op, 64<<20, nil)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{4, 1}),
		newMergeTopValuesBatch(t, proc, []int64{3, 2}),
	}))
	require.NoError(t, op.Prepare(proc))
	values, err := runMergeTopInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, values)
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerTop)
	require.True(t, ok)
	require.Positive(t, owner.Peak)

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeMergeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeTopExactCapacityBoundary(t *testing.T) {
	type boundary struct {
		used uint64
		peak uint64
	}
	build := func(t *testing.T, accountLimit uint64) (boundary, error) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		op := newAccountedMergeTop(2048)
		state := installMergeTopTestAllocation(t, op, accountLimit, nil)
		source := newMergeTopValuesBatch(t, proc, []int64{4, 3, 2, 1})
		op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		err := op.Prepare(proc)
		if err == nil {
			_, err = runMergeTopInt64(t, op, proc)
		}
		result := boundary{
			used: state.account.Snapshot().Used,
			peak: state.account.Snapshot().Peak,
		}
		op.Children[0].Free(proc, err != nil, err)
		op.Free(proc, err != nil, err)
		finalizeMergeTopTestAllocation(t, op, state)
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
		return result, err
	}

	baseline, err := build(t, 64<<20)
	require.NoError(t, err)
	require.Positive(t, baseline.used)
	require.GreaterOrEqual(t, baseline.peak, baseline.used)
	exact, err := build(t, baseline.peak)
	require.NoError(t, err)
	require.Equal(t, baseline.used, exact.used)
	_, err = build(t, baseline.peak-1)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), err)
}

func TestAccountedMergeTopCapacityFailureIsControlledAndCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeTop(2048)
	controller := &mergeTopTestCapacityController{limit: 64 << 20}
	state := installMergeTopTestAllocation(t, op, 64<<20, controller)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{3, 2, 1}),
	}))
	require.NoError(t, op.Prepare(proc))
	before := state.account.Snapshot().Used
	require.Positive(t, before)
	require.Equal(t, before, controller.freeze())

	_, err := runMergeTopInt64(t, op, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), err)
	require.Contains(t, err.Error(), "merge top memory capacity exceeded")
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeMergeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeTopFinalShufflePressureCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeTop(4)
	controller := &mergeTopTestCapacityController{limit: 64 << 20}
	state := installMergeTopTestAllocation(t, op, 64<<20, controller)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{4, 3, 2, 1}),
	}))
	require.NoError(t, op.Prepare(proc))
	end, err := op.ctr.build(op, proc, op.OpAnalyzer)
	require.NoError(t, err)
	require.False(t, end)
	require.NotNil(t, op.ctr.bat)
	require.Equal(t, state.account.Snapshot().Used, controller.freeze())

	var result vm.CallResult
	err = op.ctr.eval(op.ctr.limit, proc, op.OpAnalyzer, &result)
	require.Error(t, err)
	converted := mergeTopTerminalCapacityError(proc.Ctx, err)
	require.True(t, moerr.IsMoErrCode(converted, moerr.ErrOOM), converted)
	require.Nil(t, result.Batch)
	op.Children[0].Free(proc, true, err)
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeMergeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeTopMultiColumnAppendFailureRollsBack(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZeroNoFixed())
	defer func() {
		proc.Free()
		mpool.DeleteMPool(proc.Mp())
	}()
	op := newAccountedMergeTop(2)
	controller := &mergeTopTestCapacityController{limit: ^uint64(0)}
	state := installMergeTopTestAllocation(t, op, 64<<20, controller)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, proc.Mp()))
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(input.Vecs[1], []byte("payload"), false, proc.Mp()))
	input.SetRowCount(1)
	defer input.Clean(proc.Mp())

	op.ctr.bat = batch.NewOffHeapWithSize(2)
	for i := range input.Vecs {
		op.ctr.bat.Vecs[i] = vector.NewOffHeapVecWithType(*input.Vecs[i].GetType())
	}
	require.NoError(t, op.ctr.bat.SetAllocationAccount(op.ctr.retainedAllocation))
	require.NoError(t, op.ctr.bat.Vecs[0].PreExtend(1, proc.Mp()))
	var err error
	op.ctr.sels, err = growMergeTopSelections(
		op.ctr.sels, 2, proc, op.ctr.allocationAccount)
	require.NoError(t, err)
	op.ctr.sels = op.ctr.sels[:0]
	_, err = op.ctr.appendCheckpoints(len(op.ctr.bat.Vecs), proc)
	require.NoError(t, err)
	controller.arm()

	err = op.ctr.processBatch(2, input, proc)
	require.Error(t, err)
	require.Zero(t, op.ctr.bat.RowCount())
	require.Zero(t, op.ctr.bat.Vecs[0].Length())
	require.Zero(t, op.ctr.bat.Vecs[1].Length())
	require.Empty(t, op.ctr.sels)
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeMergeTopTestAllocation(t, op, state)
}

func TestAccountedMergeTopCancellationCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeTop(3)
	state := installMergeTopTestAllocation(t, op, 64<<20, nil)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{3, 2, 1}),
	}).WithEndOfDataCallback(cancel)
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := vm.Exec(op, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	child.Free(proc, true, err)
	op.Free(proc, true, err)
	proc.Ctx = baseCtx
	finalizeMergeTopTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedMergeTopResetClosesAllocationGeneration(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedMergeTop(2)
	registry, err := mpool.NewAllocationAccountRegistry(2, 1<<14)
	require.NoError(t, err)
	first, err := registry.Open(64 << 20)
	require.NoError(t, err)
	second, err := registry.Open(64 << 20)
	require.NoError(t, err)

	require.NoError(t, op.SetAllocationAccount(first))
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{4, 1, 3}),
	}))
	require.NoError(t, op.Prepare(proc))
	_, err = runMergeTopInt64(t, op, proc)
	require.NoError(t, err)
	require.Positive(t, first.Snapshot().Used)
	op.Children[0].Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, first.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(first))

	require.NoError(t, op.SetAllocationAccount(second))
	op.Children = nil
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newMergeTopValuesBatch(t, proc, []int64{2, 5, 0}),
	}))
	require.NoError(t, op.Prepare(proc))
	values, err := runMergeTopInt64(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{0, 2}, values)
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	require.Zero(t, second.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(second))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeTopAllocationBindingContract(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 4)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)
	op := newAccountedMergeTop(1)

	var nilOp *MergeTop
	require.ErrorIs(t, nilOp.SetAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, nilOp.ClearAllocationAccount(first), mpool.ErrAllocationAccountInvalid)
	require.ErrorIs(t, op.SetAllocationAccount(nil), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, op.SetAllocationAccount(first))
	require.NoError(t, op.SetAllocationAccount(first))
	require.ErrorIs(t, op.SetAllocationAccount(second), mpool.ErrAllocationAccountMismatch)
	require.NoError(t, op.ClearAllocationAccount(first))
	_, _, err = registry.CompleteTerminal(first)
	require.NoError(t, err)
	_, _, err = registry.CompleteTerminal(second)
	require.NoError(t, err)
}

func TestMergeTopAllocationHelperBoundaryMatrix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)

	var nilCtr *container
	require.ErrorIs(t, nilCtr.setAllocationAccount(account), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilCtr.clearAllocationAccount(account))
	ctr := &container{executorsForOrderList: []colexec.ExpressionExecutor{nil}}
	require.ErrorIs(t, ctr.setAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
	ctr.executorsForOrderList = nil
	require.NoError(t, ctr.setAllocationAccount(account))
	require.NoError(t, ctr.setAllocationAccount(account))

	_, err = growMergeTopSelections(nil, -1, proc, account)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	plain, err := growMergeTopSelections(nil, 3, proc, nil)
	require.NoError(t, err)
	require.Len(t, plain, 3)
	accounted, err := growMergeTopSelections(nil, 3, proc, account)
	require.NoError(t, err)
	accounted, err = growMergeTopSelections(accounted, 2, proc, account)
	require.NoError(t, err)
	require.Len(t, accounted, 2)
	_, err = growMergeTopSelections(nil, math.MaxInt, proc, account)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	ctr.sels = accounted

	_, err = ctr.appendCheckpoints(0, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	checkpoints, err := ctr.appendCheckpoints(2, proc)
	require.NoError(t, err)
	require.Len(t, checkpoints, 2)
	require.ErrorIs(t, ctr.clearAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
	ctr.free(proc)
	require.NoError(t, ctr.clearAllocationAccount(account))
	snapshot, _, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Zero(t, snapshot.Used)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestMergeTopTerminalCapacityErrorPreservesNonCapacityFailures(t *testing.T) {
	require.Same(t, context.Canceled,
		mergeTopTerminalCapacityError(context.Background(), context.Canceled))
	require.ErrorIs(t,
		mergeTopTerminalCapacityError(context.Background(), mpool.ErrAllocationAccountInvariant),
		mpool.ErrAllocationAccountInvariant)
	mpoolCapacity := moerr.NewMPoolCapacityNoCtxf("mpool capacity")
	require.Same(t, mpoolCapacity,
		mergeTopTerminalCapacityError(context.Background(), mpoolCapacity))

	controlled := mergeTopTerminalCapacityError(
		context.Background(),
		mpool.ErrAllocationAccountCapacity,
	)
	require.True(t, moerr.IsMoErrCode(controlled, moerr.ErrOOM), controlled)
}

func BenchmarkMergeTopAccountedResident(b *testing.B) {
	benchmarkMergeTopResident(b, true)
}

func BenchmarkMergeTopUnaccountedResident(b *testing.B) {
	benchmarkMergeTopResident(b, false)
}

func benchmarkMergeTopResident(b *testing.B, accounted bool) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	values := make([]int64, BenchmarkRows)
	for i := range values {
		values[i] = int64(len(values) - i)
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		op := newAccountedMergeTop(1024)
		var state mergeTopTestAllocation
		if accounted {
			state = installMergeTopTestAllocation(b, op, 64<<20, nil)
		}
		op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			newMergeTopValuesBatch(b, proc, values),
		}))
		b.StartTimer()
		require.NoError(b, op.Prepare(proc))
		_, err := vm.Exec(op, proc)
		require.NoError(b, err)
		b.StopTimer()
		op.Children[0].Free(proc, false, nil)
		op.Free(proc, false, nil)
		if accounted {
			finalizeMergeTopTestAllocation(b, op, state)
		}
		require.Zero(b, proc.Mp().CurrNB())
		b.StartTimer()
	}
}
