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

package order

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
	"github.com/matrixorigin/matrixone/pkg/sql/internal/ordersites"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type orderTestAllocation struct {
	registry *mpool.AllocationAccountRegistry
	account  *mpool.AllocationAccount
}

type orderTestCapacityController struct {
	mu         sync.Mutex
	used       uint64
	limit      uint64
	rejectNext bool
}

func (c *orderTestCapacityController) AcquireAllocationCapacity(capacity uint64) error {
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

func (c *orderTestCapacityController) ReleaseAllocationCapacity(capacity uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if capacity > c.used {
		panic("order test capacity underflow")
	}
	c.used -= capacity
}

func (c *orderTestCapacityController) arm() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.rejectNext = true
}

func (c *orderTestCapacityController) freeze() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.limit = c.used
	return c.used
}

func (c *orderTestCapacityController) current() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.used
}

func installOrderTestAllocation(
	t testing.TB,
	op *Order,
	limit uint64,
	controller mpool.AllocationCapacityController,
) orderTestAllocation {
	t.Helper()
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<14)
	require.NoError(t, err)
	account, err := registry.OpenWithController(limit, controller)
	require.NoError(t, err)
	require.NoError(t, op.SetAllocationAccount(account))
	return orderTestAllocation{registry: registry, account: account}
}

func finalizeOrderTestAllocation(
	t testing.TB,
	op *Order,
	state orderTestAllocation,
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

func newOrderColumnExpression(position int32, typ types.T) *plan.Expr {
	return &plan.Expr{
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: position}},
		Typ:  plan.Type{Id: int32(typ)},
	}
}

func newAccountedOrder(specs ...*plan.OrderBySpec) *Order {
	return &Order{
		OrderBySpec: specs,
		OperatorBase: vm.OperatorBase{
			OperatorInfo: vm.OperatorInfo{Idx: 0},
		},
	}
}

func newOrderPairBatch(
	t testing.TB,
	proc *process.Process,
	first []int64,
	second []int64,
) *batch.Batch {
	t.Helper()
	require.Len(t, second, len(first))
	bat := batch.NewWithSize(2)
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], first, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], second, nil, proc.Mp()))
	bat.SetRowCount(len(first))
	return bat
}

func runOrderPair(
	t testing.TB,
	op *Order,
	proc *process.Process,
) ([]int64, []int64, error) {
	t.Helper()
	result, err := vm.Exec(op, proc)
	if err != nil || result.Batch == nil {
		return nil, nil, err
	}
	return append([]int64(nil),
			vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0])...),
		append([]int64(nil),
			vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[1])...), nil
}

func TestAccountedOrderResidentMultiKeyAndExpressionLifecycle(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	computed, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"+",
		[]*plan.Expr{
			newOrderColumnExpression(0, types.T_int64),
			plan2.MakePlan2Int64ConstExprWithType(0),
		},
	)
	require.NoError(t, err)
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: computed},
		&plan.OrderBySpec{
			Expr: newOrderColumnExpression(1, types.T_int64),
			Flag: plan.OrderBySpec_DESC,
		},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	firstInput := newOrderPairBatch(t, proc,
		[]int64{2, 1, 2}, []int64{1, 3, 3})
	secondInput := newOrderPairBatch(t, proc,
		[]int64{1, 2, 1}, []int64{2, 2, 1})
	firstInput.ShuffleIDX = 7
	secondInput.ShuffleIDX = 7
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		firstInput,
		secondInput,
	}))
	require.NoError(t, op.Prepare(proc))
	first, second, err := runOrderPair(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 1, 1, 2, 2, 2}, first)
	require.Equal(t, []int64{3, 2, 1, 3, 2, 1}, second)
	require.Equal(t, int32(7), op.ctr.rbat.ShuffleIDX)
	require.NotZero(t, cap(op.ctr.resultOrderList))
	require.NotZero(t, cap(op.ctr.sortScratch.Partitions))
	require.NotZero(t, cap(op.ctr.sortScratch.Diffs))
	require.Equal(t, 6, len(op.ctr.resultOrderList))
	require.Equal(t, 6, len(op.ctr.sortScratch.Diffs))
	owner, ok := state.account.OwnerUsage(mpool.AllocationOwnerOrder)
	require.True(t, ok)
	require.Positive(t, owner.Current)
	require.Positive(t, owner.Peak)

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderVariableProjectionAcrossBatches(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		require.Equal(t, "time_zone", name)
		require.True(t, system)
		require.False(t, global)
		return "UTC", nil
	})
	variable, err := colexec.NewExpressionExecutor(proc, &plan.Expr{
		Expr: &plan.Expr_V{V: &plan.VarRef{
			Name:   "time_zone",
			System: true,
		}},
		Typ: plan.Type{Id: int32(types.T_varchar)},
	})
	require.NoError(t, err)

	newInput := func(ids []int64) *batch.Batch {
		logical := batch.New(nil)
		logical.SetRowCount(len(ids))
		zone, evalErr := variable.Eval(proc, []*batch.Batch{logical}, nil)
		require.NoError(t, evalErr)
		zone, evalErr = zone.Dup(proc.Mp())
		require.NoError(t, evalErr)

		input := batch.NewWithSize(2)
		input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
		require.NoError(t, vector.AppendFixedList(input.Vecs[0], ids, nil, proc.Mp()))
		input.Vecs[1] = zone
		input.SetRowCount(len(ids))
		return input
	}

	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newInput([]int64{4, 1}),
		newInput([]int64{3, 2}),
	}))
	require.NoError(t, op.Prepare(proc))

	result, err := vm.Exec(op, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int64{1, 2, 3, 4},
		vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[0]))
	require.Equal(t, result.Batch.RowCount(), result.Batch.Vecs[1].Length())
	for row := range result.Batch.RowCount() {
		require.Equal(t, "UTC", result.Batch.Vecs[1].GetStringAt(row))
	}

	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeOrderTestAllocation(t, op, state)
	variable.Free()
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderFinalShufflePressureCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	controller := &orderTestCapacityController{limit: 64 << 20}
	state := installOrderTestAllocation(t, op, 64<<20, controller)
	input := newOrderPairBatch(t, proc, []int64{4, 3, 2, 1}, []int64{1, 2, 3, 4})
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, op.Prepare(proc))
	_, err := op.ctr.appendBatch(proc, input)
	require.NoError(t, err)
	op.ctr.resultOrderList, op.ctr.resultOrderMP, err = growOrderSlice(
		op.ctr.resultOrderList,
		op.ctr.resultOrderMP,
		op.ctr.batWaitForSort.RowCount(),
		proc,
		op.ctr.allocationAccount,
		ordersites.OrderSelections,
	)
	require.NoError(t, err)
	for i := range op.ctr.resultOrderList {
		op.ctr.resultOrderList[i] = int64(len(op.ctr.resultOrderList) - i - 1)
	}
	require.Equal(t, state.account.Snapshot().Used, controller.freeze())

	result := vm.NewCallResult()
	err = op.ctr.sortAndSend(proc, &result)
	require.Error(t, err)
	require.Nil(t, result.Batch)
	op.Children[0].Free(proc, true, err)
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderExactCapacityBoundary(t *testing.T) {
	type boundary struct {
		used uint64
		peak uint64
	}
	build := func(t *testing.T, limit uint64) (boundary, error) {
		proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
		op := newAccountedOrder(
			&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
			&plan.OrderBySpec{Expr: newOrderColumnExpression(1, types.T_int64)},
		)
		state := installOrderTestAllocation(t, op, limit, nil)
		op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			newOrderPairBatch(t, proc, []int64{4, 3, 2, 1}, []int64{1, 2, 3, 4}),
		}))
		err := op.Prepare(proc)
		if err == nil {
			_, _, err = runOrderPair(t, op, proc)
		}
		result := boundary{
			used: state.account.Snapshot().Used,
			peak: state.account.Snapshot().Peak,
		}
		op.Children[0].Free(proc, err != nil, err)
		op.Free(proc, err != nil, err)
		finalizeOrderTestAllocation(t, op, state)
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

func TestAccountedOrderCapacityFailureIsControlledAndCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	controller := &orderTestCapacityController{limit: 64 << 20}
	state := installOrderTestAllocation(t, op, 64<<20, controller)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newOrderPairBatch(t, proc, []int64{3, 2, 1}, []int64{1, 2, 3}),
	}))
	require.NoError(t, op.Prepare(proc))
	require.Equal(t, state.account.Snapshot().Used, controller.freeze())

	result, err := vm.Exec(op, proc)
	require.Error(t, err)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOOM), err)
	require.Contains(t, err.Error(), "order memory capacity exceeded")
	require.Nil(t, result.Batch)
	op.Children[0].Free(proc, true, err)
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderMultiColumnAppendFailureRollsBack(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZeroNoFixed())
	defer func() {
		proc.Free()
		mpool.DeleteMPool(proc.Mp())
	}()
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	controller := &orderTestCapacityController{limit: ^uint64(0)}
	state := installOrderTestAllocation(t, op, 64<<20, controller)
	input := batch.NewWithSize(2)
	input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(7), false, proc.Mp()))
	input.Vecs[1] = vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(
		input.Vecs[1], []byte("payload larger than inline varlena storage"), false, proc.Mp()))
	input.SetRowCount(1)
	defer input.Clean(proc.Mp())

	op.ctr.batWaitForSort = batch.NewOffHeapWithSize(2)
	for i := range input.Vecs {
		op.ctr.batWaitForSort.Vecs[i] =
			vector.NewOffHeapVecWithType(*input.Vecs[i].GetType())
	}
	require.NoError(t,
		op.ctr.batWaitForSort.SetAllocationAccount(op.ctr.retainedAllocation))
	require.NoError(t, op.ctr.batWaitForSort.Vecs[0].PreExtend(1, proc.Mp()))
	_, err := op.ctr.appendCheckpoints(len(op.ctr.batWaitForSort.Vecs), proc)
	require.NoError(t, err)
	controller.arm()

	err = op.ctr.appendBatchAccounted(proc, input)
	require.Error(t, err)
	require.Zero(t, op.ctr.batWaitForSort.RowCount())
	require.Zero(t, op.ctr.batWaitForSort.Vecs[0].Length())
	require.Zero(t, op.ctr.batWaitForSort.Vecs[1].Length())
	op.Free(proc, true, err)
	require.Zero(t, controller.current())
	finalizeOrderTestAllocation(t, op, state)
}

func TestAccountedOrderPreservesNonemptyZeroColumnBatch(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder()
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	input := batch.NewWithSize(0)
	input.SetRowCount(1)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, op.Prepare(proc))

	result, err := vm.Exec(op, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Empty(t, result.Batch.Vecs)
	require.Equal(t, 1, result.Batch.RowCount())
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderCancellationCleans(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	baseCtx := proc.Ctx
	ctx, cancel := context.WithCancel(baseCtx)
	proc.Ctx = ctx
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newOrderPairBatch(t, proc, []int64{3, 2, 1}, []int64{1, 2, 3}),
	}).WithEndOfDataCallback(cancel)
	op.AppendChild(child)
	require.NoError(t, op.Prepare(proc))
	result, err := vm.Exec(op, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)

	child.Free(proc, true, err)
	op.Free(proc, true, err)
	proc.Ctx = baseCtx
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderResetClosesAllocationGeneration(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	registry, err := mpool.NewAllocationAccountRegistry(2, 1<<14)
	require.NoError(t, err)
	first, err := registry.Open(64 << 20)
	require.NoError(t, err)
	second, err := registry.Open(64 << 20)
	require.NoError(t, err)

	require.NoError(t, op.SetAllocationAccount(first))
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newOrderPairBatch(t, proc, []int64{3, 1, 2}, []int64{1, 2, 3}),
	}))
	require.NoError(t, op.Prepare(proc))
	values, _, err := runOrderPair(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3}, values)
	require.Positive(t, first.Snapshot().Used)
	op.Children[0].Free(proc, false, nil)
	op.Reset(proc, false, nil)
	require.Zero(t, first.Snapshot().Used)
	require.NoError(t, op.ClearAllocationAccount(first))

	require.NoError(t, op.SetAllocationAccount(second))
	op.Children = nil
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newOrderPairBatch(t, proc, []int64{5, 4}, []int64{1, 2}),
	}))
	require.NoError(t, op.Prepare(proc))
	values, _, err = runOrderPair(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{4, 5}, values)
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

func TestAccountedOrderResetFreesSlicesThroughOriginalMPool(t *testing.T) {
	firstProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	secondProc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
		&plan.OrderBySpec{Expr: newOrderColumnExpression(1, types.T_int64)},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		newOrderPairBatch(t, firstProc, []int64{2, 1}, []int64{1, 2}),
	}))
	require.NoError(t, op.Prepare(firstProc))
	_, _, err := runOrderPair(t, op, firstProc)
	require.NoError(t, err)
	require.Same(t, firstProc.Mp(), op.ctr.resultOrderMP)
	require.Same(t, firstProc.Mp(), op.ctr.sortPartitionsMP)
	require.Same(t, firstProc.Mp(), op.ctr.sortDiffsMP)
	op.Children[0].Free(firstProc, false, nil)

	op.Reset(secondProc, false, nil)
	require.Zero(t, state.account.Snapshot().Used)
	finalizeOrderTestAllocation(t, op, state)
	firstProc.Free()
	secondProc.Free()
	require.Zero(t, firstProc.Mp().CurrNB())
	require.Zero(t, secondProc.Mp().CurrNB())
}

func TestAccountedOrderDoesNotInheritSealedInputAccount(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	inputRegistry, err := mpool.NewAllocationAccountRegistry(1, 16)
	require.NoError(t, err)
	inputAccount, err := inputRegistry.Open(1 << 20)
	require.NoError(t, err)
	inputSelection, err := vector.NewAllocationAccountSelection(
		inputAccount,
		mpool.AllocationOwnerOrder,
		1, 2, 3, 4,
	)
	require.NoError(t, err)
	input := batch.NewOffHeapWithSize(2)
	for i := range input.Vecs {
		input.Vecs[i] = vector.NewOffHeapVecWithType(types.T_int64.ToType())
	}
	require.NoError(t, input.SetAllocationAccount(inputSelection))
	require.NoError(t, vector.AppendFixed(input.Vecs[0], int64(1), false, proc.Mp()))
	require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(2), false, proc.Mp()))
	input.SetRowCount(1)

	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, op.Prepare(proc))
	inputAccount.Seal()

	first, second, err := runOrderPair(t, op, proc)
	require.NoError(t, err)
	require.Equal(t, []int64{1}, first)
	require.Equal(t, []int64{2}, second)
	op.Children[0].Free(proc, false, nil)
	op.Free(proc, false, nil)
	finalizeOrderTestAllocation(t, op, state)
	_, _, err = inputRegistry.CompleteTerminal(inputAccount)
	require.NoError(t, err)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestOrderAllocationBindingAndTerminalErrorContracts(t *testing.T) {
	registry, err := mpool.NewAllocationAccountRegistry(2, 4)
	require.NoError(t, err)
	first, err := registry.Open(1 << 20)
	require.NoError(t, err)
	second, err := registry.Open(1 << 20)
	require.NoError(t, err)
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)

	var nilOp *Order
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

	require.Same(t, context.Canceled,
		orderTerminalCapacityError(context.Background(), context.Canceled))
	require.ErrorIs(t,
		orderTerminalCapacityError(context.Background(), mpool.ErrAllocationAccountInvariant),
		mpool.ErrAllocationAccountInvariant)
	mpoolCapacity := moerr.NewMPoolCapacityNoCtxf("mpool capacity")
	require.Same(t, mpoolCapacity,
		orderTerminalCapacityError(context.Background(), mpoolCapacity))
	controlled := orderTerminalCapacityError(
		context.Background(), mpool.ErrAllocationAccountCapacity)
	require.True(t, moerr.IsMoErrCode(controlled, moerr.ErrOOM), controlled)
}

func TestOrderAllocationHelperBoundaryMatrix(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	registry, err := mpool.NewAllocationAccountRegistry(1, 64)
	require.NoError(t, err)
	account, err := registry.Open(1 << 20)
	require.NoError(t, err)

	var nilCtr *container
	require.ErrorIs(t, nilCtr.setAllocationAccount(account), mpool.ErrAllocationAccountInvalid)
	require.NoError(t, nilCtr.clearAllocationAccount(account))
	ctr := &container{sortExprExecutor: []colexec.ExpressionExecutor{nil}}
	require.ErrorIs(t, ctr.setAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
	ctr.sortExprExecutor = nil
	require.NoError(t, ctr.setAllocationAccount(account))
	require.NoError(t, ctr.setAllocationAccount(account))

	_, _, err = growOrderSlice[int](nil, nil, -1, proc, account, ordersites.OrderSelections)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	_, _, err = growOrderSlice[int](nil, proc.Mp(), 1, proc, nil, ordersites.OrderSelections)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	plain, owner, err := growOrderSlice[int](nil, nil, 3, proc, nil, ordersites.OrderSelections)
	require.NoError(t, err)
	require.Len(t, plain, 3)
	require.Nil(t, owner)

	accounted, accountedOwner, err := growOrderSlice[int](
		nil, nil, 3, proc, account, ordersites.OrderSelections)
	require.NoError(t, err)
	require.Len(t, accounted, 3)
	accounted, accountedOwner, err = growOrderSlice(
		accounted, accountedOwner, 2, proc, account, ordersites.OrderSelections)
	require.NoError(t, err)
	require.Len(t, accounted, 2)
	accounted, accountedOwner, err = growOrderSlice(
		accounted, accountedOwner, 100, proc, account, ordersites.OrderSelections)
	require.NoError(t, err)
	require.Len(t, accounted, 100)
	mpool.FreeSlice(accountedOwner, accounted)
	_, _, err = growOrderSlice[uint64](
		nil, nil, math.MaxInt, proc, account, ordersites.OrderSelections)
	require.ErrorIs(t, err, mpool.ErrAllocationAllocatorLimit)
	rejectRegistry, err := mpool.NewAllocationAccountRegistry(1, 8)
	require.NoError(t, err)
	rejectController := &orderTestCapacityController{limit: 1 << 20, rejectNext: true}
	rejectAccount, err := rejectRegistry.OpenWithController(1<<20, rejectController)
	require.NoError(t, err)
	_, _, err = growOrderSlice[int](
		nil, nil, 1, proc, rejectAccount, ordersites.OrderSelections)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	_, _, err = rejectRegistry.CompleteTerminal(rejectAccount)
	require.NoError(t, err)

	_, err = ctr.appendCheckpoints(0, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountInvalid)
	checkpoints, err := ctr.appendCheckpoints(2, proc)
	require.NoError(t, err)
	require.Len(t, checkpoints, 2)
	require.ErrorIs(t, ctr.clearAllocationAccount(account), mpool.ErrAllocationAccountInvariant)
	ctr.releaseAttempt()
	require.NoError(t, ctr.clearAllocationAccount(account))
	snapshot, _, err := registry.CompleteTerminal(account)
	require.NoError(t, err)
	require.Zero(t, snapshot.Used)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderPrepareFailureRollsBackExecutors(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
		&plan.OrderBySpec{Expr: &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)

	err := op.Prepare(proc)
	require.Error(t, err)
	require.Empty(t, op.ctr.sortExprExecutor)
	require.Empty(t, op.ctr.sortVectors)
	require.Empty(t, op.ctr.desc)
	require.Empty(t, op.ctr.nullsLast)
	op.Free(proc, true, err)
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestAccountedOrderRejectsExtraBufferBeforeRetainingInput(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	op := newAccountedOrder(
		&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
	)
	state := installOrderTestAllocation(t, op, 64<<20, nil)
	input := newOrderPairBatch(t, proc, []int64{1}, []int64{2})
	input.ExtraBuf = []byte("aggregate payload")
	op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input}))
	require.NoError(t, op.Prepare(proc))

	result, err := vm.Exec(op, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "order build should not have extra buffers")
	require.Nil(t, result.Batch)
	require.Nil(t, op.ctr.batWaitForSort)
	op.Children[0].Free(proc, true, err)
	op.Free(proc, true, err)
	finalizeOrderTestAllocation(t, op, state)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func BenchmarkOrderAccountedResident(b *testing.B) {
	benchmarkOrderResident(b, true)
}

func BenchmarkOrderUnaccountedResident(b *testing.B) {
	benchmarkOrderResident(b, false)
}

func benchmarkOrderResident(b *testing.B, accounted bool) {
	proc := testutil.NewProcessWithMPool(b, "", mpool.MustNewZero())
	defer proc.Free()
	first := make([]int64, BenchmarkRows)
	second := make([]int64, BenchmarkRows)
	for i := range first {
		first[i] = int64(i * 48271 % 1024)
		second[i] = int64(i * 69621 % 100003)
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		op := newAccountedOrder(
			&plan.OrderBySpec{Expr: newOrderColumnExpression(0, types.T_int64)},
			&plan.OrderBySpec{Expr: newOrderColumnExpression(1, types.T_int64)},
		)
		var state orderTestAllocation
		if accounted {
			state = installOrderTestAllocation(b, op, 64<<20, nil)
		}
		op.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{
			newOrderPairBatch(b, proc, first, second),
		}))
		b.StartTimer()
		require.NoError(b, op.Prepare(proc))
		_, err := vm.Exec(op, proc)
		require.NoError(b, err)
		b.StopTimer()
		op.Children[0].Free(proc, false, nil)
		op.Free(proc, false, nil)
		if accounted {
			finalizeOrderTestAllocation(b, op, state)
		}
		require.Zero(b, proc.Mp().CurrNB())
		b.StartTimer()
	}
}
