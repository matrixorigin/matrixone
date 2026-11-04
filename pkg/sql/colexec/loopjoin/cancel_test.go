// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package loopjoin

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/message"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type cancelingLoopJoinExpression struct {
	colexec.ExpressionExecutor
	afterFirstEval func()
	evals          int
}

func (e *cancelingLoopJoinExpression) Eval(
	proc *process.Process, batches []*batch.Batch, selectList []bool,
) (*vector.Vector, error) {
	e.evals++
	vec, err := e.ExpressionExecutor.Eval(proc, batches, selectList)
	if e.evals == 1 && err == nil && e.afterFirstEval != nil {
		e.afterFirstEval()
	}
	return vec, err
}

func newLoopJoinCancellationFixture(
	t *testing.T, joinType plan.Node_JoinType, buildValues [][]int32,
) (*LoopJoin, *process.Process, *mpool.AllocationAccount) {
	t.Helper()
	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	join := &LoopJoin{
		NonEqCond: &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_bool)},
			Expr: &plan.Expr_Lit{Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: false},
			}},
		},
		ResultCols: []colexec.ResultPos{colexec.NewResultPos(0, 0)},
		LeftTypes:  []types.Type{types.T_int32.ToType()},
		RightTypes: []types.Type{types.T_int32.ToType()},
		JoinType:   joinType,
	}
	var child *colexec.MockOperator
	t.Cleanup(func() {
		join.Reset(proc, true, context.Canceled)
		join.Free(proc, true, context.Canceled)
		if child != nil {
			child.Free(proc, true, context.Canceled)
		}
	})
	account := installLoopJoinTestAllocation(t, join)
	switch joinType {
	case plan.Node_MARK:
		join.ResultCols = append(join.ResultCols, colexec.NewResultPos(-1, 0))
	case plan.Node_INNER, plan.Node_LEFT, plan.Node_OUTER, plan.Node_SINGLE:
		join.ResultCols = append(join.ResultCols, colexec.NewResultPos(1, 0))
	}
	buildBatches := make([]*batch.Batch, len(buildValues))
	for i, values := range buildValues {
		buildBatches[i] = makeInt32LoopJoinBatch(proc.Mp(), values)
	}
	join.ctr.mp = message.NewJoinMap(
		message.GroupSels{}, nil, nil, nil, buildBatches, proc.Mp())
	join.ctr.mp.IncRef(1)
	join.ctr.state = Probe
	child = colexec.NewMockOperator().WithBatchs([]*batch.Batch{
		makeInt32LoopJoinBatch(proc.Mp(), []int32{7}),
	})
	join.AppendChild(child)
	return join, proc, account
}

func TestLoopJoinCancellationDuringProbe(t *testing.T) {
	for _, joinType := range []plan.Node_JoinType{
		plan.Node_MARK, plan.Node_SEMI, plan.Node_ANTI, plan.Node_SINGLE,
		plan.Node_INNER, plan.Node_LEFT, plan.Node_OUTER,
	} {
		t.Run(joinType.String(), func(t *testing.T) {
			for _, cancelDuringEval := range []bool{false, true} {
				name := "normal"
				if cancelDuringEval {
					name = "cancel-after-first-eval"
				}
				t.Run(name, func(t *testing.T) {
					join, proc, _ := newLoopJoinCancellationFixture(t, joinType, [][]int32{{1}, {2}})
					ctx, cancel := context.WithCancel(proc.Ctx)
					t.Cleanup(cancel)
					proc.Ctx = ctx
					require.NoError(t, join.Prepare(proc))
					counter := &cancelingLoopJoinExpression{ExpressionExecutor: join.ctr.expr}
					if cancelDuringEval {
						counter.afterFirstEval = cancel
					}
					join.ctr.expr = counter

					result, err := vm.Exec(join, proc)
					if cancelDuringEval {
						require.ErrorIs(t, err, context.Canceled)
						require.Nil(t, result.Batch)
						require.Equal(t, 1, counter.evals, "no later build batch may run after cancellation")
						require.Nil(t, join.ctr.condVec)
						require.False(t, join.ctr.probeScanValid)
						require.False(t, join.ctr.probeScanStarted)
						return
					}
					require.NoError(t, err)
					require.Equal(t, 2, counter.evals, "both false build batches must actually be scanned")
					require.NotNil(t, result.Batch)
					if joinType == plan.Node_INNER || joinType == plan.Node_SEMI {
						require.Zero(t, result.Batch.RowCount())
					} else {
						require.Equal(t, 1, result.Batch.RowCount())
						require.Equal(t, int32(7), vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[0])
						if joinType == plan.Node_MARK {
							require.False(t, vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[1])[0])
						} else if joinType != plan.Node_ANTI {
							require.True(t, result.Batch.Vecs[1].IsNull(0))
						}
					}
				})
			}
		})
	}
}

func TestLoopJoinCancellationReleasesConditionWindow(t *testing.T) {
	join, proc, account := newLoopJoinCancellationFixture(t, plan.Node_MARK, [][]int32{{1, 2}})
	ctx, cancel := context.WithCancel(proc.Ctx)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	require.NoError(t, join.Prepare(proc))
	probe := join.GetChildren(0).(*colexec.MockOperator).GetBatchs()[0]
	build := join.ctr.mp.GetBatches()[0]
	build.Vecs[0].GetNulls().Add(1)
	join.ctr.joinBat, join.ctr.cfs = colexec.NewJoinBatch(probe, proc.Mp())
	before := account.Snapshot().Used
	var during uint64
	join.ctr.expr = &cancelingLoopJoinExpression{
		ExpressionExecutor: join.ctr.expr,
		afterFirstEval: func() {
			during = account.Snapshot().Used
			cancel()
		},
	}
	// Starting at row one forces a real borrowed-data window with an owned
	// null bitmap without allocating a million-row batch to cross the limit.
	vec, window, _, err := join.ctr.evalConditionWindow(join, proc, probe, 0, build, 1)
	if window != nil {
		defer window.Clean(proc.Mp())
	}
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, vec)
	require.Nil(t, window)
	require.Greater(t, during, before, "the fixture must own a condition-window bitmap")
	require.Equal(t, before, account.Snapshot().Used, "cancellation must release the provisional window")
	join.Reset(proc, true, err)
	join.Reset(proc, true, err)
	join.Free(proc, true, err)
	require.Zero(t, account.Snapshot().Used)
}

func TestLoopJoinCancellationBeforeCall(t *testing.T) {
	join, proc, _ := newLoopJoinCancellationFixture(t, plan.Node_MARK, [][]int32{{1}})
	require.NoError(t, join.Prepare(proc))
	// Resume with an already-owned probe batch, so a child Exec cannot mask
	// the missing cancellation boundary on LoopJoin.Call itself.
	join.ctr.inBat = join.GetChildren(0).(*colexec.MockOperator).GetBatchs()[0]
	ctx, cancel := context.WithCancel(proc.Ctx)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	cancel()
	result, err := join.Call(proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, vm.CancelResult, result)
}

func TestLoopJoinCancellationResetReuse(t *testing.T) {
	join, proc, account := newLoopJoinCancellationFixture(t, plan.Node_MARK, [][]int32{{1}, {2}})
	parentCtx := proc.Ctx
	ctx, cancel := context.WithCancel(parentCtx)
	t.Cleanup(cancel)
	proc.Ctx = ctx
	require.NoError(t, join.Prepare(proc))
	canceledExpression := &cancelingLoopJoinExpression{
		ExpressionExecutor: join.ctr.expr,
		afterFirstEval:     cancel,
	}
	join.ctr.expr = canceledExpression
	result, err := vm.Exec(join, proc)
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, result.Batch)
	require.Equal(t, 1, canceledExpression.evals)
	join.Reset(proc, true, err)
	require.Nil(t, join.ctr.expr)
	require.Nil(t, join.ctr.mp)
	require.Nil(t, join.ctr.inBat)
	require.Zero(t, account.Snapshot().Used)

	// Reuse this operator with new inputs and a context derived from the live
	// parent, not from the canceled execution generation.
	freshCtx, freshCancel := context.WithCancel(parentCtx)
	t.Cleanup(freshCancel)
	proc.Ctx = freshCtx
	child := join.GetChildren(0).(*colexec.MockOperator)
	child.Free(proc, true, err)
	child.WithBatchs([]*batch.Batch{makeInt32LoopJoinBatch(proc.Mp(), []int32{70})})
	join.ctr.mp = message.NewJoinMap(message.GroupSels{}, nil, nil, nil,
		[]*batch.Batch{makeInt32LoopJoinBatch(proc.Mp(), []int32{20})}, proc.Mp())
	join.ctr.mp.IncRef(1)
	join.ctr.state = Probe
	require.NoError(t, join.Prepare(proc))
	counter := &countingLoopJoinExpression{delegate: join.ctr.expr}
	join.ctr.expr = counter
	result, err = vm.Exec(join, proc)
	require.NoError(t, err)
	require.Equal(t, 1, result.Batch.RowCount())
	require.Equal(t, int32(70), vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])[0])
	require.False(t, vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[1])[0])
	require.Equal(t, 1, counter.evals)
	require.Equal(t, 1, canceledExpression.evals, "the old executor must not run in the new generation")
	join.Reset(proc, false, nil)
	join.Free(proc, false, nil)
	require.Zero(t, account.Snapshot().Used)
}

func TestLoopJoinCancellationWithPairCondition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		suspend     bool
		cancel      bool
		nilOnCancel bool
	}{
		{name: "cancel-before-value", cancel: true},
		{name: "cancel-before-yield", cancel: true, nilOnCancel: true},
		{name: "cancel-after-resume", suspend: true, cancel: true},
		{name: "normal-suspend-resume", suspend: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			join, proc, _ := newLoopJoinCancellationFixture(t, plan.Node_MARK, [][]int32{{1}})
			ctx, cancel := context.WithCancel(proc.Ctx)
			t.Cleanup(cancel)
			proc.Ctx = ctx
			value, err := vector.NewConstFixed(types.T_bool.ToType(), true, 1, proc.Mp())
			require.NoError(t, err)
			t.Cleanup(func() { value.Free(proc.Mp()) })
			calls := 0
			join.NonEqCond = nil
			join.PairCondition = func(*process.Process, *batch.Batch, int, *batch.Batch, int) (*vector.Vector, error) {
				calls++
				if tc.suspend && calls == 1 {
					return nil, nil
				}
				if tc.cancel {
					cancel()
				}
				if tc.nilOnCancel {
					return nil, nil
				}
				return value, nil
			}
			require.NoError(t, join.Prepare(proc))
			result, err := vm.Exec(join, proc)
			if tc.suspend {
				require.NoError(t, err)
				require.Equal(t, vm.ExecYield, result.Status)
				require.Nil(t, result.Batch)
				require.True(t, join.ctr.conditionPending)
				result, err = vm.Exec(join, proc)
				require.Equal(t, 2, calls)
			}
			if tc.cancel {
				require.ErrorIs(t, err, context.Canceled)
				require.Nil(t, result.Batch)
				require.NotEqual(t, vm.ExecYield, result.Status)
				require.False(t, join.ctr.conditionPending)
				require.False(t, join.ctr.probeScanStarted)
				require.Nil(t, join.ctr.condVec)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 1, result.Batch.RowCount())
			require.True(t, vector.MustFixedColNoTypeCheck[bool](result.Batch.Vecs[1])[0])
		})
	}
}
