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

package group

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

type retryStringExpression struct {
	colexec.ExpressionExecutor
	vec         *vector.Vector
	mp          *mpool.MPool
	selection   *vector.AllocationAccountSelection
	borrowed    bool
	calls, rows int
}

func (e *retryStringExpression) Eval(proc *process.Process, inputs []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	if e.borrowed {
		e.calls++
		result, err := e.ExpressionExecutor.Eval(proc, inputs, selectList)
		if err == nil {
			e.rows += inputs[0].RowCount()
		}
		return result, err
	}
	if e.vec != nil {
		e.vec.Free(proc.Mp())
	}
	e.mp = proc.Mp()
	e.calls++
	var err error
	e.vec, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_varchar.ToType(), e.selection)
	if err != nil {
		return nil, err
	}
	for range inputs[0].RowCount() {
		e.rows++
		value := fmt.Sprintf("%05d-%s", e.rows, strings.Repeat("x", 96))
		if err := vector.AppendBytes(e.vec, []byte(value), false, proc.Mp()); err != nil {
			return nil, err
		}
	}
	return e.vec, nil
}
func (e *retryStringExpression) ResetForNextQuery() {
	if e.vec != nil {
		e.vec.Free(e.mp)
		e.vec = nil
	}
	e.ExpressionExecutor.ResetForNextQuery()
}
func (e *retryStringExpression) Free() {
	if e.vec != nil {
		e.vec.Free(e.mp)
		e.vec = nil
	}
	e.ExpressionExecutor.Free()
}

type retryIntExpression struct {
	colexec.ExpressionExecutor
	vec         *vector.Vector
	mp          *mpool.MPool
	selection   *vector.AllocationAccountSelection
	calls, rows int
}

func (e *retryIntExpression) Eval(proc *process.Process, inputs []*batch.Batch, _ []bool) (*vector.Vector, error) {
	if e.vec != nil {
		e.vec.Free(proc.Mp())
	}
	e.mp = proc.Mp()
	e.calls++
	var err error
	e.vec, err = vector.NewOffHeapVecWithTypeAndAllocation(types.T_int32.ToType(), e.selection)
	if err != nil {
		return nil, err
	}
	for range inputs[0].RowCount() {
		e.rows++
		if err = vector.AppendFixed(e.vec, int32(e.rows), false, proc.Mp()); err != nil {
			return nil, err
		}
	}
	return e.vec, nil
}
func (e *retryIntExpression) ResetForNextQuery() {
	if e.vec != nil {
		e.vec.Free(e.mp)
		e.vec = nil
	}
	e.ExpressionExecutor.ResetForNextQuery()
}
func (e *retryIntExpression) Free() {
	if e.vec != nil {
		e.vec.Free(e.mp)
		e.vec = nil
	}
	e.ExpressionExecutor.Free()
}

func TestGroupCapacityRecoveryRetainsEvaluatedInput(t *testing.T) {
	for _, tc := range []struct {
		name         string
		rollup       bool
		partial      bool
		distinct     bool
		borrowed     bool
		failRecovery bool
	}{
		{name: "final"},
		{name: "rollup", rollup: true},
		{name: "partial-and-merge", partial: true},
		{name: "distinct-drain", distinct: true},
		{name: "borrowed-input", borrowed: true},
		{name: "capacity-exhausted-after-spill", rollup: true, failRecovery: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			input := batch.NewWithSize(3)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			keys := make([]string, 512)
			if tc.borrowed {
				for i := range keys {
					keys[i] = fmt.Sprintf("%05d-%s", i+1, strings.Repeat("x", 96))
				}
			}
			input.Vecs[0] = testutil.MakeVarcharVector(keys, nil, proc.Mp())
			input.Vecs[1] = testutil.MakeInt32Vector(make([]int32, 512), nil, proc.Mp())
			input.Vecs[2] = testutil.MakeInt32Vector(make([]int32, 512), nil, proc.Mp())
			input.SetRowCount(512)
			groupExprs := []*plan.Expr{colExpr(0, types.T_varchar)}
			if tc.rollup {
				groupExprs = append(groupExprs, colExpr(1, types.T_int32))
			}
			aggs := []aggexec.AggFuncExecExpression{sumAgg(2)}
			if tc.distinct {
				aggs = []aggexec.AggFuncExecExpression{countDistinctAgg(2)}
			}
			g := newGroupOp(proc, groupExprs, aggs)
			var allocation groupTestAllocation
			var accountInstalled bool
			var runErr error
			generation := installGroupTestResourceBudget(t, proc)
			t.Cleanup(func() {
				g.Free(proc, runErr != nil, runErr)
				if accountInstalled {
					defer finalizeGroupTestAllocation(t, g, allocation)
					require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
					require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
				}
			})
			if tc.rollup {
				g.GroupingFlag = []bool{true, false}
			}
			g.NeedEval = !tc.partial
			g.SpillMem = 1 << 30
			registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
			require.NoError(t, err)
			controller := &rejectNextGroupAllocationController{}
			account, err := registry.OpenWithController(64<<20, controller)
			require.NoError(t, err)
			require.NoError(t, g.ctr.setAllocationAccount(account))
			allocation = groupTestAllocation{generation: generation, registry: registry, account: account}
			accountInstalled = true
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{input, batch.EmptyBatch}))
			require.NoError(t, g.Prepare(proc))
			var root vm.Operator = g
			if tc.partial {
				merge := newMergeGroupOp(aggs)
				merge.AppendChild(g)
				t.Cleanup(func() { merge.Free(proc, false, nil) })
				require.NoError(t, merge.Prepare(proc))
				root = merge
			}
			key := &retryStringExpression{ExpressionExecutor: g.ctr.groupByEvaluate.Executor[0], selection: g.ctr.expressionAllocation, borrowed: tc.borrowed}
			arg := &retryIntExpression{ExpressionExecutor: g.ctr.aggArgEvaluate[0].Executor[0], selection: g.ctr.expressionAllocation}
			g.ctr.groupByEvaluate.Executor[0] = key
			g.ctr.aggArgEvaluate[0].Executor[0] = arg
			rejectedAfterSpill := false
			retainedSentinel := false
			controller.rejectWhen = func() bool {
				if !controller.rejected {
					return !g.ctr.hr.IsEmpty() && g.ctr.hr.Hash.GroupCount() == 256
				}
				if tc.failRecovery && g.ctr.hr.IsEmpty() &&
					g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"] > 0 {
					rejectedAfterSpill = true
					retainedSentinel = len(g.ctr.groupingRollup) > 1 && g.ctr.groupingRollup[1] != nil
					return true
				}
				return false
			}
			got := make(map[int]int64)
			for {
				result, err := vm.Exec(root, proc)
				if err != nil {
					runErr = err
					break
				}
				if result.Status == vm.ExecStop || result.Batch == nil {
					break
				}
				sums := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[len(groupExprs)])
				for row := range result.Batch.RowCount() {
					raw := string(result.Batch.Vecs[0].GetBytesAt(row))
					require.GreaterOrEqual(t, len(raw), 5)
					n, err := strconv.Atoi(raw[:5])
					require.NoError(t, err)
					if tc.rollup {
						require.True(t, result.Batch.Vecs[1].GetGrouping().Contains(uint64(row)))
					}
					require.NotContains(t, got, n, "group was published twice")
					got[n] = sums[row]
				}
			}
			_, rejected := controller.snapshot()
			require.True(t, rejected)
			require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
			require.Equal(t, 1, key.calls)
			require.Equal(t, 512, key.rows)
			require.Equal(t, 1, arg.calls)
			require.Equal(t, 512, arg.rows)
			if tc.failRecovery {
				require.ErrorIs(t, runErr, mpool.ErrAllocationAccountCapacity)
				require.True(t, rejectedAfterSpill)
				require.True(t, retainedSentinel)
				require.Empty(t, got)
				return
			}
			require.NoError(t, runErr)
			require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillReloadRows"])
			if tc.distinct {
				require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupDistinctSpillActivations"])
			}
			require.Len(t, got, 512)
			for n := 1; n <= 512; n++ {
				want := int64(n)
				if tc.distinct {
					want = 1
				}
				require.Equal(t, want, got[n], "key %d", n)
			}
		})
	}
}

type failOnceRetryExpression struct {
	colexec.ExpressionExecutor
	calls  int
	failAt int
}

func (e *failOnceRetryExpression) Eval(proc *process.Process, inputs []*batch.Batch, selectList []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls == e.failAt {
		return nil, mpool.ErrAllocationAccountCapacity
	}
	return e.ExpressionExecutor.Eval(proc, inputs, selectList)
}
func TestGroupCapacityErrorDuringInputEvaluationIsNotReplayed(t *testing.T) {
	for _, failure := range []string{"group", "aggregate"} {
		t.Run(failure, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() {
				proc.Free()
				require.Zero(t, proc.Mp().CurrNB())
			})
			mkInput := func(size int) *batch.Batch {
				b := batch.NewWithSize(2)
				t.Cleanup(func() { b.Clean(proc.Mp()) })
				keys := make([]int32, size)
				for i := range keys {
					keys[i] = int32(i)
				}
				b.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
				b.Vecs[1] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
				b.SetRowCount(size)
				return b
			}
			first, second := mkInput(256), mkInput(2)
			g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(1)})
			g.SpillMem = 1 << 30
			allocation := installGroupTestAllocation(t, g, proc, 64<<20)
			t.Cleanup(func() {
				g.Free(proc, true, mpool.ErrAllocationAccountCapacity)
				defer finalizeGroupTestAllocation(t, g, allocation)
				require.Zero(t, allocation.generation.Snapshot().SpillDiskUsed)
				require.Zero(t, allocation.generation.Snapshot().SpillFDUsed)
			})
			g.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}))
			require.NoError(t, g.Prepare(proc))
			key := &failOnceRetryExpression{ExpressionExecutor: g.ctr.groupByEvaluate.Executor[0]}
			arg := &failOnceRetryExpression{ExpressionExecutor: g.ctr.aggArgEvaluate[0].Executor[0]}
			if failure == "group" {
				key.failAt = 2
			} else {
				arg.failAt = 2
			}
			g.ctr.groupByEvaluate.Executor[0] = key
			g.ctr.aggArgEvaluate[0].Executor[0] = arg
			_, err := vm.Exec(g, proc)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Equal(t, 2, key.calls)
			if failure == "group" {
				require.Equal(t, 1, arg.calls)
			} else {
				require.Equal(t, 2, arg.calls)
			}
		})
	}
}

func TestGroupRollupSentinelCapacityErrorIsNotReplayed(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	makeInput := func(n int) *batch.Batch {
		values := make([]int32, n)
		for i := range values {
			values[i] = int32(i)
		}
		input := batch.NewWithSize(2)
		t.Cleanup(func() { input.Clean(proc.Mp()) })
		input.Vecs[0] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeInt32Vector(values, nil, proc.Mp())
		input.SetRowCount(n)
		return input
	}
	first, second := makeInput(256), makeInput(2)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)},
		[]aggexec.AggFuncExecExpression{sumAgg(1)})
	g.GroupingFlag = []bool{true, false}
	g.SpillMem = 1 << 30
	generation := installGroupTestResourceBudget(t, proc)
	registry, err := mpool.NewAllocationAccountRegistry(1, 1<<12)
	require.NoError(t, err)
	controller := &rejectNextGroupAllocationController{}
	account, err := registry.OpenWithController(64<<20, controller)
	require.NoError(t, err)
	require.NoError(t, g.ctr.setAllocationAccount(account))
	allocation := groupTestAllocation{generation: generation, registry: registry, account: account}
	t.Cleanup(func() {
		g.Free(proc, true, mpool.ErrAllocationAccountCapacity)
		defer finalizeGroupTestAllocation(t, g, allocation)
		require.Zero(t, generation.Snapshot().SpillDiskUsed)
		require.Zero(t, generation.Snapshot().SpillFDUsed)
	})
	g.AppendChild(colexec.NewMockOperator().
		WithBatchs([]*batch.Batch{first, second, batch.EmptyBatch}).
		WithBatchCallback(func(index int) {
			if index == 1 {
				controller.arm()
			}
		}))
	require.NoError(t, g.Prepare(proc))
	key := &failOnceRetryExpression{ExpressionExecutor: g.ctr.groupByEvaluate.Executor[0]}
	arg := &failOnceRetryExpression{ExpressionExecutor: g.ctr.aggArgEvaluate[0].Executor[0]}
	g.ctr.groupByEvaluate.Executor[0] = key
	g.ctr.aggArgEvaluate[0].Executor[0] = arg

	_, err = vm.Exec(g, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	_, rejected := controller.snapshot()
	require.True(t, rejected)
	require.Equal(t, 2, key.calls)
	require.Equal(t, 2, arg.calls)
}
