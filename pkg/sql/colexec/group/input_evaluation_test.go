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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/hashmap"
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

type countedInputExpression struct {
	colexec.ExpressionExecutor
	calls  int
	failAt int
}

func (e *countedInputExpression) Eval(proc *process.Process, inputs []*batch.Batch, selection []bool) (*vector.Vector, error) {
	e.calls++
	if e.calls == e.failAt {
		return nil, mpool.ErrAllocationAccountCapacity
	}
	return e.ExpressionExecutor.Eval(proc, inputs, selection)
}

type failSecondBatchFill struct {
	aggexec.AggFuncExec
	calls, processed, failedOffset int
}

func (a *failSecondBatchFill) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	a.calls++
	if a.calls == 2 {
		a.failedOffset = offset
		return mpool.ErrAllocationAccountCapacity
	}
	if err := a.AggFuncExec.BatchFill(offset, groups, vectors); err != nil {
		return err
	}
	a.processed += len(groups)
	return nil
}

func newInputEvaluationGroup(t *testing.T, spillMem int64, values ...[]int32) (*Group, *process.Process, *countedInputExpression, *countedInputExpression) {
	t.Helper()
	proc := testutil.NewProcess(t)
	t.Cleanup(func() {
		proc.Free()
		require.Zero(t, proc.Mp().CurrNB())
	})
	child := colexec.NewMockOperator()
	t.Cleanup(func() { child.Free(proc, false, nil) })
	for _, input := range values {
		bat := batch.NewWithSize(1)
		child.WithBatchs([]*batch.Batch{bat})
		bat.Vecs[0] = testutil.MakeInt32Vector(input, nil, proc.Mp())
		bat.SetRowCount(len(input))
	}
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	g.SpillMem = spillMem
	g.AppendChild(child)
	t.Cleanup(func() { g.Free(proc, false, nil) })
	require.NoError(t, g.Prepare(proc))

	key := &countedInputExpression{ExpressionExecutor: g.ctr.groupByEvaluate.Executor[0]}
	arg := &countedInputExpression{ExpressionExecutor: g.ctr.aggArgEvaluate[0].Executor[0]}
	g.ctr.groupByEvaluate.Executor[0] = key
	g.ctr.aggArgEvaluate[0].Executor[0] = arg
	return g, proc, key, arg
}

func TestGroupInputEvaluationErrorIsNotReplayed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		failKey  bool
		argCalls int
	}{
		{name: "group key", failKey: true, argCalls: 1},
		{name: "aggregate argument", argCalls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g, proc, key, arg := newInputEvaluationGroup(t, 1<<30, []int32{1, 2, 3}, []int32{4, 5, 6})
			if tc.failKey {
				key.failAt = 2
			} else {
				arg.failAt = 2
			}

			result, err := vm.Exec(g, proc)
			require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
			require.Nil(t, result.Batch)
			require.Equal(t, 2, key.calls)
			require.Equal(t, tc.argCalls, arg.calls)
			require.Equal(t, uint64(3), g.ctr.hr.Hash.GroupCount())
			require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
		})
	}
}

func TestGroupPartialBatchFailureIsNotReplayed(t *testing.T) {
	values := make([]int32, 2*hashmap.UnitLimit)
	for i := range values {
		values[i] = int32(i)
	}
	g, proc, key, arg := newInputEvaluationGroup(t, 1<<30, values)
	agg := &failSecondBatchFill{AggFuncExec: g.ctr.aggList[0]}
	g.ctr.aggList[0] = agg

	result, err := vm.Exec(g, proc)
	require.ErrorIs(t, err, mpool.ErrAllocationAccountCapacity)
	require.Nil(t, result.Batch)
	require.Equal(t, 2, agg.calls)
	require.Equal(t, hashmap.UnitLimit, agg.processed)
	require.Equal(t, hashmap.UnitLimit, agg.failedOffset)
	require.Equal(t, 1, key.calls)
	require.Equal(t, 1, arg.calls)
	require.Zero(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillRecords"])
}

func TestGroupSpillEvaluatesEachInputOnce(t *testing.T) {
	const rows = 512 // More than one hash insertion chunk.
	values := make([]int32, rows)
	for i := range values {
		values[i] = int32(i)
	}
	// Group-count threshold: spill each batch, but not each reload bucket.
	g, proc, key, arg := newInputEvaluationGroup(t, 400, values, values)

	got := make(map[int32]int64, rows)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		sums := vector.MustFixedColNoTypeCheck[int64](result.Batch.Vecs[1])
		for i := 0; i < result.Batch.RowCount(); i++ {
			require.NotContains(t, got, keys[i])
			got[keys[i]] = sums[i]
		}
	}
	require.Len(t, got, rows)
	for _, value := range values {
		require.Contains(t, got, value)
		require.Equal(t, int64(value)*2, got[value], "key %d", value)
	}
	require.Equal(t, 2, key.calls)
	require.Equal(t, 2, arg.calls)
	stats := g.OpAnalyzer.GetOpStats().ExtraStats
	require.Positive(t, stats["GroupSpillRecords"])
	require.Positive(t, stats["GroupSpillReloadRecords"])
}
