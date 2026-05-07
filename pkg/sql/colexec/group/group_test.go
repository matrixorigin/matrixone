// Copyright 2024 Matrix Origin
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

package group

import (
	"bytes"
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// mock batch schema: (a int32, b uuid, c varchar, d json, e datetime)
// col 0 = a int32

func colExpr(pos int32, t types.T) *plan.Expr {
	return &plan.Expr{
		Typ:  plan.Type{Id: int32(t)},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}},
	}
}

func sumAgg(pos int32) aggexec.AggFuncExecExpression {
	e, _ := function.GetFunctionByName(context.Background(), "sum", []types.Type{types.T_int32.ToType()})
	return aggexec.MakeAggFunctionExpression(e.GetEncodedOverloadID(), false, []*plan.Expr{colExpr(pos, types.T_int32)}, nil)
}

func countStarAgg() aggexec.AggFuncExecExpression {
	return aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil)
}

func newGroupOp(proc *process.Process, groupBy []*plan.Expr, aggs []aggexec.AggFuncExecExpression) *Group {
	g := NewArgument()
	g.GroupBy = groupBy
	g.Aggs = aggs
	g.NeedEval = true
	g.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return g
}

func newMergeGroupOp(aggs []aggexec.AggFuncExecExpression) *MergeGroup {
	mg := NewArgumentMergeGroup()
	mg.Aggs = aggs
	mg.OperatorBase = vm.OperatorBase{
		OperatorInfo: vm.OperatorInfo{Idx: 0, IsFirst: false, IsLast: false},
	}
	return mg
}

func resetChildren(g *Group, proc *process.Process) {
	bat := colexec.MakeMockBatchs(proc.Mp())
	op := colexec.NewMockOperator().WithBatchs([]*batch.Batch{bat})
	g.Children = nil
	g.AppendChild(op)
}

func collectBatches(t *testing.T, op vm.Operator, proc *process.Process) []*batch.Batch {
	t.Helper()

	var result []*batch.Batch
	for {
		ret, err := vm.Exec(op, proc)
		require.NoError(t, err)
		if ret.Status == vm.ExecStop || ret.Batch == nil {
			return result
		}
		result = append(result, ret.Batch)
	}
}

func cloneBatch(t *testing.T, proc *process.Process, bat *batch.Batch) *batch.Batch {
	t.Helper()

	cloned, err := bat.Dup(proc.Mp())
	require.NoError(t, err)
	cloned.ExtraBuf = append(cloned.ExtraBuf[:0], bat.ExtraBuf...)
	return cloned
}

func buildPartialGroupBatches(t *testing.T, proc *process.Process, sources []*batch.Batch, forceGroupTypesNotNull bool) []*batch.Batch {
	t.Helper()

	groupBy := []*plan.Expr{colExpr(0, types.T_int32), colExpr(1, types.T_int32)}
	partialBatches := make([]*batch.Batch, 0, len(sources))
	for _, source := range sources {
		partial := newGroupOp(proc, groupBy, []aggexec.AggFuncExecExpression{countStarAgg()})
		partial.NeedEval = false
		partial.AppendChild(colexec.NewMockOperator().WithBatchs([]*batch.Batch{source}))
		require.NoError(t, partial.Prepare(proc))
		rawPartialBatches := collectBatches(t, partial, proc)
		require.Len(t, rawPartialBatches, 1)
		for _, bat := range rawPartialBatches {
			cloned := cloneBatch(t, proc, bat)
			if forceGroupTypesNotNull {
				cloned.Vecs[0].GetType().SetNotNull(true)
				cloned.Vecs[1].GetType().SetNotNull(true)
			}
			partialBatches = append(partialBatches, cloned)
		}
		partial.Free(proc, false, nil)
	}
	return partialBatches
}

func assertMergedTicketCounts(t *testing.T, finals []*batch.Batch, wantNull, wantNonNull int64) {
	t.Helper()

	var nullCount, nonNullCount int64
	totalRows := 0
	for _, final := range finals {
		if final == nil || final.RowCount() == 0 || len(final.Vecs) == 0 {
			continue
		}
		require.Len(t, final.Vecs, 3)

		tickets := vector.MustFixedColNoTypeCheck[int32](final.Vecs[0])
		customers := vector.MustFixedColNoTypeCheck[int32](final.Vecs[1])
		counts := vector.MustFixedColNoTypeCheck[int64](final.Vecs[2])
		totalRows += final.RowCount()

		for i := 0; i < final.RowCount(); i++ {
			require.Equal(t, int32(1), tickets[i])
			if final.Vecs[1].GetNulls().Contains(uint64(i)) {
				nullCount = counts[i]
				continue
			}
			require.Equal(t, int32(10), customers[i])
			nonNullCount = counts[i]
		}
	}

	require.Equal(t, 2, totalRows)
	require.Equal(t, wantNull, nullCount)
	require.Equal(t, wantNonNull, nonNullCount)
}

func TestGroupString(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	buf := new(bytes.Buffer)
	g.String(buf)
	require.NotEmpty(t, buf.String())
}

func TestGroupPrepare(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))
	g.Free(proc, false, nil)
}

// TestGroupByWithSum: GROUP BY a, SUM(a) — two distinct rows → two groups.
func TestGroupByWithSum(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	// mock batch has 2 rows with distinct values (1, 1000) → 2 groups
	require.Equal(t, 2, rowCount)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestGroupNoGroupBy: no GROUP BY, just COUNT(*) → single row result.
func TestGroupNoGroupBy(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, nil, []aggexec.AggFuncExecExpression{countStarAgg()})
	resetChildren(g, proc)
	require.NoError(t, g.Prepare(proc))

	var rowCount int
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		rowCount += result.Batch.RowCount()
	}
	require.Equal(t, 1, rowCount)

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

// TestGroupResetAndReuse: verify Reset allows the operator to be reused correctly.
func TestGroupResetAndReuse(t *testing.T) {
	proc := testutil.NewProcess(t)
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{sumAgg(0)})

	for i := 0; i < 2; i++ {
		resetChildren(g, proc)
		require.NoError(t, g.Prepare(proc))
		for {
			result, err := vm.Exec(g, proc)
			require.NoError(t, err)
			if result.Status == vm.ExecStop || result.Batch == nil {
				break
			}
		}
		g.Reset(proc, false, nil)
	}

	g.Free(proc, false, nil)
	proc.Free()
	require.Equal(t, int64(0), proc.Mp().CurrNB())
}

func TestMergeGroupPreservesLateNullableGroupKeys(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	first := batch.NewWithSize(2)
	first.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	first.Vecs[1] = testutil.MakeInt32Vector([]int32{10, 10}, nil, proc.Mp())
	first.SetRowCount(2)

	second := batch.NewWithSize(2)
	second.Vecs[0] = testutil.MakeInt32Vector([]int32{1, 1}, nil, proc.Mp())
	second.Vecs[1] = testutil.MakeInt32Vector([]int32{0, 0}, []uint64{0, 1}, proc.Mp())
	second.SetRowCount(2)

	partialBatches := buildPartialGroupBatches(t, proc, []*batch.Batch{first, second}, true)

	merge := newMergeGroupOp([]aggexec.AggFuncExecExpression{countStarAgg()})
	merge.AppendChild(colexec.NewMockOperator().WithBatchs(partialBatches))
	require.NoError(t, merge.Prepare(proc))
	finalBatches := collectBatches(t, merge, proc)
	require.Len(t, finalBatches, 1)
	assertMergedTicketCounts(t, finalBatches, 2, 2)
	merge.Free(proc, false, nil)
}

func TestFreeAggListPartial(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 3)
	for i := 0; i < 3; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggListPartial(aggList, 2)
	freeAggListPartial(aggList, 3)
}

func TestFreeAggList(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	aggList := make([]aggexec.AggFuncExec, 2)
	for i := 0; i < 2; i++ {
		agg, err := aggexec.MakeAgg(proc.Mp(), aggexec.AggIdOfCountStar, false, types.T_int64.ToType())
		require.NoError(t, err)
		aggList[i] = agg
	}

	freeAggList(aggList)
}

func TestFreeAggListPartialWithNilEntries(t *testing.T) {
	aggList := make([]aggexec.AggFuncExec, 3)

	freeAggListPartial(aggList, 3)
	freeAggList(aggList)
}

func TestMakeAggListFreesPartialOnCreationError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(-1, false, []*plan.Expr{colExpr(0, types.T_int32)}, nil),
	})
	require.Error(t, err)
}

func TestMakeAggListFreesPartialOnExtraConfigError(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()

	ctr := &container{mp: proc.Mp()}
	_, err := ctr.makeAggList([]aggexec.AggFuncExecExpression{
		countStarAgg(),
		aggexec.MakeAggFunctionExpression(
			aggexec.AggIdOfMin,
			false,
			[]*plan.Expr{colExpr(0, types.T_int32)},
			[]byte("bad-config"),
		),
	})
	require.Error(t, err)
}
