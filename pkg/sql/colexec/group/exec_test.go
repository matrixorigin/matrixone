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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

// hackAggExecToTest 是一个不带任何逻辑的AggExec,主要用于单测中检查各种接口的调用次数。
type hackAggExecToTest struct {
	toFlush int

	aggexec.AggFuncExec
	preAllocated   int
	groupNumber    int
	doFillRow      int
	doBulkFillRow  int
	doBatchFillRow int
	doFlushTime    int
	isFree         bool
}

func (h *hackAggExecToTest) GetOptResult() aggexec.SplitResult {
	return nil
}

func (h *hackAggExecToTest) GroupGrow(more int) error {
	h.groupNumber += more
	return nil
}

func (h *hackAggExecToTest) PreAllocateGroups(more int) error {
	h.preAllocated += more
	return nil
}

func (h *hackAggExecToTest) Fill(groupIndex int, row int, vectors []*vector.Vector) error {
	h.doFillRow++
	return nil
}

func (h *hackAggExecToTest) BulkFill(groupIndex int, vectors []*vector.Vector) error {
	h.doBulkFillRow++
	return nil
}

func (h *hackAggExecToTest) BatchFill(offset int, groups []uint64, vectors []*vector.Vector) error {
	h.doBatchFillRow++
	return nil
}

var hackVecResult = vector.NewVec(types.T_int64.ToType())

func (h *hackAggExecToTest) Flush() ([]*vector.Vector, error) {
	h.doFlushTime++

	ret := make([]*vector.Vector, h.toFlush)
	for i := 0; i < h.toFlush; i++ {
		ret[i] = hackVecResult
	}
	return ret, nil
}

func (h *hackAggExecToTest) Free() {
	h.isFree = true
}

func hackMakeAggToTest(cnt int) *hackAggExecToTest {
	exec := &hackAggExecToTest{
		toFlush: cnt,
	}
	makeAggExec = func(_ aggexec.AggMemoryManager, _ int64, _ bool, _ ...types.Type) aggexec.AggFuncExec {
		return exec
	}
	return exec
}

// Group算子的单元测试需要验证以下内容：
//
// 情况一：Group算子输出最终结果。
//
// 1. 计算单纯的聚合结果，要求结果为1行且结果正确。
// 2. 计算分组聚合结果，要求组数正确，结果正确。
//
// 以上结果都仅输出一次，下一次调用将输出nil结果。
//
// 情况二：Group算子输出中间结果。
//
// 1，计算单纯的聚合结果，要求每个聚合只有1个ResultGroup,且没有输出任何group by列。
// 2. 计算分组聚合结果，要求每个聚合的组数正确，且对应的Agg中间结果正确。
//
// 单测 以
// ` select agg(y) from t group by x; ` 为例。
// 且 t 表数据如下：
// col1         col2
//   1           1
//   1           2
//   2           3
//   2           4
//   3           5
//
// 所有情况都验证以下三种情况的输入：
// 1. batch list : 1, Empty, 2, nil.
// 2. batch list : empty, nil.
// 3. batch list : nil.

func TestGroup_GetFinalEvaluation_NoneGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	// datasource.
	{
		before := proc.Mp().CurrNB()

		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{1, 1},
				{1, 2},
				{2, 3},
			}),
			batch.EmptyBatch,
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{2, 4},
				{3, 5},
			}),
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = true
		g.Exprs = nil
		g.GroupingFlag = nil
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(0)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		var final *batch.Batch
		outCnt := 0
		for {
			r, err := g.Call(proc)
			require.NoError(t, err)
			if r.Batch == nil {
				break
			}

			outCnt++
			final = r.Batch
			require.Equal(t, 1, outCnt)

			// result check.
			require.NotNil(t, final)
			if final != nil {
				require.Equal(t, 0, len(final.Aggs))
				require.Equal(t, 1, len(final.Vecs))
				require.Equal(t, hackVecResult, final.Vecs[0])
			}
			require.Equal(t, 1, exec.groupNumber)
			require.Equal(t, 2, exec.doBulkFillRow)
			require.Equal(t, 1, exec.doFlushTime)
		}

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)
		require.Equal(t, before, proc.Mp().CurrNB())
	}

	// datasource is empty.
	{
		before := proc.Mp().CurrNB()

		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = true
		g.Exprs, g.GroupingFlag = nil, nil
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(0)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		// get the initial result.
		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, b.RowCount())
			require.Equal(t, 1, len(b.Vecs))
			require.Equal(t, hackVecResult, b.Vecs[0])
			require.Equal(t, 0, len(b.Aggs))
			require.Equal(t, 1, exec.doFlushTime)
			require.Equal(t, 1, exec.groupNumber)
		}

		// next call get the nil
		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)
		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func TestGroup_GetFinalEvaluation_WithGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	// datasource.
	{
		before := proc.Mp().CurrNB()

		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{1, 1},
				{1, 2},
				{2, 3},
			}),
			batch.EmptyBatch,
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{2, 4},
				{3, 5},
			}),
			nil,
		}

		// select x, agg(y) group by x;
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = true
		g.GroupingFlag = nil
		g.PreAllocSize = 20
		g.Exprs = []*plan.Expr{newColumnExpression(0)}
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(1)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		var final *batch.Batch
		outCnt := 0
		for {
			r, err := g.Call(proc)
			require.NoError(t, err)
			if r.Batch == nil {
				break
			}

			outCnt++
			final = r.Batch
			require.Equal(t, 1, outCnt)

			// result check.
			require.Equal(t, 20, exec.preAllocated)
			require.Equal(t, 3, exec.groupNumber) // 1, 2, 3
			require.Equal(t, 2, exec.doBatchFillRow)
			require.Equal(t, 1, exec.doFlushTime)

			require.NotNil(t, final)
			if final != nil {
				require.Equal(t, 0, len(final.Aggs))
				require.Equal(t, 2, len(final.Vecs))
				require.Equal(t, hackVecResult, final.Vecs[1])

				gvs := vector.MustFixedColNoTypeCheck[int64](final.Vecs[0])
				require.Equal(t, 3, len(gvs))
				require.Equal(t, int64(1), gvs[0])
				require.Equal(t, int64(2), gvs[1])
				require.Equal(t, int64(3), gvs[2])
			}
		}

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)

		require.Equal(t, true, exec.isFree)
		require.Equal(t, before, proc.Mp().CurrNB())
	}

	// datasource is empty.
	{
		before := proc.Mp().CurrNB()
		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = true
		g.Exprs = []*plan.Expr{newColumnExpression(0)}
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(1)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		r, err := g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		exec.Free()
		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)
		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func TestGroup_GetIntermediateResult_NoneGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	// datasource.
	{
		before := proc.Mp().CurrNB()

		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{1, 1},
				{1, 2},
				{2, 3},
			}),
			batch.EmptyBatch,
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = false
		g.Exprs, g.GroupingFlag = nil, nil
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(0)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		// return intermediate result for datas[0].
		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, b.RowCount())
			require.Equal(t, 1, len(b.Aggs))
			require.Equal(t, exec, b.Aggs[0])
			require.Equal(t, 0, len(b.Vecs))
		}

		// return nothing for EmptyBatch and return nil.
		exec2 := hackMakeAggToTest(1)
		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)
		exec2.Free()

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)
		require.Equal(t, true, exec2.isFree)

		require.Equal(t, before, proc.Mp().CurrNB())
	}

	// datasource is empty.
	{
		before := proc.Mp().CurrNB()

		exec := hackMakeAggToTest(1)
		datas := []*batch.Batch{
			batch.EmptyBatch,
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = false
		g.Exprs, g.GroupingFlag = nil, nil
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(0)}, nil),
		}

		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		// if data source is empty,
		// return intermediate result for agg (count is 0, and others are null).
		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, len(b.Aggs))
			require.Equal(t, exec, b.Aggs[0])
			require.Equal(t, 1, exec.groupNumber)
			require.Equal(t, 0, exec.doBulkFillRow)
			require.Equal(t, 0, exec.doFlushTime)
		}

		// next call will get nil.
		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)

		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func TestGroup_GetIntermediateResult_WithGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	// datasource.
	{
		before := proc.Mp().CurrNB()
		datas := []*batch.Batch{
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{1, 1},
				{1, 2},
				{2, 3},
			}),
			getGroupTestBatch(proc.Mp(), [][2]int64{
				{1, 1},
				{1, 2},
				{2, 3},
			}),
			batch.EmptyBatch,
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = false
		g.Exprs = []*plan.Expr{newColumnExpression(0)}
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(1)}, nil),
		}

		exec := hackMakeAggToTest(1)
		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		// get from datas[0]
		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, len(b.Aggs))
			require.Equal(t, exec, b.Aggs[0])
			require.Equal(t, 1, len(b.Vecs))
			require.Equal(t, 2, exec.groupNumber)
			require.Equal(t, 1, exec.doBatchFillRow)
			require.Equal(t, 0, exec.doFlushTime)
		}

		// get from datas[1]
		exec2 := hackMakeAggToTest(1)
		r, err = g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, len(b.Aggs))
			require.Equal(t, exec2, b.Aggs[0])
			require.Equal(t, 1, len(b.Vecs))
			require.Equal(t, 2, exec.groupNumber)
			require.Equal(t, 1, exec.doBatchFillRow)
			require.Equal(t, 0, exec.doFlushTime)
		}

		// get from Empty and nil.
		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		require.Equal(t, true, exec.isFree)
		require.Equal(t, true, exec2.isFree)

		require.Equal(t, before, proc.Mp().CurrNB())
	}

	// datasource is empty.
	{
		before := proc.Mp().CurrNB()
		datas := []*batch.Batch{
			batch.EmptyBatch,
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)
		g.NeedEval = false
		g.Exprs = []*plan.Expr{newColumnExpression(0)}
		g.Aggs = []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(1)}, nil),
		}

		exec := hackMakeAggToTest(1)
		require.NoError(t, src.Prepare(proc))
		require.NoError(t, g.Prepare(proc))

		// get nil if datasource is just empty.
		r, err := g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		{
			require.Equal(t, 0, exec.doFlushTime)
			require.Equal(t, 0, exec.groupNumber)
			require.Equal(t, 0, exec.doBatchFillRow)
		}

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
		exec.Free()
		require.Equal(t, true, exec.isFree)

		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func getGroupTestBatch(mp *mpool.MPool, values [][2]int64) *batch.Batch {
	typ := types.T_int64.ToType()

	first := make([]int64, len(values))
	second := make([]int64, len(values))
	for i := range first {
		first[i] = values[i][0]
		second[i] = values[i][1]
	}

	v1 := testutil.NewInt64Vector(len(values), typ, mp, false, first)
	v2 := testutil.NewInt64Vector(len(values), typ, mp, false, second)

	res := batch.NewWithSize(2)
	res.Vecs[0], res.Vecs[1] = v1, v2
	res.SetRowCount(len(values))
	return res
}

func getGroupOperatorWithInputs(dataList []*batch.Batch) (*Group, *value_scan.ValueScan) {
	vscan := value_scan.NewArgument()
	vscan.Batchs = dataList

	res := &Group{
		OperatorBase: vm.OperatorBase{},
	}
	res.AppendChild(vscan)

	return res, vscan
}

func newColumnExpression(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: pos,
			},
		},
	}
}
