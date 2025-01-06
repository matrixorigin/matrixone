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

package mergegroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

// hackAggExecToTestMerge 是一个不带具体逻辑的AggExec.
// 主要用于测试在该算子中，每个接口被调用的次数，以及传入的值。
type hackAggExecToTestMerge struct {
	toFlush int
	dst     *hackAggExecToTestMerge

	aggexec.AggFuncExec
	groupNumber  int
	doFlushTime  int
	doMergeTime  int
	doBatchMerge int
	isFree       bool
}

func (h *hackAggExecToTestMerge) GetOptResult() aggexec.SplitResult {
	return nil
}

func (h *hackAggExecToTestMerge) GroupGrow(more int) error {
	h.groupNumber += more
	return nil
}

func (h *hackAggExecToTestMerge) Merge(next aggexec.AggFuncExec, groupIdx1, groupIdx2 int) error {
	h.doMergeTime++
	return nil
}

func (h *hackAggExecToTestMerge) BatchMerge(next aggexec.AggFuncExec, offset int, groups []uint64) error {
	h.doBatchMerge++
	return nil
}

func (h *hackAggExecToTestMerge) Flush() ([]*vector.Vector, error) {
	h.doFlushTime++

	ret := make([]*vector.Vector, h.toFlush)
	for i := 0; i < h.toFlush; i++ {
		ret[i] = hackVecResult
	}
	return ret, nil
}

func (h *hackAggExecToTestMerge) Free() {
	h.isFree = true
}

var hackVecResult = vector.NewVec(types.T_int64.ToType())

func hackMakeAggExecToTestMerge(r int) *hackAggExecToTestMerge {
	makeInitialAggListFromList = func(mg aggexec.AggMemoryManager, list []aggexec.AggFuncExec) []aggexec.AggFuncExec {
		res := make([]aggexec.AggFuncExec, len(list))
		for i := range res {
			res[i] = &hackAggExecToTestMerge{
				toFlush: r,
				isFree:  false,
			}
			list[i].(*hackAggExecToTestMerge).dst = res[i].(*hackAggExecToTestMerge)
		}
		return res
	}

	return &hackAggExecToTestMerge{
		toFlush: r,
		isFree:  false,
	}
}

func TestMergeGroup_WithoutGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	{
		before := proc.Mp().CurrNB()

		exec1, exec2 := hackMakeAggExecToTestMerge(1), hackMakeAggExecToTestMerge(1)
		require.NoError(t, exec1.GroupGrow(1))
		require.NoError(t, exec2.GroupGrow(1))

		datas := []*batch.Batch{
			getTestBatch(proc.Mp(), nil, exec1),
			getTestBatch(proc.Mp(), nil, exec2),
			nil,
		}
		g, src := getGroupOperatorWithInputs(datas)

		require.NoError(t, g.Prepare(proc))
		require.NoError(t, src.Prepare(proc))

		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 1, len(b.Vecs))
			require.Equal(t, hackVecResult, b.Vecs[0])
			require.Equal(t, 1, exec1.dst.groupNumber)
			require.Equal(t, 2, exec1.dst.doMergeTime)
			require.Equal(t, 1, exec1.dst.doFlushTime)
		}

		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		src.Free(proc, false, nil)
		g.Free(proc, false, nil)
		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func TestMergeGroup_WithGroupBy(t *testing.T) {
	proc := testutil.NewProcess()

	{
		before := proc.Mp().CurrNB()

		exec1, exec2 := hackMakeAggExecToTestMerge(1), hackMakeAggExecToTestMerge(1)
		exec3 := hackMakeAggExecToTestMerge(1)
		datas := []*batch.Batch{
			getTestBatch(proc.Mp(), []int64{1, 2, 3}, exec1),
			getTestBatch(proc.Mp(), []int64{2, 3, 1}, exec2),
			getTestBatch(proc.Mp(), []int64{1, 4, 2}, exec3),
			nil,
		}
		require.NoError(t, exec1.GroupGrow(3))
		require.NoError(t, exec2.GroupGrow(3))
		require.NoError(t, exec3.GroupGrow(3))

		g, src := getGroupOperatorWithInputs(datas)

		require.NoError(t, g.Prepare(proc))
		require.NoError(t, src.Prepare(proc))

		r, err := g.Call(proc)
		require.NoError(t, err)
		require.NotNil(t, r.Batch)
		if b := r.Batch; b != nil {
			require.Equal(t, 2, len(b.Vecs))
			require.Equal(t, hackVecResult, b.Vecs[1])

			vs := vector.MustFixedColNoTypeCheck[int64](b.Vecs[0])
			require.Equal(t, 4, len(vs))
			require.Equal(t, int64(1), vs[0])
			require.Equal(t, int64(2), vs[1])
			require.Equal(t, int64(3), vs[2])
			require.Equal(t, int64(4), vs[3])
			require.Equal(t, 4, exec1.dst.groupNumber) // 1, 2, 3, 4
			require.Equal(t, 3, exec1.dst.doBatchMerge)
			require.Equal(t, 1, exec1.dst.doFlushTime)
		}

		r, err = g.Call(proc)
		require.NoError(t, err)
		require.Nil(t, r.Batch)

		src.Free(proc, false, nil)
		g.Free(proc, false, nil)
		require.Equal(t, before, proc.Mp().CurrNB())
	}
}

func getGroupOperatorWithInputs(dataList []*batch.Batch) (*MergeGroup, *value_scan.ValueScan) {
	vscan := value_scan.NewArgument()
	vscan.Batchs = dataList

	res := &MergeGroup{
		OperatorBase: vm.OperatorBase{},
	}
	res.AppendChild(vscan)

	return res, vscan
}

func getTestBatch(mp *mpool.MPool, values []int64, agg aggexec.AggFuncExec) *batch.Batch {
	typ := types.T_int64.ToType()

	var res *batch.Batch
	if len(values) > 0 {
		res = batch.NewWithSize(1)

		v1 := testutil.NewInt64Vector(len(values), typ, mp, false, values)
		res.Vecs[0] = v1
		res.SetRowCount(len(values))
	} else {
		res = batch.NewWithSize(0)
		res.SetRowCount(1)
	}

	res.Aggs = []aggexec.AggFuncExec{agg}
	return res
}
