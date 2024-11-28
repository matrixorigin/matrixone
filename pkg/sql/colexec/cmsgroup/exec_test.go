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

package cmsgroup

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"testing"
)

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

func TestGroup_ShouldDoFinalEvaluation(t *testing.T) {
	proc := testutil.NewProcess()
	hackMakeAggToTest()

	// Only Aggregation.
	{
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

		g.Free(proc, false, nil)
		src.Free(proc, false, nil)
	}

}

func hackMakeAggToTest() {
	makeAggExec = func(_ aggexec.AggMemoryManager, _ int64, _ bool, _ ...types.Type) aggexec.AggFuncExec {
		return nil
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
