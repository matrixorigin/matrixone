// Copyright 2025 Matrix Origin
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
	"math/rand/v2"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestGroup_CountStar(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	mp := proc.Mp()
	before := mp.CurrNB()

	numRows := 1024
	numBatches := 1024

	var allBatches []*batch.Batch
	for i := 0; i < numBatches; i++ {
		inputValues := make([]int64, numRows)
		for i := 0; i < numRows; i++ {
			inputValues[i] = int64(i + 1)
		}
		rand.Shuffle(len(inputValues), func(i, j int) {
			inputValues[i], inputValues[j] = inputValues[j], inputValues[i]
		})

		inputVec := testutil.NewInt64Vector(numRows, types.T_int64.ToType(), mp, false, inputValues)
		inputBatch := batch.NewWithSize(1)
		inputBatch.Vecs[0] = inputVec
		inputBatch.SetRowCount(numRows)

		allBatches = append(allBatches, inputBatch)
	}

	vscan := value_scan.NewArgument()
	vscan.Batchs = allBatches

	g := &Group{
		OperatorBase: vm.OperatorBase{},
		NeedEval:     true,
		Exprs:        []*plan.Expr{newColumnExpression(0)},
		GroupingFlag: []bool{true},
		Aggs: []aggexec.AggFuncExecExpression{
			aggexec.MakeAggFunctionExpression(
				aggexec.AggIdOfCountStar,
				false,
				[]*plan.Expr{
					newColumnExpression(0),
				},
				nil,
			),
		},
	}
	g.AppendChild(vscan)

	require.NoError(t, vscan.Prepare(proc))
	require.NoError(t, g.Prepare(proc))

	g.ctr.spillThreshold = 8 * 1024

	var outputBatch *batch.Batch
	outputCount := 0
	for {
		r, err := g.Call(proc)
		require.NoError(t, err)
		if r.Batch == nil {
			break
		}
		outputCount++
		outputBatch, err = r.Batch.Dup(proc.Mp())
		require.NoError(t, err)
		require.Equal(t, 1, outputCount)
	}

	require.NotNil(t, outputBatch)
	require.Equal(t, 2, len(outputBatch.Vecs))
	require.Equal(t, numRows, outputBatch.RowCount())
	require.Equal(t, 0, len(outputBatch.Aggs))

	outputValues := vector.MustFixedColNoTypeCheck[int64](outputBatch.Vecs[0])
	countValues := vector.MustFixedColNoTypeCheck[int64](outputBatch.Vecs[1])
	require.Equal(t, numRows, len(outputValues))
	require.Equal(t, numRows, len(countValues))

	for _, v := range countValues {
		require.Equal(t, int64(numBatches), v, "Count for each unique group should be 1")
	}

	outputMap := make(map[int64]int64)
	for i, v := range outputValues {
		outputMap[v] = countValues[i]
	}

	if outputBatch != nil {
		outputBatch.Clean(proc.Mp())
	}
	g.Free(proc, false, nil)
	vscan.Free(proc, false, nil)
	require.Equal(t, before, mp.CurrNB())
}
