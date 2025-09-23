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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpill(t *testing.T) {
	proc := testutil.NewProcess(t)
	before := proc.Mp().CurrNB()

	datas := []*batch.Batch{
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{1, 10}, {1, 20}, {2, 30}, {2, 40}, {3, 50},
		}),
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{1, 10}, {2, 30}, {3, 50}, {4, 60}, {5, 70},
		}),
		nil,
	}
	afterDataCreation := proc.Mp().CurrNB()
	t.Logf("After data creation: %d bytes allocated", afterDataCreation-before)

	g, src := getGroupOperatorWithInputs(datas)
	g.NeedEval = true
	g.SpillThreshold = 1024 // Very low threshold to trigger spill quickly
	g.Exprs = []*plan.Expr{newColumnExpression(0)}
	g.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, []*plan.Expr{newColumnExpression(1)}, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{newColumnExpression(1)}, nil),
	}

	require.NoError(t, src.Prepare(proc))
	afterSrcPrepare := proc.Mp().CurrNB()
	t.Logf("After src prepare: %d bytes allocated", afterSrcPrepare-before)

	require.NoError(t, g.Prepare(proc))
	afterGroupPrepare := proc.Mp().CurrNB()
	t.Logf("After group prepare: %d bytes allocated", afterGroupPrepare-before)

	require.NotNil(t, g.SpillManager)
	require.Equal(t, int64(1024), g.SpillThreshold)

	// Process first batch - should trigger spill
	r, err := g.Call(proc)
	require.NoError(t, err)
	require.Nil(t, r.Batch) // Should be nil due to spill
	afterFirstCall := proc.Mp().CurrNB()
	t.Logf("After first call: %d bytes allocated", afterFirstCall-before)

	// Process second batch - should merge spilled data and produce results
	r, err = g.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, r.Batch)
	afterSecondCall := proc.Mp().CurrNB()
	t.Logf("After second call: %d bytes allocated", afterSecondCall-before)

	if final := r.Batch; final != nil {
		require.Equal(t, 2, len(final.Aggs))
		require.Equal(t, 2, len(final.Vecs))

		groupVec := final.Vecs[0]
		countVec := final.Vecs[1]
		sumVecs, err := final.Aggs[1].Flush()
		require.NoError(t, err)
		sumVec := sumVecs[0]

		require.Equal(t, 5, groupVec.Length())
		require.Equal(t, 5, countVec.Length())
		require.Equal(t, 5, sumVec.Length())

		groups := vector.MustFixedColNoTypeCheck[int64](groupVec)
		counts := vector.MustFixedColNoTypeCheck[int64](countVec)
		sums := vector.MustFixedColNoTypeCheck[int64](sumVec)

		expectedGroups := []int64{1, 2, 3, 4, 5}
		expectedCounts := []int64{2, 2, 2, 1, 1}
		expectedSums := []int64{40, 100, 150, 60, 70}

		for i := 0; i < 5; i++ {
			require.Equal(t, expectedGroups[i], groups[i])
			require.Equal(t, expectedCounts[i], counts[i])
			require.Equal(t, expectedSums[i], sums[i])
		}
	}

	// Final call should return nil
	r, err = g.Call(proc)
	require.NoError(t, err)
	require.Nil(t, r.Batch)
	afterThirdCall := proc.Mp().CurrNB()
	t.Logf("After third call: %d bytes allocated", afterThirdCall-before)

	g.Free(proc, false, nil)
	afterGroupFree := proc.Mp().CurrNB()
	t.Logf("After group free: %d bytes allocated", afterGroupFree-before)

	src.Free(proc, false, nil)
	afterSrcFree := proc.Mp().CurrNB()
	t.Logf("After src free: %d bytes allocated", afterSrcFree-before)

	require.Equal(t, before, proc.Mp().CurrNB())
}

func TestSpillMultipleCycles(t *testing.T) {
	proc := testutil.NewProcess(t)
	before := proc.Mp().CurrNB()

	datas := []*batch.Batch{
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{1, 1}, {2, 2}, {3, 3},
		}),
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{4, 4}, {5, 5}, {6, 6},
		}),
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{1, 7}, {2, 8}, {3, 9},
		}),
		nil,
	}

	g, src := getGroupOperatorWithInputs(datas)
	g.NeedEval = true
	g.SpillThreshold = 10
	g.Exprs = []*plan.Expr{newColumnExpression(0)}
	g.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, []*plan.Expr{newColumnExpression(1)}, nil),
	}

	require.NoError(t, src.Prepare(proc))
	require.NoError(t, g.Prepare(proc))

	r, err := g.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, r.Batch)

	if final := r.Batch; final != nil {
		require.Equal(t, 2, len(final.Vecs))
		require.Equal(t, 6, final.Vecs[0].Length())

		groups := vector.MustFixedColNoTypeCheck[int64](final.Vecs[0])
		counts := vector.MustFixedColNoTypeCheck[int64](final.Vecs[1])

		groupCounts := make(map[int64]int64)
		for i := 0; i < len(groups); i++ {
			groupCounts[groups[i]] = counts[i]
		}

		require.Equal(t, int64(2), groupCounts[1])
		require.Equal(t, int64(2), groupCounts[2])
		require.Equal(t, int64(2), groupCounts[3])
		require.Equal(t, int64(1), groupCounts[4])
		require.Equal(t, int64(1), groupCounts[5])
		require.Equal(t, int64(1), groupCounts[6])
	}

	g.Free(proc, false, nil)
	src.Free(proc, false, nil)
	require.Equal(t, before, proc.Mp().CurrNB())
}
