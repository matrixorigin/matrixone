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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestSpill(t *testing.T) {
	proc := testutil.NewProcess(t)

	before := proc.Mp().CurrNB()
	exec, restore := hackMakeAggToTest(1)
	defer restore()

	datas := []*batch.Batch{
		getGroupTestBatch(proc.Mp(), [][2]int64{
			{1, 1}, {1, 2}, {2, 3}, {2, 4}, {3, 5},
		}),
		nil,
	}

	g, src := getGroupOperatorWithInputs(datas)
	g.NeedEval = true
	g.SpillThreshold = 100
	g.Exprs = []*plan.Expr{newColumnExpression(0)}
	g.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(0, false, []*plan.Expr{newColumnExpression(1)}, nil),
	}

	require.NoError(t, src.Prepare(proc))
	require.NoError(t, g.Prepare(proc))

	require.NotNil(t, g.SpillManager)
	require.Equal(t, int64(100), g.SpillThreshold)

	r, err := g.Call(proc)
	require.NoError(t, err)
	require.NotNil(t, r.Batch)

	if final := r.Batch; final != nil {
		require.Equal(t, 0, len(final.Aggs))
		require.Equal(t, 2, len(final.Vecs))
		require.Equal(t, hackVecResult, final.Vecs[1])
	}

	r, err = g.Call(proc)
	require.NoError(t, err)
	require.Nil(t, r.Batch)

	g.Free(proc, false, nil)
	src.Free(proc, false, nil)
	require.Equal(t, true, exec.isFree)
	require.Equal(t, before, proc.Mp().CurrNB())
}
