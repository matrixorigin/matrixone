// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package group

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGroupConsumesSelectedDecimalRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(proc.Free)
	keyType := types.New(types.T_char, 1, 0)
	quantityType := types.New(types.T_decimal64, 15, 2)
	input := batch.NewWithSize(2)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	input.Vecs[0] = vector.NewVec(keyType)
	input.Vecs[1] = vector.NewVec(quantityType)
	for _, key := range []string{"A", "B", "A", "B", "B"} {
		require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte(key), false, proc.Mp()))
	}
	require.NoError(t, vector.AppendFixedList(input.Vecs[1], []types.Decimal64{100, 200, 300, 0, 400}, []bool{false, false, false, true, false}, proc.Mp()))
	input.SetRowCount(5)
	selected := make([]*vector.Vector, 2)
	admitted, err := vector.TrySelectDictionaryVectors(input.Vecs, selected, []int64{4, 1, 4, 3, 0}, 5, proc.Mp(), nil)
	require.NoError(t, err)
	require.True(t, admitted)
	for _, v := range input.Vecs {
		v.Free(proc.Mp())
	}
	input.Vecs = selected
	args := []*plan.Expr{colExprWithType(1, quantityType)}
	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfSum, false, args, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfCountStar, false, args, nil),
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	g := newGroupOp(proc, []*plan.Expr{colExprWithType(0, keyType)}, aggs)
	g.AppendChild(child)
	allocation := installGroupTestAllocation(t, g, proc, 64<<20)
	t.Cleanup(func() { g.Free(proc, false, nil); finalizeGroupTestAllocation(t, g, allocation) })
	require.NoError(t, g.Prepare(proc))
	require.True(t, g.AcceptsDictionary(0))
	sums := make(map[string]string)
	counts := make(map[string]int64)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Batch == nil {
			break
		}
		sumVector := result.Batch.Vecs[1]
		require.Equal(t, types.T_decimal128, sumVector.GetType().Oid)
		for row := 0; row < result.Batch.RowCount(); row++ {
			key := string(result.Batch.Vecs[0].GetBytesAt(row))
			sums[key] = vector.GetFixedAtNoTypeCheck[types.Decimal128](sumVector, row).Format(2)
			counts[key] = vector.GetFixedAtNoTypeCheck[int64](result.Batch.Vecs[2], row)
		}
	}
	require.Equal(t, map[string]string{"A": "1.00", "B": "10.00"}, sums)
	require.Equal(t, map[string]int64{"A": 1, "B": 4}, counts)
}
