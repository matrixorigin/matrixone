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

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/stretchr/testify/require"
)

func TestFirstRowSpillPreservesTuples(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	child := colexec.NewMockOperator()
	t.Cleanup(func() { child.Free(proc, false, nil) })
	const groups = 512
	for part := 0; part < 2; part++ {
		input := batch.NewWithSize(3)
		for i := range input.Vecs {
			input.Vecs[i] = vector.NewVec(types.T_int32.ToType())
		}
		for id := 0; id < groups; id++ {
			require.NoError(t, vector.AppendFixed(input.Vecs[0], int32(id), false, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input.Vecs[1], int32(1), part == 0, proc.Mp()))
			require.NoError(t, vector.AppendFixed(input.Vecs[2], int32(2), part == 1, proc.Mp()))
		}
		input.SetRowCount(groups)
		child.WithBatchs([]*batch.Batch{input})
	}
	aggs := []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfFirstRow, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfFirstRow, false, []*plan.Expr{colExpr(2, types.T_int32)}, nil),
	}
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, aggs)
	g.SpillMem = 64
	g.AppendChild(child)
	t.Cleanup(func() { g.Free(proc, false, nil) })
	require.NoError(t, g.Prepare(proc))
	seen := make(map[int32]bool, groups)
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		for i := 0; i < result.Batch.RowCount(); i++ {
			id := vector.GetFixedAtNoTypeCheck[int32](result.Batch.Vecs[0], i)
			require.False(t, seen[id])
			seen[id] = true
			a, b := result.Batch.Vecs[1], result.Batch.Vecs[2]
			first := a.IsNull(uint64(i)) && !b.IsNull(uint64(i)) && vector.GetFixedAtNoTypeCheck[int32](b, i) == 2
			second := !a.IsNull(uint64(i)) && vector.GetFixedAtNoTypeCheck[int32](a, i) == 1 && b.IsNull(uint64(i))
			require.True(t, first || second, "group %d must come from one source row", id)
		}
	}
	require.Len(t, seen, groups)
	require.Positive(t, g.OpAnalyzer.GetOpStats().ExtraStats["GroupSpillWriteCalls"])
}
