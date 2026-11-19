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

func TestUnboundGroupMedianAcrossResultChunks(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	const count = 2*aggexec.AggBatchSize + 1
	decimalType := types.New(types.T_decimal64, 10, 2)
	var inputs []*batch.Batch
	inputsOwned := true
	t.Cleanup(func() {
		if inputsOwned {
			for _, input := range inputs {
				input.Clean(proc.Mp())
			}
		}
	})
	for start := 0; start < count; start += aggexec.AggBatchSize {
		rows := min(aggexec.AggBatchSize, count-start)
		keys, values := make([]int32, rows), make([]int32, rows)
		decimals, isNull := make([]types.Decimal64, rows), make([]bool, rows)
		var nullRows []uint64
		for row := range rows {
			key := int32(start + row + 1)
			keys[row], values[row] = key, key
			decimals[row] = types.Decimal64(int64(key) * 100)
			if key%7 == 1 {
				isNull[row] = true
				nullRows = append(nullRows, uint64(row))
			}
		}
		input := batch.NewWithSize(3)
		inputs = append(inputs, input)
		input.Vecs[0] = testutil.MakeInt32Vector(keys, nil, proc.Mp())
		input.Vecs[1] = testutil.MakeInt32Vector(values, nullRows, proc.Mp())
		input.Vecs[2] = vector.NewVec(decimalType)
		require.NoError(t, vector.AppendFixedList(input.Vecs[2], decimals, isNull, proc.Mp()))
		input.SetRowCount(rows)
	}
	child := colexec.NewMockOperator().WithBatchs(inputs)
	t.Cleanup(func() { child.Free(proc, false, nil) })
	inputsOwned = false
	g := newGroupOp(proc, []*plan.Expr{colExpr(0, types.T_int32)}, []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{colExpr(1, types.T_int32)}, nil),
		aggexec.MakeAggFunctionExpression(aggexec.AggIdOfMedian, false, []*plan.Expr{colExprWithType(2, decimalType)}, nil),
	})
	t.Cleanup(func() { g.Free(proc, false, nil) })
	g.SpillMem = 1 << 30
	g.AppendChild(child)
	require.NoError(t, g.Prepare(proc))
	require.Nil(t, g.ctr.aggregateAllocation)

	seen := make([]bool, count)
	rows := 0
	for {
		result, err := vm.Exec(g, proc)
		require.NoError(t, err)
		if result.Status == vm.ExecStop || result.Batch == nil {
			break
		}
		keys := vector.MustFixedColNoTypeCheck[int32](result.Batch.Vecs[0])
		numbers := vector.MustFixedColNoTypeCheck[float64](result.Batch.Vecs[1])
		decimals := vector.MustFixedColNoTypeCheck[types.Decimal128](result.Batch.Vecs[2])
		for row, key := range keys {
			if key < 1 || key > count || seen[key-1] {
				t.Fatalf("invalid or duplicate key %d", key)
			}
			seen[key-1] = true
			wantNull := key%7 == 1
			if result.Batch.Vecs[1].IsNull(uint64(row)) != wantNull || result.Batch.Vecs[2].IsNull(uint64(row)) != wantNull {
				t.Fatalf("key %d: incorrect NULL state", key)
			}
			if !wantNull && (numbers[row] != float64(key) || decimals[row] != (types.Decimal128{B0_63: uint64(key) * 1000})) {
				t.Fatalf("key %d: numeric=%v decimal=%s", key, numbers[row], decimals[row].Format(3))
			}
			rows++
		}
	}
	require.Equal(t, count, rows)
}
