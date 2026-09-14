// Copyright 2026 Matrix Origin
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

package window

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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

func TestWindowOrderSpillsAndKeepsArgumentsAligned(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{3, 1, 2}, nil, proc.Mp())
	input.SetRowCount(3)

	orderExpr := newColExprWithType(1, types.T_int32.ToType())
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:       "row_number",
				WindowFunc: newFunExpr("row_number"),
				OrderBy:    []*plan.OrderBySpec{{Expr: orderExpr}},
			}},
		}},
		Aggs:           []aggexec.AggFuncExecExpression{newRowNumberAggExpr(t)},
		SpillThreshold: 1,
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int32{20, 30, 10}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int32{1, 2, 3}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[1]))
	require.Equal(t, []uint64{1, 2, 3}, vector.MustFixedColWithTypeCheck[uint64](result.Batch.Vecs[2]))
	require.Positive(t, arg.OpAnalyzer.GetOpStats().SpillRows)
	require.Positive(t, arg.OpAnalyzer.GetOpStats().SpillSize)

	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}

func TestWindowOrderSpillReevaluatesAggregateArguments(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	input := batch.NewWithSize(2)
	input.Vecs[0] = testutil.MakeInt32Vector([]int32{10, 20, 30}, nil, proc.Mp())
	input.Vecs[1] = testutil.MakeInt32Vector([]int32{3, 1, 2}, nil, proc.Mp())
	input.SetRowCount(3)

	orderExpr := newColExprWithType(1, types.T_int32.ToType())
	arg := &Window{
		WinSpecList: []*plan.Expr{{
			Expr: &plan.Expr_W{W: &plan.WindowSpec{
				Name:       "sum",
				WindowFunc: newFunExpr("sum"),
				OrderBy:    []*plan.OrderBySpec{{Expr: orderExpr}},
				Frame:      makeCumulativeFrame(),
			}},
		}},
		Aggs:           []aggexec.AggFuncExecExpression{newAggExprAt(0)},
		SpillThreshold: 1,
	}
	child := colexec.NewMockOperator().WithBatchs([]*batch.Batch{input})
	arg.AppendChild(child)
	require.NoError(t, arg.Prepare(proc))

	result, err := vm.Exec(arg, proc)
	require.NoError(t, err)
	require.NotNil(t, result.Batch)
	require.Equal(t, []int32{20, 30, 10}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[0]))
	require.Equal(t, []int32{1, 2, 3}, vector.MustFixedColWithTypeCheck[int32](result.Batch.Vecs[1]))
	require.Equal(t, []int64{20, 50, 60}, vector.MustFixedColWithTypeCheck[int64](result.Batch.Vecs[2]))
	require.Positive(t, arg.OpAnalyzer.GetOpStats().SpillRows)
	require.Positive(t, arg.OpAnalyzer.GetOpStats().SpillSize)

	child.Free(proc, false, nil)
	arg.Free(proc, false, nil)
	proc.Free()
	require.Zero(t, proc.Mp().CurrNB())
}
