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

package colexec_test

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	sqlplan "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerWideningBoundExpressionSelection(t *testing.T) {
	proc := testutil.NewProcess(t, testutil.WithFileService(nil))
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	column := &plan.Expr{Typ: types.T_int8.ToType().PlanType(), Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	expression, err := sqlplan.BindFuncExprImplByPlanExpr(proc.Ctx, "*", []*plan.Expr{column, column})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int64), expression.Typ.Id)
	require.Equal(t, "cast", expression.GetF().Args[0].GetF().Func.ObjName)
	executor, err := colexec.NewExpressionExecutor(proc, expression)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	calls := 0
	colexec.ObserveIntegerWideningCastForTest(executor, func(selection *function.FunctionSelectList) {
		calls++
		require.NotNil(t, selection, "ordinary Eval must exercise its internal selection object")
		require.False(t, selection.IgnoreAllRow())
		require.True(t, selection.ShouldEvalAllRow())
	})
	bat := batch.NewWithSize(1)
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	bat.Vecs[0] = testutil.MakeInt8Vector([]int8{-128, 0, 127}, nil, proc.Mp())
	bat.SetRowCount(3)
	before, err := bat.Vecs[0].MarshalBinary()
	require.NoError(t, err)
	for _, selection := range [][]bool{nil, {true, true, true}, nil} {
		result, err := executor.Eval(proc, []*batch.Batch{bat}, selection)
		require.NoError(t, err)
		require.Equal(t, []int64{16384, 0, 16129}, vector.MustFixedColWithTypeCheck[int64](result))
		require.Equal(t, 3, result.Length())
		require.True(t, result.GetNulls().EmptyByFlag())
	}
	require.Equal(t, 3, calls)
	after, err := bat.Vecs[0].MarshalBinary()
	require.NoError(t, err)
	require.Equal(t, before, after)
}
