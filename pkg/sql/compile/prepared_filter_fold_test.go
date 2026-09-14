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

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestBuildFoldedFilterExprsRollsBackAndCanRetry(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free() })
	params := vector.NewVec(types.T_text.ToType())
	require.NoError(t, vector.AppendBytes(params, []byte("not-a-decimal"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	t.Cleanup(func() {
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
	})
	decimalType := types.New(types.T_decimal256, 65, 30)
	column := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_decimal128), Width: 38, NotNullable: true},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "d"}},
	}
	zero := decimal128PlanLiteral(t, "0")
	condition, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), ">", []*planpb.Expr{column, zero},
	)
	require.NoError(t, err)
	goodValue := decimal128PlanLiteral(t, "1")
	badInput := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	target := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(decimalType.Oid), Width: decimalType.Width, Scale: decimalType.Scale},
		Expr: &planpb.Expr_T{T: &planpb.TargetType{}},
	}
	badCast, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), "cast", []*planpb.Expr{badInput, target},
	)
	require.NoError(t, err)
	filter, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), "if", []*planpb.Expr{condition, goodValue, badCast},
	)
	require.NoError(t, err)

	baseline, err := colexec.NewExpressionExecutor(
		proc, plan2.MakePlan2Int64ConstExprWithType(42),
	)
	require.NoError(t, err)
	executors := []colexec.ExpressionExecutor{baseline}
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})

	original := []*planpb.Expr{filter}
	probe := plan2.DeepCopyExprList(original)
	probeExecutors := append([]colexec.ExpressionExecutor(nil), executors...)
	probeExecutorStart := len(probeExecutors)
	t.Cleanup(func() {
		for _, executor := range probeExecutors[probeExecutorStart:] {
			executor.Free()
		}
	})
	_, err = plan2.ReplaceFoldExpr(proc, probe[0], &probeExecutors)
	require.NoError(t, err, "the parameter cast should build before execution evaluates it")
	require.Greater(t, len(probeExecutors), len(executors), "the failed attempt must own fold executors")
	evalErr := plan2.EvalFoldExpr(proc, probe[0], &probeExecutors)
	require.Error(t, evalErr,
		"the invalid execute-time decimal must fail while evaluating its fold")
	require.True(t, moerr.IsMoErrCode(evalErr, moerr.ErrInvalidInput), evalErr.Error())
	for _, executor := range probeExecutors[probeExecutorStart:] {
		executor.Free()
	}
	probeExecutors = probeExecutors[:probeExecutorStart]

	_, afterFailure, rebuilt, err := prepareFoldedFilterExprs(proc, original, nil, executors, true)
	require.Error(t, err)
	require.False(t, rebuilt)
	require.Len(t, afterFailure, 1, "failed construction must roll back only its new executors")
	require.Same(t, baseline, afterFailure[0], "existing fold executor IDs must remain stable")
	require.False(t, plan2.HasFoldExprForList(original), "the source plan must remain untouched")
	require.NotNil(t, filter.GetF().Args[2], "failed construction must not publish a partial tree")

	require.NoError(t, vector.SetStringAt(params, 0, "2", proc.Mp()))
	folded, afterRetry, rebuilt, err := prepareFoldedFilterExprs(
		proc, original, nil, afterFailure, true)
	executors = afterRetry
	require.NoError(t, err)
	require.True(t, rebuilt)
	require.Len(t, folded, 1)
	require.Greater(t, len(afterRetry), len(afterFailure))
	require.Same(t, baseline, afterRetry[0])
	require.True(t, plan2.HasFoldExprForList(folded))
	assertFoldExecutorIDsInRange(t, folded[0], len(afterRetry))
}

func TestPrepareFoldedFilterExprsRefreshesCachedParameter(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free() })
	paramType := types.T_varchar.ToType()
	params := vector.NewVec(paramType)
	require.NoError(t, vector.AppendBytes(params, []byte("first"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	t.Cleanup(func() {
		proc.SetPrepareParams(nil)
		params.Free(proc.Mp())
	})

	column := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen, NotNullable: true},
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{Name: "d"}},
	}
	param := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}},
	}
	filter, err := plan2.BindFuncExprImplByPlanExpr(
		context.Background(), "=", []*planpb.Expr{column, param},
	)
	require.NoError(t, err)

	baseline, err := colexec.NewExpressionExecutor(proc, plan2.MakePlan2Int64ConstExprWithType(42))
	require.NoError(t, err)
	executors := []colexec.ExpressionExecutor{baseline}
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})

	source := []*planpb.Expr{filter}
	cached, executors, rebuilt, err := prepareFoldedFilterExprs(proc, source, nil, executors, true)
	require.NoError(t, err)
	require.True(t, rebuilt)
	fold := findFoldValueForTest(t, cached[0])
	require.NotNil(t, fold)
	require.Equal(t, "first", string(fold.Data))

	for _, executor := range executors {
		executor.ResetForNextQuery()
	}
	require.NoError(t, vector.SetStringAt(params, 0, "second", proc.Mp()))
	cacheHit, nextExecutors, rebuilt, err := prepareFoldedFilterExprs(
		proc, source, cached, executors, true)
	require.NoError(t, err)
	require.False(t, rebuilt, "the same-length filter list must take the cache-hit path")
	require.Same(t, cached[0], cacheHit[0])
	require.Same(t, baseline, nextExecutors[0])
	require.Equal(t, "second", string(findFoldValueForTest(t, cacheHit[0]).Data),
		"cached filters must be reevaluated with the current execution's parameter")
	executors = nextExecutors
}

func decimal128PlanLiteral(t *testing.T, value string) *planpb.Expr {
	t.Helper()
	decimal, _, err := types.Parse128(value)
	require.NoError(t, err)
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 38, NotNullable: true},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
			Value: &planpb.Literal_Decimal128Val{Decimal128Val: &planpb.Decimal128{
				A: int64(decimal.B0_63),
				B: int64(decimal.B64_127),
			}},
		}},
	}
}

func assertFoldExecutorIDsInRange(t *testing.T, expr *planpb.Expr, executorCount int) {
	t.Helper()
	if fold := expr.GetFold(); fold != nil {
		require.GreaterOrEqual(t, int(fold.Id), 0)
		require.Less(t, int(fold.Id), executorCount)
		return
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			assertFoldExecutorIDsInRange(t, arg, executorCount)
		}
	}
}

func findFoldValueForTest(t *testing.T, expr *planpb.Expr) *planpb.FoldVal {
	t.Helper()
	if fold := expr.GetFold(); fold != nil {
		return fold
	}
	if function := expr.GetF(); function != nil {
		for _, arg := range function.Args {
			if fold := findFoldValueForTest(t, arg); fold != nil {
				return fold
			}
		}
	}
	return nil
}
