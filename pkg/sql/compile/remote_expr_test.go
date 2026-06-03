// Copyright 2021 Matrix Origin
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

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/group"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/lockop"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/onduplicatekey"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestScopeContainsVarExpr(t *testing.T) {
	scope := newScope(Normal)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{makeTestVarExpr("sql_mode")}
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	f.AppendChild(proj)
	scope.setRootOperator(f)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInSource(t *testing.T) {
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInOperatorMap(t *testing.T) {
	scope := newScope(Normal)
	op := onduplicatekey.NewArgument()
	op.OnDuplicateExpr = map[string]*plan.Expr{
		"col": makeTestVarExpr("sql_mode"),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInAggArguments(t *testing.T) {
	scope := newScope(Normal)
	op := group.NewArgument()
	op.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{makeTestVarExpr("sql_mode")}, nil),
	}
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestScopeContainsVarExprInLockRows(t *testing.T) {
	scope := newScope(Normal)
	op := lockop.NewArgumentByEngine(nil)
	op.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, makeTestVarExpr("sql_mode"), false)
	scope.setRootOperator(op)

	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInScope(t *testing.T) {
	proc := newResolveVariableProcess(t, "STRICT_TRANS_TABLES")
	scope := newScope(Normal)
	scope.DataSource = &Source{
		FilterList: []*plan.Expr{makeTestVarExpr("sql_mode")},
	}

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)
	require.False(t, scopeContainsVarExpr(scope))

	lit, ok := scope.DataSource.FilterList[0].Expr.(*plan.Expr_Lit)
	require.True(t, ok)
	require.Equal(t, "STRICT_TRANS_TABLES", lit.Lit.GetSval())
}

func TestFoldVarExprsInScopeUsesPrivateExprCopies(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope1 := newScope(Normal)
	proj1 := projection.NewArgument()
	proj1.ProjectList = []*plan.Expr{shared}
	scope1.setRootOperator(proj1)

	scope2 := newScope(Normal)
	proj2 := projection.NewArgument()
	proj2.ProjectList = []*plan.Expr{shared}
	scope2.setRootOperator(proj2)

	folded, err := foldVarExprsInScope(scope1, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	folded, err = foldVarExprsInScope(scope2, proc2)
	require.NoError(t, err)
	require.True(t, folded)

	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.NotSame(t, shared, proj1.ProjectList[0])
	require.NotSame(t, shared, proj2.ProjectList[0])
	require.NotSame(t, proj1.ProjectList[0], proj2.ProjectList[0])
	require.Equal(t, "ANSI", proj1.ProjectList[0].GetLit().GetSval())
	require.Equal(t, "TRADITIONAL", proj2.ProjectList[0].GetLit().GetSval())
}

func TestFoldVarExprsInRemoteRunScopeDoesNotMutateReusableScope(t *testing.T) {
	shared := makeTestVarExpr("sql_mode")
	proc1 := newResolveVariableProcess(t, "ANSI")
	proc2 := newResolveVariableProcess(t, "TRADITIONAL")

	scope := newScope(Remote)
	proj := projection.NewArgument()
	proj.ProjectList = []*plan.Expr{shared}
	scope.setRootOperator(proj)

	remoteScope1, folded, err := foldVarExprsInRemoteRunScope(scope, proc1)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj1 := remoteScope1.RootOp.(*projection.Projection)
	require.Equal(t, "ANSI", remoteProj1.ProjectList[0].GetLit().GetSval())

	require.True(t, scopeContainsVarExpr(scope))
	require.IsType(t, &plan.Expr_V{}, shared.Expr)
	require.Same(t, shared, proj.ProjectList[0])

	remoteScope2, folded, err := foldVarExprsInRemoteRunScope(scope, proc2)
	require.NoError(t, err)
	require.True(t, folded)
	remoteProj2 := remoteScope2.RootOp.(*projection.Projection)
	require.Equal(t, "TRADITIONAL", remoteProj2.ProjectList[0].GetLit().GetSval())

	require.NotSame(t, scope, remoteScope1)
	require.NotSame(t, scope.RootOp, remoteScope1.RootOp)
	require.NotSame(t, remoteScope1.RootOp, remoteScope2.RootOp)
	require.NotSame(t, remoteProj1.ProjectList[0], remoteProj2.ProjectList[0])
	require.True(t, scopeContainsVarExpr(scope))
}

func TestFoldVarExprsInHiddenExpressionsUsePrivateCopies(t *testing.T) {
	proc := newResolveVariableProcess(t, "ANSI")
	sharedAggExpr := makeTestVarExpr("sql_mode")
	sharedLockRows := makeTestVarExpr("sql_mode")

	scope := newScope(Normal)
	groupOp := group.NewArgument()
	groupOp.Aggs = []aggexec.AggFuncExecExpression{
		aggexec.MakeAggFunctionExpression(function.AggSumOverloadID, false, []*plan.Expr{sharedAggExpr}, nil),
	}
	lockOp := lockop.NewArgumentByEngine(nil)
	lockOp.AddLockTarget(2, nil, 0, types.T_int64.ToType(), -1, -1, nil, false)
	lockOp.AddLockTarget(1, nil, 0, types.T_int64.ToType(), -1, -1, sharedLockRows, false)
	groupOp.AppendChild(lockOp)
	scope.setRootOperator(groupOp)

	rewriteCalls := 0
	rewritten, err := lockOp.RewriteLockRowsExpressions(func(expr *plan.Expr) (*plan.Expr, bool, error) {
		require.NotNil(t, expr)
		rewriteCalls++
		return expr, false, nil
	})
	require.NoError(t, err)
	require.False(t, rewritten)
	require.Equal(t, 1, rewriteCalls)

	folded, err := foldVarExprsInScope(scope, proc)
	require.NoError(t, err)
	require.True(t, folded)

	aggArg := groupOp.Aggs[0].GetArgExpressions()[0]
	lockRows := lockOp.GetLockRowsExpressions()[0]
	require.IsType(t, &plan.Expr_V{}, sharedAggExpr.Expr)
	require.IsType(t, &plan.Expr_V{}, sharedLockRows.Expr)
	require.NotSame(t, sharedAggExpr, aggArg)
	require.NotSame(t, sharedLockRows, lockRows)
	require.Equal(t, "ANSI", aggArg.GetLit().GetSval())
	require.Equal(t, "ANSI", lockRows.GetLit().GetSval())
}

func TestScopeContainsVarExprReturnsFalseWithoutVar(t *testing.T) {
	scope := newScope(Normal)
	f := filter.NewArgument()
	f.FilterExprs = []*plan.Expr{makeTestConstBoolExpr(true)}
	scope.setRootOperator(f)

	require.False(t, scopeContainsVarExpr(scope))
}

func makeTestVarExpr(name string) *plan.Expr {
	typ := types.T_text.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_V{
			V: &plan.VarRef{
				Name:   name,
				System: true,
			},
		},
	}
}

func newResolveVariableProcess(t *testing.T, sqlMode string) *process.Process {
	proc := testutil.NewProcess(t)
	proc.SetResolveVariableFunc(func(name string, system, global bool) (interface{}, error) {
		if name == "sql_mode" {
			return sqlMode, nil
		}
		return nil, moerr.NewInternalErrorNoCtx("variable not found")
	})
	return proc
}

func makeTestConstBoolExpr(v bool) *plan.Expr {
	typ := types.T_bool.ToType()
	return &plan.Expr{
		Typ: plan2.MakePlan2Type(&typ),
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{
				Value: &plan.Literal_Bval{Bval: v},
			},
		},
	}
}
