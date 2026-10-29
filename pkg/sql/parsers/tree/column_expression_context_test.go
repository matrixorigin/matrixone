// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestColumnExpressionWindowContext(t *testing.T) {
	call := func(name string, args ...Expr) *FuncExpr {
		return &FuncExpr{Func: FuncName2ResolvableFunctionReference(NewUnresolvedColName(name)), FuncName: NewCStr(name, 1), Exprs: args}
	}
	one := NewNumVal(int64(1), "1", false, P_int64)
	variable := NewVarExpr("x", false, false, nil)
	subquery := &Subquery{Select: &Select{Select: &SelectClause{Exprs: SelectExprs{{Expr: one}}, From: &From{}}}}
	windowCase := NewCaseExpr(nil, []*When{NewWhen(one, NewParamExpr(1))}, one)
	sum := call("sum", one)
	sum.WindowSpec = &WindowSpec{HasFrame: true, PartitionBy: Exprs{variable, subquery, call("version"), windowCase}, Frame: &FrameClause{Type: Range, HasEnd: true, Start: &FrameBound{Type: Preceding, Expr: call("interval", variable, NewTimeUnitExpr("day"))}, End: &FrameBound{Type: CurrentRow}}}
	before := String(sum, dialect.MYSQL)
	var syntax ColumnExpressionSyntax
	var parameters int
	var cases []*CaseExpr
	ctx := NewFmtCtx(dialect.MYSQL)
	ctx.expressionSyntax = &syntax
	ctx.parameterCount = &parameters
	ctx.caseExpressions = &cases
	sum.Format(ctx)
	require.False(t, syntax.VariableOrSubquery)
	require.Len(t, syntax.Functions, 1)
	require.Same(t, sum, syntax.Functions[0])
	require.Equal(t, 1, parameters)
	require.Equal(t, []*CaseExpr{windowCase}, cases)
	require.Equal(t, before, ctx.String())
	// Leaving the window restores the column context for following expressions.
	outside := &BinaryExpr{Op: PLUS, Left: sum, Right: variable}
	require.True(t, InspectColumnExpressionSyntax(outside).VariableOrSubquery)
	// Function arguments are column context even when the function has a window.
	sum.Exprs = Exprs{variable}
	require.True(t, InspectColumnExpressionSyntax(sum).VariableOrSubquery)
}
