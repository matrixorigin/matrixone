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

package rule

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExprStringSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	lit := func(source uint32, null bool) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_Lit{Lit: &plan.Literal{StringSource: source, Isnull: null}}}
	}
	fn := func(id int32, overload int32, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(id, overload)},
			Args: args,
		}}}
	}
	variable := &plan.Expr{Expr: &plan.Expr_V{V: &plan.VarRef{Name: "v"}}}
	param := &plan.Expr{Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	// Literal.StringSource encodes source+1; zero is an ordinary literal.
	userVariableLit := lit(uint32(types.StringSourceUserVariable)+1, false)

	require.Equal(t, types.StringSourceLiteral, ExprStringSource(lit(0, false), proc))
	require.Equal(t, types.StringSourceUserVariable, ExprStringSource(userVariableLit, proc))
	require.Equal(t, types.StringSourceExpression, ExprStringSource(lit(99, false), proc))
	require.Equal(t, types.StringSourceUserVariable, ExprStringSource(variable, proc))
	require.Equal(t, types.StringSourceExpression, ExprStringSource(nil, proc))

	// LiteralVec stores the raw source.
	vecExpr := &plan.Expr{Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{StringSource: uint32(types.StringSourceLiteral)}}}
	require.Equal(t, types.StringSourceLiteral, ExprStringSource(vecExpr, proc))

	// A parameter has its execution's source.
	require.Equal(t, types.StringSourceExpression, ExprStringSource(param, proc))
	proc.SetPrepareParamsStringSource(types.StringSourceCOMStmt)
	require.Equal(t, types.StringSourceCOMStmt, ExprStringSource(param, proc))
	require.Equal(t, types.StringSourceExpression, ExprStringSource(param, nil))
	proc.SetPrepareParamsStringSource(types.StringSourceExpression)

	// An implicit cast is transparent; an explicit one computes a new value.
	require.Equal(t, types.StringSourceUserVariable, ExprStringSource(fn(function.CAST, 0, variable), proc))
	require.Equal(t, types.StringSourceExpression, ExprStringSource(fn(function.CAST, 1, variable), proc))

	// COALESCE takes the first arm that is not a NULL literal.
	require.Equal(t, types.StringSourceUserVariable,
		ExprStringSource(fn(function.COALESCE, 0, lit(0, true), variable, lit(0, false)), proc))
	require.Equal(t, types.StringSourceLiteral,
		ExprStringSource(fn(function.COALESCE, 0, lit(0, false), variable), proc))
	require.Equal(t, types.StringSourceExpression,
		ExprStringSource(fn(function.COALESCE, 0, lit(0, true)), proc))

	// Any other function computes a new value.
	require.Equal(t, types.StringSourceExpression, ExprStringSource(fn(function.CONCAT, 0, lit(0, false)), proc))

	source, uniform := ListStringSource([]*plan.Expr{lit(0, false), lit(0, true)}, proc)
	require.True(t, uniform)
	require.Equal(t, types.StringSourceLiteral, source)
	_, uniform = ListStringSource([]*plan.Expr{lit(0, false), variable}, proc)
	require.False(t, uniform)
}

func TestGetConstantValueStampsSource(t *testing.T) {
	proc := testutil.NewProcess(t)
	vec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(3), 1, proc.Mp())
	require.NoError(t, err)
	defer vec.Free(proc.Mp())

	require.Zero(t, GetConstantValue(vec, false, 0, types.StringSourceLiteral).StringSource)
	require.Equal(t, uint32(types.StringSourceUserVariable)+1,
		GetConstantValue(vec, false, 0, types.StringSourceUserVariable).StringSource)
	require.Equal(t, uint32(types.StringSourceExpression)+1,
		GetConstantValue(vec, false, 0, types.StringSourceExpression).StringSource)
}
