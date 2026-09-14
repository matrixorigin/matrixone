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

package function

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestNumericFunctionResultArgs(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		want  []int
	}{
		{name: "/", count: 2, want: []int{0, 1}},
		{name: "abs", count: 1, want: []int{0}},
		{name: "round", count: 2, want: []int{0}},
		{name: "truncate", count: 2, want: []int{0}},
		{name: "if", count: 3, want: []int{1, 2}},
		{name: "case", count: 3, want: []int{1, 2}},
		{name: "coalesce", count: 3, want: []int{0, 1, 2}},
		{name: "ifnull", count: 2, want: []int{0, 1}},
		{name: "nullif", count: 2, want: []int{0}},
		{name: "greatest", count: 2, want: []int{0, 1}},
		{name: "least", count: 2, want: []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := NumericFunctionResultArgs(tc.name, tc.count)
			require.True(t, ok)
			require.Equal(t, tc.want, got)
		})
	}

	for _, tc := range []struct {
		name  string
		count int
	}{
		{name: "+", count: 1},
		{name: "abs", count: 2},
		{name: "round", count: 3},
		{name: "truncate", count: 1},
		{name: "if", count: 2},
		{name: "case", count: 1},
		{name: "coalesce", count: 0},
		{name: "ifnull", count: 1},
		{name: "nullif", count: 1},
		{name: "sqrt", count: 1},
	} {
		_, ok := NumericFunctionResultArgs(tc.name, tc.count)
		require.False(t, ok, tc.name)
	}
}

func TestIsExactNumericExpression(t *testing.T) {
	integer := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}
	decimal := &plan.Expr{Typ: plan.Type{Id: int32(types.T_decimal64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}
	floating := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}
	null := &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Isnull: true}}}
	param := &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{Pos: 0}}}
	text := &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{}}}
	column := &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	makeFn := func(name string, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name}, Args: args,
		}}}
	}
	division := makeFn("/", integer, integer)
	folded := &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Src: division}}}
	implicit := makeFn("cast", division)
	explicit := makeFn("cast", integer)
	explicit.GetF().SyntaxExplicitCast = true
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{name: "nil"},
		{name: "parameter", expr: param},
		{name: "null", expr: null, want: true},
		{name: "integer", expr: integer, want: true},
		{name: "decimal", expr: decimal, want: true},
		{name: "text", expr: text},
		{name: "float", expr: floating},
		{name: "column without resolver", expr: column},
		{name: "division", expr: division, want: true},
		{name: "folded source", expr: folded, want: true},
		{name: "implicit float", expr: implicit, want: true},
		{name: "explicit float", expr: explicit},
		{name: "empty implicit cast", expr: makeFn("cast")},
		{name: "missing function", expr: &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_F{F: &plan.Function{}}}},
		{name: "coalesce", expr: makeFn("coalesce", division, integer), want: true},
		{name: "if", expr: makeFn("if", &plan.Expr{}, division, integer), want: true},
		{name: "nullif ignores comparison domain", expr: makeFn("nullif", division, floating), want: true},
		{name: "approximate branch", expr: makeFn("greatest", division, floating)},
		{name: "intrinsic approximate", expr: makeFn("sqrt", integer)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IsExactNumericExpression(tc.expr, nil))
		})
	}
	require.True(t, IsExactNumericExpression(column, func(*plan.Expr) bool { return true }))
}
