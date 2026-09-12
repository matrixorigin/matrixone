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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestExactNumericDomainControls(t *testing.T) {
	integer := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}}
	floating := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}}
	makeFn := func(name string, args ...*plan.Expr) *plan.Expr {
		return &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: name}, Args: args,
		}}}
	}
	explicit := makeFn("cast", integer)
	explicit.GetF().SyntaxExplicitCast = true
	legacy := makeFn("cast", integer)
	legacy.GetF().Func.Obj = function.EncodeOverloadID(function.CAST, 1)
	for _, tc := range []struct {
		name string
		expr *plan.Expr
		want bool
	}{
		{"nil", nil, false},
		{"marker", &plan.Expr{Typ: integer.Typ, Expr: &plan.Expr_P{P: &plan.ParamRef{}}}, false},
		{"integer", integer, true},
		{"text", &plan.Expr{Typ: plan.Type{Id: int32(types.T_text)}}, false},
		{"float", floating, false},
		{"implicit_exact", makeFn("cast", integer), true},
		{"explicit_float", explicit, false},
		{"legacy_explicit_float", legacy, false},
		{"exact_division", makeFn("/", integer, integer), true},
		{"approximate_division", makeFn("/", floating, integer), false},
		{"wrapper", makeFn("abs", makeFn("/", integer, integer)), true},
		{"intrinsic_float", makeFn("sqrt", integer), false},
		{"malformed", makeFn("/"), false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.want, IsExactNumeric(tc.expr, nil)) })
	}
	col := &plan.Expr{Typ: floating.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{}}}
	require.True(t, IsExactNumeric(col, func(*plan.Expr) bool { return true }))
	require.False(t, IsExactNumeric(col, func(*plan.Expr) bool { return false }))
	MarkExactNumeric(floating)
	MarkExactNumeric(floating)
	require.True(t, IsExactNumeric(floating, nil))
	require.Equal(t, int32(-1), floating.GetPreparedNumeric().ParamPos)
	require.False(t, floating.GetPreparedNumeric().Fallback)
}
