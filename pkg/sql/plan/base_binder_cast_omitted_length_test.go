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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// bindFirstSelectExpr parses sql as a plain select and binds its first
// projection expression with a default binder.
func bindFirstSelectExpr(t *testing.T, sql string) *Expr {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	sel := stmt.(*tree.Select)
	clause, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, "expected a plain select clause in %q", sql)
	binder := NewDefaultBinder(t.Context(), nil, nil, Type{}, nil)
	bound, err := binder.BindExpr(clause.Exprs[0].Expr, 0, false)
	require.NoError(t, err)
	return bound
}

// castBoundTarget ensures the bound expression is a cast whose target
// argument carries the expected target oid/width pair, returning the cast
// source expression.
func castBoundTarget(t *testing.T, bound *Expr, wantOid types.T, wantWidth int32) *Expr {
	t.Helper()
	fn := bound.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "cast", fn.Func.GetObjName())
	require.Len(t, fn.Args, 2)
	target := fn.Args[1]
	require.Equal(t, int32(wantOid), target.Typ.Id)
	require.Equal(t, wantWidth, target.Typ.Width)
	return fn.Args[0]
}

// TestCastOmittedCharLengthBindsUnboundedTarget verifies that the expression
// CAST(<string expr> AS CHAR) with omitted length binds to the unbounded
// TEXT target instead of VARCHAR(65535). This is what prevents the cast from
// truncating >64 KiB strings: expression results such as CONCAT can carry
// content longer than the declared width of their vector type.
func TestCastOmittedCharLengthBindsUnboundedTarget(t *testing.T) {
	for _, sql := range []string{
		"select cast(concat(repeat('a', 50000), repeat('b', 50000)) as char)",
		"select convert(concat(repeat('a', 50000), repeat('b', 50000)), char)",
		"select concat(repeat('a', 50000), repeat('b', 50000))::char",
	} {
		bound := bindFirstSelectExpr(t, sql)
		source := castBoundTarget(t, bound, types.T_text, 0)
		require.Equal(t, int32(types.T_text), bound.Typ.Id, sql)
		require.Equal(t, int32(types.T_varchar), source.Typ.Id, sql)
	}
}

// TestCastOmittedCharLengthBindsUnboundedTargetJSON covers the JSON source
// side of the same contract: a JSON value cast to an omitted-length CHAR
// keeps the serialized document intact because the target is unbounded.
func TestCastOmittedCharLengthBindsUnboundedTargetJSON(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(cast('{\"s\": \"a\"}' as json) as char)")
	source := castBoundTarget(t, bound, types.T_text, 0)
	require.Equal(t, int32(types.T_text), bound.Typ.Id)
	require.Equal(t, int32(types.T_json), source.Typ.Id)
}

// TestCastExplicitCharLengthKeepsBoundedTarget verifies that explicit widths
// keep their bounded cast targets, so over-length values truncate exactly as
// before for CAST(expr AS CHAR(N)) and CONVERT(expr, CHAR(N)).
func TestCastExplicitCharLengthKeepsBoundedTarget(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(repeat('a', 100) as char(3))")
	castBoundTarget(t, bound, types.T_char, 3)
	require.Equal(t, int32(types.T_char), bound.Typ.Id)

	convertBound := bindFirstSelectExpr(t, "select convert(repeat('a', 100), char(3))")
	castBoundTarget(t, convertBound, types.T_char, 3)
	require.Equal(t, int32(types.T_char), convertBound.Typ.Id)
}

// TestCastOmittedVarcharLengthKeepsBoundedTarget pins the scope of the
// change: an omitted-length VARCHAR cast keeps the VARCHAR(65535) default.
func TestCastOmittedVarcharLengthKeepsBoundedTarget(t *testing.T) {
	bound := bindFirstSelectExpr(t, "select cast(repeat('a', 100) as varchar)")
	castBoundTarget(t, bound, types.T_varchar, types.MaxVarcharLen)
	require.Equal(t, int32(types.T_varchar), bound.Typ.Id)
}

// TestCanonicalStringLiteralCastAcceptsTextTarget keeps the NAME_CONST
// canonical hex-literal cast whitelist aligned with the omitted-length CHAR
// binding, which now plans a TEXT target instead of VARCHAR.
func TestCanonicalStringLiteralCastAcceptsTextTarget(t *testing.T) {
	makeCast := func(targetOid types.T) *plan.Expr {
		return &plan.Expr{
			Typ: plan.Type{Id: int32(targetOid)},
			Expr: &plan.Expr_F{F: &plan.Function{
				Func: &plan.ObjectRef{ObjName: "cast"},
				Args: []*plan.Expr{
					{Typ: plan.Type{}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{IsBin: true}}},
					{Typ: plan.Type{Id: int32(targetOid)}, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
				},
			}},
		}
	}
	require.True(t, isCanonicalStringLiteralCast(makeCast(types.T_text)))
	require.True(t, isCanonicalStringLiteralCast(makeCast(types.T_varchar)))
	require.False(t, isCanonicalStringLiteralCast(makeCast(types.T_char)))
}
