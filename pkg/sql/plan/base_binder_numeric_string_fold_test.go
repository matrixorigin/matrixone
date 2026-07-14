// Copyright 2021 - 2026 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestIsPlainNumericLiteral(t *testing.T) {
	valid := []string{
		"1", "-1", "+1", "123", "1.1", "-1.5", ".5", "5.",
		"7e2", "7E2", "1.1e0", "1e-2", "1E+10", "9007199254740993",
		"18446744073709551616", "0",
	}
	for _, s := range valid {
		require.True(t, isPlainNumericLiteral(s), "%q should be numeric", s)
	}

	invalid := []string{
		"", "abc", "1.1abc", "0x1A", "1.2.3", "e2", ".e2", "1e",
		"1e+", "+", "-", ".", "1 1", "--1", "++1", "1..2",
	}
	for _, s := range invalid {
		require.False(t, isPlainNumericLiteral(s), "%q should not be numeric", s)
	}
}

func TestTryFoldNumericStringConst(t *testing.T) {
	ctx := context.TODO()
	intType := types.T_int64.ToType()
	decType := types.T_decimal64.ToType()

	strLit := func(s string) *plan.Expr {
		return makePlan2StringConstExprWithType(s)
	}

	t.Run("integer-valued strings fold to integer literals", func(t *testing.T) {
		cases := map[string]int64{
			"123":              123,
			"7e2":              700,
			"7E2":              700,
			" 42 ":             42,
			"-0":               0,
			"5.":               5,
			"9007199254740993": 9007199254740993, // above 2^53, must stay exact
		}
		for in, want := range cases {
			folded := tryFoldNumericStringConst(ctx, strLit(in), intType)
			require.NotNil(t, folded, "input %q", in)
			require.Equal(t, int32(types.T_int64), folded.Typ.Id, "input %q", in)
			require.Equal(t, want, folded.GetLit().GetI64Val(), "input %q", in)
		}
	})

	t.Run("uint64-range strings fold to unsigned literals", func(t *testing.T) {
		folded := tryFoldNumericStringConst(ctx, strLit("18446744073709551615"), intType)
		require.NotNil(t, folded)
		require.Equal(t, int32(types.T_uint64), folded.Typ.Id)
		require.Equal(t, uint64(18446744073709551615), folded.GetLit().GetU64Val())
	})

	t.Run("fractional strings fold to decimal with natural scale", func(t *testing.T) {
		cases := map[string]int32{
			"1.1":    1,
			"1.1e0":  1,
			"-1.5":   1,
			".5":     1,
			"0.10":   2,
			"1e-2":   2,
			" 1.9 ":  1,
			"1.1E+0": 1,
		}
		for in, wantScale := range cases {
			folded := tryFoldNumericStringConst(ctx, strLit(in), intType)
			require.NotNil(t, folded, "input %q", in)
			require.True(t, types.T(folded.Typ.Id).IsDecimal(), "input %q got %v", in, folded.Typ.Id)
			require.Equal(t, wantScale, folded.Typ.Scale, "input %q", in)
		}
	})

	t.Run("integers beyond uint64 fold to decimal", func(t *testing.T) {
		folded := tryFoldNumericStringConst(ctx, strLit("18446744073709551616"), intType)
		require.NotNil(t, folded)
		require.True(t, types.T(folded.Typ.Id).IsDecimal())
		require.Equal(t, int32(0), folded.Typ.Scale)
	})

	t.Run("folds against decimal operands too", func(t *testing.T) {
		folded := tryFoldNumericStringConst(ctx, strLit("1.1"), decType)
		require.NotNil(t, folded)
		require.True(t, types.T(folded.Typ.Id).IsDecimal())
	})

	t.Run("non-numeric strings do not fold", func(t *testing.T) {
		for _, s := range []string{"abc", "1.1abc", "0x1A", "", "1.2.3"} {
			require.Nil(t, tryFoldNumericStringConst(ctx, strLit(s), intType), "input %q", s)
		}
	})

	t.Run("only integer/decimal operands trigger folding", func(t *testing.T) {
		for _, other := range []types.Type{
			types.T_float32.ToType(),
			types.T_float64.ToType(),
			types.T_date.ToType(),
			types.T_datetime.ToType(),
			types.T_varchar.ToType(),
			types.T_bool.ToType(),
		} {
			require.Nil(t, tryFoldNumericStringConst(ctx, strLit("1.1"), other), "other %v", other)
		}
	})

	t.Run("non-literal and null expressions do not fold", func(t *testing.T) {
		colExpr := &plan.Expr{
			Typ:  makePlan2Type(&types.Type{Oid: types.T_varchar}),
			Expr: &plan.Expr_Col{Col: &plan.ColRef{}},
		}
		require.Nil(t, tryFoldNumericStringConst(ctx, colExpr, intType))

		nullExpr := makePlan2NullConstExprWithType()
		nullExpr.Typ.Id = int32(types.T_varchar)
		require.Nil(t, tryFoldNumericStringConst(ctx, nullExpr, intType))

		intLit := makePlan2Int64ConstExprWithType(1)
		require.Nil(t, tryFoldNumericStringConst(ctx, intLit, intType))
	})
}

// TestBindComparisonFoldsNumericString exercises the binder end to end:
// an integer column compared with a fractional string literal must
// compare against the exact decimal value, not an integer truncation
// (MySQL 8.0.28 semantics, Bug #101806).
func TestBindComparisonFoldsNumericString(t *testing.T) {
	ctx := context.TODO()
	col := &plan.Expr{
		Typ:  makePlan2Type(&types.Type{Oid: types.T_int64, Width: 64}),
		Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "a"}},
	}

	t.Run("fractional string compares as decimal", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			DeepCopyExpr(col),
			makePlan2StringConstExprWithType("1.1"),
		})
		require.NoError(t, err)
		require.Equal(t, "=", expr.GetF().GetFunc().GetObjName())
		for _, arg := range expr.GetF().GetArgs() {
			argT := types.T(arg.Typ.Id)
			require.True(t, argT.IsDecimal(), "arg type %v, want decimal", argT)
		}
	})

	t.Run("integer string stays on exact integer path", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			DeepCopyExpr(col),
			makePlan2StringConstExprWithType("9007199254740993"),
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().GetArgs() {
			require.Equal(t, int32(types.T_int64), arg.Typ.Id)
		}
	})

	t.Run("null-safe equality stays on exact integer path", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "<=>", []*plan.Expr{
			DeepCopyExpr(col),
			makePlan2StringConstExprWithType("9007199254740993"),
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().GetArgs() {
			require.Equal(t, int32(types.T_int64), arg.Typ.Id)
		}
	})

	t.Run("dynamic string column keeps exact mixed types", func(t *testing.T) {
		stringCol := &plan.Expr{
			Typ:  makePlan2Type(&types.Type{Oid: types.T_varchar}),
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "s"}},
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			stringCol,
			makePlan2Int64ConstExprWithType(9007199254740993),
		})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.GetF().GetArgs()[0].Typ.Id)
		require.Equal(t, int32(types.T_int64), expr.GetF().GetArgs()[1].Typ.Id)
	})

	checkMixedComparisons := func(t *testing.T, expr *plan.Expr) {
		var check func(*plan.Expr)
		check = func(current *plan.Expr) {
			fn := current.GetF()
			require.NotNil(t, fn)
			if fn.Func.ObjName == "=" || fn.Func.ObjName == ">=" || fn.Func.ObjName == "<=" {
				require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id)
				require.Equal(t, int32(types.T_int64), fn.Args[1].Typ.Id)
				return
			}
			require.Equal(t, "or", fn.Func.ObjName)
			for _, arg := range fn.Args {
				check(arg)
			}
		}
		check(expr)
	}

	t.Run("string column between uses mixed comparisons", func(t *testing.T) {
		stringCol := &plan.Expr{
			Typ:  makePlan2Type(&types.Type{Oid: types.T_varchar}),
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "s"}},
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*plan.Expr{
			stringCol,
			makePlan2Int64ConstExprWithType(9007199254740993),
			makePlan2Int64ConstExprWithType(9007199254740993),
		})
		require.NoError(t, err)
		fn := expr.GetF()
		require.Equal(t, "and", fn.Func.ObjName)
		for _, arg := range fn.Args {
			comparison := arg.GetF()
			require.Equal(t, int32(types.T_varchar), comparison.Args[0].Typ.Id)
			require.Equal(t, int32(types.T_int64), comparison.Args[1].Typ.Id)
		}
	})

	t.Run("string column in uses mixed comparisons", func(t *testing.T) {
		stringCol := &plan.Expr{
			Typ:  makePlan2Type(&types.Type{Oid: types.T_varchar}),
			Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "s"}},
		}
		values := &plan.Expr{
			Typ: makePlan2Type(&types.Type{Oid: types.T_int64}),
			Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
				makePlan2Int64ConstExprWithType(9007199254740993),
				makePlan2Int64ConstExprWithType(9007199254740994),
			}}},
		}
		expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*plan.Expr{stringCol, values})
		require.NoError(t, err)
		checkMixedComparisons(t, expr)
	})

	t.Run("between folds both bounds", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "between", []*plan.Expr{
			DeepCopyExpr(col),
			makePlan2StringConstExprWithType("1.1"),
			makePlan2StringConstExprWithType("1.9"),
		})
		require.NoError(t, err)
		f := expr.GetF()
		require.NotNil(t, f)
		switch f.GetFunc().GetObjName() {
		case "between":
			for _, arg := range f.GetArgs()[1:] {
				argT := types.T(arg.Typ.Id)
				require.True(t, argT.IsDecimal(), "bound type %v, want decimal", argT)
			}
		case "and":
			// Bound without a proc, between falls back to and(>=, <=);
			// each bound must still be the folded decimal constant.
			for _, cmp := range f.GetArgs() {
				cmpF := cmp.GetF()
				require.NotNil(t, cmpF)
				boundT := types.T(cmpF.GetArgs()[1].Typ.Id)
				require.True(t, boundT.IsDecimal(), "bound type %v, want decimal", boundT)
			}
		default:
			t.Fatalf("unexpected function %s", f.GetFunc().GetObjName())
		}
	})

	t.Run("two string literals keep string comparison", func(t *testing.T) {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
			makePlan2StringConstExprWithType("1.1"),
			makePlan2StringConstExprWithType("1.10"),
		})
		require.NoError(t, err)
		for _, arg := range expr.GetF().GetArgs() {
			require.True(t, types.T(arg.Typ.Id).IsMySQLString())
		}
	})
}

func TestBuildPlanKeepsMixedNumericStringComparison(t *testing.T) {
	mock := NewMockOptimizer(true)
	stmts, err := mysql.Parse(mock.CurrentContext().GetContext(),
		"select n_name from nation where n_name = 9007199254740993", 1)
	require.NoError(t, err)
	query, err := NewBaseOptimizer(mock.CurrentContext()).Optimize(stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, query)
	var filter *plan.Expr
	for _, node := range query.Nodes {
		if len(node.FilterList) > 0 {
			filter = node.FilterList[0]
			break
		}
	}
	require.NotNil(t, filter)
	args := filter.GetF().GetArgs()
	require.Equal(t, int32(types.T_varchar), args[0].Typ.Id)
	require.Equal(t, int32(types.T_int64), args[1].Typ.Id)
}
