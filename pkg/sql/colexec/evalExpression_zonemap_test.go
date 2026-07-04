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

package colexec_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestEvaluateFilterByZoneMapNullableInListIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "keep", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapStillPrunesFalseEquality(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapAndKeepsKnownFalsePruning(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	falseEq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)

	nullExpr := plan2.MakePlan2StringConstExprWithType("")
	nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
	nullEq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		nullExpr,
	})
	require.NoError(t, err)

	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{falseEq, nullEq})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapAndKeepsPossibleTrueBoolRange(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	eq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExprAt(1),
		plan2.MakePlan2StringConstExprWithType("keep"),
	})
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{makeBoolColExpr(), eq})
	require.NoError(t, err)

	meta := makeBoolAndVarcharBlockMeta(false, true, "key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0, 1: 1}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullableInListWithoutMatchPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "zzz", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullableNotInListPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharNotInExpr(t, ctx, "zzz", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNotInExpandedWithBareNullPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	neqValue, err := plan2.BindFuncExprImplByPlanExpr(ctx, "!=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)
	neqNull, err := plan2.BindFuncExprImplByPlanExpr(ctx, "!=", []*plan.Expr{
		makeVarcharColExpr(),
		makeBareNullExpr(),
	})
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{neqValue, neqNull})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNotEqualBareNullPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	for _, op := range []string{"!=", "<>"} {
		t.Run(op, func(t *testing.T) {
			expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
				makeVarcharColExpr(),
				makeBareNullExpr(),
			})
			require.NoError(t, err)

			meta := makeVarcharBlockMeta("key", "keep")
			zms, vecs := makeZoneMapEvalScratch(expr)

			selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
			require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
		})
	}
}

func TestEvaluateFilterByZoneMapUnknownResultIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharColExpr()
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullComparisonsPrune(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	for _, op := range []string{">", "<", ">=", "<=", "=", "!=", "<>"} {
		t.Run(op, func(t *testing.T) {
			expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
				makeVarcharColExpr(),
				makeBareNullExpr(),
			})
			require.NoError(t, err)

			meta := makeVarcharBlockMeta("key", "keep")
			zms, vecs := makeZoneMapEvalScratch(expr)

			selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
			require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
		})
	}

	t.Run("between", func(t *testing.T) {
		expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "between", []*plan.Expr{
			makeVarcharColExpr(),
			makeBareNullExpr(),
			plan2.MakePlan2StringConstExprWithType("zzz"),
		})
		require.NoError(t, err)

		meta := makeVarcharBlockMeta("key", "keep")
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})
}

func TestEvaluateFilterByZoneMapInListWithUnknownMemberIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: []*plan.Expr{
				// Unknown no-column item covers the conservative native IN fallback.
				{Typ: makeVarcharColExpr().Typ},
				makeBareNullExpr(),
			}},
		},
	}
	expr := makeNativeVarcharInExpr(t, ctx, []*plan.Expr{
		makeVarcharColExprAt(0),
		listExpr,
	})

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapInListWithUninitializedLHSIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "zzz", true)
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func makeVarcharInExpr(t *testing.T, ctx context.Context, value string, withNull bool) *plan.Expr {
	t.Helper()

	listValues := []*plan.Expr{plan2.MakePlan2StringConstExprWithType(value)}
	if withNull {
		nullExpr := plan2.MakePlan2StringConstExprWithType("")
		nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
		listValues = append(listValues, nullExpr)
	}

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: listValues},
		},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "in", []*plan.Expr{
		makeVarcharColExpr(),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func makeNativeVarcharInExpr(t *testing.T, ctx context.Context, args []*plan.Expr) *plan.Expr {
	t.Helper()

	varcharType := types.T_varchar.ToType()
	fGet, err := function.GetFunctionByName(ctx, "in", []types.Type{varcharType, varcharType})
	require.NoError(t, err)
	returnType := fGet.GetReturnType()
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(returnType.Oid),
			Width: returnType.Width,
			Scale: returnType.Scale,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     fGet.GetEncodedOverloadID(),
					ObjName: "in",
				},
				Args: args,
			},
		},
	}
}

func makeVarcharNotInExpr(t *testing.T, ctx context.Context, value string, withNull bool) *plan.Expr {
	t.Helper()

	listValues := []*plan.Expr{plan2.MakePlan2StringConstExprWithType(value)}
	if withNull {
		nullExpr := plan2.MakePlan2StringConstExprWithType("")
		nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
		listValues = append(listValues, nullExpr)
	}

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: listValues},
		},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "not_in", []*plan.Expr{
		makeVarcharColExpr(),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func makeVarcharColExpr() *plan.Expr {
	return makeVarcharColExprAt(0)
}

func makeVarcharColExprAt(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar), Width: 16},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: pos, Name: "k"},
		},
	}
}

func makeBoolColExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "flag"},
		},
	}
}

func makeBareNullExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_any)},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Isnull: true},
		},
	}
}

func makeVarcharBlockMeta(values ...string) objectio.BlockObject {
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)

	zm := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(zm, []byte(value))
	}
	meta.MustGetColumn(0).SetZoneMap(zm)
	return meta
}

func makeBoolAndVarcharBlockMeta(minBool, maxBool bool, values ...string) objectio.BlockObject {
	dataMeta := objectio.BuildMetaData(1, 2)
	meta := dataMeta.GetBlockMeta(0)

	boolZM := index.NewZM(types.T_bool, 0)
	index.UpdateZM(boolZM, types.EncodeBool(&minBool))
	index.UpdateZM(boolZM, types.EncodeBool(&maxBool))
	meta.MustGetColumn(0).SetZoneMap(boolZM)

	varcharZM := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(varcharZM, []byte(value))
	}
	meta.MustGetColumn(1).SetZoneMap(varcharZM)
	return meta
}

func makeZoneMapEvalScratch(expr *plan.Expr) ([]objectio.ZoneMap, []*vector.Vector) {
	need := plan2.AssignAuxIdForExpr(expr, 0)
	return make([]objectio.ZoneMap, need), make([]*vector.Vector, need)
}
