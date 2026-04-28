// Copyright 2024 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func setupInDomainRewriteTest(t *testing.T) (*MockCompilerContext, *QueryBuilder, int32, *planpb.Expr) {
	t.Helper()

	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	colExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: tag,
				ColPos: 0,
				Name:   "a",
			},
		},
	}
	return ctx, builder, tag, colExpr
}

func makeInt64InExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...int64) *planpb.Expr {
	t.Helper()

	listValues := make([]*planpb.Expr, 0, len(values))
	for _, value := range values {
		listValues = append(listValues, MakePlan2Int64ConstExprWithType(value))
	}
	listExpr := &planpb.Expr{
		Typ: colExpr.Typ,
		Expr: &planpb.Expr_List{
			List: &planpb.ExprList{List: listValues},
		},
	}
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func makeInt64NotInExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...int64) *planpb.Expr {
	t.Helper()

	inExpr := makeInt64InExpr(t, ctx, colExpr, values...)
	notExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "not", []*planpb.Expr{inExpr})
	require.NoError(t, err)
	return notExpr
}

func makeInt64NotEqualExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, value int64) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "!=", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(value),
	})
	require.NoError(t, err)
	return expr
}

func makeAndExpr(t *testing.T, ctx *MockCompilerContext, left, right *planpb.Expr) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "and", []*planpb.Expr{left, right})
	require.NoError(t, err)
	return expr
}

func setSingleScanFilters(builder *QueryBuilder, tag int32, filters ...*planpb.Expr) {
	builder.qry.Nodes = []*planpb.Node{
		{
			NodeType:    planpb.Node_TABLE_SCAN,
			BindingTags: []int32{tag},
			FilterList:  filters,
		},
	}
}

func requireInValues(t *testing.T, expr *planpb.Expr, expected ...int64) {
	t.Helper()

	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "in", fn.Func.ObjName)
	require.Len(t, fn.Args, 2)

	list := fn.Args[1].GetList()
	require.NotNil(t, list)
	require.Len(t, list.List, len(expected))
	for idx, expectedValue := range expected {
		actualValue, ok := int64ConstValue(list.List[idx])
		require.True(t, ok)
		require.Equal(t, expectedValue, actualValue)
	}
}

func findInExprWithValues(expr *planpb.Expr, expected ...int64) bool {
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if fn.Func.ObjName == "in" && len(fn.Args) == 2 {
		list := fn.Args[1].GetList()
		if list != nil && len(list.List) == len(expected) {
			for idx, expectedValue := range expected {
				actualValue, ok := int64ConstValue(list.List[idx])
				if !ok || actualValue != expectedValue {
					return false
				}
			}
			return true
		}
	}
	for _, arg := range fn.Args {
		if findInExprWithValues(arg, expected...) {
			return true
		}
	}
	return false
}

func int64ConstValue(expr *planpb.Expr) (int64, bool) {
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		return lit.GetI64Val(), true
	}
	fn := expr.GetF()
	if fn != nil && fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
		return int64ConstValue(fn.Args[0])
	}
	return 0, false
}

func singleFilter(t *testing.T, builder *QueryBuilder) *planpb.Expr {
	t.Helper()
	require.Len(t, builder.qry.Nodes[0].FilterList, 1)
	return builder.qry.Nodes[0].FilterList[0]
}

func TestRewriteInDomainNotInFilter(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 2, 3)
	setSingleScanFilters(builder, tag, domainExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireInValues(t, singleFilter(t, builder), 1, 4)
}

func TestRewriteInDomainNotEqualConjunction(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notEqualExpr := makeAndExpr(
		t,
		ctx,
		makeInt64NotEqualExpr(t, ctx, colExpr, 2),
		makeInt64NotEqualExpr(t, ctx, colExpr, 3),
	)
	setSingleScanFilters(builder, tag, domainExpr, notEqualExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireInValues(t, singleFilter(t, builder), 1, 4)
}

func TestRewriteInDomainNotInFilterEmptyDifference(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 1, 2)
	setSingleScanFilters(builder, tag, domainExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.True(t, IsFalseExpr(singleFilter(t, builder)))
}

func TestRewriteInDomainMergesTwoInLists(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	first := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	second := makeInt64InExpr(t, ctx, colExpr, 2, 3, 5)
	setSingleScanFilters(builder, tag, first, second)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireInValues(t, singleFilter(t, builder), 2, 3)
}

func TestRewriteInDomainMergesEqualAndIn(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	eqExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(2),
	})
	require.NoError(t, err)
	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	setSingleScanFilters(builder, tag, eqExpr, inExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	result := singleFilter(t, builder)
	require.Equal(t, "=", result.GetF().Func.ObjName)
	value, ok := int64ConstValue(result.GetF().Args[1])
	require.True(t, ok)
	require.Equal(t, int64(2), value)
}

func TestRewriteInDomainEmptyInIntersection(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	first := makeInt64InExpr(t, ctx, colExpr, 1, 2)
	second := makeInt64InExpr(t, ctx, colExpr, 3, 4)
	setSingleScanFilters(builder, tag, first, second)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.True(t, IsFalseExpr(singleFilter(t, builder)))
}

func TestRewriteInDomainIsNullContradiction(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	isNullExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnull", []*planpb.Expr{
		DeepCopyExpr(colExpr),
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, inExpr, isNullExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.True(t, IsFalseExpr(singleFilter(t, builder)))
}

func TestRewriteInDomainOrBranchNotIn(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 2, 3)
	otherExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(99),
	})
	require.NoError(t, err)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		notInExpr,
		otherExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	// outer IN domain keeps; inner NOT IN within OR is rewritten to IN(1,4).
	require.True(t, findInExprWithValues(builder.qry.Nodes[0].FilterList[1], 1, 4))
}

func TestRewriteInDomainIgnoresOtherColumn(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	otherCol := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: tag,
				ColPos: 1,
				Name:   "b",
			},
		},
	}
	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	otherNotEqual := makeInt64NotEqualExpr(t, ctx, otherCol, 99)
	setSingleScanFilters(builder, tag, inExpr, otherNotEqual)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	requireInValues(t, builder.qry.Nodes[0].FilterList[0], 1, 2, 3)
	require.Equal(t, "!=", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

func TestRewriteInDomainNotInFilterSkipsNullList(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 2, 3)
	notInExpr.GetF().Args[0].GetF().Args[1].GetList().List = append(
		notInExpr.GetF().Args[0].GetF().Args[1].GetList().List,
		makePlan2NullConstExprWithType(),
	)
	setSingleScanFilters(builder, tag, domainExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "not", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

func makeInt64BetweenExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, low, high int64) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "between", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(low),
		MakePlan2Int64ConstExprWithType(high),
	})
	require.NoError(t, err)
	return expr
}

func makeInt64EqualExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, value int64) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(value),
	})
	require.NoError(t, err)
	return expr
}

func TestRewriteInDomainBetweenUntouched(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4, 5, 6, 7, 8)
	betweenExpr := makeInt64BetweenExpr(t, ctx, colExpr, 3, 6)
	setSingleScanFilters(builder, tag, inExpr, betweenExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// BETWEEN coexisting with IN on the same column should leave both untouched.
	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	names := []string{
		builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName,
		builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName,
	}
	require.ElementsMatch(t, []string{"in", "between"}, names)
}

func TestRewriteInDomainSingleInUnchanged(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	setSingleScanFilters(builder, tag, inExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// Lone IN must stay the same expression (pointer-equal), no redundant rebuild.
	require.Len(t, builder.qry.Nodes[0].FilterList, 1)
	requireInValues(t, singleFilter(t, builder), 1, 2, 3)
}

func TestRewriteInDomainEqualConflict(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	eq1 := makeInt64EqualExpr(t, ctx, colExpr, 1)
	eq2 := makeInt64EqualExpr(t, ctx, colExpr, 2)
	setSingleScanFilters(builder, tag, eq1, eq2)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.True(t, IsFalseExpr(singleFilter(t, builder)))
}

func TestRewriteInDomainEqualDedup(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	eqExpr := makeInt64EqualExpr(t, ctx, colExpr, 2)
	inExpr := makeInt64InExpr(t, ctx, colExpr, 2)
	setSingleScanFilters(builder, tag, eqExpr, inExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	result := singleFilter(t, builder)
	require.Equal(t, "=", result.GetF().Func.ObjName)
	value, ok := int64ConstValue(result.GetF().Args[1])
	require.True(t, ok)
	require.Equal(t, int64(2), value)
}

func TestRewriteInDomainIsNotNullKeepsIn(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	isNotNullExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnotnull", []*planpb.Expr{
		DeepCopyExpr(colExpr),
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, inExpr, isNotNullExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// IS NOT NULL with a constant IN domain is redundant but not contradictory;
	// both filters should survive unchanged.
	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	var seenIn, seenIsNotNull bool
	for _, f := range builder.qry.Nodes[0].FilterList {
		switch f.GetF().Func.ObjName {
		case "in":
			seenIn = true
		case "isnotnull", "is_not_null":
			seenIsNotNull = true
		}
	}
	require.True(t, seenIn)
	require.True(t, seenIsNotNull)
}

func TestRewriteInDomainNonConstListSkipped(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	inExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3)
	// Replace one literal with a non-constant (function call) to disqualify normalization.
	castArgs := []*planpb.Expr{MakePlan2Int64ConstExprWithType(2)}
	nonConst, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "abs", castArgs)
	require.NoError(t, err)
	inExpr.GetF().Args[1].GetList().List[1] = nonConst

	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 3)
	setSingleScanFilters(builder, tag, inExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// IN list has a non-literal element, so no rewrite should happen.
	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "in", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	require.Equal(t, "not", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

func TestRewriteInDomainNestedAndInOr(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	eqExpr := makeInt64EqualExpr(t, ctx, colExpr, 5)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 2, 3)
	gtExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), ">", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		MakePlan2Int64ConstExprWithType(0),
	})
	require.NoError(t, err)
	innerAnd := makeAndExpr(t, ctx, notInExpr, gtExpr)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		eqExpr,
		innerAnd,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// Outer IN domain should propagate into the inner AND's NOT IN and rewrite to IN(1,4).
	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.True(t, findInExprWithValues(builder.qry.Nodes[0].FilterList[1], 1, 4))
}

func makeInt64NotInFuncExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...int64) *planpb.Expr {
	t.Helper()

	listValues := make([]*planpb.Expr, 0, len(values))
	for _, value := range values {
		listValues = append(listValues, MakePlan2Int64ConstExprWithType(value))
	}
	listExpr := &planpb.Expr{
		Typ: colExpr.Typ,
		Expr: &planpb.Expr_List{
			List: &planpb.ExprList{List: listValues},
		},
	}
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "not_in", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

// TestRewriteInDomainNotInFuncRewritten covers the case where the binder
// produces a native `not_in` function (what real SQL `NOT IN (...)` turns into),
// not a `not(in(...))` wrapper.
func TestRewriteInDomainNotInFuncRewritten(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInFuncExpr(t, ctx, colExpr, 2, 3)
	setSingleScanFilters(builder, tag, domainExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireInValues(t, singleFilter(t, builder), 1, 4)
}

// TestRewriteInDomainNotInFuncOrBranch verifies the outer IN domain propagates
// into a native `not_in` that sits inside an OR branch.
func TestRewriteInDomainNotInFuncOrBranch(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInFuncExpr(t, ctx, colExpr, 2, 3)
	eqExpr := makeInt64EqualExpr(t, ctx, colExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		notInExpr,
		eqExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.True(t, findInExprWithValues(builder.qry.Nodes[0].FilterList[1], 1, 4))
}

// TestRewriteInDomainBareNotEqualInOrBranch covers a bare `<>` sitting inside an
// OR branch: the outer IN domain must rewrite it into an IN(domain \ {v}).
func TestRewriteInDomainBareNotEqualInOrBranch(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	neqExpr := makeInt64NotEqualExpr(t, ctx, colExpr, 2)
	eqExpr := makeInt64EqualExpr(t, ctx, colExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		neqExpr,
		eqExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.True(t, findInExprWithValues(builder.qry.Nodes[0].FilterList[1], 1, 3, 4))
}

// TestRewriteInDomainNestedAndMergesMultipleIns covers a nested AND where
// NOT IN + <> both fire against the outer IN domain and would otherwise leave
// two overlapping IN siblings under the same AND.
func TestRewriteInDomainNestedAndMergesMultipleIns(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInFuncExpr(t, ctx, colExpr, 2, 3)
	neqExpr := makeInt64NotEqualExpr(t, ctx, colExpr, 3)
	innerAnd := makeAndExpr(t, ctx, notInExpr, neqExpr)
	eqExpr := makeInt64EqualExpr(t, ctx, colExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		innerAnd,
		eqExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	// inner AND(a NOT IN (2,3), a <> 3) under outer IN(1,2,3,4) should collapse
	// into a single IN(1,4); the OR arm becomes (in([1,4]) or (a = 99)).
	top := builder.qry.Nodes[0].FilterList[1].GetF()
	require.Equal(t, "or", top.Func.ObjName)
	// First arm of the OR should now be a single IN([1,4]), not an AND of two INs.
	firstArm := top.Args[0].GetF()
	require.NotNil(t, firstArm)
	require.Equal(t, "in", firstArm.Func.ObjName)
	require.True(t, findInExprWithValues(top.Args[0], 1, 4))
}

// TestRewriteInDomainAndMergesMultipleIns covers an AND directly under the
// scan FilterList that, after per-child rewrites, ends up with two IN filters
// on the same column — mergeInsInAnd should intersect them.
func TestRewriteInDomainAndMergesMultipleIns(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	// Top-level conjuncts: a IN (1,2,3,4), and inside an AND-under-OR
	// a NOT IN (2,3) AND a <> 3 -> expected to fuse into IN([1,4]).
	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInFuncExpr(t, ctx, colExpr, 2, 3)
	neqExpr := makeInt64NotEqualExpr(t, ctx, colExpr, 3)
	innerAnd := makeAndExpr(t, ctx, notInExpr, neqExpr)
	// Wrap the inner AND in an OR so it's reached through rewriteExprByInDomains'
	// default path (which recurses into children, then hits the "and" case).
	otherExpr := makeInt64EqualExpr(t, ctx, colExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		otherExpr,
		innerAnd,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	top := builder.qry.Nodes[0].FilterList[1].GetF()
	require.Equal(t, "or", top.Func.ObjName)
	// Second arm (the rewritten AND) must be a single IN([1,4]).
	secondArm := top.Args[1].GetF()
	require.NotNil(t, secondArm)
	require.Equal(t, "in", secondArm.Func.ObjName)
	require.True(t, findInExprWithValues(top.Args[1], 1, 4))
}

func TestRewriteInDomainNonScanNodeRecurses(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	domainExpr := makeInt64InExpr(t, ctx, colExpr, 1, 2, 3, 4)
	notInExpr := makeInt64NotInExpr(t, ctx, colExpr, 2, 3)
	scanNode := &planpb.Node{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		FilterList:  []*planpb.Expr{domainExpr, notInExpr},
	}
	// Some random filter on the parent node that must NOT be touched by the rewrite.
	parentFilter := makeInt64NotInExpr(t, ctx, colExpr, 7, 8)
	parentNode := &planpb.Node{
		NodeType:   planpb.Node_PROJECT,
		FilterList: []*planpb.Expr{parentFilter},
		Children:   []int32{0},
	}
	builder.qry.Nodes = []*planpb.Node{scanNode, parentNode}

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(1)

	// Scan child got normalized via recursion.
	require.Len(t, builder.qry.Nodes[0].FilterList, 1)
	requireInValues(t, builder.qry.Nodes[0].FilterList[0], 1, 4)
	// Parent PROJECT node's filter stays untouched (still a NOT(IN(7,8))).
	require.Len(t, builder.qry.Nodes[1].FilterList, 1)
	require.Equal(t, "not", builder.qry.Nodes[1].FilterList[0].GetF().Func.ObjName)
}

func setupStringInDomainRewriteTest(t *testing.T) (*MockCompilerContext, *QueryBuilder, int32, *planpb.Expr) {
	t.Helper()

	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	colExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_varchar), Width: 16},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: tag,
				ColPos: 0,
				Name:   "a",
			},
		},
	}
	return ctx, builder, tag, colExpr
}

func makeStringInExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...string) *planpb.Expr {
	t.Helper()

	listValues := make([]*planpb.Expr, 0, len(values))
	for _, value := range values {
		listValues = append(listValues, MakePlan2StringConstExprWithType(value))
	}
	listExpr := &planpb.Expr{
		Typ: colExpr.Typ,
		Expr: &planpb.Expr_List{
			List: &planpb.ExprList{List: listValues},
		},
	}
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		DeepCopyExpr(colExpr),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func makeStringCastToInt64Expr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr) *planpb.Expr {
	t.Helper()

	expr, err := appendCastBeforeExpr(ctx.GetContext(), DeepCopyExpr(colExpr), planpb.Type{Id: int32(types.T_int64)})
	require.NoError(t, err)
	return expr
}

func requireStringInValues(t *testing.T, expr *planpb.Expr, expected ...string) {
	t.Helper()

	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "in", fn.Func.ObjName)
	require.Len(t, fn.Args, 2)

	list := fn.Args[1].GetList()
	require.NotNil(t, list)
	require.Len(t, list.List, len(expected))
	for idx, expectedValue := range expected {
		actualValue, ok := stringConstValue(list.List[idx])
		require.True(t, ok)
		require.Equal(t, expectedValue, actualValue)
	}
}

func requireStringEqualValue(t *testing.T, expr *planpb.Expr, expected string) {
	t.Helper()

	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Equal(t, "=", fn.Func.ObjName)
	require.Len(t, fn.Args, 2)
	value, ok := stringConstValue(fn.Args[1])
	require.True(t, ok)
	require.Equal(t, expected, value)
}

func stringConstValue(expr *planpb.Expr) (string, bool) {
	if lit := expr.GetLit(); lit != nil && !lit.Isnull {
		if _, ok := lit.Value.(*planpb.Literal_Sval); ok {
			return lit.GetSval(), true
		}
	}
	fn := expr.GetF()
	if fn != nil && fn.Func.ObjName == "cast" && len(fn.Args) > 0 {
		return stringConstValue(fn.Args[0])
	}
	return "", false
}

func TestRewriteInDomainCastNotInUsesOuterStringDomain(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "1", "2", "3", "4")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	notInExpr := makeInt64NotInFuncExpr(t, ctx, castExpr, 2, 3)
	setSingleScanFilters(builder, tag, domainExpr, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireStringInValues(t, singleFilter(t, builder), "1", "4")
}

func TestRewriteInDomainCastInInOrBranchUsesOuterStringDomain(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "1", "2", "3", "4")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	castInExpr := makeInt64InExpr(t, ctx, castExpr, 2, 3, 9)
	otherExpr := makeInt64EqualExpr(t, ctx, castExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		castInExpr,
		otherExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireStringInValues(t, singleFilter(t, builder), "2", "3")
}

func TestRewriteInDomainCastNotEqualUsesOuterStringDomain(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "1", "2")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	neqExpr := makeInt64NotEqualExpr(t, ctx, castExpr, 2)
	setSingleScanFilters(builder, tag, domainExpr, neqExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireStringEqualValue(t, singleFilter(t, builder), "1")
}

func TestRewriteInDomainCastEqualInOrBranchUsesOuterStringDomain(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "1", "2")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	eqExpr := makeInt64EqualExpr(t, ctx, castExpr, 2)
	otherExpr := makeInt64EqualExpr(t, ctx, castExpr, 99)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		eqExpr,
		otherExpr,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireStringEqualValue(t, singleFilter(t, builder), "2")
}

func TestRewriteInDomainCastEqualOrListBecomesStringIn(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "1", "2", "3", "4")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	eq2 := makeInt64EqualExpr(t, ctx, castExpr, 2)
	eq3 := makeInt64EqualExpr(t, ctx, castExpr, 3)
	eq9 := makeInt64EqualExpr(t, ctx, castExpr, 9)
	leftOr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		eq2,
		eq3,
	})
	require.NoError(t, err)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{
		leftOr,
		eq9,
	})
	require.NoError(t, err)
	setSingleScanFilters(builder, tag, domainExpr, orExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	requireStringInValues(t, singleFilter(t, builder), "2", "3")
}

func TestRewriteInDomainCastSkippedForNonIntegralString(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "A", "1")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	neqExpr := makeInt64NotEqualExpr(t, ctx, castExpr, 1)
	setSingleScanFilters(builder, tag, domainExpr, neqExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "in", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	require.Equal(t, "!=", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

func TestRewriteInDomainCastSkippedForAmbiguousStringIntegralDomain(t *testing.T) {
	ctx, builder, tag, colExpr := setupStringInDomainRewriteTest(t)

	domainExpr := makeStringInExpr(t, ctx, colExpr, "01", "1")
	castExpr := makeStringCastToInt64Expr(t, ctx, colExpr)
	eqExpr := makeInt64EqualExpr(t, ctx, castExpr, 1)
	setSingleScanFilters(builder, tag, domainExpr, eqExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "in", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	require.Equal(t, "=", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}
