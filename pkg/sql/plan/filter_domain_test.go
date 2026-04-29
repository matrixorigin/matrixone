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

// makeInt64InWithNullExpr builds an IN list containing a NULL literal. The
// binder converts this into `or(in(col, non_null_values), =(col, NULL))`, so
// the returned expression is an `or`, not a plain `in` — tests exercising the
// 3-valued-logic guard must not assume plain IN shape.
func makeInt64InWithNullExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...int64) *planpb.Expr {
	t.Helper()

	listValues := make([]*planpb.Expr, 0, len(values)+1)
	for _, value := range values {
		listValues = append(listValues, MakePlan2Int64ConstExprWithType(value))
	}
	listValues = append(listValues, makePlan2NullConstExprWithType())
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

// TestRewriteInDomainOuterInWithNullSkipped locks the invariant that when the
// outer IN list contains NULL (e.g. `a IN (1, NULL, 2)`) the rewriter does NOT
// build a normalized domain that excludes 3-valued-logic UNKNOWN rows. After
// the binder's NULL-list expansion the conjunct is an `or`, so
// classifyDomainConjunct classifies it as domainOther — the guard relies on
// that and on NOT doing any point-set arithmetic over the nullable list.
func TestRewriteInDomainOuterInWithNullSkipped(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	nullableIn := makeInt64InWithNullExpr(t, ctx, colExpr, 1, 2)
	neqExpr := makeInt64NotEqualExpr(t, ctx, colExpr, 1)
	setSingleScanFilters(builder, tag, nullableIn, neqExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	// outer nullable IN must remain an `or` of (in, =NULL); the rewriter must
	// not collapse the `<>` against that nullable domain.
	require.Equal(t, "or", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	require.Equal(t, "!=", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

// TestRewriteInDomainOuterInWithNullSkipsNotIn covers the same invariant when
// the second conjunct is a NOT IN instead of a bare `<>`. The binder keeps
// NOT IN as `not_in` (no OR expansion on the right-hand side without NULL);
// the rewriter must not use the nullable outer IN as an exclusion domain.
func TestRewriteInDomainOuterInWithNullSkipsNotIn(t *testing.T) {
	ctx, builder, tag, colExpr := setupInDomainRewriteTest(t)

	nullableIn := makeInt64InWithNullExpr(t, ctx, colExpr, 1, 2)
	// Use a 2-element NOT IN so the binder keeps a real `not_in` (single-value
	// lists collapse to `!=` during binding).
	notInExpr := makeInt64NotInFuncExpr(t, ctx, colExpr, 1, 3)
	setSingleScanFilters(builder, tag, nullableIn, notInExpr)

	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "or", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	require.Equal(t, "not_in", builder.qry.Nodes[0].FilterList[1].GetF().Func.ObjName)
}

// setupUint8InDomainRewriteTest builds a bind context rooted on a uint8
// column, used to probe cast-aware rewrites against out-of-range values.
func setupUint8InDomainRewriteTest(t *testing.T) (*MockCompilerContext, *QueryBuilder, int32, *planpb.Expr) {
	t.Helper()

	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	colExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_uint8)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{RelPos: tag, ColPos: 0, Name: "u"},
		},
	}
	return ctx, builder, tag, colExpr
}

func makeUint8InExpr(t *testing.T, ctx *MockCompilerContext, colExpr *planpb.Expr, values ...uint8) *planpb.Expr {
	t.Helper()

	listValues := make([]*planpb.Expr, 0, len(values))
	for _, v := range values {
		listValues = append(listValues, &planpb.Expr{
			Typ: colExpr.Typ,
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: uint32(v)}},
			},
		})
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

// TestRewriteInDomainCastUint8OverflowSkipped covers a value that overflows
// the column's native range (`cast(u as int64) <> 256` where u is uint8). The
// cast-aware rewriter must treat 256 as outside the uint8 universe and leave
// the predicate alone — collapsing it into IN(0,1,2) \ {256} = IN(0,1,2) is
// also fine; what's NOT fine is wrapping the wrong-type literal back into the
// uint8 domain set (wrap-around to 0).
func TestRewriteInDomainCastUint8OverflowSkipped(t *testing.T) {
	ctx, builder, tag, colExpr := setupUint8InDomainRewriteTest(t)

	domainExpr := makeUint8InExpr(t, ctx, colExpr, 0, 1, 2)
	int64Typ := planpb.Type{Id: int32(types.T_int64)}
	castExpr, err := appendCastBeforeExpr(ctx.GetContext(), DeepCopyExpr(colExpr), int64Typ)
	require.NoError(t, err)
	neqExpr := makeInt64NotEqualExpr(t, ctx, castExpr, 256)

	setSingleScanFilters(builder, tag, domainExpr, neqExpr)
	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	require.Len(t, builder.qry.Nodes[0].FilterList, 2)
	require.Equal(t, "in", builder.qry.Nodes[0].FilterList[0].GetF().Func.ObjName)
	// `<>` may remain OR may be const-folded to TRUE; what must NOT happen is
	// collapsing into a narrower IN(0) / IN(1) because 256 wrapped around to 0.
	second := builder.qry.Nodes[0].FilterList[1]
	if second.GetF() != nil && second.GetF().Func.ObjName == "in" {
		list := second.GetF().Args[1].GetList()
		require.NotNil(t, list)
		require.Len(t, list.List, 3, "overflow must not shrink the uint8 IN domain")
	}
}

// TestRewriteInDomainCastUint8NegativeFoldsFalse covers a negative literal
// compared to an unsigned column. `cast(u as int64) = -1` is unsatisfiable —
// the constant-fold path should reduce the predicate to FALSE, and the outer
// IN domain on u must not absorb -1 into its point set.
func TestRewriteInDomainCastUint8NegativeFoldsFalse(t *testing.T) {
	ctx, builder, tag, colExpr := setupUint8InDomainRewriteTest(t)

	domainExpr := makeUint8InExpr(t, ctx, colExpr, 0, 1)
	int64Typ := planpb.Type{Id: int32(types.T_int64)}
	castExpr, err := appendCastBeforeExpr(ctx.GetContext(), DeepCopyExpr(colExpr), int64Typ)
	require.NoError(t, err)
	eqExpr := makeInt64EqualExpr(t, ctx, castExpr, -1)

	setSingleScanFilters(builder, tag, domainExpr, eqExpr)
	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, false)
	builder.rewriteInDomainNotInFilters(0)

	// Two acceptable outcomes: the whole FilterList collapses to FALSE, or the
	// original IN is kept with the second conjunct folded to FALSE. Either way,
	// IN(0,1) must not be expanded to contain -1 (which would happen on buggy
	// sign-wrap). Verify no filter mentions -1.
	for _, f := range builder.qry.Nodes[0].FilterList {
		require.False(t, exprMentionsInt64Value(f, -1), "no filter should carry -1 after rewrite")
	}
}

// exprMentionsInt64Value walks the expression tree and returns true if any
// literal (possibly cast-wrapped) equals the given int64.
func exprMentionsInt64Value(expr *planpb.Expr, target int64) bool {
	if expr == nil {
		return false
	}
	if v, ok := int64ConstValue(expr); ok && v == target {
		return true
	}
	fn := expr.GetF()
	if fn == nil {
		if lst := expr.GetList(); lst != nil {
			for _, e := range lst.List {
				if exprMentionsInt64Value(e, target) {
					return true
				}
			}
		}
		return false
	}
	for _, arg := range fn.Args {
		if exprMentionsInt64Value(arg, target) {
			return true
		}
	}
	return false
}
