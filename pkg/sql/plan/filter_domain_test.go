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
