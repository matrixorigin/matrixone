// Copyright 2026 Matrix Origin
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
	"context"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestFloatDomainKeyNormalizesSignedZero(t *testing.T) {
	operand := &domainFilterOperand{
		hasCast: true,
		castTyp: planpb.Type{Id: int32(types.T_float64)},
	}
	positiveKey, ok := constLiteralKeyForOperand(MakePlan2Float64ConstExprWithType(0), operand)
	require.True(t, ok)
	negativeKey, ok := constLiteralKeyForOperand(
		MakePlan2Float64ConstExprWithType(math.Copysign(0, -1)), operand)
	require.True(t, ok)
	require.Equal(t, positiveKey, negativeKey)
}

func TestUnwrapConstLiteralRecognizesExplicitCast(t *testing.T) {
	expr, err := appendCastBeforeExprWithName(
		context.Background(),
		MakePlan2Int64ConstExprWithType(42),
		planpb.Type{Id: int32(types.T_decimal128), Width: 20, Scale: 0},
		"cast_explicit",
	)
	require.NoError(t, err)

	lit, typ, ok := unwrapConstLiteral(expr)
	require.True(t, ok)
	require.NotNil(t, lit)
	require.Equal(t, int32(types.T_decimal128), typ.Id)
	require.Equal(t, int64(42), lit.GetI64Val())
}

func TestDoMergeFiltersOnCompositeKeyKeepsNonSortKeyRanges(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	cGt := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 2, "c"), 2)
	cLt := makeExprOptBinaryInt64Expr(t, ctx, "<", makeExprOptInt64Col(tag, 2, "c"), 4)

	ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, cGt, cLt)

	require.Len(t, ret, 3)
	requireFuncNames(t, ret, ">", "<")
	requireNoFuncNames(t, ret, "in_range", "between")
}

func TestDoMergeFiltersOnCompositeKeyMergesSortKeyRanges(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bGt := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 1, "b"), 2)
	bLt := makeExprOptBinaryInt64Expr(t, ctx, "<", makeExprOptInt64Col(tag, 1, "b"), 4)

	ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, bGt, bLt)

	requireFuncNames(t, ret, "in_range")
}

func TestCompositeKeyPushesCompoundAndPartBlockFilters(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bGt := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 1, "b"), 2)
	bLt := makeExprOptBinaryInt64Expr(t, ctx, "<", makeExprOptInt64Col(tag, 1, "b"), 4)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    tableDef,
		FilterList:  []*planpb.Expr{aEq, bGt, bLt},
	}}

	parts := builder.collectCompositePartBlockFilters(0)
	builder.qry.Nodes[0].FilterList = builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, bGt, bLt)
	builder.retainConsumedCompositePartBlockFilters(parts)
	foldTableScanFilters(builder.compCtx.GetProcess(), builder.qry, 0, true)
	builder.appendCompoundKeyBlockFilters(0)
	builder.appendCompoundKeyBlockFilters(0)
	builder.appendCompositePartBlockFilters(parts)

	filters := builder.qry.Nodes[0].BlockFilterList
	require.Len(t, filters, 4)
	requireFuncNames(t, filters, "in_range", "=", ">", "<")
	require.Equal(t, int32(3), filters[0].GetF().Args[0].GetCol().ColPos, "compound filter must target hidden PK")
	preserved := existingCompositeBlockFilters(builder.qry.Nodes[0])
	require.Len(t, preserved, 4)
	require.Same(t, filters[0], preserved[0], "stats passes must transfer owned block filters without deep copies")

	// Re-optimizing an AP/high-cost plan must not lose constituent filters when
	// stats rebuilds BlockFilterList from the already-compounded FilterList.
	parts = builder.collectCompositePartBlockFilters(0)
	builder.qry.Nodes[0].BlockFilterList = builder.qry.Nodes[0].BlockFilterList[:1]
	builder.retainConsumedCompositePartBlockFilters(parts)
	builder.appendCompositePartBlockFilters(parts)
	require.Len(t, builder.qry.Nodes[0].BlockFilterList, 4)
	builder.qry.Nodes[0].BlockFilterList = append(
		builder.qry.Nodes[0].BlockFilterList,
		DeepCopyExpr(builder.qry.Nodes[0].BlockFilterList[0]),
	)
	builder.deduplicateBlockFilters(0)
	require.Len(t, builder.qry.Nodes[0].BlockFilterList, 4)
}

func TestCompositeKeyPartBlockFiltersRespectDisableHint(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.optimizerHints = &OptimizerHints{blockFilter: 2}
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    tableDef,
		FilterList: []*planpb.Expr{
			makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1),
		},
	}}

	require.Empty(t, builder.collectCompositePartBlockFilters(0))
	builder.qry.Nodes[0].FilterList = builder.doMergeFiltersOnCompositeKey(
		tableDef, tag, builder.qry.Nodes[0].FilterList...)
	builder.appendCompoundKeyBlockFilters(0)
	require.Empty(t, builder.qry.Nodes[0].BlockFilterList)
}

func TestCompositeKeyPreservesPartWhenRewriteMutatesPredicateInPlace(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    tableDef,
		FilterList:  []*planpb.Expr{aEq},
	}}

	parts := builder.collectCompositePartBlockFilters(0)
	builder.mergeFiltersOnCompositeKey(0)
	builder.retainConsumedCompositePartBlockFilters(parts)
	builder.appendCompoundKeyBlockFilters(0)
	builder.appendCompositePartBlockFilters(parts)
	require.Len(t, builder.qry.Nodes[0].BlockFilterList, 2)
	requireFuncNames(t, builder.qry.Nodes[0].BlockFilterList, "prefix_eq", "=")
}

func TestCompositeKeyPartBlockFiltersDoNotRestoreUnchangedPredicates(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	filter := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 1, "b"), 2)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    makeExprOptCompositeSortKeyTableDef(),
		FilterList:  []*planpb.Expr{filter},
	}}

	candidates := builder.collectCompositePartBlockFilters(0)
	builder.retainConsumedCompositePartBlockFilters(candidates)
	require.Empty(t, candidates, "unchanged predicates must be left to later rewrites and stats")
}

func TestCompositeKeyPartBlockFiltersTrackCanonicalizedPredicateIdentity(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	filter := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 1, "b"), 2)
	filter.GetF().Args[0], filter.GetF().Args[1] = filter.GetF().Args[1], filter.GetF().Args[0]
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    makeExprOptCompositeSortKeyTableDef(),
		FilterList:  []*planpb.Expr{filter},
	}}

	candidates := builder.collectCompositePartBlockFilters(0)
	builder.mergeFiltersOnCompositeKey(0)
	builder.retainConsumedCompositePartBlockFilters(candidates)
	require.Empty(t, candidates, "an in-place canonicalized predicate was not consumed")
}

func TestDeduplicateBlockFiltersAcrossPlannerRepresentations(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	tag := int32(1)
	a := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 0, "a"), 1)
	b := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 0, "a"), 2)
	c := makeExprOptBinaryInt64Expr(t, ctx, ">", makeExprOptInt64Col(tag, 0, "a"), 3)
	leftOr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{a, b})
	require.NoError(t, err)
	nestedOr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{leftOr, c})
	require.NoError(t, err)
	flatOr := &planpb.Expr{
		Typ: nestedOr.Typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: nestedOr.GetF().Func,
			Args: []*planpb.Expr{DeepCopyExpr(a), DeepCopyExpr(b), DeepCopyExpr(c)},
		}},
	}

	listIn := makeExprOptInExpr(t, ctx, tag, 1, 11, 15, 110, 210)
	mp := mpool.MustNew(t.Name())
	vectorIn := DeepCopyExpr(listIn)
	vectorIn.GetF().Args[1] = MakePlan2Int64VecExprWithType(mp, 11, 15, 110, 210)
	differentIn := DeepCopyExpr(listIn)
	differentIn.GetF().Args[1] = MakePlan2Int64VecExprWithType(mp, 11, 15, 110, 211)

	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType: planpb.Node_TABLE_SCAN,
		BlockFilterList: []*planpb.Expr{
			nestedOr, flatOr,
			listIn, vectorIn, differentIn,
		},
	}}
	builder.deduplicateBlockFilters(0)

	require.Len(t, builder.qry.Nodes[0].BlockFilterList, 3)
	require.Same(t, nestedOr, builder.qry.Nodes[0].BlockFilterList[0])
	require.Same(t, listIn, builder.qry.Nodes[0].BlockFilterList[1])
	require.Same(t, differentIn, builder.qry.Nodes[0].BlockFilterList[2])
}

func TestDeduplicateBlockFiltersHandlesConstantLiteralVec(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	tag := int32(1)
	listIn := makeExprOptInExpr(t, ctx, tag, 1, 42, 42)
	mp := mpool.MustNew(t.Name())
	constantVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(42), 2, mp)
	require.NoError(t, err)
	defer constantVec.Free(mp)
	data, err := constantVec.MarshalBinary()
	require.NoError(t, err)
	constantIn := DeepCopyExpr(listIn)
	constantIn.GetF().Args[1] = &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  2,
			Data: data,
		}},
	}

	filters := deduplicateBlockFilterList([]*planpb.Expr{listIn, constantIn})
	require.Len(t, filters, 1)
	require.Same(t, listIn, filters[0])

	emptyVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(0), 0, mp)
	require.NoError(t, err)
	defer emptyVec.Free(mp)
	emptyData, err := emptyVec.MarshalBinary()
	require.NoError(t, err)
	emptySet, ok := blockFilterConstantSet(&planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  0,
			Data: emptyData,
		}},
	})
	require.True(t, ok)
	require.Empty(t, emptySet)
}

func TestDeduplicateBlockFiltersRetainsDifferentCompoundPredicates(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	tag := int32(1)
	tableDef := makeExprOptCompositeSortKeyTableDef()
	left := makeExprOptInExpr(t, ctx, tag, 3, 11, 15)
	right := makeExprOptInExpr(t, ctx, tag, 3, 11, 16)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:        planpb.Node_TABLE_SCAN,
		BindingTags:     []int32{tag},
		TableDef:        tableDef,
		BlockFilterList: []*planpb.Expr{left, right},
	}}

	builder.deduplicateBlockFilters(0)
	require.Equal(t, []*planpb.Expr{left, right}, builder.qry.Nodes[0].BlockFilterList)
}

func BenchmarkDeduplicateBlockFiltersLargeIN(b *testing.B) {
	const (
		filterCount = 128
		valueCount  = 512
	)
	filters := make([]*planpb.Expr, filterCount)
	mp := mpool.MustNew(b.Name())
	for filterIndex := range filters {
		vec := vector.NewVec(types.T_int64.ToType())
		for valueIndex := range valueCount {
			value := int64(valueIndex)
			if valueIndex == valueCount-1 {
				value += int64(filterIndex)
			}
			require.NoError(b, vector.AppendFixed(vec, value, false, mp))
		}
		data, err := vec.MarshalBinary()
		require.NoError(b, err)
		vec.Free(mp)
		filters[filterIndex] = &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "in"},
				Args: []*planpb.Expr{
					makeExprOptInt64Col(1, 0, "a"),
					{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: valueCount, Data: data}}},
				},
			}},
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result := deduplicateBlockFilterList(filters)
		if len(result) != filterCount {
			b.Fatalf("unexpected deduplicated filter count: %d", len(result))
		}
	}
}

func makeExprOptCompositeSortKeyTableDef() *planpb.TableDef {
	return &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "__mo_cpkey", Typ: planpb.Type{Id: int32(types.T_varchar)}},
		},
		Name2ColIndex: map[string]int32{
			"a":          0,
			"b":          1,
			"c":          2,
			"__mo_cpkey": 3,
		},
		Pkey: &planpb.PrimaryKeyDef{
			PkeyColName: "__mo_cpkey",
			Names:       []string{"a", "b"},
		},
	}
}

func makeExprOptInt64Col(tag, pos int32, name string) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{
				RelPos: tag,
				ColPos: pos,
				Name:   name,
			},
		},
	}
}

func makeExprOptBinaryInt64Expr(t *testing.T, ctx *MockCompilerContext, op string, col *planpb.Expr, val int64) *planpb.Expr {
	t.Helper()

	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), op, []*planpb.Expr{
		col,
		MakePlan2Int64ConstExprWithType(val),
	})
	require.NoError(t, err)
	return expr
}

func makeExprOptInExpr(t *testing.T, ctx *MockCompilerContext, tag, pos int32, values ...int64) *planpb.Expr {
	t.Helper()
	items := make([]*planpb.Expr, len(values))
	for i, value := range values {
		items[i] = MakePlan2Int64ConstExprWithType(value)
	}
	expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, pos, "b"),
		{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}},
		},
	})
	require.NoError(t, err)
	return expr
}

func requireFuncNames(t *testing.T, filters []*planpb.Expr, names ...string) {
	t.Helper()

	hits := make(map[string]bool, len(names))
	for _, name := range names {
		hits[name] = false
	}
	for _, expr := range filters {
		if fn := expr.GetF(); fn != nil {
			if _, ok := hits[fn.Func.ObjName]; ok {
				hits[fn.Func.ObjName] = true
			}
		}
	}
	for name, hit := range hits {
		require.Truef(t, hit, "expected function %s in filters", name)
	}
}

func requireNoFuncNames(t *testing.T, filters []*planpb.Expr, names ...string) {
	t.Helper()

	for _, expr := range filters {
		fn := expr.GetF()
		if fn == nil {
			continue
		}
		for _, name := range names {
			require.NotEqual(t, name, fn.Func.ObjName)
		}
	}
}
