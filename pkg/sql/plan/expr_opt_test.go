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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/stretchr/testify/require"
)

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

func TestDoMergeFiltersOnCompositeKeySupportsFoldedInVector(t *testing.T) {
	testCases := []struct {
		name            string
		tableDef        *planpb.TableDef
		expectedFunc    string
		expectedSortCol int32
	}{
		{
			name:            "full composite primary key",
			tableDef:        makeExprOptCompositeSortKeyTableDef(),
			expectedFunc:    "in",
			expectedSortCol: 3,
		},
		{
			name:            "prefix composite primary key",
			tableDef:        makeExprOptThreePartCompositeSortKeyTableDef(),
			expectedFunc:    "prefix_in",
			expectedSortCol: 3,
		},
		{
			name:            "composite cluster key",
			tableDef:        makeExprOptCompositeClusterKeyTableDef(),
			expectedFunc:    "in",
			expectedSortCol: 4,
		},
	}

	for _, rhsKind := range []string{"list", "folded vector"} {
		for _, tc := range testCases {
			t.Run(rhsKind+"/"+tc.name, func(t *testing.T) {
				ctx := NewMockCompilerContext(true)
				builder := NewQueryBuilder(planpb.Query_SELECT, ctx, rhsKind == "folded vector", false)
				tag := builder.genNewBindTag()
				aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
				bIn := makeExprOptInExpr(t, ctx, tag, 1, 2, 3)

				var originalData []byte
				if rhsKind == "folded vector" {
					bIn.GetF().Args[1] = MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp(), 2, 3)
					originalData = append(originalData, bIn.GetF().Args[1].GetVec().Data...)
				}

				ret := builder.doMergeFiltersOnCompositeKey(tc.tableDef, tag, aEq, bIn)
				require.Len(t, ret, 1)
				requireCompositeKeyInFilter(t, ret[0], tc.expectedFunc, tc.expectedSortCol, 2, false)
				if rhsKind == "folded vector" {
					require.Equal(t, originalData, bIn.GetF().Args[1].GetVec().Data)
				}

				// Re-optimization may canonicalize full-key ORs back to IN, but it
				// must not introduce another compound predicate or component filter.
				again := builder.doMergeFiltersOnCompositeKey(tc.tableDef, tag, ret...)
				require.Len(t, again, 1)
				requireCompositeKeyInFilter(t, again[0], tc.expectedFunc, tc.expectedSortCol, 2, false)
			})
		}
	}
}

func TestDoMergeFiltersOnCompositeKeyConsumesConstantFoldedInVector(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bIn := makeExprOptInExpr(t, ctx, tag, 1, 2, 3)

	folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bIn, ctx.GetProcess(), false, true)
	require.NoError(t, err)
	require.NotNil(t, folded.GetF().Args[1].GetVec())

	ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, aEq, folded)
	require.Len(t, ret, 1)
	requireCompositeKeyInFilter(t, ret[0], "in", 3, 2, false)
}

func TestDoMergeFiltersOnCompositeKeyPreservesNullableFoldedInVector(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		makeExprOptNullableInt64Vec(t, ctx.GetProcess().Mp(), 2),
	})
	require.NoError(t, err)

	ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, aEq, bIn)
	require.Len(t, ret, 1)
	requireCompositeKeyInFilter(t, ret[0], "in", 3, 2, true)
	require.True(t, exprContainsNullLiteral(ret[0]))
}

func TestDoMergeFiltersOnCompositeKeySupportsFoldedStringInVector(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	tableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bCol := &planpb.Expr{
		Typ: tableDef.Cols[1].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: tag,
			ColPos: 1,
			Name:   "b",
		}},
	}
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		bCol,
		makeExprOptStringVec(t, ctx.GetProcess().Mp(), "one", "two"),
	})
	require.NoError(t, err)

	ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, bIn)
	require.Len(t, ret, 1)
	requireCompositeKeyInFilter(t, ret[0], "in", 3, 2, false)
}

func TestDoMergeFiltersOnCompositeKeyHandlesConstAndEmptyFoldedInVectors(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()

	constVec, err := vector.NewConstFixed(types.T_int64.ToType(), int64(2), 4, ctx.GetProcess().Mp())
	require.NoError(t, err)
	constData, err := constVec.MarshalBinary()
	require.NoError(t, err)
	constVec.Free(ctx.GetProcess().Mp())
	constIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 4, Data: constData}}},
	})
	require.NoError(t, err)
	constRet := builder.doMergeFiltersOnCompositeKey(
		tableDef,
		tag,
		makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1),
		constIn,
	)
	require.Len(t, constRet, 1)
	requireCompositeKeyInFilter(t, constRet[0], "in", 3, 1, false)

	emptyIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp()),
	})
	require.NoError(t, err)
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	emptyRet := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, emptyIn)
	require.Equal(t, []*planpb.Expr{aEq, emptyIn}, emptyRet)
}

func TestCompositeKeyFoldedInVectorRetainsPartBlockFilters(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp(), 2, 3),
	})
	require.NoError(t, err)
	builder.qry.Nodes = []*planpb.Node{{
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{tag},
		TableDef:    tableDef,
		FilterList:  []*planpb.Expr{aEq, bIn},
	}}

	parts := builder.collectCompositePartBlockFilters(0)
	builder.qry.Nodes[0].FilterList = builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, bIn)
	builder.retainConsumedCompositePartBlockFilters(parts)
	foldTableScanFilters(ctx.GetProcess(), builder.qry, 0, true)
	builder.appendCompoundKeyBlockFilters(0)
	builder.appendCompositePartBlockFilters(parts)

	filters := builder.qry.Nodes[0].BlockFilterList
	require.Len(t, filters, 3)
	require.True(t, hasTopLevelFilterOnColumn(filters, "in", 3))
	require.True(t, hasTopLevelFilterOnColumn(filters, "=", 0))
	require.True(t, hasTopLevelFilterOnColumn(filters, "in", 1))
}

func TestDoMergeFiltersOnCompositeKeyMergesFoldedInVectorInsideOr(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp(), 1, 2),
	})
	require.NoError(t, err)
	bEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 1, "b"), 3)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{bIn, bEq})
	require.NoError(t, err)

	ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, orExpr)
	require.Len(t, ret, 1)
	require.Equal(t, "in", ret[0].GetF().Func.ObjName)
	require.Len(t, ret[0].GetF().Args[1].GetList().List, 3)
}

func TestDoMergeFiltersOnCompositeKeyRejectsMalformedFoldedInVector(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 1}}},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, aEq, bIn)
		require.Equal(t, []*planpb.Expr{aEq, bIn}, ret)
	})

	bEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 1, "b"), 3)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{bIn, bEq})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, orExpr)
		require.Len(t, ret, 1)
		require.Equal(t, "or", ret[0].GetF().Func.ObjName)
		require.Equal(t, []*planpb.Expr{bIn, bEq}, ret[0].GetF().Args)
	})
}

func TestDoMergeFiltersOnCompositeKeyRejectsFoldedInVectorTypeMismatch(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	aEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1)
	bIn := makeExprOptInExpr(t, ctx, tag, 1, 2, 3)

	// Keep the plan metadata as int64 while corrupting the encoded vector type.
	// A compound rewrite would serialize varchar values and no longer match the
	// int64 values stored in the hidden composite key.
	rhs := MakePlan2StringVecExprWithType(ctx.GetProcess().Mp(), "2", "3")
	rhs.Typ = planpb.Type{Id: int32(types.T_int64)}
	bIn.GetF().Args[1] = rhs

	require.NotPanics(t, func() {
		ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, aEq, bIn)
		require.Equal(t, []*planpb.Expr{aEq, bIn}, ret)
	})

	bEq := makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 1, "b"), 4)
	orExpr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "or", []*planpb.Expr{bIn, bEq})
	require.NoError(t, err)
	require.NotPanics(t, func() {
		ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, orExpr)
		require.Len(t, ret, 1)
		require.Equal(t, "or", ret[0].GetF().Func.ObjName)
		require.Equal(t, []*planpb.Expr{bIn, bEq}, ret[0].GetF().Args)
	})
}

func TestDoMergeFiltersOnCompositeKeySupportsScaleSensitiveFoldedInVector(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, true, false)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	decimalType := types.New(types.T_decimal64, 10, 2)
	tableDef.Cols[1].Typ = makePlan2Type(&decimalType)

	values := []types.Decimal64{250, 375}
	vec := vector.NewVec(decimalType)
	for _, value := range values {
		require.NoError(t, vector.AppendFixed(vec, value, false, ctx.GetProcess().Mp()))
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(ctx.GetProcess().Mp())

	bCol := &planpb.Expr{
		Typ: tableDef.Cols[1].Typ,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
			RelPos: tag,
			ColPos: 1,
			Name:   "b",
		}},
	}
	bIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		bCol,
		{Typ: makePlan2Type(&decimalType), Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{Len: 2, Data: data}}},
	})
	require.NoError(t, err)

	ret := builder.doMergeFiltersOnCompositeKey(
		tableDef,
		tag,
		makeExprOptBinaryInt64Expr(t, ctx, "=", makeExprOptInt64Col(tag, 0, "a"), 1),
		bIn,
	)
	require.Len(t, ret, 1)
	requireCompositeKeyInFilter(t, ret[0], "in", 3, len(values), false)
}

func TestDoMergeFiltersOnCompositeKeyRetainsMalformedPredicate(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	bCol := makeExprOptInt64Col(tag, 1, "b")
	constant := MakePlan2Int64ConstExprWithType(2)

	for _, tc := range []struct {
		name     string
		funcName string
		args     []*planpb.Expr
	}{
		{name: "in", funcName: "in", args: []*planpb.Expr{bCol}},
		{name: "between", funcName: "between", args: []*planpb.Expr{bCol, constant}},
		{name: "in_range", funcName: "in_range", args: []*planpb.Expr{bCol, constant, constant}},
		{name: "range", funcName: ">", args: []*planpb.Expr{bCol}},
		{name: "range-nil-left", funcName: ">", args: []*planpb.Expr{nil, constant}},
		{name: "range-nil-right", funcName: ">", args: []*planpb.Expr{bCol, nil}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			predicate := &planpb.Expr{
				Typ: planpb.Type{Id: int32(types.T_bool)},
				Expr: &planpb.Expr_F{F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: tc.funcName},
					Args: tc.args,
				}},
			}
			require.NotPanics(t, func() {
				ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, predicate)
				require.Equal(t, []*planpb.Expr{predicate}, ret)
			})
		})
	}
}

func TestDoMergeFiltersOnCompositeKeyRetainsUnaryNonMergeableOr(t *testing.T) {
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	tag := builder.genNewBindTag()
	arm := MakePlan2BoolConstExprWithType(true)
	orExpr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: "or"},
			Args: []*planpb.Expr{arm},
		}},
	}

	require.NotPanics(t, func() {
		ret := builder.doMergeFiltersOnCompositeKey(makeExprOptCompositeSortKeyTableDef(), tag, orExpr)
		require.Equal(t, []*planpb.Expr{arm}, ret)
	})
}

func TestInRHSValuesMaterializesFoldedVectorValues(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	expr := MakePlan2StringVecExprWithType(mp, "safe value")
	originalData := append([]byte(nil), expr.GetVec().Data...)

	values, ok := inRHSValues(expr, expr.Typ)
	require.True(t, ok)
	require.Len(t, values, 1)
	require.Equal(t, "safe value", values[0].GetLit().GetSval())
	require.True(t, values[0].Typ.NotNullable)
	require.Equal(t, originalData, expr.GetVec().Data)
	for i := range expr.GetVec().Data {
		expr.GetVec().Data[i] = 0
	}
	require.Equal(t, "safe value", values[0].GetLit().GetSval())
}

func TestInRHSValuesRejectsOversizedFoldedVector(t *testing.T) {
	mp := mpool.MustNew(t.Name())
	source := MakePlan2Int64VecExprWithType(mp, 1)
	data := append([]byte(nil), source.GetVec().Data...)
	logicalLength := uint32(1024)
	copy(data[1+types.TSize:], types.EncodeUint32(&logicalLength))
	expr := &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  int32(logicalLength),
			Data: data,
		}},
	}

	require.NotPanics(t, func() {
		values, ok := inRHSValues(expr, expr.Typ)
		require.False(t, ok)
		require.Nil(t, values)
		_, ok = blockFilterConstantSet(expr)
		require.False(t, ok)
	})
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

func BenchmarkDoMergeFiltersOnCompositeKeyInRepresentations(b *testing.B) {
	const valueCount = 256
	ctx := NewMockCompilerContext(true)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	tag := builder.genNewBindTag()
	tableDef := makeExprOptCompositeSortKeyTableDef()
	aEq, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		makeExprOptInt64Col(tag, 0, "a"),
		MakePlan2Int64ConstExprWithType(1),
	})
	if err != nil {
		b.Fatal(err)
	}
	listItems := make([]*planpb.Expr, valueCount)
	values := make([]int64, valueCount)
	for i := range listItems {
		values[i] = int64(i)
		listItems[i] = MakePlan2Int64ConstExprWithType(values[i])
	}
	listIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: listItems}}},
	})
	if err != nil {
		b.Fatal(err)
	}
	vectorIn, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "in", []*planpb.Expr{
		makeExprOptInt64Col(tag, 1, "b"),
		MakePlan2Int64VecExprWithType(ctx.GetProcess().Mp(), values...),
	})
	if err != nil {
		b.Fatal(err)
	}

	for _, tc := range []struct {
		name string
		in   *planpb.Expr
	}{
		{name: "list", in: listIn},
		{name: "folded-vector", in: vectorIn},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				ret := builder.doMergeFiltersOnCompositeKey(tableDef, tag, aEq, tc.in)
				if len(ret) != 1 {
					b.Fatalf("unexpected filter count: %d", len(ret))
				}
			}
		})
	}
}

func makeExprOptCompositeSortKeyTableDef() *planpb.TableDef {
	return &planpb.TableDef{
		Cols: []*planpb.ColDef{
			{Name: "a", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "b", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "c", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "__mo_cpkey", Typ: planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
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

func makeExprOptThreePartCompositeSortKeyTableDef() *planpb.TableDef {
	tableDef := makeExprOptCompositeSortKeyTableDef()
	tableDef.Pkey.Names = []string{"a", "b", "c"}
	return tableDef
}

func makeExprOptCompositeClusterKeyTableDef() *planpb.TableDef {
	tableDef := makeExprOptCompositeSortKeyTableDef()
	clusterKeyName := util.BuildCompositeClusterByColumnName([]string{"a", "b"})
	clusterKeyCol := &planpb.ColDef{
		Name: clusterKeyName,
		Typ:  planpb.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen},
	}
	tableDef.Cols = append(tableDef.Cols, clusterKeyCol)
	tableDef.Name2ColIndex[clusterKeyName] = int32(len(tableDef.Cols) - 1)
	tableDef.ClusterBy = &planpb.ClusterByDef{
		Name:         clusterKeyName,
		CompCbkeyCol: clusterKeyCol,
	}
	return tableDef
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

func makeExprOptNullableInt64Vec(t *testing.T, mp *mpool.MPool, value int64) *planpb.Expr {
	t.Helper()
	vec := vector.NewVec(types.T_int64.ToType())
	require.NoError(t, vector.AppendFixed(vec, value, false, mp))
	require.NoError(t, vector.AppendFixed(vec, int64(0), true, mp))
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  2,
			Data: data,
		}},
	}
}

func makeExprOptStringVec(t *testing.T, mp *mpool.MPool, values ...string) *planpb.Expr {
	t.Helper()
	typ := types.New(types.T_varchar, types.MaxVarcharLen, 0)
	vec := vector.NewVec(typ)
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), false, mp))
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(mp)
	return &planpb.Expr{
		Typ: makePlan2Type(&typ),
		Expr: &planpb.Expr_Vec{Vec: &planpb.LiteralVec{
			Len:  int32(len(values)),
			Data: data,
		}},
	}
}

func requireCompositeKeyInFilter(t *testing.T, expr *planpb.Expr, expectedFunc string, sortCol int32, valueCount int, allowOr bool) {
	t.Helper()
	fn := expr.GetF()
	require.NotNil(t, fn)
	if expectedFunc == "prefix_in" {
		require.Equal(t, expectedFunc, fn.Func.ObjName)
		require.Equal(t, sortCol, fn.Args[0].GetCol().ColPos)
		require.Len(t, fn.Args[1].GetList().List, valueCount)
		return
	}

	if fn.Func.ObjName == "in" {
		require.Equal(t, sortCol, fn.Args[0].GetCol().ColPos)
		require.Len(t, fn.Args[1].GetList().List, valueCount)
		return
	}
	if valueCount == 1 && fn.Func.ObjName == "=" {
		require.Equal(t, sortCol, fn.Args[0].GetCol().ColPos)
		return
	}
	if !allowOr {
		require.Equal(t, expectedFunc, fn.Func.ObjName)
		return
	}

	// The generic IN binder can preserve list elements with different NULL/cast
	// semantics as OR-ed equalities. That remains equivalent to a full-key IN.
	require.Equal(t, "or", fn.Func.ObjName)
	var args []*planpb.Expr
	flattenLogicalExpressions(expr, "or", &args)
	require.Len(t, args, valueCount)
	for _, arg := range args {
		argFn := arg.GetF()
		require.Equal(t, "=", argFn.Func.ObjName)
		require.Equal(t, sortCol, argFn.Args[0].GetCol().ColPos)
	}
}

func hasTopLevelFilterOnColumn(filters []*planpb.Expr, name string, colPos int32) bool {
	for _, filter := range filters {
		fn := filter.GetF()
		if fn != nil && fn.Func.ObjName == name && len(fn.Args) > 0 &&
			fn.Args[0].GetCol() != nil && fn.Args[0].GetCol().ColPos == colPos {
			return true
		}
	}
	return false
}

func exprContainsNullLiteral(expr *planpb.Expr) bool {
	if expr == nil {
		return false
	}
	if lit := expr.GetLit(); lit != nil {
		return lit.Isnull
	}
	if fn := expr.GetF(); fn != nil {
		for _, arg := range fn.Args {
			if exprContainsNullLiteral(arg) {
				return true
			}
		}
	}
	if list := expr.GetList(); list != nil {
		for _, item := range list.List {
			if exprContainsNullLiteral(item) {
				return true
			}
		}
	}
	return false
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
