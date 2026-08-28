// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func distinctAggTestCol(typ types.T, tag, pos int32, ndv float64) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(typ)},
		Ndv:  ndv,
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos}},
	}
}

func distinctAggTestExpr(
	functionID int32,
	distinct bool,
	resultType planpb.Type,
	args ...*planpb.Expr,
) *planpb.Expr {
	id := function.EncodeOverloadID(functionID, 0)
	if distinct {
		id = int64(uint64(id) | function.Distinct)
	}
	return &planpb.Expr{
		Typ: resultType,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{Obj: id},
			Args: args,
		}},
	}
}

func newDistinctAggTestBuilder(
	groupNDV float64,
	distinctNDV float64,
	aggs []*planpb.Expr,
) (*QueryBuilder, *planpb.Node) {
	ctx := &MockCompilerContext{ctx: context.Background()}
	child := &planpb.Node{
		NodeId:      0,
		NodeType:    planpb.Node_TABLE_SCAN,
		BindingTags: []int32{1},
		Stats: &planpb.Stats{
			Outcnt:       1_000_000,
			Selectivity:  1,
			HashmapStats: &planpb.HashMapStats{},
		},
	}
	agg := &planpb.Node{
		NodeId:      1,
		NodeType:    planpb.Node_AGG,
		Children:    []int32{0},
		GroupBy:     []*planpb.Expr{distinctAggTestCol(types.T_int32, 1, 0, groupNDV)},
		AggList:     aggs,
		BindingTags: []int32{10, 11},
		Stats: &planpb.Stats{
			Outcnt:      groupNDV,
			Selectivity: 1,
			HashmapStats: &planpb.HashMapStats{
				HashmapSize: groupNDV,
			},
		},
	}
	for _, expr := range agg.AggList {
		if fn := expr.GetF(); fn != nil && fn.Func != nil &&
			uint64(fn.Func.Obj)&function.Distinct != 0 && len(fn.Args) == 1 {
			fn.Args[0].Ndv = distinctNDV
		}
	}
	builder := &QueryBuilder{
		qry:         &planpb.Query{Nodes: []*planpb.Node{child, agg}},
		compCtx:     ctx,
		ctxByNode:   []*BindContext{nil, nil},
		tag2Table:   make(map[int32]*planpb.TableDef),
		tag2NodeID:  make(map[int32]int32),
		nextBindTag: 20,
	}
	return builder, agg
}

func TestOptimizeSingleCountDistinctBuildsDistinctKeyPath(t *testing.T) {
	key := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	countDistinct := distinctAggTestExpr(
		function.COUNT, true,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, key)
	builder, outer := newDistinctAggTestBuilder(10, 1_000_000, []*planpb.Expr{countDistinct})
	originalTags := append([]int32(nil), outer.BindingTags...)

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 3)
	inner := builder.qry.Nodes[2]
	require.Equal(t, []int32{2}, outer.Children)
	require.Equal(t, originalTags, outer.BindingTags)
	require.Len(t, inner.GroupBy, 2)
	require.Empty(t, inner.AggList)
	require.Zero(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct)

	shuffleCol, marked := builder.distinctKeyShuffleCols[inner]
	require.True(t, marked)
	require.Equal(t, int32(1), shuffleCol)
	determineShuffleForGroupBy(inner, builder)
	require.True(t, inner.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(1), inner.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, planpb.ShuffleType_Hash, inner.Stats.HashmapStats.ShuffleType)
}

func TestOptimizeSingleCountDistinctPathSelectionAndFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name        string
		keyType     types.T
		groupNDV    float64
		distinctNDV float64
		global      bool
		directKey   bool
		groupingSet bool
		wantRewrite bool
		wantShuffle bool
	}{
		{name: "few final owners", keyType: types.T_int64, groupNDV: 10,
			distinctNDV: 1_000_000, directKey: true, wantRewrite: true, wantShuffle: true},
		{name: "global aggregate", keyType: types.T_varchar, groupNDV: 1,
			distinctNDV: 1_000_000, global: true, directKey: true,
			wantRewrite: true, wantShuffle: true},
		{name: "path A boundary", keyType: types.T_int64,
			groupNDV: shuffleDistinctGroupMinNDV, distinctNDV: 1_000_000,
			directKey: true, wantRewrite: true},
		{name: "small distinct state", keyType: types.T_int64, groupNDV: 10,
			distinctNDV: 10, directKey: true, wantRewrite: true},
		{name: "missing distinct statistics", keyType: types.T_int64, groupNDV: 10,
			distinctNDV: -1, directKey: true, wantRewrite: true},
		{name: "unsupported shuffle key", keyType: types.T_float64, groupNDV: 10,
			distinctNDV: 1_000_000, directKey: true, wantRewrite: true},
		{name: "expression key", keyType: types.T_int64, groupNDV: 10,
			distinctNDV: 1_000_000, wantRewrite: true},
		{name: "inactive grouping set", keyType: types.T_int64, groupNDV: 10,
			distinctNDV: 1_000_000, directKey: true, groupingSet: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := distinctAggTestCol(tc.keyType, 1, 1, tc.distinctNDV)
			if !tc.directKey {
				key.Expr = &planpb.Expr_Lit{Lit: &planpb.Literal{
					Value: &planpb.Literal_I64Val{I64Val: 7},
				}}
			}
			agg := distinctAggTestExpr(function.COUNT, true,
				planpb.Type{Id: int32(types.T_int64)}, key)
			builder, outer := newDistinctAggTestBuilder(
				tc.groupNDV, tc.distinctNDV, []*planpb.Expr{agg})
			if tc.global {
				outer.GroupBy = nil
				outer.Stats.HashmapStats.HashmapSize = 1
			}
			if tc.groupingSet {
				outer.GroupingFlag = []bool{false}
			}

			builder.optimizeDistinctAgg(1)

			rewritten := len(builder.qry.Nodes) == 3
			require.Equal(t, tc.wantRewrite, rewritten)
			marked := false
			if rewritten {
				_, marked = builder.distinctKeyShuffleCols[builder.qry.Nodes[2]]
			}
			require.Equal(t, tc.wantShuffle, marked)
		})
	}
}

func TestOptimizeDistinctKeyPathLeavesMixedAggregatesUnchanged(t *testing.T) {
	key := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	value := distinctAggTestCol(types.T_decimal128, 1, 2, 100)
	widePayload := distinctAggTestCol(types.T_varchar, 1, 3, 100)
	widePayload.Typ.Width = 1 << 20
	aggs := []*planpb.Expr{
		distinctAggTestExpr(function.COUNT, true,
			planpb.Type{Id: int32(types.T_int64)}, key),
		distinctAggTestExpr(function.SUM, false,
			planpb.Type{Id: int32(types.T_decimal128), Width: 38}, value),
		distinctAggTestExpr(function.AVG, false,
			planpb.Type{Id: int32(types.T_decimal128), Width: 38}, value),
		distinctAggTestExpr(function.MIN, false, widePayload.Typ, widePayload),
	}
	builder, outer := newDistinctAggTestBuilder(1, 1_000_000, aggs)
	outer.GroupBy = nil
	originalChild := outer.Children[0]
	originalIDs := make([]int64, len(outer.AggList))
	for i := range outer.AggList {
		originalIDs[i] = outer.AggList[i].GetF().Func.Obj
	}

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 2,
		"ordinary aggregate state must not be replicated per DISTINCT key")
	require.Equal(t, originalChild, outer.Children[0])
	require.Empty(t, builder.distinctKeyShuffleCols)
	for i := range outer.AggList {
		require.Equal(t, originalIDs[i], outer.AggList[i].GetF().Func.Obj)
	}
}

func TestOptimizeSingleSumDistinctKeepsExistingLogicalRewriteOnly(t *testing.T) {
	key := distinctAggTestCol(types.T_decimal128, 1, 1, 1_000_000)
	sumDistinct := distinctAggTestExpr(function.SUM, true,
		planpb.Type{Id: int32(types.T_decimal128), Width: 38}, key)
	builder, _ := newDistinctAggTestBuilder(1, 1_000_000, []*planpb.Expr{sumDistinct})

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 3)
	require.Empty(t, builder.distinctKeyShuffleCols,
		"Path B is limited to COUNT(DISTINCT)")
}
