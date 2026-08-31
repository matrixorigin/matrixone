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
	"math"
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
	inputRows float64,
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
			Outcnt:       inputRows,
			Selectivity:  1,
			BlockNum:     int32(inputRows/8192) + 1,
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
		qry:         &planpb.Query{Nodes: []*planpb.Node{child, agg}, Steps: []int32{1}},
		compCtx:     ctx,
		ctxByNode:   []*BindContext{nil, nil},
		tag2Table:   make(map[int32]*planpb.TableDef),
		tag2NodeID:  make(map[int32]int32),
		nextBindTag: 20,
	}
	return builder, agg
}

func TestOptimizeSingleCountDistinctBuildsLocalPreDedupPath(t *testing.T) {
	key := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	countDistinct := distinctAggTestExpr(
		function.COUNT, true,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, key)
	builder, outer := newDistinctAggTestBuilder(
		1_000_000, 10, 1_000_000, []*planpb.Expr{countDistinct})
	originalTags := append([]int32(nil), outer.BindingTags...)

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 4)
	localPair := builder.qry.Nodes[2]
	finalPair := builder.qry.Nodes[3]
	require.Equal(t, []int32{0}, localPair.Children)
	require.Equal(t, []int32{2}, finalPair.Children)
	require.Equal(t, []int32{3}, outer.Children)
	require.Equal(t, originalTags, outer.BindingTags)
	require.Len(t, localPair.GroupBy, 2)
	require.Len(t, finalPair.GroupBy, 2)
	require.Empty(t, localPair.AggList)
	require.Empty(t, finalPair.AggList)
	require.Zero(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct)

	_, localMarked := builder.distinctKeyLocalPreAggs[localPair]
	require.True(t, localMarked)
	shuffleCol, finalMarked := builder.distinctKeyShuffleCols[finalPair]
	require.True(t, finalMarked)
	require.Equal(t, int32(1), shuffleCol)
	for _, groupBy := range finalPair.GroupBy {
		require.Equal(t, localPair.BindingTags[0], groupBy.GetCol().RelPos,
			"the exchange must consume locally deduplicated pair rows")
	}
	for _, groupBy := range outer.GroupBy {
		require.Equal(t, finalPair.BindingTags[0], groupBy.GetCol().RelPos)
	}
	require.Equal(t, finalPair.BindingTags[0], outer.AggList[0].GetF().Args[0].GetCol().RelPos)

	// Prove the planner-only ownership markers override both ordinary heuristics:
	// the local stage stays local even with stale/high shuffle state, while the
	// final pair stage shuffles despite having an unshuffled Group child.
	localPair.Stats.HashmapStats.Shuffle = true
	localPair.Stats.HashmapStats.HashmapSize = 3_000_000
	determineShuffleForGroupBy(localPair, builder)
	require.False(t, localPair.Stats.HashmapStats.Shuffle)
	determineShuffleForGroupBy(finalPair, builder)
	require.True(t, finalPair.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(1), finalPair.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, planpb.ShuffleType_Hash, finalPair.Stats.HashmapStats.ShuffleType)

	wrapped := &planpb.Plan{Plan: &planpb.Plan_Query{Query: builder.qry}}
	CalcQueryDOP(wrapped, 4, 1, ExecTypeAP_ONECN)
	require.Equal(t, int32(4), localPair.Stats.Dop)
	require.Equal(t, int32(4), finalPair.Stats.Dop,
		"the shuffled pair merge must expose multiple physical owners")
}

func TestOptimizePadSpaceCountDistinctBuildsLocalPreDedupPath(t *testing.T) {
	key := distinctAggTestCol(types.T_varchar, 1, 1, 1_000_000)
	key.Typ.PadSpace = true
	countDistinct := distinctAggTestExpr(
		function.COUNT, true,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, key)
	builder, outer := newDistinctAggTestBuilder(
		1_000_000, 10, 1_000_000, []*planpb.Expr{countDistinct})

	require.NoError(t, builder.optimizeDistinctAgg(1))
	require.Len(t, builder.qry.Nodes, 4)

	localPair := builder.qry.Nodes[2]
	finalPair := builder.qry.Nodes[3]
	require.Len(t, localPair.GroupBy, 3,
		"local pre-dedup keeps the visible value and adds a PAD SPACE equality key")
	require.Equal(t, []int32{0, 2}, localPair.GroupByHashKey)
	require.True(t, isCastOverload(localPair.GroupBy[2], 3))

	_, localMarked := builder.distinctKeyLocalPreAggs[localPair]
	require.True(t, localMarked)
	shuffleCol, finalMarked := builder.distinctKeyShuffleCols[finalPair]
	require.True(t, finalMarked)
	require.Equal(t, int32(2), shuffleCol,
		"the distributed stage must partition by the canonical PAD SPACE key")
	require.Equal(t, finalPair.BindingTags[0], outer.AggList[0].GetF().Args[0].GetCol().RelPos)
}

func TestDistinctKeyPreAggregationRequiresEveryGroupingKeyNDV(t *testing.T) {
	newBuilder := func(secondNDV float64) (*QueryBuilder, *planpb.Node) {
		key := distinctAggTestCol(types.T_int64, 1, 2, 1_000_000)
		countDistinct := distinctAggTestExpr(
			function.COUNT, true,
			planpb.Type{Id: int32(types.T_int64), NotNullable: true}, key)
		builder, outer := newDistinctAggTestBuilder(
			1_000_000, 10, 1_000_000, []*planpb.Expr{countDistinct})
		outer.GroupBy = append(outer.GroupBy,
			distinctAggTestCol(types.T_int32, 1, 1, secondNDV))
		return builder, outer
	}

	for _, tc := range []struct {
		name string
		ndv  float64
	}{
		{name: "missing component", ndv: -1},
		{name: "NaN component", ndv: math.NaN()},
		{name: "infinite component", ndv: math.Inf(1)},
	} {
		t.Run(tc.name+" disables path", func(t *testing.T) {
			builder, _ := newBuilder(tc.ndv)
			builder.optimizeDistinctAgg(1)

			require.Len(t, builder.qry.Nodes, 3,
				"one known low-NDV key cannot stand in for an unreliable composite key")
		})
	}

	t.Run("known low-NDV components enable path", func(t *testing.T) {
		builder, _ := newBuilder(5)
		builder.optimizeDistinctAgg(1)

		require.Len(t, builder.qry.Nodes, 4)
	})
}

func TestOptimizeSingleCountDistinctPathSelectionAndFallbacks(t *testing.T) {
	for _, tc := range []struct {
		name        string
		inputRows   float64
		keyType     types.T
		groupNDV    float64
		distinctNDV float64
		global      bool
		directKey   bool
		groupingSet bool
		wantRewrite bool
		wantPathB   bool
	}{
		{name: "few final owners", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: 1_000_000, directKey: true,
			wantRewrite: true, wantPathB: true},
		{name: "global aggregate", inputRows: 1_000_000, keyType: types.T_varchar,
			groupNDV: 1, distinctNDV: 1_000_000, global: true, directKey: true,
			wantRewrite: true, wantPathB: true},
		{name: "worker-local duplicate adversary", inputRows: 1_000_000,
			keyType: types.T_int64, groupNDV: 1, distinctNDV: 300_000,
			global: true, directKey: true, wantRewrite: true, wantPathB: true},
		{name: "path A boundary", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: shuffleDistinctGroupMinNDV, distinctNDV: 1_000_000,
			directKey: true, wantRewrite: true},
		{name: "small distinct state", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: 10, directKey: true, wantRewrite: true},
		{name: "missing distinct statistics", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: -1, directKey: true, wantRewrite: true},
		{name: "low distinct ratio", inputRows: 100_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: 1_000_000, directKey: true, wantRewrite: true},
		{name: "unsupported shuffle key", inputRows: 1_000_000, keyType: types.T_float64,
			groupNDV: 10, distinctNDV: 1_000_000, directKey: true, wantRewrite: true},
		{name: "expression key", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: 1_000_000, wantRewrite: true},
		{name: "inactive grouping set", inputRows: 1_000_000, keyType: types.T_int64,
			groupNDV: 10, distinctNDV: 1_000_000, directKey: true, groupingSet: true},
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
				tc.inputRows, tc.groupNDV, tc.distinctNDV, []*planpb.Expr{agg})
			if tc.global {
				outer.GroupBy = nil
				outer.Stats.HashmapStats.HashmapSize = 1
			}
			if tc.groupingSet {
				outer.GroupingFlag = []bool{false}
			}

			builder.optimizeDistinctAgg(1)

			rewritten := len(builder.qry.Nodes) > 2
			pathB := len(builder.qry.Nodes) == 4
			require.Equal(t, tc.wantRewrite, rewritten)
			require.Equal(t, tc.wantPathB, pathB)
			if pathB {
				localPair := builder.qry.Nodes[2]
				finalPair := builder.qry.Nodes[3]
				_, localMarked := builder.distinctKeyLocalPreAggs[localPair]
				_, finalMarked := builder.distinctKeyShuffleCols[finalPair]
				require.True(t, localMarked)
				require.True(t, finalMarked)
			}
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
	builder, outer := newDistinctAggTestBuilder(1_000_000, 1, 1_000_000, aggs)
	outer.GroupBy = nil
	originalChild := outer.Children[0]
	originalIDs := make([]int64, len(outer.AggList))
	for i := range outer.AggList {
		originalIDs[i] = outer.AggList[i].GetF().Func.Obj
	}

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 2)
	require.Equal(t, originalChild, outer.Children[0])
	require.Empty(t, builder.distinctKeyLocalPreAggs)
	require.Empty(t, builder.distinctKeyShuffleCols)
	for i := range outer.AggList {
		require.Equal(t, originalIDs[i], outer.AggList[i].GetF().Func.Obj)
	}
}

func TestOptimizeSingleSumDistinctKeepsExistingLogicalRewriteOnly(t *testing.T) {
	key := distinctAggTestCol(types.T_decimal128, 1, 1, 1_000_000)
	sumDistinct := distinctAggTestExpr(function.SUM, true,
		planpb.Type{Id: int32(types.T_decimal128), Width: 38}, key)
	builder, _ := newDistinctAggTestBuilder(
		1_000_000, 1, 1_000_000, []*planpb.Expr{sumDistinct})

	builder.optimizeDistinctAgg(1)

	require.Len(t, builder.qry.Nodes, 3)
	require.Empty(t, builder.distinctKeyLocalPreAggs)
	require.Empty(t, builder.distinctKeyShuffleCols)
}
