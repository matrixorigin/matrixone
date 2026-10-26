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
	"fmt"
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
	determineShuffleForGroupBy(localPair, builder, false)
	require.False(t, localPair.Stats.HashmapStats.Shuffle)
	determineShuffleForGroupBy(finalPair, builder, false)
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

func TestOptimizeSingleAvgDistinctBuildsLogicalRewrite(t *testing.T) {
	key := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	avgDistinct := distinctAggTestExpr(function.AVG, true,
		planpb.Type{Id: int32(types.T_float64)}, key)
	builder, outer := newDistinctAggTestBuilder(
		1_000_000, 10, 1_000_000, []*planpb.Expr{avgDistinct})

	require.NoError(t, builder.optimizeDistinctAgg(1))

	require.Len(t, builder.qry.Nodes, 3)
	inner := builder.qry.Nodes[2]
	require.Equal(t, []int32{2}, outer.Children)
	require.Len(t, inner.GroupBy, 2)
	require.Empty(t, inner.AggList)
	require.Zero(t, uint64(outer.AggList[0].GetF().Func.Obj)&function.Distinct)
	functionID, _ := function.DecodeOverloadID(outer.AggList[0].GetF().Func.Obj)
	require.Equal(t, int32(function.AVG), functionID)
	require.False(t, RequiresSingleStageDistinctAgg(outer))
	require.Empty(t, builder.distinctKeyLocalPreAggs)
	require.Empty(t, builder.distinctKeyShuffleCols)
}

func TestOptimizeAvgDistinctKeepsConservativeFallbacks(t *testing.T) {
	newAvg := func() *planpb.Expr {
		return distinctAggTestExpr(
			function.AVG,
			true,
			planpb.Type{Id: int32(types.T_float64)},
			distinctAggTestCol(types.T_int64, 1, 1, 100),
		)
	}

	t.Run("mixed aggregates", func(t *testing.T) {
		builder, aggregate := newDistinctAggTestBuilder(
			1_000, 10, 100,
			[]*planpb.Expr{
				newAvg(),
				distinctAggTestExpr(
					function.SUM,
					false,
					planpb.Type{Id: int32(types.T_int64)},
					distinctAggTestCol(types.T_int64, 1, 2, 100),
				),
			},
		)

		require.NoError(t, builder.optimizeDistinctAgg(1))
		require.Len(t, builder.qry.Nodes, 2)
		require.True(t, RequiresSingleStageDistinctAgg(aggregate))
	})

	t.Run("grouping set", func(t *testing.T) {
		builder, aggregate := newDistinctAggTestBuilder(
			1_000, 10, 100, []*planpb.Expr{newAvg()})
		aggregate.GroupingFlag = []bool{false}

		require.NoError(t, builder.optimizeDistinctAgg(1))
		require.Len(t, builder.qry.Nodes, 2)
		require.True(t, RequiresSingleStageDistinctAgg(aggregate))
	})
}

func TestTupleCountDistinctKeepsCompleteKeysAndSelectsExchangeComponent(t *testing.T) {
	for _, nullable := range []bool{false, true} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("nullable=%v/reverse=%v", nullable, reverse), func(t *testing.T) {
				low := distinctAggTestCol(types.T_int32, 1, 1, 8)
				high := distinctAggTestCol(types.T_int64, 1, 2, 1_000_000)
				low.Typ.NotNullable, high.Typ.NotNullable = !nullable, !nullable
				args := []*planpb.Expr{low, high}
				selected := 1
				if reverse {
					args[0], args[1] = args[1], args[0]
					selected = 0
				}
				count := distinctAggTestExpr(function.COUNT, true, planpb.Type{Id: int32(types.T_int64)}, args...)
				builder, outer := newDistinctAggTestBuilder(1_000_000, 4, 0, []*planpb.Expr{count})
				// An old physical key must not absorb either new tuple component.
				outer.GroupBy = append(outer.GroupBy, distinctAggTestCol(types.T_int32, 1, 3, 4))
				outer.GroupByHashKey = []int32{0}
				require.NoError(t, builder.optimizeDistinctAgg(1))
				if nullable {
					require.Len(t, builder.qry.Nodes, 2)
					require.NotZero(t, uint64(count.GetF().Func.Obj)&function.Distinct)
					return
				}
				final := builder.qry.Nodes[outer.Children[0]]
				require.Equal(t, planpb.Node_AGG, final.NodeType, "eligible tuple must receive a final pair Group")
				require.Len(t, final.Children, 1)
				local := builder.qry.Nodes[final.Children[0]]
				require.Empty(t, local.AggList)
				require.Empty(t, final.AggList)
				require.Equal(t, []int32{0, 2, 3}, local.GroupByHashKey)
				require.Equal(t, local.GroupByHashKey, final.GroupByHashKey)
				require.Equal(t, int32(2+selected), builder.distinctKeyShuffleCols[final])
				require.Len(t, count.GetF().Args, 1)
				require.Zero(t, uint64(count.GetF().Func.Obj)&function.Distinct)
				require.Equal(t, final.BindingTags[0], count.GetF().Args[0].GetCol().RelPos)
				require.Equal(t, []int32{0}, outer.GroupByHashKey)
				require.Equal(t, int32(0), local.Children[0])
			})
		}
	}
}

func TestTupleCountDistinctRetainsConservativeFallbacks(t *testing.T) {
	for _, reason := range []string{"low ndv", "missing ndv", "small input", "nan ndv", "infinite ndv", "unsupported type", "expression", "rollup", "mixed", "high group ndv", "unknown provenance"} {
		t.Run(reason, func(t *testing.T) {
			a := distinctAggTestCol(types.T_int32, 1, 1, 8)
			b := distinctAggTestCol(types.T_int64, 1, 2, 1_000_000)
			a.Typ.NotNullable, b.Typ.NotNullable = true, true
			count := distinctAggTestExpr(function.COUNT, true, planpb.Type{Id: int32(types.T_int64)}, a, b)
			builder, outer := newDistinctAggTestBuilder(1_000_000, 4, 0, []*planpb.Expr{count})
			switch reason {
			case "low ndv":
				b.Ndv = 8
			case "missing ndv":
				b.Ndv = 0
			case "small input":
				builder.qry.Nodes[0].Stats.Outcnt = 10
			case "nan ndv":
				b.Ndv = math.NaN()
			case "infinite ndv":
				b.Ndv = math.Inf(1)
			case "unsupported type":
				a.Typ.Id = int32(types.T_float64)
			case "expression":
				b.Expr = MakePlan2Int64ConstExprWithType(1).Expr
			case "rollup":
				outer.GroupingFlag = []bool{false}
			case "mixed":
				outer.AggList = append(outer.AggList, distinctAggTestExpr(function.SUM, false, b.Typ, b))
			case "unknown provenance":
				b.GetCol().RelPos = 99
			case "high group ndv":
				outer.GroupBy[0].Ndv = 1_000_000
			}
			require.NoError(t, builder.optimizeDistinctAgg(1))
			require.Len(t, builder.qry.Nodes, 2)
			require.NotZero(t, uint64(count.GetF().Func.Obj)&function.Distinct)
			require.Empty(t, builder.distinctKeyShuffleCols)
		})
	}
}

func TestTupleCountDistinctUsesAllColumnKeyContract(t *testing.T) {
	a := distinctAggTestCol(types.T_int32, 1, 1, 8)
	b := distinctAggTestCol(types.T_int64, 1, 2, 1_000_000)
	a.Typ.NotNullable, b.Typ.NotNullable = true, true
	count := distinctAggTestExpr(function.COUNT, true, planpb.Type{Id: int32(types.T_int64)}, a, b)
	builder, outer := newDistinctAggTestBuilder(1_000_000, 1, 0, []*planpb.Expr{count})
	outer.GroupBy = nil
	require.NoError(t, builder.optimizeDistinctAgg(1))
	final := builder.qry.Nodes[outer.Children[0]]
	require.Equal(t, planpb.Node_AGG, final.NodeType, "eligible tuple must receive a final pair Group")
	require.Len(t, final.Children, 1)
	local := builder.qry.Nodes[final.Children[0]]
	require.Len(t, local.GroupBy, 2)
	require.Len(t, final.GroupBy, 2)
	require.Empty(t, local.GroupByHashKey, "empty is the existing all-column equality contract")
	require.Empty(t, final.GroupByHashKey)
}

func TestTupleCountDistinctCanonicalizesCharBeforeExchange(t *testing.T) {
	a := distinctAggTestCol(types.T_char, 1, 1, 1_000_000)
	a.Typ.Width, a.Typ.NotNullable = 32, true
	b := distinctAggTestCol(types.T_int64, 1, 2, 8)
	b.Typ.NotNullable = true
	count := distinctAggTestExpr(function.COUNT, true, planpb.Type{Id: int32(types.T_int64)}, a, b)
	builder, outer := newDistinctAggTestBuilder(1_000_000, 4, 0, []*planpb.Expr{count})
	require.NoError(t, builder.optimizeDistinctAgg(1))
	final := builder.qry.Nodes[outer.Children[0]]
	require.Equal(t, planpb.Node_AGG, final.NodeType, "eligible tuple must receive a final pair Group")
	require.Len(t, final.Children, 1)
	local := builder.qry.Nodes[final.Children[0]]
	require.True(t, isCastOverload(local.GroupBy[1], 2), "exchange must not see raw CHAR padding")
	require.Equal(t, int32(types.T_varchar), local.GroupBy[1].Typ.Id)
	require.Equal(t, int32(1), builder.distinctKeyShuffleCols[final])
}

func TestTupleCountDistinctProvesNullabilityThroughOuterJoin(t *testing.T) {
	a := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	b := distinctAggTestCol(types.T_int64, 2, 1, 1_000_000)
	a.Typ.NotNullable, b.Typ.NotNullable = true, true
	count := distinctAggTestExpr(function.COUNT, true, planpb.Type{Id: int32(types.T_int64)}, a, b)
	builder, outer := newDistinctAggTestBuilder(1_000_000, 1, 0, []*planpb.Expr{count})
	right := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{2}, Stats: builder.qry.Nodes[0].Stats}
	join := &planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_LEFT, Children: []int32{0, 2}, Stats: builder.qry.Nodes[0].Stats}
	builder.qry.Nodes = append(builder.qry.Nodes, right, join)
	builder.ctxByNode = append(builder.ctxByNode, nil, nil)
	outer.Children, outer.GroupBy = []int32{3}, nil
	require.NoError(t, builder.optimizeDistinctAgg(1))
	require.Len(t, builder.qry.Nodes, 4)
	require.Equal(t, []int32{3}, outer.Children)
	require.NotZero(t, uint64(count.GetF().Func.Obj)&function.Distinct,
		"NULL-extended columns retain the exact-state path regardless of bind-time NOT NULL")
}
