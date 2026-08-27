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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
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
	t *testing.T,
	groupNDV float64,
	distinctNDV float64,
	aggs []*planpb.Expr,
) (*QueryBuilder, *planpb.Node) {
	t.Helper()
	ctx := &MockCompilerContext{ctx: context.Background()}
	proc := ctx.GetProcess()
	rt := moruntime.ServiceRuntime(proc.GetService())
	original, hadOriginal := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCLatestVersion)
	t.Cleanup(func() {
		if hadOriginal {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, original)
		}
	})

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
	groupBy := distinctAggTestCol(types.T_int32, 1, 0, groupNDV)
	agg := &planpb.Node{
		NodeId:      1,
		NodeType:    planpb.Node_AGG,
		Children:    []int32{0},
		GroupBy:     []*planpb.Expr{groupBy},
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
		if fn := expr.GetF(); fn != nil &&
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

func TestOptimizeMixedDistinctAggBuildsDistinctKeyPath(t *testing.T) {
	distinctKey := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
	value := distinctAggTestCol(types.T_int32, 1, 2, 100)
	countDistinct := distinctAggTestExpr(
		function.COUNT, true,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, distinctKey)
	sum := distinctAggTestExpr(
		function.SUM, false, planpb.Type{Id: int32(types.T_int64)}, value)
	count := distinctAggTestExpr(
		function.COUNT, false,
		planpb.Type{Id: int32(types.T_int64), NotNullable: true}, value)
	min := distinctAggTestExpr(
		function.MIN, false, planpb.Type{Id: int32(types.T_int32)}, value)
	avg := distinctAggTestExpr(
		function.AVG, false, planpb.Type{Id: int32(types.T_float64)}, value)
	builder, outer := newDistinctAggTestBuilder(
		t, 10, 1_000_000, []*planpb.Expr{countDistinct, sum, count, min, avg})
	originalTags := append([]int32(nil), outer.BindingTags...)

	builder.optimizeDistinctAgg(1)
	require.Len(t, builder.qry.Nodes, 3)
	inner := builder.qry.Nodes[2]
	require.Equal(t, []int32{2}, outer.Children)
	require.Equal(t, originalTags, outer.BindingTags,
		"the outward aggregate binding contract must remain stable")
	require.Len(t, inner.GroupBy, 2)
	require.Len(t, inner.AggList, 5,
		"SUM, COUNT, MIN, and AVG's SUM+COUNT helpers are materialized once")

	outerIDs := make([]int32, len(outer.AggList))
	for i, expr := range outer.AggList {
		outerIDs[i], _ = function.DecodeOverloadID(expr.GetF().Func.Obj)
	}
	require.Equal(t, []int32{
		function.COUNT,
		function.INTERNAL_SUM_COMBINE,
		function.INTERNAL_COUNT_COMBINE,
		function.MIN,
		function.INTERNAL_AVG_COMBINE,
	}, outerIDs)
	require.Len(t, outer.AggList[4].GetF().Args, 3)
	require.True(t, outer.AggList[4].GetF().Args[2].GetLit().Isnull)
	require.Equal(t, int32(types.T_float64), outer.AggList[4].GetF().Args[2].Typ.Id)

	shuffleCol, marked := builder.distinctKeyShuffleCols[inner]
	require.True(t, marked)
	require.Equal(t, int32(1), shuffleCol)
	determineShuffleForGroupBy(inner, builder)
	require.True(t, inner.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(1), inner.Stats.HashmapStats.ShuffleColIdx)
	require.Equal(t, planpb.ShuffleType_Hash, inner.Stats.HashmapStats.ShuffleType)
}

func TestOptimizeMixedDistinctAggPathSelectionAndFallbacks(t *testing.T) {
	makeAggs := func(secondDistinctKey bool) []*planpb.Expr {
		distinctKey := distinctAggTestCol(types.T_int64, 1, 1, 1_000_000)
		secondKey := distinctKey
		if secondDistinctKey {
			secondKey = distinctAggTestCol(types.T_int64, 1, 3, 1_000_000)
		}
		return []*planpb.Expr{
			distinctAggTestExpr(function.COUNT, true,
				planpb.Type{Id: int32(types.T_int64)}, distinctKey),
			distinctAggTestExpr(function.COUNT, true,
				planpb.Type{Id: int32(types.T_int64)}, secondKey),
			distinctAggTestExpr(function.SUM, false,
				planpb.Type{Id: int32(types.T_int64)},
				distinctAggTestCol(types.T_int32, 1, 2, 100)),
		}
	}

	for _, tc := range []struct {
		name              string
		groupNDV          float64
		distinctNDV       float64
		secondDistinctKey bool
		global            bool
		inactiveGrouping  bool
		protocol          int64
		wantRewrite       bool
	}{
		{name: "few final owners", groupNDV: 10, distinctNDV: 1_000_000,
			protocol: defines.MORPCLatestVersion, wantRewrite: true},
		{name: "global aggregate", groupNDV: 1, distinctNDV: 1_000_000,
			global: true, protocol: defines.MORPCLatestVersion, wantRewrite: true},
		{name: "path A boundary", groupNDV: shuffleDistinctGroupMinNDV,
			distinctNDV: 1_000_000, protocol: defines.MORPCLatestVersion},
		{name: "small distinct state", groupNDV: 10,
			distinctNDV: threshHoldForShuffleGroup, protocol: defines.MORPCLatestVersion},
		{name: "different distinct set", groupNDV: 10, distinctNDV: 1_000_000,
			secondDistinctKey: true, protocol: defines.MORPCLatestVersion},
		{name: "inactive grouping set", groupNDV: 10, distinctNDV: 1_000_000,
			inactiveGrouping: true, protocol: defines.MORPCLatestVersion},
		{name: "rolling upgrade", groupNDV: 10, distinctNDV: 1_000_000,
			protocol: defines.MORPCVersion32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder, outer := newDistinctAggTestBuilder(
				t, tc.groupNDV, tc.distinctNDV, makeAggs(tc.secondDistinctKey))
			if tc.global {
				outer.GroupBy = nil
				outer.Stats.HashmapStats.HashmapSize = 1
			}
			if tc.inactiveGrouping {
				outer.GroupingFlag = []bool{false}
			}
			proc := builder.compCtx.GetProcess()
			moruntime.ServiceRuntime(proc.GetService()).SetGlobalVariables(
				moruntime.MOProtocolVersion, tc.protocol)
			builder.optimizeDistinctAgg(1)
			require.Equal(t, tc.wantRewrite, len(builder.qry.Nodes) == 3)
		})
	}
}

func TestOptimizeSingleCountDistinctForcesOnlyLargeDistinctShuffle(t *testing.T) {
	for _, tc := range []struct {
		name        string
		keyType     types.T
		distinctNDV float64
		wantShuffle bool
	}{
		{name: "large", keyType: types.T_int64,
			distinctNDV: 1_000_000, wantShuffle: true},
		{name: "small", keyType: types.T_int64, distinctNDV: 10},
		{name: "unsupported shuffle key", keyType: types.T_float64,
			distinctNDV: 1_000_000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key := distinctAggTestCol(tc.keyType, 1, 1, tc.distinctNDV)
			agg := distinctAggTestExpr(function.COUNT, true,
				planpb.Type{Id: int32(types.T_int64)}, key)
			builder, outer := newDistinctAggTestBuilder(t, 10, tc.distinctNDV,
				[]*planpb.Expr{agg})
			outer.GroupBy = nil
			outer.Stats.HashmapStats.HashmapSize = 1
			builder.optimizeDistinctAgg(1)
			require.Len(t, builder.qry.Nodes, 3,
				"the established logical rewrite remains unconditional")
			_, marked := builder.distinctKeyShuffleCols[builder.qry.Nodes[2]]
			require.Equal(t, tc.wantShuffle, marked)
		})
	}
}
