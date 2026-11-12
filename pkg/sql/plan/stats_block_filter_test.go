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
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/stretchr/testify/require"
)

type zoneMapStatsTestCompilerContext struct {
	*MockCompilerContext
	stats *statsinfo.StatsInfo
	err   error
}

func (ctx *zoneMapStatsTestCompilerContext) Stats(*ObjectRef, *Snapshot) (*statsinfo.StatsInfo, error) {
	return ctx.stats, ctx.err
}

func TestCalcScanStatsSimpleBlockFilters(t *testing.T) {
	ctx := context.Background()
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, name, args)
		require.NoError(t, err)
		return expr
	}
	col := GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, 1)
	bound := MakePlan2Int64ConstExprWithType(100)
	between := bind("between", col, bound, MakePlan2Int64ConstExprWithType(150))
	for _, tc := range []struct {
		name                               string
		expr                               *planpb.Expr
		hints                              *OptimizerHints
		missing, statsErr, partition, want bool
	}{
		{name: "full overlap", expr: between, want: true},
		{name: "default hint", expr: between, hints: &OptimizerHints{}, want: true},
		{name: "unrelated hint", expr: between, hints: &OptimizerHints{execType: 2}, want: true},
		{name: "force", expr: between, hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "disabled", expr: between, hints: &OptimizerHints{blockFilter: 2}},
		{name: "missing stats", expr: between, missing: true, want: true},
		{name: "stats error", expr: between, statsErr: true, want: true},
		{name: "missing disabled", expr: between, missing: true, hints: &OptimizerHints{blockFilter: 2}},
		{name: "partition keeps old admission", expr: between, partition: true},
		{name: "partition missing stats", expr: between, partition: true, missing: true},
		{name: "column comparison", expr: bind("=", col, GetColExpr(col.Typ, 1, 0))},
		{name: "column arithmetic", expr: bind(">", bind("+", col, bound), bound)},
		{name: "force arithmetic", expr: bind(">", bind("+", col, bound), bound), hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "nonmonotonic", expr: bind(">", bind("sin", col), bound), hints: &OptimizerHints{blockFilter: 1}},
		{name: "volatile", expr: bind(">", col, bind("rand"))},
		{name: "volatile forced", expr: bind(">", col, bind("rand")), hints: &OptimizerHints{blockFilter: 1}},
		{name: "constant expression", expr: bind(">", col, bind("+", bound, bound)), want: true},
		{name: "parameter", expr: bind(">", col, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}), want: true},
		{name: "in range", expr: bind("in_range", col, bound, bound, MakePlan2Uint8ConstExprWithType(0)), want: true},
		{name: "commuted", expr: bind("<", bound, col), want: true},
		{name: "eq", expr: bind("=", col, bound), want: true},
		{name: "lt", expr: bind("<", col, bound), want: true},
		{name: "le", expr: bind("<=", col, bound), want: true},
		{name: "gt", expr: bind(">", col, bound), want: true},
		{name: "ge", expr: bind(">=", col, bound), want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := &statsinfo.StatsInfo{TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": {Overlap: 1}}}
			if tc.missing {
				stats = nil
			}
			compCtx := &zoneMapStatsTestCompilerContext{MockCompilerContext: &MockCompilerContext{ctx: ctx}, stats: stats}
			if tc.statsErr {
				compCtx.err = errors.New("statistics unavailable")
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compCtx, false, false)
			builder.optimizerHints = tc.hints
			filter := DeepCopyExpr(tc.expr)
			filter.Selectivity = 0.5
			node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
				TableDef: &planpb.TableDef{TblId: 42, Name: "ordinary",
					Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
					Cols: []*planpb.ColDef{{Name: "id", Primary: true, Typ: col.Typ}, {Name: "k", Seqnum: 1, Typ: col.Typ}}},
				FilterList: []*planpb.Expr{filter}}
			if tc.partition {
				node.TableDef.Partition = &planpb.Partition{}
			}
			for range 2 {
				actual := calcScanStats(node, builder)
				if tc.missing || tc.statsErr {
					require.Equal(t, DefaultStats(), actual)
				} else {
					require.Equal(t, 60000*estimateFilterBlockSelectivity(ctx, filter, node.TableDef, stats), actual.Cost)
				}
				if tc.want {
					require.Len(t, node.BlockFilterList, 1)
					require.True(t, exprStructuralEqual(filter, node.BlockFilterList[0]))
					require.NotSame(t, filter, node.BlockFilterList[0])
				} else {
					require.Empty(t, node.BlockFilterList)
				}
				require.Same(t, filter, node.FilterList[0])
			}
		})
	}
}

func TestMissingStatsPreservesExistingBlockFilters(t *testing.T) {
	ctx := context.Background()
	col := GetColExpr(planpb.Type{Id: int32(types.T_varchar)}, 1, 0)
	prefix, err := BindFuncExprImplByPlanExpr(ctx, "prefix_eq", []*planpb.Expr{col, MakePlan2StringConstExprWithType("existing")})
	require.NoError(t, err)
	filter, err := BindFuncExprImplByPlanExpr(ctx, "=", []*planpb.Expr{col, MakePlan2StringConstExprWithType("new")})
	require.NoError(t, err)
	for _, disabled := range []bool{false, true} {
		builder := NewQueryBuilder(planpb.Query_SELECT, &zoneMapStatsTestCompilerContext{MockCompilerContext: &MockCompilerContext{ctx: ctx}}, false, false)
		builder.optimizerHints = &OptimizerHints{}
		if disabled {
			builder.optimizerHints.blockFilter = 2
		}
		original := []*planpb.Expr{prefix}
		node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
			TableDef:   &planpb.TableDef{TblId: 42, Name: catalog.IndexTableNamePrefix + "test"},
			FilterList: []*planpb.Expr{filter}, BlockFilterList: original}
		for range 2 {
			require.Equal(t, DefaultStats(), calcScanStats(node, builder))
			if disabled {
				require.Empty(t, node.BlockFilterList)
			} else {
				require.Len(t, node.BlockFilterList, 2)
			}
			require.Equal(t, []*planpb.Expr{prefix}, original)
		}
	}
}
