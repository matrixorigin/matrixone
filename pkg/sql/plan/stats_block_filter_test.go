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
	column := func(pos int32) *planpb.Expr {
		return GetColExpr(planpb.Type{Id: int32(types.T_int64)}, 1, pos)
	}
	bound := func(value int64) *planpb.Expr { return MakePlan2Int64ConstExprWithType(value) }
	rangeExpr := func() *planpb.Expr { return bind("between", column(1), bound(100), bound(150)) }

	type testCase struct {
		name         string
		expr         func() *planpb.Expr
		hints        *OptimizerHints
		overlap      *statsinfo.ShuffleRange
		missingStats bool
		statsErr     bool
		want         bool
	}
	tests := []testCase{
		{name: "missing overlap", expr: rangeExpr, want: true},
		{name: "full overlap", expr: rangeExpr, overlap: &statsinfo.ShuffleRange{Overlap: 1}, want: true},
		{name: "default hints", expr: rangeExpr, hints: &OptimizerHints{}, want: true},
		{name: "unrelated hint", expr: rangeExpr, hints: &OptimizerHints{execType: 2}, want: true},
		{name: "forced", expr: rangeExpr, hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "disabled", expr: rangeExpr, hints: &OptimizerHints{blockFilter: 2}},
		{name: "missing stats", expr: rangeExpr, missingStats: true, want: true},
		{name: "stats error", expr: rangeExpr, statsErr: true, want: true},
		{name: "missing stats disabled", expr: rangeExpr, missingStats: true, hints: &OptimizerHints{blockFilter: 2}},
		{name: "primary control", expr: func() *planpb.Expr { return bind("between", column(0), bound(100), bound(150)) }, want: true},
		{name: "column comparison", expr: func() *planpb.Expr { return bind("=", column(1), column(0)) }},
		{name: "column arithmetic", expr: func() *planpb.Expr { return bind(">", bind("+", column(1), bound(1)), bound(150)) }},
		{name: "forced column arithmetic", expr: func() *planpb.Expr { return bind(">", bind("+", column(1), bound(1)), bound(150)) }, hints: &OptimizerHints{blockFilter: 1}, want: true},
		{name: "unsupported function", expr: func() *planpb.Expr { return bind(">", bind("sin", column(1)), bound(0)) }, hints: &OptimizerHints{blockFilter: 1}},
		{name: "volatile bound", expr: func() *planpb.Expr { return bind(">", column(1), bind("rand")) }, hints: &OptimizerHints{blockFilter: 1}},
		{name: "constant bound expression", expr: func() *planpb.Expr { return bind(">", column(1), bind("+", bound(99), bound(1))) }, want: true},
		{name: "parameter bound", expr: func() *planpb.Expr {
			return bind(">", column(1), &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}})
		}, want: true},
		{name: "in range", expr: func() *planpb.Expr {
			return bind("in_range", column(1), bound(100), bound(150), MakePlan2Uint8ConstExprWithType(0))
		}, want: true},
		{name: "commuted equality", expr: func() *planpb.Expr { return bind("=", bound(100), column(1)) }, want: true},
		{name: "commuted inequality", expr: func() *planpb.Expr { return bind("<", bound(100), column(1)) }, want: true},
		{name: "eq", expr: func() *planpb.Expr { return bind("=", column(1), bound(100)) }, want: true},
		{name: "lt", expr: func() *planpb.Expr { return bind("<", column(1), bound(100)) }, want: true},
		{name: "le", expr: func() *planpb.Expr { return bind("<=", column(1), bound(100)) }, want: true},
		{name: "gt", expr: func() *planpb.Expr { return bind(">", column(1), bound(100)) }, want: true},
		{name: "ge", expr: func() *planpb.Expr { return bind(">=", column(1), bound(100)) }, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stats := &statsinfo.StatsInfo{
				TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": test.overlap},
			}
			if test.missingStats {
				stats = nil
			}
			compCtx := &zoneMapStatsTestCompilerContext{
				MockCompilerContext: &MockCompilerContext{ctx: ctx},
				stats:               stats,
			}
			if test.statsErr {
				compCtx.err = errors.New("statistics unavailable")
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compCtx, false, false)
			builder.optimizerHints = test.hints
			filter := test.expr()
			// 固定行选择率，使普通列在缺失/高 overlap 时得到块选择率 1。
			filter.Selectivity = 0.5
			node := &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN,
				ObjRef:   &planpb.ObjectRef{Obj: 42},
				TableDef: &planpb.TableDef{
					TblId: 42, Name: "non_primary_zonemap",
					Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
					Cols: []*planpb.ColDef{
						{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int64)}},
						{Name: "k", Seqnum: 1, Typ: planpb.Type{Id: int32(types.T_int64)}},
					},
				},
				FilterList: []*planpb.Expr{filter},
			}
			for range 2 {
				actual := calcScanStats(node, builder)
				if test.missingStats || test.statsErr {
					require.Equal(t, DefaultStats(), actual, "准入不能伪造统计")
				} else {
					blockSel := estimateFilterBlockSelectivity(ctx, filter, node.TableDef, stats)
					require.Equal(t, 60000*blockSel, actual.Cost, "不能靠降低估算成本绕过准入")
				}
				if test.want {
					require.Len(t, node.BlockFilterList, 1, "重复计算不能丢失或重复添加谓词")
					require.True(t, blockFilterSemanticallyEquivalent(filter, node.BlockFilterList[0]))
					require.NotSame(t, filter, node.BlockFilterList[0])
				} else {
					require.Empty(t, node.BlockFilterList)
				}
				require.Same(t, filter, node.FilterList[0], "行过滤必须保留")
			}
		})
	}
}
