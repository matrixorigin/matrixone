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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
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
	inList := func(items ...*planpb.Expr) *planpb.Expr {
		return bind("in", col, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_List{
			List: &planpb.ExprList{List: items},
		}})
	}
	in := inList(bound, MakePlan2Int64ConstExprWithType(150))
	null := &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}}}
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	inVector := func(values []int64) *planpb.Expr {
		v := vector.NewVec(types.T_int64.ToType())
		defer v.Free(mp)
		require.NoError(t, vector.AppendFixedList(v, values, nil, mp))
		data, err := v.MarshalBinary()
		require.NoError(t, err)
		return MakeInExpr(ctx, col, int32(len(values)), data, false)
	}
	for _, tc := range []struct {
		name                               string
		expr                               *planpb.Expr
		hints                              *OptimizerHints
		missing, statsErr, partition, want bool
	}{
		{name: "in list full overlap", expr: in, want: true},
		{name: "in vector", expr: inVector([]int64{100, 150}), want: true},
		{name: "in empty vector", expr: inVector(nil), want: true},
		{name: "in missing stats", expr: in, missing: true, want: true},
		{name: "in stats error", expr: in, statsErr: true, want: true},
		{name: "in disabled", expr: in, hints: &OptimizerHints{blockFilter: 2}},
		{name: "in missing disabled", expr: in, missing: true, hints: &OptimizerHints{blockFilter: 2}},
		{name: "in parameter list", expr: inList(bound, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}), want: true},
		{name: "in nullable list", expr: inList(null, bound), want: true},
		{name: "in all null", expr: inList(null), want: true},
		{name: "in correlated list", expr: inList(bound, GetColExpr(col.Typ, 1, 0))},
		{name: "in volatile list", expr: inList(bind("rand"), bound)},
		{name: "in volatile forced", expr: inList(bind("rand"), bound), hints: &OptimizerHints{blockFilter: 1}},
		{name: "in computed column", expr: bind("in", bind("+", col, bound), DeepCopyExpr(in.GetF().Args[1]))},
		{name: "not in", expr: bind("not", DeepCopyExpr(in))},
		{name: "in partition keeps old admission", expr: in, partition: true},
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

// 通过真实解析/绑定路径证明常量 IN 的列表和折叠向量都能到达块过滤。
func TestPlanNonPrimaryInBlockFilters(t *testing.T) {
	for _, tc := range []struct {
		sql            string
		scans, filters int
		columnType     types.T
	}{
		{"select n_name from nation where n_name in ('2024.12', '2025.12')", 1, 1, 0},
		{"select n_name from nation where n_name in (null, '2025.12', '2024.12', '2025.12')", 1, 1, 0},
		{"select n_regionkey from nation where n_regionkey in (1, 3)", 1, 1, 0},
		{"select n_name from nation where n_name in (select n_name from nation2)", 2, 0, 0},
		{"select n_regionkey from nation where n_regionkey in (100, cast('NaN' as double))", 1, 0, types.T_float64},
		{"select n_regionkey from nation where n_regionkey in (cast('NaN' as double), 100)", 1, 0, types.T_float64},
		{"select n_regionkey from nation where n_regionkey in (100, 150)", 1, 0, types.T_float32},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			mock := NewMockCompilerContext(false)
			// 通用 mock 以 map 遍历序号分配 ID，可能落入系统表保留 ID。
			mock.tables["nation"].TblId = 42001
			mock.tables["nation2"].TblId = 42002
			if tc.columnType != 0 {
				for _, col := range mock.tables["nation"].Cols {
					if col.Name == "n_regionkey" {
						col.Typ = planpb.Type{Id: int32(tc.columnType)}
					}
				}
			}
			ctx := &zoneMapStatsTestCompilerContext{MockCompilerContext: mock, stats: &statsinfo.StatsInfo{
				TableCnt: 60000, BlockNumber: 9,
				ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"n_name": {Overlap: 1}, "n_regionkey": {Overlap: 1}},
			}}
			stmt, err := mysql.ParseOne(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			p, err := BuildPlan(ctx, stmt, false)
			require.NoError(t, err)
			scans, inFilters := 0, 0
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType != planpb.Node_TABLE_SCAN {
					continue
				}
				scans++
				for _, filter := range node.BlockFilterList {
					if filter.GetF().GetFunc().GetObjName() == "in" {
						inFilters++
						require.NotNil(t, filter.GetF().Args[0].GetCol())
						require.True(t, isRuntimeConstExpr(filter.GetF().Args[1]))
					}
				}
			}
			require.Equal(t, tc.scans, scans)
			require.Equal(t, tc.filters, inFilters, "只有稳定常量列表可下推；子查询 IN 不在该准入范围")
		})
	}
}

// 浮点 List/Vec 可能含执行期才出现的 NaN，不能走新增的无估计收益准入。
func TestFloatInKeepsExistingZoneMapAdmission(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	t.Cleanup(func() {
		defer mpool.DeleteMPool(mp)
		require.Zero(t, mp.CurrNB())
	})
	for _, oid := range []types.T{types.T_float32, types.T_float64} {
		t.Run(oid.String(), func(t *testing.T) {
			col := GetColExpr(planpb.Type{Id: int32(oid)}, 1, 1)
			finite, nan := MakePlan2Float64ConstExprWithType(100), MakePlan2Float64ConstExprWithType(math.NaN())
			v := vector.NewVec(oid.ToType())
			defer v.Free(mp)
			if oid == types.T_float32 {
				finite, nan = MakePlan2Float32ConstExprWithType(100), MakePlan2Float32ConstExprWithType(float32(math.NaN()))
				require.NoError(t, vector.AppendFixedList(v, []float32{100, float32(math.NaN())}, nil, mp))
			} else {
				require.NoError(t, vector.AppendFixedList(v, []float64{100, math.NaN()}, nil, mp))
			}
			data, err := v.MarshalBinary()
			require.NoError(t, err)
			list := func(items ...*planpb.Expr) *planpb.Expr {
				expr, err := BindFuncExprImplByPlanExpr(ctx, "in", []*planpb.Expr{col, {
					Typ: col.Typ, Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}},
				}})
				require.NoError(t, err)
				return expr
			}
			for _, tc := range []struct {
				name string
				expr *planpb.Expr
			}{
				{"finite list", list(finite, finite)},
				{"nan last", list(finite, nan)},
				{"nan first", list(nan, finite)},
				{"nan vector", MakeInExpr(ctx, col, 2, data, false)},
				{"parameter", list(finite, &planpb.Expr{Typ: col.Typ, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}})},
			} {
				t.Run(tc.name, func(t *testing.T) {
					require.False(t, isSimpleZoneMapFilter(tc.expr))
					for _, missing := range []bool{false, true} {
						stats := &statsinfo.StatsInfo{TableCnt: 60000, BlockNumber: 9,
							ShuffleRangeMap: map[string]*statsinfo.ShuffleRange{"k": {Overlap: 1}}}
						if missing {
							stats = nil
						}
						builder := NewQueryBuilder(planpb.Query_SELECT, &zoneMapStatsTestCompilerContext{
							MockCompilerContext: &MockCompilerContext{ctx: ctx}, stats: stats,
						}, false, false)
						filter := DeepCopyExpr(tc.expr)
						node := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, ObjRef: &planpb.ObjectRef{Obj: 42},
							TableDef: &planpb.TableDef{TblId: 42, Name: "ordinary",
								Pkey: &planpb.PrimaryKeyDef{Names: []string{"id"}, PkeyColName: "id"},
								Cols: []*planpb.ColDef{{Name: "id", Primary: true, Typ: planpb.Type{Id: int32(types.T_int32)}},
									{Name: "k", Seqnum: 1, Typ: col.Typ}}}, FilterList: []*planpb.Expr{filter}}
						calcScanStats(node, builder)
						require.Empty(t, node.BlockFilterList, "missing statistics: %v", missing)
						require.Same(t, filter, node.FilterList[0])
					}
				})
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
