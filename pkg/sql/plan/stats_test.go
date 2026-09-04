// Copyright 2025 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	index2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestStatsInfoUsableWithoutPersistedObjects(t *testing.T) {
	for _, test := range []struct {
		name  string
		stats *pb.StatsInfo
		want  bool
	}{
		{name: "missing"},
		{name: "empty", stats: &pb.StatsInfo{}},
		{name: "negative table count", stats: &pb.StatsInfo{TableCnt: -1}},
		{name: "nan table count", stats: &pb.StatsInfo{TableCnt: math.NaN()}},
		{name: "infinite table count", stats: &pb.StatsInfo{TableCnt: math.Inf(1)}},
		{name: "persisted object with nan table count", stats: &pb.StatsInfo{
			AccurateObjectNumber: 1, TableCnt: math.NaN(),
		}},
		{name: "completed empty table", stats: &pb.StatsInfo{TableName: "events"}, want: true},
		{name: "persisted objects", stats: &pb.StatsInfo{AccurateObjectNumber: 1}, want: true},
		{name: "committed rows before flush", stats: &pb.StatsInfo{TableCnt: 1}, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, StatsInfoUsable(test.stats))
		})
	}
}

type tableDefStatsTestCompilerContext struct {
	*MockCompilerContext
	stats       *pb.StatsInfo
	gotTableDef *planpb.TableDef
}

func (c *tableDefStatsTestCompilerContext) StatsWithTableDef(
	_ *planpb.ObjectRef,
	tableDef *planpb.TableDef,
	_ *Snapshot,
) (*pb.StatsInfo, error) {
	c.gotTableDef = tableDef
	return c.stats, nil
}

func TestStatsForTableDefUsesVersionAwareCompilerContext(t *testing.T) {
	tableDef := &planpb.TableDef{TblId: 42, Version: 7}
	want := &pb.StatsInfo{TableCnt: 42}
	ctx := &tableDefStatsTestCompilerContext{
		MockCompilerContext: &MockCompilerContext{},
		stats:               want,
	}

	got, err := statsForTableDef(ctx, &planpb.ObjectRef{Obj: 42}, tableDef, nil)
	require.NoError(t, err)
	require.Same(t, want, got)
	require.Same(t, tableDef, ctx.gotTableDef)
}

func TestCalcBlockSelectivityUsingShuffleRangeBareColumn(t *testing.T) {
	t.Run("bare column uses the generic overlap estimate", func(t *testing.T) {
		expr := &planpb.Expr{
			Selectivity: 0.5,
			Expr: &planpb.Expr_Col{
				Col: &planpb.ColRef{Name: "enabled"},
			},
		}

		require.Equal(t, 1.0, calcBlockSelectivityUsingShuffleRange(nil, "enabled", expr))
	})

	t.Run("special function keeps its direct estimate", func(t *testing.T) {
		expr := &planpb.Expr{
			Selectivity: 0.25,
			Expr: &planpb.Expr_F{
				F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "prefix_eq"}},
			},
		}

		require.Equal(t, 0.25, calcBlockSelectivityUsingShuffleRange(nil, "enabled", expr))
	})
}

func TestAssertStatsInheritChildWithoutSelectivityDiscount(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_UPDATE, &MockCompilerContext{ctx: context.Background()}, false, false)
	childStats := &planpb.Stats{
		TableCnt:    1000,
		Outcnt:      750,
		Cost:        1234,
		Selectivity: 0.75,
		BlockNum:    9,
	}
	builder.qry.Nodes = []*planpb.Node{
		{NodeType: planpb.Node_TABLE_SCAN, Stats: childStats},
		{NodeType: planpb.Node_ASSERT, Children: []int32{0}},
	}

	ReCalcNodeStats(1, builder, false, false, false)

	require.Equal(t, childStats, builder.qry.Nodes[1].Stats)
	require.NotSame(t, childStats, builder.qry.Nodes[1].Stats)
}

func TestSafeStatsRatiosAvoidNonFiniteSelectivity(t *testing.T) {
	t.Run("limit never increases cardinality", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats:    &planpb.Stats{Outcnt: 10, Cost: 10, Selectivity: 1},
			Limit:    MakePlan2Uint64ConstExprWithType(100),
		}
		builder.qry.Nodes = []*planpb.Node{node}

		ReCalcNodeStats(0, builder, false, false, false)

		require.Equal(t, float64(10), node.Stats.Outcnt)
	})

	t.Run("offset estimate remains idempotent", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats:    &planpb.Stats{Outcnt: 10, Cost: 10, Selectivity: 1},
			Limit:    MakePlan2Uint64ConstExprWithType(8),
			Offset:   MakePlan2Uint64ConstExprWithType(7),
		}
		builder.qry.Nodes = []*planpb.Node{node}

		ReCalcNodeStats(0, builder, false, false, false)
		ReCalcNodeStats(0, builder, false, false, false)

		require.Equal(t, float64(8), node.Stats.Outcnt)
	})

	t.Run("scan block budget includes offset", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		stats := &planpb.Stats{Outcnt: 100000, Cost: 100000, Selectivity: 1, BlockNum: 20}
		limit := MakePlan2Uint64ConstExprWithType(10)
		offset := MakePlan2Uint64ConstExprWithType(50000)

		applyScanPaginationToStats(stats, limit, offset, builder)
		applyScanPaginationToStats(stats, limit, offset, builder)

		require.Equal(t, float64(10), stats.Outcnt)
		require.Equal(t, int32(7), stats.BlockNum)
	})

	t.Run("limit over zero cost", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats:    &planpb.Stats{},
			Limit: &planpb.Expr{
				Expr: &planpb.Expr_Lit{
					Lit: &planpb.Literal{
						Value: &planpb.Literal_U64Val{U64Val: 10},
					},
				},
			},
		}
		builder.qry.Nodes = []*planpb.Node{node}

		ReCalcNodeStats(0, builder, false, false, false)

		require.True(t, isFinite(node.Stats.Selectivity), "selectivity = %v", node.Stats.Selectivity)
	})

	t.Run("runtime filter over zero table count", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		scanNode := &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN,
			Stats: &planpb.Stats{
				TableCnt: 0,
				Outcnt:   5,
				BlockNum: 1,
			},
		}
		buildNode := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats: &planpb.Stats{
				Outcnt: 10,
			},
		}
		joinNode := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			JoinType: planpb.Node_INDEX,
			Children: []int32{0, 1},
		}
		builder.qry.Nodes = []*planpb.Node{scanNode, buildNode, joinNode}

		recalcStatsByRuntimeFilter(scanNode, joinNode, builder)

		require.True(t, isFinite(scanNode.Stats.Selectivity), "selectivity = %v", scanNode.Stats.Selectivity)
	})

	t.Run("prefix equality over zero table count", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "prefix_eq"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}},
					},
				},
			},
		}
		stats := &pb.StatsInfo{TableCnt: 0}

		selectivity := estimateExprSelectivity(expr, builder, stats)

		require.True(t, isFinite(selectivity), "selectivity = %v", selectivity)
	})

	t.Run("tp force over zero table count", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN,
			Stats: &planpb.Stats{
				TableCnt: 0,
				Outcnt:   10,
				Cost:     10,
			},
		}
		builder.qry.Nodes = []*planpb.Node{node}

		forceScanNodeStatsTP(0, builder)

		require.True(t, isFinite(node.Stats.Selectivity), "selectivity = %v", node.Stats.Selectivity)
	})
}

func TestStatsSelectivityClampAvoidsNonFiniteJoin(t *testing.T) {
	t.Run("not over year equality stays in range", func(t *testing.T) {
		builder := newStatsTestBuilderWithNDV("d", 1)
		col := &planpb.Expr{
			Expr: &planpb.Expr_Col{
				Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "d"},
			},
		}
		yearExpr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "year"},
					Args: []*planpb.Expr{col},
				},
			},
		}
		eqExpr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "="},
					Args: []*planpb.Expr{
						yearExpr,
						{
							Expr: &planpb.Expr_P{
								P: &planpb.ParamRef{Pos: 0},
							},
						},
					},
				},
			},
		}
		notExpr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "not"},
					Args: []*planpb.Expr{eqExpr},
				},
			},
		}

		selectivity := estimateExprSelectivity(notExpr, builder, nil)

		require.True(t, isFinite(selectivity), "selectivity = %v", selectivity)
		require.GreaterOrEqual(t, selectivity, 0.0)
		require.LessOrEqual(t, selectivity, 1.0)
	})

	t.Run("join clamps invalid child selectivity before pow", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		left := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats: &planpb.Stats{
				Outcnt:      10,
				Cost:        10,
				Selectivity: -364,
				BlockNum:    1,
			},
		}
		right := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats: &planpb.Stats{
				Outcnt:      10,
				Cost:        10,
				Selectivity: 0.5,
				BlockNum:    1,
			},
		}
		join := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			JoinType: planpb.Node_INNER,
			Children: []int32{0, 1},
			Stats:    DefaultStats(),
		}
		builder.qry.Nodes = []*planpb.Node{left, right, join}

		ReCalcNodeStats(2, builder, false, false, false)

		require.True(t, isFinite(join.Stats.Selectivity), "selectivity = %v", join.Stats.Selectivity)
		require.GreaterOrEqual(t, join.Stats.Selectivity, 0.0)
		require.LessOrEqual(t, join.Stats.Selectivity, 1.0)
		require.True(t, isFinite(join.Stats.Outcnt), "outcnt = %v", join.Stats.Outcnt)
	})

	t.Run("anti join clamps invalid right selectivity for outcnt", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		left := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats: &planpb.Stats{
				Outcnt:      100,
				Cost:        100,
				Selectivity: 0.5,
				BlockNum:    1,
			},
		}
		right := &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN,
			Stats: &planpb.Stats{
				Outcnt:      10,
				Cost:        10,
				Selectivity: math.Inf(1),
				BlockNum:    1,
			},
		}
		join := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			JoinType: planpb.Node_ANTI,
			Children: []int32{0, 1},
			Stats:    DefaultStats(),
		}
		builder.qry.Nodes = []*planpb.Node{left, right, join}

		ReCalcNodeStats(2, builder, false, false, false)

		require.True(t, isFinite(join.Stats.Outcnt), "outcnt = %v", join.Stats.Outcnt)
		require.Equal(t, 50.0, join.Stats.Outcnt)
		require.GreaterOrEqual(t, join.Stats.Outcnt, 0.0)
		require.True(t, isFinite(join.Stats.Selectivity), "selectivity = %v", join.Stats.Selectivity)
		require.GreaterOrEqual(t, join.Stats.Selectivity, 0.0)
		require.LessOrEqual(t, join.Stats.Selectivity, 1.0)
	})
}

func TestAntiJoinCardinalityUsesPrimaryKeyLowerBound(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	makeEquality := func(leftPos, rightPos int32) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
			GetColExpr(intType, 10, leftPos),
			GetColExpr(intType, 20, rightPos),
		})
		require.NoError(t, err)
		return expr
	}
	makeBuilder := func(onList []*planpb.Expr) (*QueryBuilder, *planpb.Node) {
		left := &planpb.Node{
			NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10},
			TableDef: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "pk1", Typ: intType}, {Name: "pk2", Typ: intType}},
				Name2ColIndex: map[string]int32{"pk1": 0, "pk2": 1},
				Pkey:          &planpb.PrimaryKeyDef{Names: []string{"pk1", "pk2"}},
			},
			Stats: &planpb.Stats{Outcnt: 1000, Cost: 1000, Selectivity: 1, BlockNum: 1},
		}
		right := &planpb.Node{
			NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{20},
			TableDef: &planpb.TableDef{
				Cols:          []*planpb.ColDef{{Name: "k1", Typ: intType}, {Name: "k2", Typ: intType}},
				Name2ColIndex: map[string]int32{"k1": 0, "k2": 1},
			},
			Stats: &planpb.Stats{Outcnt: 100, Cost: 100, Selectivity: 1, BlockNum: 1},
		}
		join := &planpb.Node{
			NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_ANTI,
			Children: []int32{0, 1}, OnList: onList, Stats: DefaultStats(),
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
		builder.qry.Nodes = []*planpb.Node{left, right, join}
		return builder, join
	}

	t.Run("complete primary key bounds the number of eliminated rows", func(t *testing.T) {
		builder, join := makeBuilder([]*planpb.Expr{makeEquality(0, 0), makeEquality(1, 1)})

		ReCalcNodeStats(2, builder, false, false, false)

		require.Equal(t, 900.0, join.Stats.Outcnt)
	})

	t.Run("partial primary key keeps the uncertainty default", func(t *testing.T) {
		builder, join := makeBuilder([]*planpb.Expr{makeEquality(0, 0)})

		ReCalcNodeStats(2, builder, false, false, false)

		require.Equal(t, 500.0, join.Stats.Outcnt)
	})

	t.Run("right primary key does not prove a left-side lower bound", func(t *testing.T) {
		builder, join := makeBuilder([]*planpb.Expr{makeEquality(0, 0), makeEquality(1, 1)})
		builder.qry.Nodes[0].TableDef.Pkey = nil
		builder.qry.Nodes[1].TableDef.Pkey = &planpb.PrimaryKeyDef{Names: []string{"k1", "k2"}}

		ReCalcNodeStats(2, builder, false, false, false)

		require.Equal(t, 500.0, join.Stats.Outcnt)
	})

	t.Run("rollback hint restores the legacy estimate", func(t *testing.T) {
		builder, join := makeBuilder([]*planpb.Expr{makeEquality(0, 0), makeEquality(1, 1)})
		builder.optimizerHints = &OptimizerHints{outerAntiPlanning: 1}

		ReCalcNodeStats(2, builder, false, false, false)

		require.Equal(t, 0.0, join.Stats.Outcnt)
	})

	for _, test := range []struct {
		name       string
		rollback   bool
		wantOutcnt float64
	}{
		{name: "enabled", wantOutcnt: 5},
		{name: "rollback", rollback: true, wantOutcnt: 0},
	} {
		t.Run("right anti after physical swap "+test.name, func(t *testing.T) {
			builder, join := makeBuilder([]*planpb.Expr{makeEquality(0, 0), makeEquality(1, 1)})
			builder.qry.Nodes[0].Stats = &planpb.Stats{
				Outcnt: 10, Cost: 10, Selectivity: 1, BlockNum: 2,
			}
			builder.qry.Nodes[1].Stats = &planpb.Stats{
				Outcnt: 10_000, Cost: 10_000, Selectivity: 1, BlockNum: 100,
			}
			if test.rollback {
				builder.optimizerHints = &OptimizerHints{outerAntiPlanning: 1}
			}

			builder.determineBuildAndProbeSide(2, false)
			require.True(t, join.IsRightJoin)
			builder.swapJoinChildren(2)
			require.Equal(t, []int32{1, 0}, join.Children)
			reCalcNodeStatsAfterSwap(2, builder, false, false, false)

			require.Equal(t, test.wantOutcnt, join.Stats.Outcnt)
			require.LessOrEqual(t, join.Stats.Outcnt, 10.0)
			require.Equal(t, int32(2), join.Stats.BlockNum)
		})
	}
}

func newStatsTestBuilderWithNDV(colName string, ndv float64) *QueryBuilder {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 1000
	stats.NdvMap[colName] = ndv
	statsCache.Set(1, stats)
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.tag2Table[0] = &planpb.TableDef{
		TblId: 1,
		Cols: []*planpb.ColDef{
			{
				Name: colName,
				Typ:  planpb.Type{Id: int32(types.T_date)},
			},
		},
	}
	return builder
}

func TestMissingColumnMapsUseUnknownStatsFallbacks(t *testing.T) {
	builder := newStatsTestBuilderWithNDV("d", 10)
	wrapper := builder.compCtx.GetStatsCache().Get(1)
	stats := wrapper.GetStats()
	delete(stats.NdvMap, "d")
	col := &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "d"}
	expr := &planpb.Expr{Expr: &planpb.Expr_Col{Col: col}}

	require.Equal(t, float64(-1), builder.getColNdv(col))
	require.Equal(t, 0.1, getNullSelectivity(expr, builder, true))
	require.Equal(t, 0.9, getNullSelectivity(expr, builder, false))
}

func TestCompleteStatsSizeMapRejectsPartialGenerations(t *testing.T) {
	tableDef := &planpb.TableDef{Cols: []*planpb.ColDef{
		{Name: "a"}, {Name: "b"}, {Name: "__hidden", Hidden: true},
	}}
	stats := NewStatsInfo()
	stats.SizeMap["a"] = 10
	_, complete := completeStatsSizeMap(stats, tableDef)
	require.False(t, complete)

	stats.SizeMap["b"] = 20
	total, complete := completeStatsSizeMap(stats, tableDef)
	require.True(t, complete)
	require.Equal(t, uint64(30), total)

	stats.SizeMap["a"] = math.MaxUint64
	_, complete = completeStatsSizeMap(stats, tableDef)
	require.False(t, complete)
}

func TestPrimaryKeyStatsShortcutsRequireSQLEqualityCompatibleKey(t *testing.T) {
	tests := []struct {
		name         string
		typ          planpb.Type
		wantHighNDV  bool
		wantNDVRatio float64
	}{
		{
			name:         "integer primary key control",
			typ:          planpb.Type{Id: int32(types.T_int64), NotNullable: true},
			wantHighNDV:  true,
			wantNDVRatio: 1,
		},
		{
			name:         "float signed zero uses measured NDV",
			typ:          planpb.Type{Id: int32(types.T_float64), NotNullable: true},
			wantNDVRatio: 0.1,
		},
		{
			name:         "char pad space uses measured NDV",
			typ:          planpb.Type{Id: int32(types.T_char), Width: 8, NotNullable: true},
			wantNDVRatio: 0.1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newStatsTestBuilderWithNDV("id", 100)
			table := builder.tag2Table[0]
			table.Cols[0].Typ = test.typ
			table.Pkey = &planpb.PrimaryKeyDef{
				PkeyColName: "id",
				Names:       []string{"id"},
			}

			require.Equal(t, test.wantHighNDV, isHighNdvCols([]int32{0}, table, builder))
			require.Equal(t, test.wantNDVRatio, builder.getColNDVRatio([]int32{0}, table))
		})
	}
}

func TestStatsCacheReportsWholeCacheReset(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	for tableID := uint64(0); tableID <= statsCacheMaxSize; tableID++ {
		require.False(t, statsCache.SetAndReportReset(tableID, stats))
	}
	require.True(t, statsCache.SetAndReportReset(statsCacheMaxSize+1, stats))
	removed := statsCache.Get(0)
	retained := statsCache.Get(statsCacheMaxSize + 1)
	require.False(t, removed.Exists())
	require.True(t, retained.Exists())
}

type statsCacheCompilerContext struct {
	*MockCompilerContext
	statsCache *StatsCache
}

func (ctx *statsCacheCompilerContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}

func makeQueryWithScan(tableType string, rowsize float64, blockNum int32) *planpb.Query {
	n := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{TableType: tableType},
		Stats: &planpb.Stats{
			Rowsize:  rowsize,
			BlockNum: blockNum,
		},
	}
	return &planpb.Query{
		Nodes: []*planpb.Node{n},
		Steps: []int32{0},
	}
}

func makeLimitExprForStatsTest() *planpb.Expr {
	return &planpb.Expr{
		Expr: &planpb.Expr_Lit{
			Lit: &planpb.Literal{
				Value: &planpb.Literal_U64Val{U64Val: 10},
			},
		},
	}
}

func TestGetExecType_VectorIndex_WideRows_OneCN(t *testing.T) {
	// rowsize just above threshold, blockNum between oneCN and multiCN thresholds
	q := makeQueryWithScan(catalog.SystemSI_IVFFLAT_TblType_Entries, float64(RowSizeThreshold+1), LargeBlockThresholdForOneCN+1)
	got := GetExecType(q, false, false)
	if got != ExecTypeAP_ONECN {
		t.Fatalf("expected ExecTypeAP_ONECN, got %v", got)
	}
}

func TestGetExecType_VectorIndex_WideRows_MultiCN(t *testing.T) {
	q := makeQueryWithScan(catalog.Hnsw_TblType_Storage, float64(RowSizeThreshold+1), LargeBlockThresholdForMultiCN+1)
	got := GetExecType(q, false, false)
	if got != ExecTypeAP_MULTICN {
		t.Fatalf("expected ExecTypeAP_MULTICN, got %v", got)
	}
}

func TestGetExecType_VectorIndex_WideRows_MultiCNCappedForDDL(t *testing.T) {
	q := makeQueryWithScan(catalog.Hnsw_TblType_Storage, float64(RowSizeThreshold+1), LargeBlockThresholdForMultiCN+1)
	got := GetExecType(q, true, false)
	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_NonVectorTable_NotForcedByRowsize(t *testing.T) {
	// Non-vector tables should not trigger rowsize shortcut; with small blockNum, expect TP
	q := makeQueryWithScan("normal_table", float64(RowSizeThreshold+10), LargeBlockThresholdForOneCN)
	got := GetExecType(q, false, false)
	if got != ExecTypeTP {
		t.Fatalf("expected ExecTypeTP for non-vector table, got %v", got)
	}
}

func TestGetExecType_VectorIndexScanUsesMultiCN(t *testing.T) {
	query := &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeId:          0,
			NodeType:        planpb.Node_VECTOR_INDEX_SCAN,
			Stats:           &planpb.Stats{BlockNum: 1, Cost: 1, Outcnt: 1},
			VectorIndexScan: &planpb.VectorIndexScan{},
		}},
	}
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(query, false, false))
}

func TestGetExecType_VectorIndexScanRespectsOneCNAndDDL(t *testing.T) {
	makeQuery := func(force bool) *planpb.Query {
		return &planpb.Query{
			Steps: []int32{0},
			Nodes: []*planpb.Node{{
				NodeId:          0,
				NodeType:        planpb.Node_VECTOR_INDEX_SCAN,
				Stats:           &planpb.Stats{BlockNum: 1, Cost: 1, Outcnt: 1, ForceOneCN: force},
				VectorIndexScan: &planpb.VectorIndexScan{},
			}},
		}
	}
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(makeQuery(true), false, false))
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(makeQuery(false), true, false))
}

func TestDetermineBuildSidePreservesDeclaredRuntimeFilterDependency(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	builder.qry.Nodes = []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VECTOR_INDEX_SCAN, Stats: &planpb.Stats{Outcnt: 2}},
		{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{Outcnt: 100}},
		{
			NodeId:                 2,
			NodeType:               planpb.Node_JOIN,
			JoinType:               planpb.Node_INNER,
			Children:               []int32{0, 1},
			Stats:                  &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
			RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{{Tag: 1, UseMembershipFilter: true}},
		},
	}
	builder.determineBuildAndProbeSide(2, false)
	require.Equal(t, []int32{0, 1}, builder.qry.Nodes[2].Children)
	require.False(t, builder.qry.Nodes[2].IsRightJoin)
}

func TestDetermineBuildSideCostsUnfilteredIndexByRetainedBytes(t *testing.T) {
	const (
		indexNodeID    int32 = 0
		dataScanNodeID int32 = 1
		dataNodeID     int32 = 2
		joinNodeID     int32 = 3
	)

	newBuilder := func(indexRows, indexRowSize, dataRows, dataRowSize float64, filteredIndex bool) *QueryBuilder {
		statsCache := NewStatsCache()
		dataStats := NewStatsInfo()
		dataStats.TableCnt = dataRows
		dataStats.SizeMap = map[string]uint64{
			"id":      uint64(dataRows * 8),
			"payload": uint64(dataRows * max(dataRowSize-32, 0)),
		}
		statsCache.Set(1, dataStats)
		indexStats := NewStatsInfo()
		indexStats.TableCnt = indexRows
		indexStats.SizeMap = map[string]uint64{
			catalog.IndexTableIndexColName: uint64(indexRows * max(indexRowSize-24, 0)),
		}
		statsCache.Set(2, indexStats)
		ctx := &statsCacheCompilerContext{
			MockCompilerContext: NewMockCompilerContext(false),
			statsCache:          statsCache,
		}
		builder := NewQueryBuilder(planpb.Query_UPDATE, ctx, false, true)
		dataCols := []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: "payload", Typ: planpb.Type{Id: int32(types.T_varchar), Width: 2_454}},
			{Name: catalog.Row_ID, Typ: planpb.Type{Id: int32(types.T_Rowid), Width: 16}, Hidden: true},
		}
		dataProject := make([]*planpb.Expr, len(dataCols))
		for i, col := range dataCols {
			dataProject[i] = &planpb.Expr{
				Typ: col.Typ,
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 10, ColPos: int32(i),
				}},
			}
		}
		indexNode := &planpb.Node{
			NodeId:   indexNodeID,
			NodeType: planpb.Node_TABLE_SCAN,
			TableDef: &planpb.TableDef{
				TblId:     2,
				Name:      catalog.SecondaryIndexTableNamePrefix + "build_cost",
				TableType: catalog.SystemIndexRel,
				Cols: []*planpb.ColDef{
					{Name: catalog.IndexTableIndexColName, Typ: planpb.Type{Id: int32(types.T_varchar)}},
					{Name: catalog.Row_ID, Typ: planpb.Type{Id: int32(types.T_Rowid)}, Hidden: true},
				},
			},
			Stats: &planpb.Stats{Outcnt: indexRows, Rowsize: indexRowSize},
		}
		if filteredIndex {
			indexNode.FilterList = []*planpb.Expr{{}}
		}
		builder.qry.Nodes = []*planpb.Node{
			indexNode,
			{
				NodeId:      dataScanNodeID,
				NodeType:    planpb.Node_TABLE_SCAN,
				BindingTags: []int32{10},
				TableDef:    &planpb.TableDef{TblId: 1, Name: "data", TableType: catalog.SystemOrdinaryRel, Cols: dataCols},
				Stats:       &planpb.Stats{Outcnt: dataRows, Rowsize: dataRowSize},
			},
			{
				NodeId:      dataNodeID,
				NodeType:    planpb.Node_PROJECT,
				Children:    []int32{dataScanNodeID},
				ProjectList: dataProject,
				Stats:       &planpb.Stats{Outcnt: dataRows, Rowsize: 100},
			},
			{
				NodeId:   joinNodeID,
				NodeType: planpb.Node_JOIN,
				JoinType: planpb.Node_INNER,
				Children: []int32{indexNodeID, dataNodeID},
				Stats:    &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
			},
		}
		builder.tag2NodeID[10] = dataScanNodeID
		return builder
	}

	t.Run("equal rows build narrow index", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 20_000_000, 2_454, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("project default width does not hide wider retained target", func(t *testing.T) {
		builder := newBuilder(100_000, 295, 100_000, 2_312, false)
		require.Equal(t, float64(100), builder.qry.Nodes[dataNodeID].Stats.Rowsize)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("project default width does not exaggerate narrow target", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 12, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("index output rowid prevents narrow target regression", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 40, false)
		indexStats := builder.getStatsInfoByTableID(2).GetStats()
		indexStats.SizeMap[catalog.IndexTableIndexColName] = 100_000 * 24
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("rewritten wide value is not costed from its old scan width", func(t *testing.T) {
		builder := newBuilder(100_000, 295, 100_000, 2_312, false)
		builder.qry.Nodes[dataNodeID].ProjectList[1] = &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_varchar), Width: 2_454},
		}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("nested direct projections retain target lineage", func(t *testing.T) {
		builder := newBuilder(100_000, 295, 100_000, 2_312, false)
		outerProjectID := int32(len(builder.qry.Nodes))
		outerProject := &planpb.Node{
			NodeId:      outerProjectID,
			NodeType:    planpb.Node_PROJECT,
			Children:    []int32{dataNodeID},
			BindingTags: []int32{11},
			Stats:       &planpb.Stats{Outcnt: 100_000, Rowsize: 100},
		}
		for i, expr := range builder.qry.Nodes[dataNodeID].ProjectList {
			outerProject.ProjectList = append(outerProject.ProjectList, &planpb.Expr{
				Typ: expr.Typ,
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 11, ColPos: int32(i),
				}},
			})
		}
		builder.qry.Nodes = append(builder.qry.Nodes, outerProject)
		builder.tag2NodeID[11] = dataNodeID
		builder.qry.Nodes[joinNodeID].Children[1] = outerProjectID
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{outerProjectID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("projection that drops target schema keeps existing policy", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 2_312, false)
		builder.qry.Nodes[dataNodeID].ProjectList = builder.qry.Nodes[dataNodeID].ProjectList[:1]
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("projection with mismatched target type keeps existing policy", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 2_312, false)
		builder.qry.Nodes[dataNodeID].ProjectList[1].Typ.Width++
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("cyclic and malformed projection lineage is bounded", func(t *testing.T) {
		for _, test := range []struct {
			name   string
			mutate func(*QueryBuilder)
			want   []int32
		}{
			{name: "cyclic project", mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[dataNodeID].Children[0] = dataNodeID
			}, want: []int32{indexNodeID, dataNodeID}},
			{name: "invalid binding", mutate: func(builder *QueryBuilder) {
				builder.tag2NodeID[10] = int32(len(builder.qry.Nodes) + 1)
			}, want: []int32{indexNodeID, dataNodeID}},
			{name: "one invalid slot leaves a valid lower bound", mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[dataNodeID].ProjectList[0].GetCol().ColPos = 1_000
			}, want: []int32{dataNodeID, indexNodeID}},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(100_000, 32, 100_000, 2_312, false)
				test.mutate(builder)
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, test.want, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("many narrow target columns accumulate retained width", func(t *testing.T) {
		builder := newBuilder(1_000, 64, 1_000, 32, false)
		scan := builder.qry.Nodes[dataScanNodeID]
		project := builder.qry.Nodes[dataNodeID]
		rowIDCol := scan.TableDef.Cols[len(scan.TableDef.Cols)-1]
		scan.TableDef.Cols = scan.TableDef.Cols[:len(scan.TableDef.Cols)-1]
		for i := 0; i < 64; i++ {
			scan.TableDef.Cols = append(scan.TableDef.Cols, &planpb.ColDef{
				Name: "fixed", Typ: planpb.Type{Id: int32(types.T_int64)},
			})
		}
		scan.TableDef.Cols = append(scan.TableDef.Cols, rowIDCol)
		project.ProjectList = make([]*planpb.Expr, len(scan.TableDef.Cols))
		for i, col := range scan.TableDef.Cols {
			project.ProjectList[i] = &planpb.Expr{
				Typ: col.Typ,
				Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
					RelPos: 10, ColPos: int32(i),
				}},
			}
		}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("complete index schema is an upper bound", func(t *testing.T) {
		for _, test := range []struct {
			name     string
			extraCol *planpb.ColDef
		}{
			{name: "observed wide extra column", extraCol: &planpb.ColDef{Name: "extra", Typ: planpb.Type{Id: int32(types.T_varchar)}}},
			{name: "nil column", extraCol: nil},
			{name: "unknown future column", extraCol: &planpb.ColDef{Name: "future", Typ: planpb.Type{Id: 255}}},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(1_000, 64, 1_000, 128, false)
				builder.qry.Nodes[indexNodeID].TableDef.Cols = append(builder.qry.Nodes[indexNodeID].TableDef.Cols, test.extraCol)
				if test.extraCol != nil && test.extraCol.Name == "extra" {
					builder.getStatsInfoByTableID(2).GetStats().SizeMap[test.extraCol.Name] = 1_000 * 256
				}
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("missing variable-width index stats keep existing policy", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 2_312, false)
		delete(builder.getStatsInfoByTableID(2).GetStats().SizeMap, catalog.IndexTableIndexColName)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("unknown target width still supplies a safe lower bound", func(t *testing.T) {
		builder := newBuilder(100_000, 25, 100_000, 2_312, false)
		delete(builder.getStatsInfoByTableID(1).GetStats().SizeMap, "payload")
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("zero variable-width index stats keep existing policy", func(t *testing.T) {
		builder := newBuilder(100_000, 24, 100_000, 2_312, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("fixed target type falls back to physical width", func(t *testing.T) {
		builder := newBuilder(100_000, 32, 100_000, 40, false)
		builder.qry.Nodes[dataScanNodeID].TableDef.Cols[1].Typ = planpb.Type{Id: int32(types.T_int64)}
		builder.qry.Nodes[dataNodeID].ProjectList[1].Typ = planpb.Type{Id: int32(types.T_int64)}
		delete(builder.getStatsInfoByTableID(1).GetStats().SizeMap, "payload")
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("unavailable scan stats keep existing policy", func(t *testing.T) {
		builder := newBuilder(1_000, 32, 1_000, 2_312, false)
		builder.qry.Nodes[dataScanNodeID].Stats = DefaultStats()
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("default target root stats keep existing policy", func(t *testing.T) {
		builder := newBuilder(1_000, 32, 1_000, 2_312, false)
		builder.qry.Nodes[dataNodeID].Stats = DefaultStats()
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("unavailable index stats keep existing policy", func(t *testing.T) {
		builder := newBuilder(1_000, 32, 1_000, 2_312, false)
		builder.qry.Nodes[indexNodeID].Stats = DefaultStats()
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("secondary index identity follows reserved catalog name", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 20_000_000, 2_454, false)
		builder.qry.Nodes[indexNodeID].TableDef.TableType = catalog.SystemOrdinaryRel
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("stale larger index cardinality does not suppress width win", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 1_000_000, 2_454, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("stale larger right index cardinality does not undo width win", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 1_000_000, 2_454, false)
		builder.qry.Nodes[joinNodeID].Children = []int32{dataNodeID, indexNodeID}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("dominating right index remains build side", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 20_000_000, 2_454, false)
		builder.qry.Nodes[joinNodeID].Children = []int32{dataNodeID, indexNodeID}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("equal narrow widths preserve target build despite stale rows", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 10, 32, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("filtered index keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, true)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("block-filtered index keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[indexNodeID].BlockFilterList = []*planpb.Expr{{}}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("paginated index keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[indexNodeID].Limit = makePlan2Int64ConstExprWithType(10)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("offset index keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[indexNodeID].Offset = makePlan2Int64ConstExprWithType(10)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("filtered target keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataScanNodeID].FilterList = []*planpb.Expr{{}}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("literal true filters remain unrestricted", func(t *testing.T) {
		for _, test := range []struct {
			name        string
			filterIndex bool
		}{
			{name: "target", filterIndex: false},
			{name: "index", filterIndex: true},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(20, 32, 20, 2_454, false)
				if test.filterIndex {
					builder.qry.Nodes[indexNodeID].FilterList = []*planpb.Expr{MakePlan2BoolConstExprWithType(true)}
				} else {
					builder.qry.Nodes[dataScanNodeID].FilterList = []*planpb.Expr{MakePlan2BoolConstExprWithType(true)}
				}
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("literal false and mixed filters remain selective", func(t *testing.T) {
		for _, test := range []struct {
			name    string
			filters []*planpb.Expr
		}{
			{name: "false", filters: []*planpb.Expr{MakePlan2BoolConstExprWithType(false)}},
			{name: "true and false", filters: []*planpb.Expr{MakePlan2BoolConstExprWithType(true), MakePlan2BoolConstExprWithType(false)}},
			{name: "nil", filters: []*planpb.Expr{nil}},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(20, 32, 20, 2_454, false)
				builder.qry.Nodes[dataScanNodeID].FilterList = test.filters
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("block-filtered target keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataScanNodeID].BlockFilterList = []*planpb.Expr{{}}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("paginated target keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataNodeID].Limit = makePlan2Int64ConstExprWithType(10)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("offset target keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataNodeID].Offset = makePlan2Int64ConstExprWithType(10)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("joined update target keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		joinedScanID := int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeId: joinedScanID, NodeType: planpb.Node_TABLE_SCAN,
			TableDef: &planpb.TableDef{Name: "joined", TableType: catalog.SystemOrdinaryRel},
			Stats:    &planpb.Stats{Outcnt: 20, Rowsize: 32},
		})
		joinedInputID := int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeId: joinedInputID, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{dataScanNodeID, joinedScanID},
			Stats:    &planpb.Stats{Outcnt: 20, Rowsize: 2_454, HashmapStats: &planpb.HashMapStats{}},
		})
		builder.qry.Nodes[dataNodeID].Children = []int32{joinedInputID}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("prior secondary maintenance join remains eligible", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		priorIndexID := int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeId: priorIndexID, NodeType: planpb.Node_TABLE_SCAN,
			TableDef: &planpb.TableDef{
				Name: catalog.SecondaryIndexTableNamePrefix + "prior", TableType: catalog.SystemIndexRel,
			},
			Stats: &planpb.Stats{Outcnt: 20, Rowsize: 32},
		})
		priorJoinID := int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeId: priorJoinID, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{dataNodeID, priorIndexID},
			Stats:    &planpb.Stats{Outcnt: 20, Rowsize: 2_500, HashmapStats: &planpb.HashMapStats{}},
		})
		builder.qry.Nodes[joinNodeID].Children = []int32{indexNodeID, priorJoinID}
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{priorJoinID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("ordinary join keeps existing cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[indexNodeID].TableDef.Name = "ordinary_table"
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("internal table cannot stand in for update target", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataScanNodeID].TableDef.Name = catalog.UniqueIndexTableNamePrefix + "not_a_target"
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("nonordinary table cannot stand in for update target", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[dataScanNodeID].TableDef.TableType = catalog.SystemClusterRel
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("unique hidden index is outside regular secondary maintenance policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 2_454, false)
		builder.qry.Nodes[indexNodeID].TableDef.Name = catalog.UniqueIndexTableNamePrefix + "build_cost"
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("fewer index rows but wider index preserves cardinality policy", func(t *testing.T) {
		builder := newBuilder(10, 2_454, 20, 32, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("equal retained bytes preserve cardinality policy", func(t *testing.T) {
		builder := newBuilder(20, 32, 20, 32, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("statement join orientation and eligibility cross product", func(t *testing.T) {
		stmtTypes := []planpb.Query_StatementType{
			planpb.Query_UNKNOWN,
			planpb.Query_SELECT,
			planpb.Query_INSERT,
			planpb.Query_DELETE,
			planpb.Query_UPDATE,
			planpb.Query_MERGE,
		}
		joinTypes := []planpb.Node_JoinType{
			planpb.Node_INNER,
			planpb.Node_LEFT,
			planpb.Node_RIGHT,
			planpb.Node_OUTER,
			planpb.Node_SEMI,
			planpb.Node_ANTI,
			planpb.Node_SINGLE,
			planpb.Node_MARK,
			planpb.Node_INDEX,
			planpb.Node_L2,
			planpb.Node_DEDUP,
			planpb.Node_ASOF,
			planpb.Node_ASOF_LEFT,
		}
		type shape struct {
			name                                string
			indexWidth, targetWidth             float64
			filterIndex, filterTarget, eligible bool
		}
		shapes := []shape{
			{name: "eligible wide target", indexWidth: 32, targetWidth: 2_454, eligible: true},
			{name: "filtered index", indexWidth: 32, targetWidth: 2_454, filterIndex: true},
			{name: "filtered target", indexWidth: 32, targetWidth: 2_454, filterTarget: true},
			{name: "index not narrower", indexWidth: 64, targetWidth: 64},
		}

		for _, stmtType := range stmtTypes {
			for _, joinType := range joinTypes {
				for _, indexOnLeft := range []bool{false, true} {
					for _, shape := range shapes {
						builder := newBuilder(20, shape.indexWidth, 20, shape.targetWidth, shape.filterIndex)
						builder.qry.StmtType = stmtType
						builder.qry.Nodes[joinNodeID].JoinType = joinType
						if !indexOnLeft {
							builder.qry.Nodes[joinNodeID].Children = []int32{dataNodeID, indexNodeID}
						}
						if shape.filterTarget {
							builder.qry.Nodes[dataScanNodeID].FilterList = []*planpb.Expr{{}}
						}

						reference := newBuilder(20, shape.indexWidth, 20, shape.targetWidth, shape.filterIndex)
						reference.qry.StmtType = planpb.Query_SELECT
						reference.qry.Nodes[joinNodeID].JoinType = joinType
						if !indexOnLeft {
							reference.qry.Nodes[joinNodeID].Children = []int32{dataNodeID, indexNodeID}
						}
						if shape.filterTarget {
							reference.qry.Nodes[dataScanNodeID].FilterList = []*planpb.Expr{{}}
						}

						builder.determineBuildAndProbeSide(joinNodeID, false)
						reference.determineBuildAndProbeSide(joinNodeID, false)
						context := "stmt=%s join=%s indexOnLeft=%t shape=%s"
						args := []any{stmtType, joinType, indexOnLeft, shape.name}
						if stmtType == planpb.Query_UPDATE && joinType == planpb.Node_INNER && shape.eligible {
							require.Equalf(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children, context, args...)
						} else {
							require.Equalf(t, reference.qry.Nodes[joinNodeID].Children, builder.qry.Nodes[joinNodeID].Children, context, args...)
							require.Equalf(t, reference.qry.Nodes[joinNodeID].IsRightJoin, builder.qry.Nodes[joinNodeID].IsRightJoin, context, args...)
						}
					}
				}
			}
		}
	})

	t.Run("eligible decision is invariant across data scale and stats drift", func(t *testing.T) {
		for _, test := range []struct {
			name                  string
			indexRows, targetRows float64
		}{
			{name: "one row", indexRows: 1, targetRows: 1},
			{name: "one thousand rows", indexRows: 1_000, targetRows: 1_000},
			{name: "one million rows", indexRows: 1_000_000, targetRows: 1_000_000},
			{name: "index stats much larger", indexRows: 1_000_000_000, targetRows: 1_000},
			{name: "target stats much larger", indexRows: 1_000, targetRows: 1_000_000_000},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(test.indexRows, 64, test.targetRows, 2_454, false)
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("retained width boundary is strict", func(t *testing.T) {
		for _, test := range []struct {
			name         string
			indexRowSize float64
			want         []int32
		}{
			{name: "just narrower", indexRowSize: 63.999, want: []int32{dataNodeID, indexNodeID}},
			{name: "equal", indexRowSize: 64, want: []int32{indexNodeID, dataNodeID}},
			{name: "just wider", indexRowSize: 64.001, want: []int32{indexNodeID, dataNodeID}},
		} {
			t.Run(test.name, func(t *testing.T) {
				builder := newBuilder(1_000, test.indexRowSize, 1_000, 64, false)
				builder.determineBuildAndProbeSide(joinNodeID, false)
				require.Equal(t, test.want, builder.qry.Nodes[joinNodeID].Children)
			})
		}
	})

	t.Run("invalid width falls back to cardinality", func(t *testing.T) {
		builder := newBuilder(10, math.NaN(), 20, 2_454, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("invalid target scan row count falls back to cardinality", func(t *testing.T) {
		builder := newBuilder(10, 32, 20, 2_454, false)
		builder.qry.Nodes[dataScanNodeID].Stats.Outcnt = math.NaN()
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("empty index is a valid zero-byte build", func(t *testing.T) {
		builder := newBuilder(0, 32, 20_000_000, 2_454, false)
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{dataNodeID, indexNodeID}, builder.qry.Nodes[joinNodeID].Children)
	})

	t.Run("full outer join remains directional", func(t *testing.T) {
		builder := newBuilder(20_000_000, 32, 20_000_000, 2_454, false)
		builder.qry.Nodes[joinNodeID].JoinType = planpb.Node_OUTER
		builder.determineBuildAndProbeSide(joinNodeID, false)
		require.Equal(t, []int32{indexNodeID, dataNodeID}, builder.qry.Nodes[joinNodeID].Children)
		require.True(t, builder.qry.Nodes[joinNodeID].IsRightJoin)
	})
}

func TestEstimatedHashBuildRetainedBytesRejectsUnsafeStats(t *testing.T) {
	for _, test := range []struct {
		name string
		node *planpb.Node
	}{
		{name: "nil node"},
		{name: "nil stats", node: &planpb.Node{}},
		{name: "default stats", node: &planpb.Node{Stats: DefaultStats()}},
		{name: "negative rows", node: &planpb.Node{Stats: &planpb.Stats{Outcnt: -1, Rowsize: 32}}},
		{name: "nan rows", node: &planpb.Node{Stats: &planpb.Stats{Outcnt: math.NaN(), Rowsize: 32}}},
		{name: "infinite width", node: &planpb.Node{Stats: &planpb.Stats{Outcnt: 1, Rowsize: math.Inf(1)}}},
		{name: "zero width", node: &planpb.Node{Stats: &planpb.Stats{Outcnt: 1}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, ok := estimatedHashBuildRetainedBytes(test.node)
			require.False(t, ok)
		})
	}

	t.Run("overflow saturates", func(t *testing.T) {
		got, ok := estimatedHashBuildRetainedBytes(&planpb.Node{
			Stats: &planpb.Stats{Outcnt: math.MaxFloat64, Rowsize: 2},
		})
		require.True(t, ok)
		require.Equal(t, math.MaxFloat64, got)
	})

}

func TestFixedTypeRetainedBytesSchemaMatrix(t *testing.T) {
	for _, test := range []struct {
		name string
		oid  types.T
		want float64
	}{
		{name: "bit", oid: types.T_bit, want: 8},
		{name: "bool", oid: types.T_bool, want: 1},
		{name: "int8", oid: types.T_int8, want: 1},
		{name: "int16", oid: types.T_int16, want: 2},
		{name: "int32", oid: types.T_int32, want: 4},
		{name: "int64", oid: types.T_int64, want: 8},
		{name: "uint8", oid: types.T_uint8, want: 1},
		{name: "uint16", oid: types.T_uint16, want: 2},
		{name: "uint32", oid: types.T_uint32, want: 4},
		{name: "uint64", oid: types.T_uint64, want: 8},
		{name: "float32", oid: types.T_float32, want: 4},
		{name: "float64", oid: types.T_float64, want: 8},
		{name: "decimal64", oid: types.T_decimal64, want: 8},
		{name: "decimal128", oid: types.T_decimal128, want: 16},
		{name: "decimal256", oid: types.T_decimal256, want: 32},
		{name: "date", oid: types.T_date, want: 4},
		{name: "time", oid: types.T_time, want: 8},
		{name: "datetime", oid: types.T_datetime, want: 8},
		{name: "timestamp", oid: types.T_timestamp, want: 8},
		{name: "year", oid: types.T_year, want: 2},
		{name: "uuid", oid: types.T_uuid, want: 16},
		{name: "enum", oid: types.T_enum, want: 2},
		{name: "transaction timestamp", oid: types.T_TS, want: 12},
		{name: "row id", oid: types.T_Rowid, want: 24},
		{name: "block id", oid: types.T_Blockid, want: 20},
		{name: "object id", oid: types.T_Objectid, want: 18},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := fixedTypeRetainedBytes(test.oid)
			require.True(t, ok)
			require.Equal(t, test.want, got)
		})
	}

	for _, test := range []struct {
		name string
		oid  types.T
	}{
		{name: "any", oid: types.T_any},
		{name: "star", oid: types.T_star},
		{name: "reserved int128", oid: types.T_int128},
		{name: "reserved uint128", oid: types.T_uint128},
		{name: "interval expression", oid: types.T_interval},
		{name: "tuple", oid: types.T_tuple},
		{name: "unknown future type", oid: types.T(255)},
		{name: "char needs observed stats", oid: types.T_char},
		{name: "varchar needs observed stats", oid: types.T_varchar},
		{name: "json needs observed stats", oid: types.T_json},
		{name: "binary needs observed stats", oid: types.T_binary},
		{name: "varbinary needs observed stats", oid: types.T_varbinary},
		{name: "geometry needs observed stats", oid: types.T_geometry},
		{name: "geometry32 needs observed stats", oid: types.T_geometry32},
		{name: "blob needs observed stats", oid: types.T_blob},
		{name: "text needs observed stats", oid: types.T_text},
		{name: "datalink needs observed stats", oid: types.T_datalink},
		{name: "float32 vector needs observed stats", oid: types.T_array_float32},
		{name: "float64 vector needs observed stats", oid: types.T_array_float64},
		{name: "bf16 vector needs observed stats", oid: types.T_array_bf16},
		{name: "float16 vector needs observed stats", oid: types.T_array_float16},
		{name: "int8 vector needs observed stats", oid: types.T_array_int8},
		{name: "uint8 vector needs observed stats", oid: types.T_array_uint8},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := fixedTypeRetainedBytes(test.oid)
			require.False(t, ok)
			require.Zero(t, got)
		})
	}

	for id := 0; id <= int(^uint8(0)); id++ {
		got, ok := fixedTypeRetainedBytes(types.T(id))
		if ok {
			require.Greater(t, got, float64(0), "type id %d", id)
			require.False(t, math.IsNaN(got), "type id %d", id)
			require.False(t, math.IsInf(got, 0), "type id %d", id)
		} else {
			require.Zero(t, got, "type id %d", id)
		}
	}
}

func TestHasRecursiveScanHandlesGeneralPlanGraphs(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false), false, true)
	builder.qry.Nodes = []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN},
		{NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}},
		{NodeId: 2, NodeType: planpb.Node_JOIN, Children: []int32{1, 1}},
		{NodeId: 3, NodeType: planpb.Node_RECURSIVE_SCAN},
		{NodeId: 4, NodeType: planpb.Node_PROJECT, Children: []int32{4}},
		{NodeId: 5, NodeType: planpb.Node_PROJECT, Children: []int32{-1, 999}},
		{NodeId: 6, NodeType: planpb.Node_PROJECT, Children: []int32{2, 3}},
	}

	require.False(t, builder.hasRecursiveScan(nil))
	require.False(t, builder.hasRecursiveScan(builder.qry.Nodes[0]))
	require.False(t, builder.hasRecursiveScan(builder.qry.Nodes[2]))
	require.False(t, builder.hasRecursiveScan(builder.qry.Nodes[4]))
	require.False(t, builder.hasRecursiveScan(builder.qry.Nodes[5]))
	require.True(t, builder.hasRecursiveScan(builder.qry.Nodes[3]))
	require.True(t, builder.hasRecursiveScan(builder.qry.Nodes[6]))
}

func TestDetermineBuildSidePreservesCTEHashBuildDrainProof(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64)}
	joinCond, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(intType, 10, 0),
		GetColExpr(intType, 20, 0),
	})
	require.NoError(t, err)

	makeBuilder := func(scanOption string) *QueryBuilder {
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
		builder.qry.Nodes = []*planpb.Node{
			{
				NodeId: 0, NodeType: planpb.Node_TABLE_SCAN,
				BindingTags: []int32{10}, Stats: &planpb.Stats{Outcnt: 10},
			},
			{
				NodeId: 1, NodeType: planpb.Node_SINK_SCAN,
				BindingTags: []int32{20}, Stats: &planpb.Stats{Outcnt: 1000},
				ExtraOptions: scanOption,
			},
			{
				NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_SEMI,
				Children: []int32{0, 1}, OnList: []*planpb.Expr{joinCond},
				Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
			},
		}
		return builder
	}

	t.Run("marked build remains logical right", func(t *testing.T) {
		builder := makeBuilder(materialized.CTEHashBuildScanOption)
		builder.determineBuildAndProbeSide(2, false)
		require.False(t, builder.qry.Nodes[2].IsRightJoin)
	})

	t.Run("unmarked build retains cost choice", func(t *testing.T) {
		builder := makeBuilder("")
		builder.determineBuildAndProbeSide(2, false)
		require.True(t, builder.qry.Nodes[2].IsRightJoin)
	})

	t.Run("marked LEFT build remains logical right", func(t *testing.T) {
		builder := makeBuilder(materialized.CTEHashBuildScanOption)
		builder.qry.Nodes[2].JoinType = planpb.Node_LEFT
		builder.determineBuildAndProbeSide(2, false)
		require.False(t, builder.qry.Nodes[2].IsRightJoin)
	})
}

func TestDetermineInnerBuildSidePreservesCTEHashBuildDrainProof(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64)}
	joinCond, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(intType, 10, 0),
		GetColExpr(intType, 20, 0),
	})
	require.NoError(t, err)

	makeBuilder := func(leftOption, rightOption string) *QueryBuilder {
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
		builder.qry.Nodes = []*planpb.Node{
			{
				NodeId: 0, NodeType: planpb.Node_SINK_SCAN,
				BindingTags: []int32{10}, Stats: &planpb.Stats{Outcnt: 10},
				ExtraOptions: leftOption,
			},
			{
				NodeId: 1, NodeType: planpb.Node_SINK_SCAN,
				BindingTags: []int32{20}, Stats: &planpb.Stats{Outcnt: 1000},
				ExtraOptions: rightOption,
			},
			{
				NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{0, 1}, OnList: []*planpb.Expr{joinCond},
				Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}},
			},
		}
		return builder
	}

	t.Run("marked logical left moves to build", func(t *testing.T) {
		builder := makeBuilder(materialized.CTEHashBuildScanOption, "")
		builder.determineBuildAndProbeSide(2, false)
		require.Equal(t, []int32{1, 0}, builder.qry.Nodes[2].Children)
	})

	t.Run("marked logical right remains build", func(t *testing.T) {
		builder := makeBuilder("", materialized.CTEHashBuildScanOption)
		builder.determineBuildAndProbeSide(2, false)
		require.Equal(t, []int32{0, 1}, builder.qry.Nodes[2].Children)
	})

	t.Run("marked logical right remains shuffle build", func(t *testing.T) {
		builder := makeBuilder("", materialized.CTEHashBuildScanOption)
		builder.qry.Nodes[2].Stats.HashmapStats.Shuffle = true
		builder.determineBuildAndProbeSide(2, false)
		require.Equal(t, []int32{0, 1}, builder.qry.Nodes[2].Children)
		require.True(t, builder.qry.Nodes[2].Stats.HashmapStats.Shuffle)
	})
}

func TestDeepCopyIndexReaderParamCopiesOrigFuncName(t *testing.T) {
	oldParam := &planpb.IndexReaderParam{
		OrigFuncName:   "l2_distance",
		Limit:          makeLimitExprForStatsTest(),
		PartitionCnCnt: 2,
		PartitionCnIdx: 1,
		DistRange: &planpb.DistRange{
			LowerBoundType: planpb.BoundType_EXCLUSIVE,
			LowerBound:     makeLimitExprForStatsTest(),
		},
	}

	got := DeepCopyIndexReaderParam(oldParam)

	require.NotSame(t, oldParam, got)
	require.Equal(t, oldParam.OrigFuncName, got.OrigFuncName)
	require.Equal(t, oldParam.PartitionCnCnt, got.PartitionCnCnt)
	require.Equal(t, oldParam.PartitionCnIdx, got.PartitionCnIdx)
	require.NotSame(t, oldParam.Limit, got.Limit)
	require.NotSame(t, oldParam.DistRange, got.DistRange)
}

// TestUpdateStatsInfo_Decimal64_NegativeValues tests that negative decimal64 values
// are correctly converted to float64 for statistics
func TestUpdateStatsInfo_Decimal64_NegativeValues(t *testing.T) {
	// Test case: DECIMAL(10, 2) with negative values
	// Example: -123.45 and 456.78
	scale := int32(2)

	// Create negative decimal: -123.45
	negativeValue, err := types.Decimal64FromFloat64(-123.45, 10, scale)
	require.NoError(t, err)

	// Create positive decimal: 456.78
	positiveValue, err := types.Decimal64FromFloat64(456.78, 10, scale)
	require.NoError(t, err)

	// Create zonemap with negative min and positive max
	zm := index2.NewZM(types.T_decimal64, scale)
	minBuf := types.EncodeDecimal64(&negativeValue)
	maxBuf := types.EncodeDecimal64(&positiveValue)
	index2.UpdateZM(zm, minBuf)
	index2.UpdateZM(zm, maxBuf)

	// Create table definition with decimal64 column
	tableDef := &planpb.TableDef{
		Name: "test_table",
		Cols: []*planpb.ColDef{
			{
				Name: "balance",
				Typ: planpb.Type{
					Id:    int32(types.T_decimal64),
					Scale: scale,
					Width: 10,
				},
			},
			{
				Name: catalog.Row_ID,
			},
		},
	}

	// Create TableStatsInfo
	info := &TableStatsInfo{
		ColumnZMs:     []index2.ZM{zm},
		DataTypes:     []types.Type{types.New(types.T_decimal64, 10, scale)},
		ColumnNDVs:    []float64{2},
		NullCnts:      []int64{0},
		ColumnSize:    []int64{8},
		ShuffleRanges: []*pb.ShuffleRange{nil},
	}

	// Create StatsInfo
	statsInfo := &pb.StatsInfo{
		MinValMap:       make(map[string]float64),
		MaxValMap:       make(map[string]float64),
		NdvMap:          make(map[string]float64),
		DataTypeMap:     make(map[string]uint64),
		NullCntMap:      make(map[string]uint64),
		SizeMap:         make(map[string]uint64),
		ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
	}

	// Call UpdateStatsInfo
	UpdateStatsInfo(info, tableDef, statsInfo)

	// Verify results
	minVal := statsInfo.MinValMap["balance"]
	maxVal := statsInfo.MaxValMap["balance"]

	// The key assertion: min should be less than max
	require.Less(t, minVal, maxVal, "Min value should be less than max value")

	// Verify approximate values (allowing for floating point precision)
	require.InDelta(t, -123.45, minVal, 0.01, "Min value should be approximately -123.45")
	require.InDelta(t, 456.78, maxVal, 0.01, "Max value should be approximately 456.78")

	// Before the fix, minVal would have been a huge positive number like 18446744073514074000
	// This check ensures that didn't happen
	require.Greater(t, minVal, -1000.0, "Min value should not be an extremely large number")
	require.Less(t, minVal, 0.0, "Min value should be negative")
}

// TestUpdateStatsInfo_Decimal128_NegativeValues tests that negative decimal128 values
// are correctly converted with proper scale
func TestUpdateStatsInfo_Decimal128_NegativeValues(t *testing.T) {
	// Test case: DECIMAL(20, 4) with negative values
	scale := int32(4)

	// Create negative decimal: -9876543210.1234
	negativeValue, err := types.Decimal128FromFloat64(-9876543210.1234, 20, scale)
	require.NoError(t, err)

	// Create positive decimal: 1234567890.5678
	positiveValue, err := types.Decimal128FromFloat64(1234567890.5678, 20, scale)
	require.NoError(t, err)

	// Create zonemap
	zm := index2.NewZM(types.T_decimal128, scale)
	minBuf := types.EncodeDecimal128(&negativeValue)
	maxBuf := types.EncodeDecimal128(&positiveValue)
	index2.UpdateZM(zm, minBuf)
	index2.UpdateZM(zm, maxBuf)

	// Create table definition
	tableDef := &planpb.TableDef{
		Name: "test_table",
		Cols: []*planpb.ColDef{
			{
				Name: "amount",
				Typ: planpb.Type{
					Id:    int32(types.T_decimal128),
					Scale: scale,
					Width: 20,
				},
			},
			{
				Name: catalog.Row_ID,
			},
		},
	}

	// Create TableStatsInfo
	info := &TableStatsInfo{
		ColumnZMs:     []index2.ZM{zm},
		DataTypes:     []types.Type{types.New(types.T_decimal128, 20, scale)},
		ColumnNDVs:    []float64{2},
		NullCnts:      []int64{0},
		ColumnSize:    []int64{16},
		ShuffleRanges: []*pb.ShuffleRange{nil},
	}

	// Create StatsInfo
	statsInfo := &pb.StatsInfo{
		MinValMap:       make(map[string]float64),
		MaxValMap:       make(map[string]float64),
		NdvMap:          make(map[string]float64),
		DataTypeMap:     make(map[string]uint64),
		NullCntMap:      make(map[string]uint64),
		SizeMap:         make(map[string]uint64),
		ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
	}

	// Call UpdateStatsInfo
	UpdateStatsInfo(info, tableDef, statsInfo)

	// Verify results
	minVal := statsInfo.MinValMap["amount"]
	maxVal := statsInfo.MaxValMap["amount"]

	// The key assertion: min should be less than max
	require.Less(t, minVal, maxVal, "Min value should be less than max value")

	// Verify approximate values
	require.InDelta(t, -9876543210.1234, minVal, 0.01, "Min value should be approximately -9876543210.1234")
	require.InDelta(t, 1234567890.5678, maxVal, 0.01, "Max value should be approximately 1234567890.5678")

	// Ensure min is negative and within reasonable range
	require.Less(t, minVal, 0.0, "Min value should be negative")
}

// TestUpdateStatsInfo_ShuffleRangeWrittenWhenZoneMapNotInited tests the fix for the bug where
// UpdateStatsInfo used to continue (skip the rest of the loop) when ColumnZMs[i].IsInited() was false,
// so that even when collect had populated info.ShuffleRanges[i], it was never written to s.ShuffleRangeMap.
// After the fix, we only skip decoding min/max from ZoneMap when not inited; we still run the ShuffleRange
// block and allow writing when canFill is true (relaxed so that !IsInited() does not block the write).
func TestUpdateStatsInfo_ShuffleRangeWrittenWhenZoneMapNotInited(t *testing.T) {
	// ZoneMap left not inited (no UpdateZM call) — simulates collect seeing objects where this column's ZM was never inited.
	zm := index2.NewZM(types.T_int64, 0)
	require.False(t, zm.IsInited(), "ZoneMap must be not inited to test this path")

	// ShuffleRange produced by collect (e.g. from NDV accumulation in later objects).
	sr := NewShuffleRange(false)
	sr.Update(1, 100000, 200000, 0)

	// TableCnt and NDV above thresholds so canFill can be true (ShuffleThreshHoldOfNDV = 50000).
	tableCnt := 200000.0
	colNDV := 60000.0

	tableDef := &planpb.TableDef{
		Name: "test_table",
		Cols: []*planpb.ColDef{
			{Name: "id", Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Name: catalog.Row_ID},
		},
	}

	info := &TableStatsInfo{
		ColumnZMs:     []index2.ZM{zm},
		DataTypes:     []types.Type{types.New(types.T_int64, 0, 0)},
		ColumnNDVs:    []float64{colNDV},
		NullCnts:      []int64{0},
		ColumnSize:    []int64{8},
		ShuffleRanges: []*pb.ShuffleRange{sr},
		TableRowCount: tableCnt,
	}

	s := &pb.StatsInfo{
		MinValMap:       make(map[string]float64),
		MaxValMap:       make(map[string]float64),
		NdvMap:          make(map[string]float64),
		DataTypeMap:     make(map[string]uint64),
		NullCntMap:      make(map[string]uint64),
		SizeMap:         make(map[string]uint64),
		ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
	}

	UpdateStatsInfo(info, tableDef, s)

	// Before the fix: continue skipped the ShuffleRange block, so ShuffleRangeMap stayed empty.
	require.NotNil(t, s.ShuffleRangeMap["id"], "ShuffleRange must be written when ZoneMap is not inited but ShuffleRange is present and other canFill conditions are met")
	require.Nil(t, info.ShuffleRanges[0], "UpdateStatsInfo nils out info.ShuffleRanges after copying to s")
}

// TestUpdateStatsInfo_Decimal_DifferentScales tests decimal conversion with various scales
func TestUpdateStatsInfo_Decimal_DifferentScales(t *testing.T) {
	testCases := []struct {
		name     string
		scale    int32
		minFloat float64
		maxFloat float64
	}{
		{"scale_0", 0, -100.0, 200.0},
		{"scale_2", 2, -99.99, 199.99},
		{"scale_4", 4, -1234.5678, 5678.1234},
		{"scale_6", 6, -0.123456, 0.987654},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create decimal values
			minDec, err := types.Decimal64FromFloat64(tc.minFloat, 18, tc.scale)
			require.NoError(t, err)

			maxDec, err := types.Decimal64FromFloat64(tc.maxFloat, 18, tc.scale)
			require.NoError(t, err)

			// Create zonemap
			zm := index2.NewZM(types.T_decimal64, tc.scale)
			minBuf := types.EncodeDecimal64(&minDec)
			maxBuf := types.EncodeDecimal64(&maxDec)
			index2.UpdateZM(zm, minBuf)
			index2.UpdateZM(zm, maxBuf)

			// Create table definition
			tableDef := &planpb.TableDef{
				Name: "test_table",
				Cols: []*planpb.ColDef{
					{
						Name: "value",
						Typ: planpb.Type{
							Id:    int32(types.T_decimal64),
							Scale: tc.scale,
							Width: 18,
						},
					},
					{
						Name: catalog.Row_ID,
					},
				},
			}

			// Create TableStatsInfo
			info := &TableStatsInfo{
				ColumnZMs:     []index2.ZM{zm},
				DataTypes:     []types.Type{types.New(types.T_decimal64, 18, tc.scale)},
				ColumnNDVs:    []float64{2},
				NullCnts:      []int64{0},
				ColumnSize:    []int64{8},
				ShuffleRanges: []*pb.ShuffleRange{nil},
			}

			// Create StatsInfo
			statsInfo := &pb.StatsInfo{
				MinValMap:       make(map[string]float64),
				MaxValMap:       make(map[string]float64),
				NdvMap:          make(map[string]float64),
				DataTypeMap:     make(map[string]uint64),
				NullCntMap:      make(map[string]uint64),
				SizeMap:         make(map[string]uint64),
				ShuffleRangeMap: make(map[string]*pb.ShuffleRange),
			}

			// Call UpdateStatsInfo
			UpdateStatsInfo(info, tableDef, statsInfo)

			// Verify results
			minVal := statsInfo.MinValMap["value"]
			maxVal := statsInfo.MaxValMap["value"]

			require.Less(t, minVal, maxVal, "Min value should be less than max value")
			require.InDelta(t, tc.minFloat, minVal, 0.01, "Min value mismatch")
			require.InDelta(t, tc.maxFloat, maxVal, 0.01, "Max value mismatch")
		})
	}
}

// TestCalcNodeDOP_ParallelMergeableDistinctAggregation verifies that exact
// DISTINCT states supported by MergeGroup retain the normal parallel DOP.
func TestCalcNodeDOP_ParallelMergeableDistinctAggregation(t *testing.T) {
	countID := function.EncodeOverloadID(function.COUNT, 0)
	countWithDistinct := int64(uint64(countID) | uint64(function.Distinct))
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    &planpb.Stats{BlockNum: 128, HashmapStats: &planpb.HashMapStats{}},
					},
					// AGG node with COUNT(DISTINCT ...)
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    &planpb.Stats{BlockNum: 128, HashmapStats: &planpb.HashMapStats{}},
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											// COUNT with Distinct flag: use uint64 to avoid overflow, then convert to int64
											// Similar to having_binder.go:144, we need to convert to uint64 first
											Obj:     countWithDistinct,
											ObjName: "count",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP with multiple CPUs
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "AGG node should have Stats")
	require.Equal(t, ncpu, aggNode.Stats.Dop)
	require.False(t, aggNode.Stats.ForceOneCN)

	childNode := p.GetQuery().Nodes[0]
	require.NotNil(t, childNode.Stats, "Child node should have Stats")
	require.Equal(t, ncpu, childNode.Stats.Dop)
}

// TestCalcNodeDOP_NonDistinctAggregation tests that non-distinct aggregation nodes
// are not affected by the distinct aggregation logic
func TestCalcNodeDOP_NonDistinctAggregation(t *testing.T) {
	// Create a plan with an AGG node containing COUNT (without DISTINCT)
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with COUNT (no DISTINCT)
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    DefaultStats(),
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											Obj:     int64(function.COUNT), // COUNT without Distinct flag
											ObjName: "count",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP with multiple CPUs
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that the AGG node does NOT have ForceOneCN=true
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "AGG node should have Stats")
	require.False(t, aggNode.Stats.ForceOneCN, "Non-distinct aggregation should NOT have ForceOneCN=true")
	// Dop should be calculated normally (not forced to 1)
	require.Greater(t, aggNode.Stats.Dop, int32(0), "Non-distinct aggregation should have Dop > 0")
}

// TestCalcNodeDOP_NonMergeableDistinctAggregationWithNilStats keeps the
// conservative single-stage fallback for DISTINCT aggregates without an exact
// parallel merge contract.
func TestCalcNodeDOP_NonMergeableDistinctAggregationWithNilStats(t *testing.T) {
	avgID := function.EncodeOverloadID(function.AVG, 0)
	avgWithDistinct := int64(uint64(avgID) | uint64(function.Distinct))
	p := &planpb.Plan{
		Plan: &planpb.Plan_Query{
			Query: &planpb.Query{
				Nodes: []*planpb.Node{
					// Child node (scan)
					{
						NodeId:   0,
						NodeType: planpb.Node_TABLE_SCAN,
						Stats:    DefaultStats(),
					},
					// AGG node with AVG(DISTINCT ...) but nil Stats
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    nil,
						AggList: []*planpb.Expr{
							{
								Expr: &planpb.Expr_F{
									F: &planpb.Function{
										Func: &planpb.ObjectRef{
											Obj:     avgWithDistinct,
											ObjName: "avg",
										},
										Args: []*planpb.Expr{
											{
												Typ: planpb.Type{Id: int32(types.T_int64)},
												Expr: &planpb.Expr_Col{
													Col: &planpb.ColRef{ColPos: 0},
												},
											},
										},
									},
								},
							},
						},
					},
				},
				Steps: []int32{1},
			},
		},
		IsPrepare: false,
	}

	// Call CalcNodeDOP
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that Stats was created and set correctly
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "Stats should be created for distinct aggregation")
	require.Equal(t, int32(1), aggNode.Stats.Dop)
	require.True(t, aggNode.Stats.ForceOneCN)
}

func TestGetExprNdv(t *testing.T) {
	ctx := context.Background()
	mockCtx := &MockCompilerContext{ctx: ctx}
	builder := NewQueryBuilder(planpb.Query_SELECT, mockCtx, false, false)

	// Setup test table with stats
	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		Stats: &planpb.Stats{
			TableCnt: 1000,
		},
	})

	// Mock column stats
	colRef := &planpb.ColRef{
		RelPos: 0,
		ColPos: 0,
		Name:   "test_col",
	}
	builder.tag2Table = make(map[int32]*TableDef)
	builder.tag2Table[0] = &TableDef{
		Name: "test_table",
		Cols: []*ColDef{
			{
				Name: "test_col",
				Typ:  planpb.Type{Id: int32(types.T_int64)},
			},
		},
	}

	t.Run("year function", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "year"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// year divides by 365, so result can be negative if col ndv is -1
		require.NotEqual(t, 0.0, ndv)
	})

	t.Run("substring function", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "substring"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.LessOrEqual(t, ndv, 25.0)
	})

	t.Run("mod with i64 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 10}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Unknown NDV should remain unknown
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with i32 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "mod"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I32Val{I32Val: 5}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with i16 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I16Val{I16Val: 3}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with i8 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I8Val{I8Val: 2}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with u64 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 100}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with u32 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U32Val{U32Val: 50}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with u16 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U16Val{U16Val: 20}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with u8 literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_U8Val{U8Val: 7}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with negative literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: -5}}}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Negative modValue falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with non-literal", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
						{Expr: &planpb.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Non-literal falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("mod with single arg", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "%"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Single arg falls back to column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("default function", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "abs"},
					Args: []*planpb.Expr{
						{Expr: &planpb.Expr_Col{Col: colRef}},
					},
				},
			},
		}
		ndv := getExprNdv(expr, builder)
		// Default function returns column NDV
		require.Equal(t, -1.0, ndv)
	})

	t.Run("zero-argument function", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_F{
				F: &planpb.Function{
					Func: &planpb.ObjectRef{ObjName: "rand"},
				},
			},
		}
		require.Equal(t, -1.0, getExprNdv(expr, builder))
	})

	t.Run("column reference", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_Col{Col: colRef},
		}
		ndv := getExprNdv(expr, builder)
		require.True(t, ndv > 0 || ndv == -1)
	})

	t.Run("unsupported expr type", func(t *testing.T) {
		expr := &planpb.Expr{
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 42}},
			},
		}
		ndv := getExprNdv(expr, builder)
		require.Equal(t, -1.0, ndv)
	})
}

// compareStats must be a valid strict weak ordering (transitive + antisymmetric)
// so it is safe to use with slices.SortFunc. See issue #25702.
func TestCompareStatsIsStrictWeakOrdering(t *testing.T) {
	// The review counter-example that broke the old sliding-window comparator:
	// A~B and B~C (within 0.01) but A and C fall in different buckets. With the
	// 0.01-grid bucketing there is no cycle.
	a := &Stats{Selectivity: 0.000, Outcnt: 3}
	b := &Stats{Selectivity: 0.009, Outcnt: 2}
	c := &Stats{Selectivity: 0.018, Outcnt: 1}
	// a and b share selectivity bucket 0 -> compare outcnt (3 vs 2) -> b before a.
	require.True(t, compareStats(b, a) < 0)
	require.True(t, compareStats(a, b) > 0)
	require.True(t, compareStats(b, c) < 0) // bucket 0 < bucket 1
	require.True(t, compareStats(a, c) < 0) // bucket 0 < bucket 1
	// consistent order b < a < c (the old comparator produced a b<a<c<b cycle).

	// antisymmetry + transitivity over a grid of selectivity/outcnt values.
	var xs []*Stats
	for _, s := range []float64{-0.01, 0, 0.004, 0.009, 0.01, 0.015, 0.02, 0.099, 0.1, 0.5, 1.0} {
		for _, o := range []float64{0, 1, 2, 100} {
			xs = append(xs, &Stats{Selectivity: s, Outcnt: o})
		}
	}
	lt := func(x, y *Stats) bool { return compareStats(x, y) < 0 }
	for _, x := range xs {
		for _, y := range xs {
			// antisymmetry: not (x<y and y<x)
			require.False(t, lt(x, y) && lt(y, x))
			for _, z := range xs {
				// transitivity: x<y and y<z => x<z
				if lt(x, y) && lt(y, z) {
					require.True(t, lt(x, z), "transitivity: %+v < %+v < %+v", x, y, z)
				}
			}
		}
	}
}
