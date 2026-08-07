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
	"fmt"
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	index2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

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
		require.GreaterOrEqual(t, join.Stats.Outcnt, 0.0)
		require.True(t, isFinite(join.Stats.Selectivity), "selectivity = %v", join.Stats.Selectivity)
		require.GreaterOrEqual(t, join.Stats.Selectivity, 0.0)
		require.LessOrEqual(t, join.Stats.Selectivity, 1.0)
	})
}

func TestSampledNDVChangesIntermediateCardinalityAndBuildSide(t *testing.T) {
	build := func(t *testing.T, joinKeyNDV float64) (*QueryBuilder, *planpb.Node, *planpb.Node) {
		t.Helper()

		statsCache := NewStatsCache()
		for tableID := uint64(1); tableID <= 3; tableID++ {
			stats := NewStatsInfo()
			stats.TableCnt = 10_000_000
			stats.NdvMap["k"] = joinKeyNDV
			if tableID == 2 {
				stats.NdvMap["k"] = 1_000_000
			}
			statsCache.Set(tableID, stats)
		}
		ctx := &statsCacheCompilerContext{
			MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
			statsCache:          statsCache,
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

		makeScan := func(tag int32, tableID uint64, rows float64) *planpb.Node {
			tableDef := &planpb.TableDef{
				TblId: tableID,
				Cols: []*planpb.ColDef{{
					Name: "k",
					Typ:  planpb.Type{Id: int32(types.T_int64)},
				}},
			}
			if tableID == 2 {
				tableDef.Pkey = &planpb.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"}
				tableDef.Name2ColIndex = map[string]int32{"k": 0}
			}
			builder.tag2Table[tag] = tableDef
			return &planpb.Node{
				NodeType:    planpb.Node_TABLE_SCAN,
				BindingTags: []int32{tag},
				TableDef:    tableDef,
				Stats: &planpb.Stats{
					TableCnt:     rows,
					Outcnt:       rows,
					Cost:         rows,
					Selectivity:  1,
					BlockNum:     1,
					HashmapStats: &planpb.HashMapStats{},
				},
			}
		}
		makeEquality := func(leftTag, rightTag int32) *planpb.Expr {
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: leftTag, ColPos: 0, Name: "k"}}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rightTag, ColPos: 0, Name: "k"}}},
			})
			require.NoError(t, err)
			return expr
		}

		left := makeScan(1, 1, 10_000_000)
		right := makeScan(2, 2, 1_000_000)
		third := makeScan(3, 3, 5_000_000)
		firstJoin := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			JoinType: planpb.Node_INNER,
			Children: []int32{0, 1},
			OnList:   []*planpb.Expr{makeEquality(1, 2)},
			Stats:    DefaultStats(),
		}
		parentJoin := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			JoinType: planpb.Node_INNER,
			Children: []int32{3, 2},
			OnList:   []*planpb.Expr{makeEquality(1, 3)},
			Stats:    DefaultStats(),
		}
		builder.qry.Nodes = []*planpb.Node{left, right, third, firstJoin, parentJoin}

		ReCalcNodeStats(3, builder, false, false, false)
		ReCalcNodeStats(4, builder, false, false, false)
		builder.determineBuildAndProbeSide(4, false)
		return builder, firstJoin, parentJoin
	}

	_, lowNDVJoin, lowNDVParent := build(t, 10_000)
	require.Equal(t, float64(10_000_000), lowNDVJoin.Stats.Outcnt)
	require.Equal(t, []int32{3, 2}, lowNDVParent.Children,
		"large intermediate stays on the probe side and the smaller third table is built")

	_, highNDVJoin, highNDVParent := build(t, 10_000_000)
	require.Equal(t, float64(1_000_000), highNDVJoin.Stats.Outcnt)
	require.Equal(t, []int32{2, 3}, highNDVParent.Children,
		"small intermediate becomes the build side")

	_, missingNDVJoin, missingNDVParent := build(t, 0)
	require.Equal(t, float64(10_000_000), missingNDVJoin.Stats.Outcnt,
		"missing key statistics retain the previous row-count fallback")
	require.Equal(t, []int32{3, 2}, missingNDVParent.Children)
}

func TestEquiJoinNDVStillAppliesResidualPredicates(t *testing.T) {
	statsCache := NewStatsCache()
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

	makeScan := func(tag int32, tableID uint64) *planpb.Node {
		stats := NewStatsInfo()
		stats.TableCnt = 1_000
		stats.NdvMap["k"] = 1_000
		stats.NdvMap["v"] = 100
		statsCache.Set(tableID, stats)
		tableDef := &planpb.TableDef{
			TblId: tableID,
			Cols: []*planpb.ColDef{
				{Name: "k", Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Name: "v", Typ: planpb.Type{Id: int32(types.T_int64)}},
			},
		}
		builder.tag2Table[tag] = tableDef
		return &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: tableDef,
			Stats: &planpb.Stats{
				TableCnt: 1_000, Outcnt: 1_000, Cost: 1_000, Selectivity: 1,
				BlockNum: 1, HashmapStats: &planpb.HashMapStats{},
			},
		}
	}
	makeCol := func(tag, pos int32, name string) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: tag, ColPos: pos, Name: name}},
		}
	}
	makePred := func(name string, leftPos, rightPos int32, colName string) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
			makeCol(1, leftPos, colName), makeCol(2, rightPos, colName),
		})
		require.NoError(t, err)
		return expr
	}

	left := makeScan(1, 1)
	right := makeScan(2, 2)
	equality := makePred("=", 0, 0, "k")
	secondEquality := makePred("=", 1, 1, "v")
	residual := makePred("!=", 1, 1, "v")
	builder.qry.Nodes = []*planpb.Node{left, right}

	joinWithEqualities := &planpb.Node{
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{0, 1}, OnList: []*planpb.Expr{equality, secondEquality}, Stats: DefaultStats(),
	}
	builder.qry.Nodes = append(builder.qry.Nodes, joinWithEqualities)
	ReCalcNodeStats(2, builder, false, false, false)
	require.Equal(t, float64(1_000), joinWithEqualities.Stats.Outcnt,
		"additional equi keys are not guessed independent without multi-column NDV")

	joinWithResidual := &planpb.Node{
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{0, 1}, OnList: []*planpb.Expr{equality, residual}, Stats: DefaultStats(),
	}
	builder.qry.Nodes = append(builder.qry.Nodes, joinWithResidual)
	ReCalcNodeStats(3, builder, false, false, false)
	require.Equal(t, float64(900), joinWithResidual.Stats.Outcnt,
		"the NDV denominator must not suppress a non-equality join predicate")
}

func TestSampledNDVDoesNotAmplifyUnboundedManyToManyJoin(t *testing.T) {
	statsCache := NewStatsCache()
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

	makeScan := func(tag int32, tableID uint64) *planpb.Node {
		stats := NewStatsInfo()
		stats.TableCnt = 1_000_000
		stats.NdvMap["k"] = 100
		statsCache.Set(tableID, stats)
		tableDef := &planpb.TableDef{
			TblId: tableID,
			Cols:  []*planpb.ColDef{{Name: "k", Typ: planpb.Type{Id: int32(types.T_int64)}}},
		}
		builder.tag2Table[tag] = tableDef
		return &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: tableDef,
			Stats: &planpb.Stats{
				TableCnt: 1_000_000, Outcnt: 1_000_000, Cost: 1_000_000, Selectivity: 1,
				BlockNum: 1, HashmapStats: &planpb.HashMapStats{},
			},
		}
	}
	left := makeScan(1, 1)
	right := makeScan(2, 2)
	predicate, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0, Name: "k"}}},
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 2, ColPos: 0, Name: "k"}}},
	})
	require.NoError(t, err)
	join := &planpb.Node{
		NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
		Children: []int32{0, 1}, OnList: []*planpb.Expr{predicate}, Stats: DefaultStats(),
	}
	builder.qry.Nodes = []*planpb.Node{left, right, join}

	ReCalcNodeStats(2, builder, false, false, false)

	require.Equal(t, float64(1_000_000), join.Stats.Outcnt,
		"a sampled single-column NDV is not a safe fanout bound for an unconstrained many-to-many join")
}

func TestStandaloneFilterUsesExpressionSelectivity(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
	predicate, err := BindFuncExprImplByPlanExpr(context.Background(), "!=", []*planpb.Expr{
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}},
		{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 1}}},
	})
	require.NoError(t, err)

	child := &planpb.Node{
		NodeType: planpb.Node_VALUE_SCAN,
		Stats: &planpb.Stats{
			Outcnt:      1_000,
			Cost:        1_000,
			Selectivity: 1,
			BlockNum:    1,
		},
	}
	filter := &planpb.Node{
		NodeType:   planpb.Node_FILTER,
		Children:   []int32{0},
		FilterList: []*planpb.Expr{predicate},
		Stats:      DefaultStats(),
	}
	builder.qry.Nodes = []*planpb.Node{child, filter}

	ReCalcNodeStats(1, builder, false, false, false)

	require.Equal(t, 0.9, filter.Stats.Selectivity)
	require.Equal(t, float64(900), filter.Stats.Outcnt)
}

func TestContainedUniqueJoinCardinalityRequiresReliableContainment(t *testing.T) {
	tests := []struct {
		name              string
		uniqueOnLeft      bool
		withPrimaryKey    bool
		uniqueOutcnt      float64
		uniqueSelectivity float64
		probeMax          float64
		uniqueMax         float64
		want              float64
	}{
		{"complete dense unique right", false, true, 1_000, 1, 1_000, 1_000, 1_000_000},
		{"complete dense unique left", true, true, 1_000, 1, 1_000, 1_000, 1_000_000},
		{"adjacent sentinel keeps conservative upper bound", false, true, 1_000, 1, 1_001, 1_000, 1_000_000},
		{"insufficient range coverage falls back", false, true, 1_000, 1, 2_000, 1_000, 1_000},
		{"filtered unique side falls back", false, true, 500, 0.5, 1_000, 1_000, 500},
		{"sparse unique domain falls back", false, true, 1_000, 1, 1_000, 10_000, 1_000},
		{"missing uniqueness falls back", false, false, 1_000, 1, 1_000, 1_000, 1_000_000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statsCache := NewStatsCache()
			addStats := func(tableID uint64, rows, ndv, max float64) {
				stats := NewStatsInfo()
				stats.TableCnt = rows
				stats.NdvMap["k"] = ndv
				stats.MinValMap["k"] = 1
				stats.MaxValMap["k"] = max
				statsCache.Set(tableID, stats)
			}
			addStats(1, 1_000_000, 5_000_000, tt.probeMax)
			addStats(2, 1_000, 1_000, tt.uniqueMax)
			ctx := &statsCacheCompilerContext{
				MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
				statsCache:          statsCache,
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

			makeScan := func(tag int32, tableID uint64, rows, outcnt, selectivity float64, primary bool) *planpb.Node {
				tableDef := &planpb.TableDef{
					TblId: tableID,
					Cols: []*planpb.ColDef{{
						Name: "k", Typ: planpb.Type{Id: int32(types.T_int64), NotNullable: primary},
					}},
					Name2ColIndex: map[string]int32{"k": 0},
				}
				if primary {
					tableDef.Pkey = &planpb.PrimaryKeyDef{Names: []string{"k"}, PkeyColName: "k"}
				}
				builder.tag2Table[tag] = tableDef
				return &planpb.Node{
					NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: tableDef,
					Stats: &planpb.Stats{
						TableCnt: rows, Outcnt: outcnt, Cost: outcnt, Selectivity: selectivity,
						BlockNum: 1, HashmapStats: &planpb.HashMapStats{},
					},
				}
			}
			probe := makeScan(1, 1, 1_000_000, 1_000_000, 1, false)
			unique := makeScan(2, 2, 1_000, tt.uniqueOutcnt, tt.uniqueSelectivity, tt.withPrimaryKey)
			left, right := probe, unique
			leftTag, rightTag := int32(1), int32(2)
			if tt.uniqueOnLeft {
				left, right = unique, probe
				leftTag, rightTag = rightTag, leftTag
			}
			predicate, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: leftTag, ColPos: 0, Name: "k"}}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rightTag, ColPos: 0, Name: "k"}}},
			})
			require.NoError(t, err)
			join := &planpb.Node{
				NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{0, 1}, OnList: []*planpb.Expr{predicate}, Stats: DefaultStats(),
			}
			builder.qry.Nodes = []*planpb.Node{left, right, join}

			ReCalcNodeStats(2, builder, false, false, false)

			require.InDelta(t, tt.want, join.Stats.Outcnt, 1e-6)
		})
	}
}

func TestFilteredCompositeUniqueJoinUsesRetainedTupleRatio(t *testing.T) {
	const keyCount = 2
	t.Run("composite key uses filtered tuple ratio", func(t *testing.T) {
		statsCache := NewStatsCache()
		ctx := &statsCacheCompilerContext{
			MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
			statsCache:          statsCache,
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)

		makeScan := func(tag int32, tableID uint64, rows, outcnt, selectivity float64, unique bool) *planpb.Node {
			cols := make([]*planpb.ColDef, keyCount)
			name2ColIndex := make(map[string]int32, keyCount)
			stats := NewStatsInfo()
			stats.TableCnt = rows
			for i := range keyCount {
				name := fmt.Sprintf("k%d", i)
				cols[i] = &planpb.ColDef{Name: name, Typ: planpb.Type{Id: int32(types.T_int64)}}
				name2ColIndex[name] = int32(i)
				stats.NdvMap[name] = 100
			}
			statsCache.Set(tableID, stats)
			tableDef := &planpb.TableDef{TblId: tableID, Cols: cols, Name2ColIndex: name2ColIndex}
			if unique {
				names := make([]string, keyCount)
				for i := range keyCount {
					names[i] = fmt.Sprintf("k%d", i)
				}
				tableDef.Pkey = &planpb.PrimaryKeyDef{Names: names, PkeyColName: names[0]}
			}
			builder.tag2Table[tag] = tableDef
			return &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: tableDef,
				Stats: &planpb.Stats{
					TableCnt: rows, Outcnt: outcnt, Cost: outcnt, Selectivity: selectivity,
					BlockNum: 1, HashmapStats: &planpb.HashMapStats{},
				},
			}
		}

		probe := makeScan(1, 1, 1_000_000, 1_000_000, 1, false)
		unique := makeScan(2, 2, 100_000, 20_000, 0.2, true)
		predicates := make([]*planpb.Expr, keyCount)
		for i := range keyCount {
			name := fmt.Sprintf("k%d", i)
			predicate, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: int32(i), Name: name}}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 2, ColPos: int32(i), Name: name}}},
			})
			require.NoError(t, err)
			predicates[i] = predicate
		}
		join := &planpb.Node{
			NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{0, 1}, OnList: predicates, Stats: DefaultStats(),
		}
		builder.qry.Nodes = []*planpb.Node{probe, unique, join}

		ReCalcNodeStats(2, builder, false, false, false)

		require.Equal(t, float64(200_000), join.Stats.Outcnt)
	})
}

func TestUniqueKeyCanSpanBothInnerJoinInputs(t *testing.T) {
	for _, rightUnique := range []bool{true, false} {
		t.Run(fmt.Sprintf("right_unique_%v", rightUnique), func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
			makeTable := func(tag int32, names []string, primary []string) *planpb.Node {
				cols := make([]*planpb.ColDef, len(names))
				name2ColIndex := make(map[string]int32, len(names))
				for i, name := range names {
					cols[i] = &planpb.ColDef{Name: name, Typ: planpb.Type{Id: int32(types.T_int64)}}
					name2ColIndex[name] = int32(i)
				}
				tableDef := &planpb.TableDef{Cols: cols, Name2ColIndex: name2ColIndex}
				if len(primary) > 0 {
					tableDef.Pkey = &planpb.PrimaryKeyDef{Names: primary, PkeyColName: primary[0]}
				}
				return &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{tag}, TableDef: tableDef}
			}
			left := makeTable(1, []string{"a", "b"}, []string{"a", "b"})
			rightPrimary := []string(nil)
			if rightUnique {
				rightPrimary = []string{"b"}
			}
			right := makeTable(2, []string{"b"}, rightPrimary)
			predicate, err := BindFuncExprImplByPlanExpr(context.Background(), "=", []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 1, Name: "b"}}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 2, ColPos: 0, Name: "b"}}},
			})
			require.NoError(t, err)
			join := &planpb.Node{
				NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{0, 1}, OnList: []*planpb.Expr{predicate},
			}
			builder.qry.Nodes = []*planpb.Node{left, right, join}

			requested := map[uint64]struct{}{
				colRefKey(&planpb.ColRef{RelPos: 1, ColPos: 0}): {},
				colRefKey(&planpb.ColRef{RelPos: 2, ColPos: 0}): {},
			}
			require.Equal(t, rightUnique, uniqueColsInSubtree(2, requested, builder))
		})
	}
}

func TestTPCDSQ64SampledStatsChangeRiskyJoinTopology(t *testing.T) {
	type fixtureColumn struct {
		name string
		ndv  float64
		min  float64
		max  float64
		typ  types.T
	}

	statsCache := NewStatsCache()
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	columnPos := make(map[int32]map[string]int32)
	nextTableID := uint64(1)

	addScan := func(tag int32, rows, outcnt float64, columns ...fixtureColumn) int32 {
		tableID := nextTableID
		nextTableID++
		stats := NewStatsInfo()
		stats.TableCnt = rows
		defs := make([]*planpb.ColDef, len(columns))
		name2ColIndex := make(map[string]int32, len(columns))
		columnPos[tag] = make(map[string]int32, len(columns))
		for i, column := range columns {
			stats.NdvMap[column.name] = column.ndv
			stats.MinValMap[column.name] = column.min
			stats.MaxValMap[column.name] = column.max
			defs[i] = &planpb.ColDef{Name: column.name, Typ: planpb.Type{Id: int32(column.typ)}}
			columnPos[tag][column.name] = int32(i)
			name2ColIndex[column.name] = int32(i)
		}
		statsCache.Set(tableID, stats)
		tableDef := &planpb.TableDef{TblId: tableID, Cols: defs, Name2ColIndex: name2ColIndex}
		builder.tag2Table[tag] = tableDef
		node := &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			BindingTags: []int32{tag},
			TableDef:    tableDef,
			Stats: &planpb.Stats{
				TableCnt: rows, Outcnt: outcnt, Cost: outcnt,
				Selectivity: outcnt / rows, BlockNum: 1,
				HashmapStats: &planpb.HashMapStats{},
			},
		}
		builder.qry.Nodes = append(builder.qry.Nodes, node)
		return int32(len(builder.qry.Nodes) - 1)
	}
	markPrimaryKey := func(nodeID int32, names ...string) {
		tableDef := builder.qry.Nodes[nodeID].TableDef
		tableDef.Pkey = &planpb.PrimaryKeyDef{Names: names, PkeyColName: names[0]}
		for _, name := range names {
			tableDef.Cols[tableDef.Name2ColIndex[name]].Typ.NotNullable = true
		}
	}
	makeCol := func(tag int32, name string) *planpb.Expr {
		def := builder.tag2Table[tag].Cols[columnPos[tag][name]]
		return &planpb.Expr{
			Typ: def.Typ,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: tag, ColPos: columnPos[tag][name], Name: name,
			}},
		}
	}
	makeBinary := func(name string, leftTag int32, leftCol string, rightTag int32, rightCol string) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{
			makeCol(leftTag, leftCol), makeCol(rightTag, rightCol),
		})
		require.NoError(t, err)
		return expr
	}
	addJoin := func(left, right int32, predicates ...*planpb.Expr) int32 {
		node := &planpb.Node{
			NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{left, right}, OnList: predicates, Stats: DefaultStats(),
		}
		builder.qry.Nodes = append(builder.qry.Nodes, node)
		nodeID := int32(len(builder.qry.Nodes) - 1)
		ReCalcNodeStats(nodeID, builder, false, false, false)
		return nodeID
	}

	// These are the sampled table counts and NDVs captured with the failing
	// TPC-DS 1T Q64 plan in #26742.  Keep the fixture typed and focused on the
	// cardinalities that select the store_returns hash-join topology.
	ss := addScan(1, 2_879_987_999, 2_879_987_999,
		fixtureColumn{"ss_sold_date_sk", 1_810, 0, 73_049, types.T_int64},
		fixtureColumn{"ss_store_sk", 499, 0, 1_000, types.T_int64},
		fixtureColumn{"ss_promo_sk", 1_489, 0, 1_500, types.T_int64},
		fixtureColumn{"ss_hdemo_sk", 7_898, 0, 7_200, types.T_int64},
		fixtureColumn{"ss_cdemo_sk", 22_262_368.548327174, 0, 1_920_800, types.T_int64},
		fixtureColumn{"ss_addr_sk", 24_253_356.946427356, 0, 6_000_000, types.T_int64},
		fixtureColumn{"ss_customer_sk", 24_780_616.288858734, 0, 12_000_000, types.T_int64},
		fixtureColumn{"ss_item_sk", 2_162_541.164143982, 1, 300_000, types.T_int64},
		fixtureColumn{"ss_ticket_number", 9_797_871.261877812, 1, 239_811_735, types.T_int64})
	d1 := addScan(2, 73_049, 73_049.0/202,
		fixtureColumn{"d_date_sk", 73_049, 1, 73_049, types.T_int64})
	store := addScan(3, 1_002, 1_002,
		fixtureColumn{"s_store_sk", 1_002, 1, 1_002, types.T_int64})
	promotion := addScan(4, 1_500, 1_500,
		fixtureColumn{"p_promo_sk", 1_500, 1, 1_500, types.T_int64})
	hd1 := addScan(5, 7_200, 7_200,
		fixtureColumn{"hd_demo_sk", 7_200, 1, 7_200, types.T_int64},
		fixtureColumn{"hd_income_band_sk", 20, 1, 20, types.T_int64})
	ib1 := addScan(6, 20, 20,
		fixtureColumn{"ib_income_band_sk", 20, 1, 20, types.T_int64})
	cd1 := addScan(7, 1_920_800, 1_920_800,
		fixtureColumn{"cd_demo_sk", 1_920_800, 1, 1_920_800, types.T_int64},
		fixtureColumn{"cd_marital_status", 5, 0, 0, types.T_varchar})
	ad1 := addScan(8, 6_000_000, 6_000_000,
		fixtureColumn{"ca_address_sk", 6_000_000, 1, 6_000_000, types.T_int64})
	markPrimaryKey(ss, "ss_item_sk", "ss_ticket_number")
	markPrimaryKey(d1, "d_date_sk")
	markPrimaryKey(store, "s_store_sk")
	markPrimaryKey(promotion, "p_promo_sk")
	markPrimaryKey(hd1, "hd_demo_sk")
	markPrimaryKey(ib1, "ib_income_band_sk")
	markPrimaryKey(cd1, "cd_demo_sk")
	markPrimaryKey(ad1, "ca_address_sk")

	hd1WithIncome := addJoin(hd1, ib1, makeBinary("=", 5, "hd_income_band_sk", 6, "ib_income_band_sk"))
	fact := addJoin(ss, d1, makeBinary("=", 1, "ss_sold_date_sk", 2, "d_date_sk"))
	dateJoin := builder.qry.Nodes[fact]
	fact = addJoin(fact, store, makeBinary("=", 1, "ss_store_sk", 3, "s_store_sk"))
	fact = addJoin(fact, promotion, makeBinary("=", 1, "ss_promo_sk", 4, "p_promo_sk"))
	fact = addJoin(fact, hd1WithIncome, makeBinary("=", 1, "ss_hdemo_sk", 5, "hd_demo_sk"))
	fact = addJoin(fact, cd1, makeBinary("=", 1, "ss_cdemo_sk", 7, "cd_demo_sk"))
	fact = addJoin(fact, ad1, makeBinary("=", 1, "ss_addr_sk", 8, "ca_address_sk"))

	customer := addScan(9, 12_000_000, 12_000_000,
		fixtureColumn{"c_customer_sk", 12_000_000, 1, 12_000_000, types.T_int64},
		fixtureColumn{"c_current_hdemo_sk", 7_180, 0, 7_200, types.T_int64},
		fixtureColumn{"c_first_sales_date_sk", 3_618, 1, 73_049, types.T_int64},
		fixtureColumn{"c_first_shipto_date_sk", 3_614, 1, 73_049, types.T_int64},
		fixtureColumn{"c_current_cdemo_sk", 1_366_731, 0, 1_920_800, types.T_int64},
		fixtureColumn{"c_current_addr_sk", 1_556_334, 3, 6_000_000, types.T_int64})
	hd2 := addScan(10, 7_200, 7_200,
		fixtureColumn{"hd_demo_sk", 7_200, 1, 7_200, types.T_int64},
		fixtureColumn{"hd_income_band_sk", 20, 1, 20, types.T_int64})
	ib2 := addScan(11, 20, 20,
		fixtureColumn{"ib_income_band_sk", 20, 1, 20, types.T_int64})
	d2 := addScan(12, 73_049, 73_049,
		fixtureColumn{"d_date_sk", 73_049, 1, 73_049, types.T_int64})
	d3 := addScan(13, 73_049, 73_049,
		fixtureColumn{"d_date_sk", 73_049, 1, 73_049, types.T_int64})
	cd2 := addScan(14, 1_920_800, 1_920_800,
		fixtureColumn{"cd_demo_sk", 1_920_800, 1, 1_920_800, types.T_int64},
		fixtureColumn{"cd_marital_status", 5, 0, 0, types.T_varchar})
	ad2 := addScan(15, 6_000_000, 6_000_000,
		fixtureColumn{"ca_address_sk", 6_000_000, 1, 6_000_000, types.T_int64})
	markPrimaryKey(customer, "c_customer_sk")
	markPrimaryKey(hd2, "hd_demo_sk")
	markPrimaryKey(ib2, "ib_income_band_sk")
	markPrimaryKey(d2, "d_date_sk")
	markPrimaryKey(d3, "d_date_sk")
	markPrimaryKey(cd2, "cd_demo_sk")
	markPrimaryKey(ad2, "ca_address_sk")

	hd2WithIncome := addJoin(hd2, ib2, makeBinary("=", 10, "hd_income_band_sk", 11, "ib_income_band_sk"))
	customerBranch := addJoin(customer, hd2WithIncome, makeBinary("=", 9, "c_current_hdemo_sk", 10, "hd_demo_sk"))
	customerBranch = addJoin(customerBranch, d2, makeBinary("=", 9, "c_first_sales_date_sk", 12, "d_date_sk"))
	customerBranch = addJoin(customerBranch, d3, makeBinary("=", 9, "c_first_shipto_date_sk", 13, "d_date_sk"))
	customerBranch = addJoin(customerBranch, cd2, makeBinary("=", 9, "c_current_cdemo_sk", 14, "cd_demo_sk"))
	customerBranch = addJoin(customerBranch, ad2, makeBinary("=", 9, "c_current_addr_sk", 15, "ca_address_sk"))
	fact = addJoin(fact, customerBranch, makeBinary("=", 1, "ss_customer_sk", 9, "c_customer_sk"))

	maritalFilter := &planpb.Node{
		NodeType: planpb.Node_FILTER, Children: []int32{fact},
		FilterList: []*planpb.Expr{makeBinary("!=", 7, "cd_marital_status", 14, "cd_marital_status")},
		Stats:      DefaultStats(),
	}
	builder.qry.Nodes = append(builder.qry.Nodes, maritalFilter)
	filterID := int32(len(builder.qry.Nodes) - 1)
	ReCalcNodeStats(filterID, builder, false, false, false)

	storeReturns := addScan(16, 287_999_764, 287_999_764,
		fixtureColumn{"sr_item_sk", 1_062_228.7817510783, 1, 295_699, types.T_int64},
		fixtureColumn{"sr_ticket_number", 3_563_997.0795, 2, 239_999_998, types.T_int64})
	markPrimaryKey(storeReturns, "sr_item_sk", "sr_ticket_number")
	returnsJoinID := addJoin(storeReturns, filterID,
		makeBinary("=", 16, "sr_item_sk", 1, "ss_item_sk"),
		makeBinary("=", 16, "sr_ticket_number", 1, "ss_ticket_number"))
	returnsJoin := builder.qry.Nodes[returnsJoinID]
	builder.determineBuildAndProbeSide(returnsJoinID, false)
	ReCalcNodeStats(returnsJoinID, builder, false, false, false)
	determineShuffleForJoin(returnsJoin, builder)

	const oldDateJoinEstimate = 14_257_366.33
	const oldMaritalFilterEstimate = 712_868.32
	require.InDelta(t, 575_406_824.95, dateJoin.Stats.Outcnt, 1)
	require.InDelta(t, 517_866_142.46, maritalFilter.Stats.Outcnt, 1)
	require.Greater(t, dateJoin.Stats.Outcnt, oldDateJoinEstimate*40)
	require.Greater(t, maritalFilter.Stats.Outcnt, oldMaritalFilterEstimate*700)
	require.Equal(t, []int32{filterID, storeReturns}, returnsJoin.Children,
		"the smaller store_returns input becomes the hash-build side")
	require.InDelta(t, 287_999_764, returnsJoin.Stats.HashmapStats.HashmapSize, 1)
	require.True(t, returnsJoin.Stats.HashmapStats.Shuffle,
		"the corrected build estimate crosses the shuffle threshold")
	t.Logf("Q64 sampled stats: date join %.0f -> %.0f, marital filter %.0f -> %.0f, returns join %.0f, shuffle=%v",
		oldDateJoinEstimate, dateJoin.Stats.Outcnt,
		oldMaritalFilterEstimate, maritalFilter.Stats.Outcnt,
		returnsJoin.Stats.Outcnt, returnsJoin.Stats.HashmapStats.Shuffle)
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

func makeQueryWithScanStats(tableType string, rowsize float64, tableCnt float64, blockNum int32, nodes ...*planpb.Node) *planpb.Query {
	scan := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{TableType: tableType},
		Stats: &planpb.Stats{
			Rowsize:  rowsize,
			TableCnt: tableCnt,
			BlockNum: blockNum,
		},
	}
	qryNodes := append([]*planpb.Node{scan}, nodes...)
	return &planpb.Query{
		Nodes: qryNodes,
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

func makeFunctionScanForStatsTest(funcName string, limit *planpb.Expr) *planpb.Node {
	param := &planpb.IndexReaderParam{Limit: limit}
	if funcName == ivfflatplan.IVFFLATSearchFuncName {
		param.OrigFuncName = "l2_distance"
	}
	return &planpb.Node{
		NodeType: planpb.Node_FUNCTION_SCAN,
		Stats:    &planpb.Stats{},
		TableDef: &planpb.TableDef{
			TableType: "func_table",
			TblFunc:   &planpb.TableFunction{Name: funcName},
		},
		IndexReaderParam: param,
		Children:         []int32{0},
	}
}

func makeShuffleJoinForStatsTest(exprBased bool) *planpb.Node {
	right := &planpb.Expr{
		Expr: &planpb.Expr_Col{
			Col: &planpb.ColRef{ColPos: 1},
		},
	}
	if exprBased {
		right = &planpb.Expr{
			Expr: &planpb.Expr_Lit{
				Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}},
			},
		}
	}
	return &planpb.Node{
		NodeType: planpb.Node_JOIN,
		Stats: &planpb.Stats{
			HashmapStats: &planpb.HashMapStats{
				Shuffle:       true,
				ShuffleColIdx: 0,
			},
		},
		OnList: []*planpb.Expr{
			{
				Expr: &planpb.Expr_F{
					F: &planpb.Function{
						Args: []*planpb.Expr{
							{
								Expr: &planpb.Expr_Col{
									Col: &planpb.ColRef{ColPos: 0},
								},
							},
							right,
						},
					},
				},
			},
		},
	}
}

func makeIvfEntriesOrderByLimitParamForStatsTest() *planpb.IndexReaderParam {
	return &planpb.IndexReaderParam{
		OrderBy:      []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}},
		Limit:        makeLimitExprForStatsTest(),
		OrigFuncName: "l2_distance",
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

func TestGetExecType_IvfSearchEntries_InternalIndexReaderScanUsesMultiCNEvenWithTinyStats(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		1,
		1,
		1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_MULTICN, got)
}

func TestGetExecType_IvfSearchEntries_InternalIndexReaderScanDoesNotRequireStatsEstimate(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		0,
		0,
		1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_MULTICN, got)
}

func TestGetExecType_IvfSearchEntries_RowsizeShortcutDoesNotDowngradeMultiCN(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		float64(RowSizeThreshold+1),
		1,
		LargeBlockThresholdForOneCN+1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_MULTICN, got)
}

func TestGetExecType_IvfSearchEntries_InternalIndexReaderScanMultiCNCappedForDDL(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		1,
		1,
		1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, true, false)

	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_IvfSearchFunctionScanUsesMultiCNEvenWithTinyStats(t *testing.T) {
	q := &planpb.Query{
		Nodes: []*planpb.Node{
			makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest()),
		},
		Steps: []int32{0},
	}

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_MULTICN, got)
}

func TestGetExecType_IvfSearchFunctionScanMultiCNCappedForDDL(t *testing.T) {
	q := &planpb.Query{
		Nodes: []*planpb.Node{
			makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest()),
		},
		Steps: []int32{0},
	}

	got := GetExecType(q, true, false)

	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_IvfSearchFunctionScanRespectsForceOneCN(t *testing.T) {
	q := &planpb.Query{
		Nodes: []*planpb.Node{
			makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest()),
		},
		Steps: []int32{0},
	}
	q.Nodes[0].Stats.ForceOneCN = true

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_IvfSearchFunctionScanWithNormalShuffleIsTraversalOrderIndependent(t *testing.T) {
	tests := []struct {
		name     string
		ivfFirst bool
	}{
		{name: "ivf before shuffle", ivfFirst: true},
		{name: "shuffle before ivf"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ivf := makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest())
			shuffle := makeShuffleJoinForStatsTest(false)
			nodes := []*planpb.Node{shuffle, ivf}
			if tt.ivfFirst {
				nodes = []*planpb.Node{ivf, shuffle}
			}
			q := &planpb.Query{Nodes: nodes, Steps: []int32{0}}

			got := GetExecType(q, false, false)

			require.Equal(t, ExecTypeAP_MULTICN, got)
		})
	}
}

func TestGetExecType_IvfSearchFunctionScanWithShuffleKeepsHardOneCNBlockers(t *testing.T) {
	tests := []struct {
		name             string
		txnHaveDDL       bool
		exprBasedShuffle bool
		forceOneCN       bool
	}{
		{name: "transaction DDL or write restriction", txnHaveDDL: true},
		{name: "expression based shuffle", exprBasedShuffle: true},
		{name: "explicit ForceOneCN", forceOneCN: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ivf := makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest())
			ivf.Stats.ForceOneCN = tt.forceOneCN
			q := &planpb.Query{
				Nodes: []*planpb.Node{ivf, makeShuffleJoinForStatsTest(tt.exprBasedShuffle)},
				Steps: []int32{0},
			}

			got := GetExecType(q, tt.txnHaveDDL, false)

			require.Equal(t, ExecTypeAP_ONECN, got)
		})
	}
}

func TestGetExecType_ForceOneCNPrecedesLargeStatsForEveryTraversalOrder(t *testing.T) {
	largeScan := func(forceOneCN bool) *planpb.Node {
		return &planpb.Node{
			NodeType: planpb.Node_TABLE_SCAN,
			TableDef: &planpb.TableDef{},
			Stats: &planpb.Stats{
				BlockNum:   int32(BlockThresholdForOneCN + 1),
				Cost:       float64(costThresholdForOneCN + 1),
				ForceOneCN: forceOneCN,
			},
		}
	}
	forcedIvf := func() *planpb.Node {
		n := makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest())
		n.Stats.ForceOneCN = true
		return n
	}

	tests := []struct {
		name  string
		nodes []*planpb.Node
	}{
		{name: "large ordinary scan before forced IVF", nodes: []*planpb.Node{largeScan(false), forcedIvf()}},
		{name: "forced IVF before large ordinary scan", nodes: []*planpb.Node{forcedIvf(), largeScan(false)}},
		{name: "large forced IVF scan", nodes: []*planpb.Node{func() *planpb.Node {
			n := forcedIvf()
			n.Stats.BlockNum = int32(BlockThresholdForOneCN + 1)
			n.Stats.Cost = float64(costThresholdForOneCN + 1)
			return n
		}()}},
		{name: "large forced ordinary scan", nodes: []*planpb.Node{largeScan(true)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := GetExecType(&planpb.Query{Nodes: tt.nodes, Steps: []int32{0}}, false, false)
			require.Equal(t, ExecTypeAP_ONECN, got)
		})
	}
}

func TestGetExecType_ProductionIvfPlanShapeUsesMultiCNWithoutHint(t *testing.T) {
	baseScan := &planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{},
		Stats: &planpb.Stats{
			BlockNum: 50,
			Cost:     400010,
		},
	}
	ivf := makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest())
	ivf.Stats.Cost = 0
	join := makeShuffleJoinForStatsTest(false)
	join.Stats.BlockNum = 50
	join.Stats.Cost = 400010

	for _, nodes := range [][]*planpb.Node{
		{baseScan, ivf, join},
		{baseScan, join, ivf},
		{join, ivf, baseScan},
		{join, baseScan, ivf},
		{ivf, baseScan, join},
		{ivf, join, baseScan},
	} {
		got := GetExecType(&planpb.Query{Nodes: nodes, Steps: []int32{0}}, false, false)
		require.Equal(t, ExecTypeAP_MULTICN, got)
	}
}

func TestGetExecType_IvfSearchEntries_MultiCNCappedForExprBasedShuffle(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		1,
		1,
		1,
		&planpb.Node{
			NodeType: planpb.Node_JOIN,
			Stats: &planpb.Stats{
				HashmapStats: &planpb.HashMapStats{
					Shuffle:       true,
					ShuffleColIdx: 0,
				},
			},
			OnList: []*planpb.Expr{
				{
					Expr: &planpb.Expr_F{
						F: &planpb.Function{
							Args: []*planpb.Expr{
								{
									Expr: &planpb.Expr_Col{
										Col: &planpb.ColRef{ColPos: 0},
									},
								},
								{
									Expr: &planpb.Expr_Lit{
										Lit: &planpb.Literal{Value: &planpb.Literal_U64Val{U64Val: 1}},
									},
								},
							},
						},
					},
				},
			},
		},
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_IvfSearchEntries_MultiCNCappedForDDLEvenWithManyBlocks(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		float64(RowSizeThreshold+1),
		500*1024,
		LargeBlockThresholdForMultiCN+1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, true, false)

	require.Equal(t, ExecTypeAP_ONECN, got)
}

func TestGetExecType_IvfSearchEntries_InternalIndexReaderScanRequiresSearchShape(t *testing.T) {
	tests := []struct {
		name  string
		param *planpb.IndexReaderParam
	}{
		{
			name: "nil index reader param",
		},
		{
			name:  "limit only is not enough",
			param: &planpb.IndexReaderParam{Limit: makeLimitExprForStatsTest()},
		},
		{
			name:  "order only is not enough",
			param: &planpb.IndexReaderParam{OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := makeQueryWithScanStats(
				catalog.SystemSI_IVFFLAT_TblType_Entries,
				1,
				1,
				1,
			)
			q.Nodes[0].IndexReaderParam = tt.param

			got := GetExecType(q, false, false)

			require.Equal(t, ExecTypeTP, got)
		})
	}
}

func TestGetExecType_IvfSearchEntries_FunctionScanDoesNotPromoteUnrelatedEntriesScan(t *testing.T) {
	searchNode := makeFunctionScanForStatsTest("generate_series", makeLimitExprForStatsTest())
	q := makeQueryWithScanStats(
		catalog.SystemSI_IVFFLAT_TblType_Entries,
		1,
		1,
		1,
		searchNode,
	)

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeTP, got)
}

func TestGetExecType_IvfSearchMultiCN_DoesNotApplyToOtherTableTypes(t *testing.T) {
	q := makeQueryWithScanStats(
		catalog.Hnsw_TblType_Storage,
		1,
		1,
		1,
	)
	q.Nodes[0].IndexReaderParam = makeIvfEntriesOrderByLimitParamForStatsTest()

	got := GetExecType(q, false, false)

	require.Equal(t, ExecTypeTP, got)
}

func TestIsIvfSearchEntriesTableScan_UnhappyPaths(t *testing.T) {
	require.False(t, isIvfSearchEntriesTableScan(nil))
	require.False(t, isIvfSearchEntriesTableScan(&planpb.Node{NodeType: planpb.Node_VALUE_SCAN}))
	require.False(t, isIvfSearchEntriesTableScan(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{TableType: catalog.Hnsw_TblType_Storage},
	}))
	require.True(t, isIvfSearchEntriesTableScan(&planpb.Node{
		NodeType: planpb.Node_TABLE_SCAN,
		TableDef: &planpb.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
	}))
}

func TestIsIvfEntriesIndexReaderScan_UnhappyPaths(t *testing.T) {
	require.False(t, isIvfEntriesIndexReaderScan(&planpb.Node{}))
	require.False(t, isIvfEntriesIndexReaderScan(&planpb.Node{
		IndexReaderParam: &planpb.IndexReaderParam{Limit: makeLimitExprForStatsTest()},
	}))
	require.False(t, isIvfEntriesIndexReaderScan(&planpb.Node{
		IndexReaderParam: &planpb.IndexReaderParam{OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}}},
	}))
	require.False(t, isIvfEntriesIndexReaderScan(&planpb.Node{
		IndexReaderParam: &planpb.IndexReaderParam{
			OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}},
			Limit:   makeLimitExprForStatsTest(),
		},
	}))
	require.True(t, isIvfEntriesIndexReaderScan(&planpb.Node{
		IndexReaderParam: &planpb.IndexReaderParam{
			OrigFuncName: "l2_distance",
		},
	}))
}

func TestIsIvfSearchEntriesInternalScan(t *testing.T) {
	tests := []struct {
		name string
		node *planpb.Node
		want bool
	}{
		{
			name: "nil node",
		},
		{
			name: "wrong table type",
			node: &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN,
				TableDef: &planpb.TableDef{TableType: catalog.Hnsw_TblType_Storage},
				IndexReaderParam: &planpb.IndexReaderParam{
					OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}},
					Limit:   makeLimitExprForStatsTest(),
				},
			},
		},
		{
			name: "not table scan",
			node: &planpb.Node{
				NodeType: planpb.Node_VALUE_SCAN,
				TableDef: &planpb.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
				IndexReaderParam: &planpb.IndexReaderParam{
					OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}},
					Limit:   makeLimitExprForStatsTest(),
				},
			},
		},
		{
			name: "limit only is not internal search",
			node: &planpb.Node{
				NodeType:         planpb.Node_TABLE_SCAN,
				TableDef:         &planpb.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
				IndexReaderParam: &planpb.IndexReaderParam{Limit: makeLimitExprForStatsTest()},
			},
		},
		{
			name: "order by limit without ivf marker",
			node: &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN,
				TableDef: &planpb.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
				IndexReaderParam: &planpb.IndexReaderParam{
					OrderBy: []*planpb.OrderBySpec{{Expr: &planpb.Expr{}}},
					Limit:   makeLimitExprForStatsTest(),
				},
			},
		},
		{
			name: "valid original distance function without limit",
			node: &planpb.Node{
				NodeType: planpb.Node_TABLE_SCAN,
				TableDef: &planpb.TableDef{TableType: catalog.SystemSI_IVFFLAT_TblType_Entries},
				IndexReaderParam: &planpb.IndexReaderParam{
					OrigFuncName: "l2_distance",
				},
			},
			want: true,
		},
		{
			name: "direct ivf search function without rewrite marker",
			node: func() *planpb.Node {
				n := makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest())
				n.IndexReaderParam.OrigFuncName = ""
				return n
			}(),
		},
		{
			name: "valid ivf search function scan",
			node: makeFunctionScanForStatsTest(ivfflatplan.IVFFLATSearchFuncName, makeLimitExprForStatsTest()),
			want: true,
		},
		{
			name: "unrelated function scan",
			node: makeFunctionScanForStatsTest("generate_series", makeLimitExprForStatsTest()),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, IsIvfSearchEntriesInternalScan(tt.node))
		})
	}
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

// TestCalcNodeDOP_DistinctAggregation tests that distinct aggregation nodes
// are correctly set to Dop=1 and ForceOneCN=true
func TestCalcNodeDOP_DistinctAggregation(t *testing.T) {
	// Create a plan with an AGG node containing COUNT(DISTINCT ...)
	// Use a variable to avoid constant overflow when combining COUNT with Distinct flag
	countVal := uint64(function.COUNT)
	distinctVal := uint64(function.Distinct)
	countWithDistinct := int64(countVal | distinctVal)
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
					// AGG node with COUNT(DISTINCT ...)
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

	// Verify that the AGG node has Dop=1 and ForceOneCN=true
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "AGG node should have Stats")
	require.Equal(t, int32(1), aggNode.Stats.Dop, "Distinct aggregation should have Dop=1")
	require.True(t, aggNode.Stats.ForceOneCN, "Distinct aggregation should have ForceOneCN=true")

	// Verify that child node also has Dop=1 (recursively set)
	childNode := p.GetQuery().Nodes[0]
	require.NotNil(t, childNode.Stats, "Child node should have Stats")
	require.Equal(t, int32(1), childNode.Stats.Dop, "Child node should have Dop=1 (recursively set)")
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

// TestCalcNodeDOP_DistinctAggregationWithNilStats tests that distinct aggregation
// nodes with nil Stats are handled correctly
func TestCalcNodeDOP_DistinctAggregationWithNilStats(t *testing.T) {
	// Create a plan with an AGG node containing COUNT(DISTINCT ...) but no Stats
	// Use a variable to avoid constant overflow when combining COUNT with Distinct flag
	countVal := uint64(function.COUNT)
	distinctVal := uint64(function.Distinct)
	countWithDistinct := int64(countVal | distinctVal)
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
					// AGG node with COUNT(DISTINCT ...) but nil Stats
					{
						NodeId:   1,
						NodeType: planpb.Node_AGG,
						Children: []int32{0},
						Stats:    nil, // nil Stats
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

	// Call CalcNodeDOP
	ncpu := int32(8)
	lencn := 2
	CalcNodeDOP(p, 1, ncpu, lencn)

	// Verify that Stats was created and set correctly
	aggNode := p.GetQuery().Nodes[1]
	require.NotNil(t, aggNode.Stats, "Stats should be created for distinct aggregation")
	require.Equal(t, int32(1), aggNode.Stats.Dop, "Distinct aggregation should have Dop=1")
	require.True(t, aggNode.Stats.ForceOneCN, "Distinct aggregation should have ForceOneCN=true")
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
