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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	pb "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	index2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/stretchr/testify/require"
)

func TestReCalcInnerJoinCardinality(t *testing.T) {
	ctx := NewMockCompilerContext(false, nil)
	typ := planpb.Type{Id: int32(types.T_int64)}
	equality, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(typ, 1, 0), GetColExpr(typ, 2, 0),
	})
	require.NoError(t, err)
	equality.Ndv = 2
	for _, tc := range []struct {
		name                                 string
		left, right, leftSel, rightSel, want float64
		on                                   bool
	}{
		{name: "cartesian", left: 2, right: 3, leftSel: 1, rightSel: 1, want: 6},
		{name: "singleton", left: 1, right: 3, leftSel: 1, rightSel: 1, want: 3},
		{name: "empty left", right: 3, leftSel: 1, rightSel: 1},
		{name: "empty right", left: 3, leftSel: 1, rightSel: 1},
		{name: "both empty", leftSel: 1, rightSel: 1},
		{name: "filtered inputs", left: 2, right: 3, leftSel: .25, rightSel: .5, want: 6},
		{name: "equality control", left: 2, right: 3, leftSel: 1, rightSel: 1, want: 3, on: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
			join := &planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{0, 1}, Stats: DefaultStats()}
			if tc.on {
				join.OnList = []*planpb.Expr{equality}
			}
			builder.qry.Nodes = []*planpb.Node{
				{NodeType: planpb.Node_VALUE_SCAN, Stats: &planpb.Stats{Outcnt: tc.left, Selectivity: tc.leftSel}},
				{NodeType: planpb.Node_VALUE_SCAN, Stats: &planpb.Stats{Outcnt: tc.right, Selectivity: tc.rightSel}},
				join,
			}
			for range 2 {
				ReCalcNodeStats(2, builder, false, false, false)
				require.Equal(t, tc.want, join.Stats.Outcnt)
				require.Equal(t, tc.right, join.Stats.HashmapStats.HashmapSize)
			}
		})
	}
}

func TestReCalcCartesianChainCardinality(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, nil), false, false)
	const want = 300_000_000
	for i, rows := range []float64{1000, 10, 3, 1000, 10} {
		leafID := int32(len(builder.qry.Nodes))
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeType: planpb.Node_VALUE_SCAN, Stats: &planpb.Stats{Outcnt: rows, Selectivity: 1},
		})
		if i > 0 {
			leftID := leafID - 1
			builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
				NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
				Children: []int32{leftID, leafID}, Stats: DefaultStats(),
			})
			ReCalcNodeStats(leafID+1, builder, false, false, false)
		}
	}
	root := builder.qry.Nodes[len(builder.qry.Nodes)-1]
	require.Equal(t, float64(want), root.Stats.Outcnt)
	// The row limit is owned by the existing generic stats tail, rather than
	// reimplemented in cartesian estimation.
	for _, limit := range []uint64{0, 1, 7} {
		root.Limit = MakePlan2Uint64ConstExprWithType(limit)
		ReCalcNodeStats(int32(len(builder.qry.Nodes)-1), builder, false, false, false)
		require.Equal(t, float64(limit), root.Stats.Outcnt)
	}
}

func TestGetExecTypeAdaptiveTopIgnoresDeferredForceOneCN(t *testing.T) {
	large := &planpb.Stats{BlockNum: int32(BlockThresholdForOneCN + 1), Cost: costThresholdForOneCN + 1}
	qry := &planpb.Query{
		Steps: []int32{3},
		Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_INDEX_SEARCH_SCAN, Stats: DeepCopyStats(large)},
			{NodeId: 1, NodeType: planpb.Node_INDEX_SEARCH_SCAN, Stats: &planpb.Stats{ForceOneCN: true}},
			{NodeId: 2, NodeType: planpb.Node_TABLE_SCAN, Stats: &planpb.Stats{}},
			{NodeId: 3, NodeType: planpb.Node_ADAPTIVE_TOP, Children: []int32{0, 1, 2}, Stats: DeepCopyStats(large)},
		},
	}
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(qry, false, false))
	require.True(t, qry.Nodes[1].Stats.ForceOneCN, "PRE keeps its activation-time local constraint")

	qry.Steps = []int32{1}
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(qry, false, false), "an active ForceOneCN scan remains local")
	qry.Steps = nil
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(qry, false, false), "an incomplete graph must remain conservative")
}

func TestHintQueryTypePreservesExecutionContracts(t *testing.T) {
	for _, tc := range []struct {
		name string
		hint int
		want *planpb.Stats
	}{
		{name: "no override", want: DefaultStats()},
		{name: "minimal", hint: 1, want: DefaultMinimalStats()},
		{name: "big", hint: 2, want: DefaultBigStats()},
		{name: "huge", hint: 3, want: DefaultHugeStats()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stats := DefaultStats()
			stats.ForceOneCN = true
			stats.Dop = 3
			stats.Sql = "scan source"
			hashmap := &planpb.HashMapStats{
				HashmapSize: 7, HashOnPK: true, Shuffle: true,
				ShuffleColIdx: 2, ShuffleColMin: 1, ShuffleColMax: 9,
				Ranges: []float64{2, 5},
			}
			stats.HashmapStats = hashmap
			builder := &QueryBuilder{
				qry:            &planpb.Query{Nodes: []*planpb.Node{{Stats: stats}}},
				optimizerHints: &OptimizerHints{execType: tc.hint},
			}

			builder.hintQueryType()

			want := *tc.want
			want.ForceOneCN, want.Dop, want.Sql, want.HashmapStats = true, 3, "scan source", hashmap
			require.Equal(t, &want, stats)
			require.Same(t, hashmap, stats.HashmapStats)
		})
	}
}

func TestHintQueryTypeKeepsRuntimeFilterDeliveryLocal(t *testing.T) {
	for _, hint := range []int{1, 2, 3} {
		t.Run(fmt.Sprintf("execType-%d", hint), func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(t, false)
			join := builder.qry.Nodes[2]
			join.JoinType = planpb.Node_DEDUP
			join.RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{{Tag: 1}}
			builder.forceJoinOnOneCN(2, false)
			require.True(t, builder.qry.Nodes[0].Stats.ForceOneCN)
			builder.optimizerHints = &OptimizerHints{execType: hint}

			builder.hintQueryType()

			require.True(t, builder.qry.Nodes[0].Stats.ForceOneCN,
				"cost overrides must not distribute a current-CN runtime-filter consumer")
			require.True(t, builder.qry.Nodes[1].Stats.ForceOneCN)
			require.NotEqual(t, ExecTypeAP_MULTICN, GetExecType(builder.qry, false, false))

			// The same cost hint still permits distributed execution when there is
			// no locality constraint. This is not a global single-CN fallback.
			for _, node := range builder.qry.Nodes {
				node.Stats.ForceOneCN = false
			}
			if hint > 1 {
				require.Equal(t, ExecTypeAP_MULTICN, GetExecType(builder.qry, false, false))
			}
		})
	}
}

func TestAggregateBlockEstimateSaturates(t *testing.T) {
	for _, tc := range []struct {
		name string
		rows float64
		want int32
	}{
		{name: "partial block", rows: objectio.BlockMaxRows - 1, want: 1},
		{name: "block boundary", rows: objectio.BlockMaxRows, want: 2},
		{name: "last representable hint", rows: float64(math.MaxInt32-1) * objectio.BlockMaxRows, want: math.MaxInt32},
		{name: "hint overflow", rows: float64(math.MaxInt32) * objectio.BlockMaxRows, want: math.MaxInt32},
		{name: "unknown row upper bound", rows: float64(^uint64(0)), want: math.MaxInt32},
	} {
		t.Run(tc.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(t, false)
			builder.qry.Nodes[0].Stats.Outcnt = tc.rows
			builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
				NodeType: planpb.Node_AGG, NodeId: 3, Children: []int32{0}, Stats: DefaultStats(),
			})
			ReCalcNodeStats(3, builder, false, false, false)
			require.Equal(t, tc.want, builder.qry.Nodes[3].Stats.BlockNum)
		})
	}
}

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
	gotObject   *planpb.ObjectRef
	gotSnapshot *Snapshot
	err         error
}

func (c *tableDefStatsTestCompilerContext) StatsWithTableDef(
	obj *planpb.ObjectRef,
	tableDef *planpb.TableDef,
	snapshot *Snapshot,
) (*pb.StatsInfo, error) {
	c.gotTableDef, c.gotObject, c.gotSnapshot = tableDef, obj, snapshot
	return c.stats, c.err
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

func TestResidualInequalityUsesInputAsHashBuildCapacityBound(t *testing.T) {
	for _, test := range []struct {
		name              string
		operator          string
		wantCapacityBound float64
		wantBound         bool
	}{
		{name: "inequality uses input bound", operator: "<>", wantCapacityBound: 1_000, wantBound: true},
		{name: "other residual has no bound", operator: ">"},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
			left := &planpb.Node{
				NodeType: planpb.Node_VALUE_SCAN,
				Stats:    &planpb.Stats{Outcnt: 10_000, Cost: 10_000, Selectivity: 1},
			}
			input := &planpb.Node{
				NodeType: planpb.Node_VALUE_SCAN,
				Stats:    &planpb.Stats{Outcnt: 1_000, Cost: 1_000, Selectivity: 1},
			}
			filter := &planpb.Node{
				NodeType: planpb.Node_FILTER,
				Children: []int32{1},
				FilterList: []*planpb.Expr{{
					Expr: &planpb.Expr_F{F: &planpb.Function{
						Func: &planpb.ObjectRef{ObjName: test.operator},
						Args: []*planpb.Expr{
							{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}}},
							{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 1, ColPos: 0}}},
						},
					}},
				}},
			}
			join := &planpb.Node{
				NodeType: planpb.Node_JOIN,
				JoinType: planpb.Node_INNER,
				Children: []int32{0, 2},
			}
			builder.qry.Nodes = []*planpb.Node{left, input, filter, join}

			ReCalcNodeStats(2, builder, false, false, false)
			ReCalcNodeStats(3, builder, false, false, false)

			require.Equal(t, 0.05, filter.Stats.Selectivity)
			require.Equal(t, float64(50), filter.Stats.Outcnt)
			require.Equal(t, float64(50), join.Stats.HashmapStats.HashmapSize)
			capacityBound, ok := residualInequalityBuildCapacityBound(2, builder)
			require.Equal(t, test.wantBound, ok)
			require.Equal(t, test.wantCapacityBound, capacityBound)
		})
	}
}

func TestContainsResidualInequalityIgnoresNestedScalarComparison(t *testing.T) {
	inequality := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: "<>"},
	}}}
	comparison := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
		Func: &planpb.ObjectRef{ObjName: ">"},
		Args: []*planpb.Expr{inequality},
	}}}

	require.False(t, containsInequalityExpr(comparison))
}

func TestSafeStatsRatiosAvoidNonFiniteSelectivity(t *testing.T) {
	t.Run("limit never increases cardinality", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType:   planpb.Node_VALUE_SCAN,
			RowsetData: &planpb.RowsetData{RowCount: 10},
			Stats:      &planpb.Stats{Outcnt: 10, Cost: 10, Selectivity: 1},
			Limit:      MakePlan2Uint64ConstExprWithType(100),
		}
		builder.qry.Nodes = []*planpb.Node{node}

		ReCalcNodeStats(0, builder, false, false, false)

		require.Equal(t, float64(10), node.Stats.Outcnt)
	})

	t.Run("offset estimate remains idempotent", func(t *testing.T) {
		builder := NewQueryBuilder(planpb.Query_SELECT, &MockCompilerContext{ctx: context.Background()}, false, false)
		node := &planpb.Node{
			NodeType:   planpb.Node_VALUE_SCAN,
			RowsetData: &planpb.RowsetData{RowCount: 10},
			Stats:      &planpb.Stats{Outcnt: 10, Cost: 10, Selectivity: 1},
			Limit:      MakePlan2Uint64ConstExprWithType(8),
			Offset:     MakePlan2Uint64ConstExprWithType(7),
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
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
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

func TestEstimateAndSelectivityNormalizesSameColumnRanges(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableCnt = 300_000
	stats.DataTypeMap["price"] = uint64(types.T_decimal64)
	stats.MinValMap["price"] = 0.09
	stats.MaxValMap["price"] = 99.99
	stats.DataTypeMap["other"] = uint64(types.T_decimal64)
	stats.MinValMap["other"] = 0.09
	stats.MaxValMap["other"] = 99.99
	statsCache.Set(1, stats)
	ctx := &statsCacheCompilerContext{
		MockCompilerContext: &MockCompilerContext{ctx: context.Background()},
		statsCache:          statsCache,
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.tag2Table[0] = &planpb.TableDef{
		TblId: 1,
		Cols: []*planpb.ColDef{
			{Name: "price", Typ: planpb.Type{Id: int32(types.T_decimal64), Scale: 2}},
			{Name: "other", Typ: planpb.Type{Id: int32(types.T_decimal64), Scale: 2}},
		},
	}

	decimalRange := func(column int32, name string, value int64) *planpb.Expr {
		columnExpr := &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Scale: 2},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0, ColPos: column, Name: builder.tag2Table[0].Cols[column].Name,
			}},
		}
		literalExpr := &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Scale: 2},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: value}},
			}},
		}
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, []*planpb.Expr{columnExpr, literalExpr})
		require.NoError(t, err)
		return bound
	}
	and := func(left, right *planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "and"},
				Args: []*planpb.Expr{left, right},
			}},
		}
	}

	// The four predicates represent one interval, [36, 45]. Their estimate
	// must not shrink merely because equivalent, weaker bounds are repeated.
	overlapping := and(
		decimalRange(0, ">=", 3600),
		and(decimalRange(0, ">=", 3500), and(
			decimalRange(0, "<=", 4500), decimalRange(0, "<=", 5000))),
	)
	wantInterval := (45.0 - 36.0 + 1) / (99.99 - 0.09)
	require.InDelta(t, wantInterval, estimateExprSelectivity(overlapping, builder, stats), 1e-12)

	strongerLowerBound := and(decimalRange(0, ">=", 3500), decimalRange(0, ">=", 3600))
	wantLowerBound := (99.99 - 36.0 + 1) / (99.99 - 0.09)
	require.InDelta(t, wantLowerBound, estimateExprSelectivity(strongerLowerBound, builder, stats), 1e-12)

	contradictory := and(decimalRange(0, ">=", 5000), decimalRange(0, "<=", 4500))
	require.InDelta(t, 0.00000001, estimateExprSelectivity(contradictory, builder, stats), 1e-15)
	exclusivePoint := and(decimalRange(0, ">", 4500), decimalRange(0, "<=", 4500))
	require.InDelta(t, 0.00000001, estimateExprSelectivity(exclusivePoint, builder, stats), 1e-15)
	inclusivePoint := and(decimalRange(0, ">=", 4500), decimalRange(0, "<=", 4500))
	require.Greater(t, estimateExprSelectivity(inclusivePoint, builder, stats), 0.00000001)

	// Predicates on different columns remain independent.
	independent := and(decimalRange(0, ">=", 3600), decimalRange(1, "<=", 4500))
	wantIndependent := andSelectivity(
		(99.99-36.0+1)/(99.99-0.09),
		(45.0-0.09+1)/(99.99-0.09),
	)
	require.InDelta(t, wantIndependent, estimateExprSelectivity(independent, builder, stats), 1e-12)

	// Moving an out-of-domain endpoint to the observed boundary must preserve
	// the fact that the boundary value satisfies the original predicate.
	stats.MinValMap["price"] = 10
	stats.MaxValMap["price"] = 10
	redundantLowerBounds := and(decimalRange(0, ">", 100), decimalRange(0, ">", 200))
	require.Equal(t, 1.0, estimateExprSelectivity(redundantLowerBounds, builder, stats))
	redundantUpperBounds := and(decimalRange(0, "<", 2000), decimalRange(0, "<", 3000))
	require.Equal(t, 1.0, estimateExprSelectivity(redundantUpperBounds, builder, stats))
}

func TestEstimateConjunctiveYearRange(t *testing.T) {
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.DataTypeMap["d"] = uint64(types.T_date)
	stats.MinValMap["d"] = float64(types.DateFromCalendar(2020, 1, 1))
	stats.MaxValMap["d"] = float64(types.DateFromCalendar(2030, 1, 1))
	statsCache.Set(1, stats)
	ctx := &statsCacheCompilerContext{MockCompilerContext: &MockCompilerContext{ctx: context.Background()}, statsCache: statsCache}
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
	builder.tag2Table[0] = &planpb.TableDef{TblId: 1, Cols: []*planpb.ColDef{{Name: "d", Typ: planpb.Type{Id: int32(types.T_date)}}}}
	col := &planpb.Expr{Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0, Name: "d"}}}
	year := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "year"}, Args: []*planpb.Expr{col}}}}
	lit := func(value int64) *planpb.Expr {
		return &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}}}
	}
	between := &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{Func: &planpb.ObjectRef{ObjName: "between"}, Args: []*planpb.Expr{year, lit(2022), lit(2025)}}}}
	got, ok := estimateConjunctiveRangeSelectivity([]*planpb.Expr{between}, builder)
	require.True(t, ok)
	require.InDelta(t, 0.4, got, 1e-12)
	between.GetF().Args[2] = &planpb.Expr{}
	_, ok = estimateConjunctiveRangeSelectivity([]*planpb.Expr{between}, builder)
	require.False(t, ok)
}

func TestRangeSelectivityClipsToDomain(t *testing.T) {
	const empty = 1e-8
	for _, tc := range []struct {
		name                   string
		min, max, lower, upper float64
		want                   float64
	}{
		{name: "below domain", min: 0, max: 999, lower: -500, upper: -100, want: empty},
		{name: "above domain", min: 0, max: 999, lower: 1100, upper: 1200, want: empty},
		{name: "wide disjoint range", min: 0, max: 999, lower: 1000, upper: 3000, want: empty},
		{name: "left overlap", min: 0, max: 100, lower: -20, upper: 19, want: 0.2},
		{name: "right overlap", min: 0, max: 100, lower: 81, upper: 120, want: 0.2},
		{name: "inside domain", min: 0, max: 100, lower: 20, upper: 39, want: 0.2},
		{name: "exact domain", min: 0, max: 100, lower: 0, upper: 100, want: 1},
		{name: "covers domain", min: 0, max: 100, lower: -20, upper: 120, want: 1},
		{name: "touches minimum", min: 0, max: 100, lower: -20, upper: 0, want: 0.01},
		{name: "touches maximum", min: 0, max: 100, lower: 100, upper: 120, want: 0.01},
		{name: "interior point", min: 0, max: 100, lower: 20, upper: 20, want: 0.01},
		{name: "reversed adjacent bounds", min: 0, max: 100, lower: 21, upper: 20, want: empty},
		{name: "singleton exact match", min: 10, max: 10, lower: 10, upper: 10, want: 1},
		{name: "singleton covered", min: 10, max: 10, lower: 9, upper: 11, want: 1},
		{name: "singleton excluded below", min: 10, max: 10, lower: 8, upper: 9, want: empty},
		{name: "singleton excluded above", min: 10, max: 10, lower: 11, upper: 12, want: empty},
		{name: "invalid statistics", min: 100, max: 0, lower: 20, upper: 39, want: 0.1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, op := range []string{"between", "in_range"} {
				t.Run(op, func(t *testing.T) {
					literals := []*planpb.Literal{
						{Value: &planpb.Literal_I64Val{I64Val: int64(tc.lower)}},
						{Value: &planpb.Literal_I64Val{I64Val: int64(tc.upper)}},
					}
					require.InDelta(t, tc.want,
						calcSelectivityByMinMax(op, tc.min, tc.max, types.T_int64, literals), 1e-12)

					// Decimal conversion and its separate estimator must obey the same interval contract.
					expr := makeDecimalBetweenExpr(tc.lower, tc.upper, 2)
					expr.GetF().Func.ObjName = op
					require.InDelta(t, tc.want,
						calcSelectivityByMinMaxForDecimal(op, tc.min, tc.max, expr), 1e-12)
				})
			}
		})
	}

	t.Run("unsupported literal preserves fallback", func(t *testing.T) {
		valid := &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: 20}}
		invalid := &planpb.Literal{Value: &planpb.Literal_Sval{Sval: "not a number"}}
		for _, literals := range [][]*planpb.Literal{{invalid, valid}, {valid, invalid}} {
			require.Equal(t, 0.1, calcSelectivityByMinMax("between", 0, 100, types.T_int64, literals))
		}
	})
}

func TestBoundBetweenSelectivityClipsToDomain(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	part := ctx.tables["part"]
	require.NotNil(t, part)
	stats := NewStatsInfo()
	stats.TableName, stats.TableCnt = "part", 1000
	stats.DataTypeMap["p_retailprice"] = uint64(types.T_decimal64)
	stats.MinValMap["p_retailprice"], stats.MaxValMap["p_retailprice"] = 0, 100
	cache := NewStatsCache()
	cache.Set(part.TblId, stats)
	compiler := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: ctx, statsCache: cache,
	}}
	for _, tc := range []struct {
		name, bounds string
		want         float64
	}{
		{name: "disjoint", bounds: "-50.00 and -10.00", want: 1e-8},
		{name: "overlap", bounds: "-20.00 and 19.00", want: 0.2},
		{name: "cover", bounds: "-20.00 and 120.00", want: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := mysql.Parse(compiler.GetContext(),
				"select p_partkey from part where p_retailprice between "+tc.bounds, 1)
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			defer stmts[0].Free()
			built, err := BuildPlan(compiler, stmts[0], false, nil)
			require.NoError(t, err)
			var scan *planpb.Node
			for _, node := range built.GetQuery().Nodes {
				if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef.Name == "part" {
					scan = node
					break
				}
			}
			require.NotNil(t, scan)
			require.InDelta(t, tc.want, scan.Stats.Selectivity, 1e-12)
		})
	}
}

func TestEstimateIntervalSelectivityPreservesClippedBoundaryMembership(t *testing.T) {
	const emptySelectivity = 0.00000001
	tests := []struct {
		name                     string
		lower, upper             float64
		hasLower, lowerInclusive bool
		hasUpper, upperInclusive bool
		want                     float64
	}{
		{
			name:  "exclusive lower below singleton domain",
			lower: 1, hasLower: true,
			want: 1,
		},
		{
			name:  "exclusive upper above singleton domain",
			upper: 20, hasUpper: true,
			want: 1,
		},
		{
			name:  "exclusive bounds outside singleton domain",
			lower: 1, hasLower: true,
			upper: 20, hasUpper: true,
			want: 1,
		},
		{
			name:  "exclusive lower at singleton boundary",
			lower: 10, hasLower: true,
			want: emptySelectivity,
		},
		{
			name:  "exclusive upper at singleton boundary",
			upper: 10, hasUpper: true,
			want: emptySelectivity,
		},
		{
			name:  "inclusive singleton",
			lower: 10, hasLower: true, lowerInclusive: true,
			upper: 10, hasUpper: true, upperInclusive: true,
			want: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := estimateIntervalSelectivity(
				10, 10,
				test.lower, test.hasLower, test.lowerInclusive,
				test.upper, test.hasUpper, test.upperInclusive,
			)
			require.Equal(t, test.want, got)
		})
	}
}

func TestBoundFilterListNormalizesSameColumnRanges(t *testing.T) {
	mock := NewMockCompilerContext(false, newPlanTestProcess(t))
	part := mock.tables["part"]
	require.NotNil(t, part)
	statsCache := NewStatsCache()
	stats := NewStatsInfo()
	stats.TableName = "part"
	stats.TableCnt = 200_000
	stats.DataTypeMap["p_retailprice"] = uint64(types.T_decimal64)
	stats.MinValMap["p_retailprice"] = 0.09
	stats.MaxValMap["p_retailprice"] = 99.99
	statsCache.Set(part.TblId, stats)
	ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: mock, statsCache: statsCache,
	}}

	stmts, err := mysql.Parse(ctx.GetContext(), `
		select p_partkey from part
		where p_retailprice >= 36.00 and p_retailprice >= 35.00
		  and p_retailprice <= 45.00 and p_retailprice <= 50.00`, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	defer stmts[0].Free()
	built, err := BuildPlan(ctx, stmts[0], false, nil)
	require.NoError(t, err)

	var scan *planpb.Node
	for _, node := range built.GetQuery().Nodes {
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef.Name == "part" {
			scan = node
			break
		}
	}
	require.NotNil(t, scan)
	require.InDelta(t, (45.0-36.0+1)/(99.99-0.09), scan.Stats.Selectivity, 1e-12)
}

// TestContradictoryPredicateScanEstimate reproduces #32068 end to end with the
// SQL planner pipeline: contradictory point predicates on one column fold to a
// single constant-false filter, so the scan carrying that filter must be
// estimated near zero, never as a full-table count.
func TestContradictoryPredicateScanEstimate(t *testing.T) {
	mock := NewMockCompilerContext(false, newPlanTestProcess(t))
	part := mock.tables["part"]
	require.NotNil(t, part)
	nation := mock.tables["nation"]
	require.NotNil(t, nation)
	orders := mock.tables["orders"]
	require.NotNil(t, orders)

	statsCache := NewStatsCache()
	partStats := NewStatsInfo()
	partStats.TableName = "part"
	partStats.TableCnt = 1_000_000
	partStats.DataTypeMap["p_retailprice"] = uint64(types.T_decimal64)
	partStats.MinValMap["p_retailprice"] = 900
	partStats.MaxValMap["p_retailprice"] = 1100
	partStats.NdvMap["p_retailprice"] = 1000
	partStats.DataTypeMap["p_size"] = uint64(types.T_int32)
	partStats.MinValMap["p_size"] = 0
	partStats.MaxValMap["p_size"] = 100
	partStats.NdvMap["p_size"] = 101
	statsCache.Set(part.TblId, partStats)
	ordersStats := NewStatsInfo()
	ordersStats.TableName = "orders"
	ordersStats.TableCnt = 32
	ordersStats.DataTypeMap["o_orderkey"] = uint64(types.T_int64)
	ordersStats.MinValMap["o_orderkey"] = 0
	ordersStats.MaxValMap["o_orderkey"] = 31
	ordersStats.NdvMap["o_orderkey"] = 32
	statsCache.Set(orders.TblId, ordersStats)
	nationStats := NewStatsInfo()
	nationStats.TableName = "nation"
	nationStats.TableCnt = 100_000
	nationStats.DataTypeMap["n_comment"] = uint64(types.T_varchar)
	nationStats.NdvMap["n_comment"] = 10_000
	statsCache.Set(nation.TblId, nationStats)
	fpts := mock.tables["fpts"]
	require.NotNil(t, fpts)
	fptsStats := NewStatsInfo()
	fptsStats.TableName = "fpts"
	fptsStats.TableCnt = 3
	fptsStats.DataTypeMap["f"] = uint64(types.T_float64)
	fptsStats.MinValMap["f"] = 0
	fptsStats.MaxValMap["f"] = 100
	fptsStats.NdvMap["f"] = 3
	fptsStats.DataTypeMap["g"] = uint64(types.T_float32)
	fptsStats.MinValMap["g"] = 0
	fptsStats.MaxValMap["g"] = 100
	fptsStats.NdvMap["g"] = 3
	statsCache.Set(fpts.TblId, fptsStats)
	ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: mock, statsCache: statsCache,
	}}

	buildScan := func(sql string) *planpb.Node {
		stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		defer stmts[0].Free()
		built, err := BuildPlan(ctx, stmts[0], false, nil)
		require.NoError(t, err)
		var scan *planpb.Node
		for _, node := range built.GetQuery().Nodes {
			if node.NodeType == planpb.Node_TABLE_SCAN {
				scan = node
			}
		}
		require.NotNil(t, scan)
		return scan
	}

	// Every falsification of a point domain reduces the scan filter to one
	// constant-false literal; the estimate of an always-empty filter must stay
	// near zero no matter which rewrite produced it. A satisfiable same-column
	// range must not defer that fold of an already-contradictory point domain,
	// including when the BETWEEN bounds keep the between function alive until
	// domain normalization.
	for _, sql := range []string{
		"select p_partkey from part where p_retailprice = 1000.40 and p_retailprice = 1000.50",
		"select p_partkey from part where p_retailprice = 1000.40 and p_retailprice <> 1000.40",
		"select n_nationkey from nation where n_comment = 'x' and n_comment is null",
		"select p_partkey from part where p_retailprice = 1000.40 and p_retailprice = 1000.50 and p_retailprice between 900 and 1100",
		"select p_partkey from part where p_retailprice = 1000.40 and p_retailprice <> 1000.40 and p_retailprice between 900 and 1100",
		"select p_partkey from part where p_size = 1 and p_size = 2 and p_size between 0 and 100",
		"select p_partkey from part where p_size = 1 and p_size <> 1 and p_size between 0 and 100",
		"select p_partkey from part where p_retailprice = 1000.40 and p_retailprice = 1000.50 and p_retailprice between 900.00 and 1100.00",
		"select p_partkey from part where p_name = 'x' and p_name = 'y' and p_name between 'a' and 'z'",
		// CAST('x' AS SIGNED) folds to an int64 literal with the cast's
		// declared width/scale; conflicting with a plain integer literal of a
		// different value must still fold (round-8 companion).
		"select o_orderkey from orders where o_orderkey = 20 and o_orderkey = cast('21' as signed)",
		"select o_orderkey from orders where o_orderkey = 20 and o_orderkey = cast('21' as signed) and o_orderkey between 0 and 100",
		// A DIV-folded literal retains its folded source in Literal.Src; that
		// retained source must not make the materialized payload a second SQL
		// value, so a genuinely distinct DIV fold still folds
		// (round-9 companion).
		"select o_orderkey from orders where o_orderkey = 19 and o_orderkey = (cast('81.00' as decimal(8,2)) div 1)",
		"select o_orderkey from orders where o_orderkey = 19 and o_orderkey = (cast('81.00' as decimal(8,2)) div 1) and o_orderkey between 0 and 100",
		// Scaled double/float casts of a genuinely distinct payload keep their
		// fold, and a float pair scaled to different comparison values
		// (19.004 at scale 2 rounds to 19.00, at scale 3 to 19.004) still
		// tests distinct points (round-9 companion).
		"select f_id from fpts where f = cast('19' as double(8,2)) and f = cast('81' as double(12,2))",
		"select f_id from fpts where f = cast('19' as double(8,2)) and f = cast('81' as double(12,2)) and f between 0e0 and 100e0",
		"select f_id from fpts where g = cast('19.004' as float(8,2)) and g = cast('19.004' as float(12,3)) and g between 0e0 and 100e0",
	} {
		scan := buildScan(sql)
		require.True(t, IsFalseExpr(colexec.RewriteFilterExprList(scan.FilterList)), sql)
		require.Equal(t, 0.00000001, scan.Stats.Selectivity, sql)
		require.Less(t, scan.Stats.Outcnt, 1.0, sql)
	}

	// Duplicate equality keeps the point estimate of the single surviving value.
	duplicate := buildScan("select p_partkey from part where p_retailprice = 1000.40 and p_retailprice = 1000.40")
	require.Equal(t, 0.001, duplicate.Stats.Selectivity)

	// A compatible equality plus a weaker bound keeps an estimate that never
	// exceeds the smaller operand's selectivity.
	compatible := buildScan("select p_partkey from part where p_retailprice = 1000.40 and p_retailprice >= 950.00")
	require.Greater(t, compatible.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, compatible.Stats.Selectivity, 0.001+1e-12)

	// A satisfiable same-column BETWEEN keeps a real estimate bounded by the
	// equality operand; falsification must not over-prune it.
	satisfiableRange := buildScan("select p_partkey from part where p_retailprice = 1000.40 and p_retailprice between 900 and 1100")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableRange.FilterList)))
	require.Greater(t, satisfiableRange.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, satisfiableRange.Stats.Selectivity, 0.001+1e-12)

	// The two satisfiable controls above keep the same bounds when the between
	// function itself survives domain rewriting (typed peers, int and varchar).
	satisfiableIntRange := buildScan("select p_partkey from part where p_size = 1 and p_size between 0 and 100")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableIntRange.FilterList)))
	require.Greater(t, satisfiableIntRange.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, satisfiableIntRange.Stats.Selectivity, 1.0/101+1e-12)
	satisfiableTypedRange := buildScan("select p_partkey from part where p_retailprice = 1000.40 and p_retailprice between 900.00 and 1100.00")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableTypedRange.FilterList)))
	require.Greater(t, satisfiableTypedRange.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, satisfiableTypedRange.Stats.Selectivity, 0.001+1e-12)

	// A plain integer literal and the SIGNED-cast spelling of the same value
	// are one SQL domain point: the satisfiable pair keeps its real estimate
	// with and without a covering BETWEEN (round-8).
	satisfiableCastPair := buildScan("select o_orderkey from orders where o_orderkey = 19 and o_orderkey = cast('19' as signed) and o_orderkey between 0 and 100")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableCastPair.FilterList)))
	require.Greater(t, satisfiableCastPair.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, satisfiableCastPair.Stats.Selectivity, 1.0/32+1e-12)
	satisfiableCastPairNoRange := buildScan("select o_orderkey from orders where o_orderkey = 19 and o_orderkey = cast('19' as signed)")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableCastPairNoRange.FilterList)))
	require.Greater(t, satisfiableCastPairNoRange.Stats.Selectivity, 0.0)
	require.LessOrEqual(t, satisfiableCastPairNoRange.Stats.Selectivity, 1.0/32+1e-12)

	// A plain integer literal and the DIV-folded materialization of the same
	// value are one SQL domain point: the satisfiable pair keeps its real
	// predicates and estimate in both orders, with and without a covering
	// BETWEEN (round-9).
	for _, sql := range []string{
		"select o_orderkey from orders where o_orderkey = 19 and o_orderkey = (cast('19.00' as decimal(8,2)) div 1) and o_orderkey between 0 and 100",
		"select o_orderkey from orders where (cast('19.00' as decimal(8,2)) div 1) = o_orderkey and o_orderkey = 19 and o_orderkey between 0 and 100",
		"select o_orderkey from orders where o_orderkey = 19 and o_orderkey = (cast('19.00' as decimal(8,2)) div 1)",
		"select o_orderkey from orders where (cast('19.00' as decimal(8,2)) div 1) = o_orderkey and o_orderkey = 19",
	} {
		satisfiableDivPeer := buildScan(sql)
		require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableDivPeer.FilterList)), sql)
		require.Greater(t, satisfiableDivPeer.Stats.Selectivity, 0.0, sql)
		require.LessOrEqual(t, satisfiableDivPeer.Stats.Selectivity, 1.0/32+1e-12, sql)
	}

	// Satisfiable cross-width and cross-scale DOUBLE casts of one value keep
	// their real predicates and estimate in both orders, with and without a
	// covering BETWEEN; FLOAT cross-width spellings of one value behave the
	// same (round-9).
	for _, sql := range []string{
		"select f_id from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2)) and f between 0e0 and 100e0",
		"select f_id from fpts where cast('19' as double(12,2)) = f and f = cast('19' as double(8,2)) and f between 0e0 and 100e0",
		"select f_id from fpts where f = cast('19' as double(8,2)) and f = cast('19' as double(12,2))",
		"select f_id from fpts where f = cast('19.5' as double(8,1)) and f = cast('19.50' as double(12,2)) and f between 0e0 and 100e0",
		"select f_id from fpts where g = cast('19.00' as float(8,2)) and g = cast('19.00' as float(12,2)) and g between 0e0 and 100e0",
	} {
		satisfiableFloatCast := buildScan(sql)
		require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(satisfiableFloatCast.FilterList)), sql)
		require.Greater(t, satisfiableFloatCast.Stats.Selectivity, 0.0, sql)
		require.LessOrEqual(t, satisfiableFloatCast.Stats.Selectivity, 1.0/3+1e-12, sql)
	}

	// An impossible interval stays near zero through the grouped-range path.
	emptyRange := buildScan("select p_partkey from part where p_retailprice > 1095.00 and p_retailprice < 905.00")
	require.Equal(t, 0.00000001, emptyRange.Stats.Selectivity)
}

// TestSignedZeroPointDomainSQLValueEquality pins the SQL value semantics of
// the domain keys: floating +0 and -0 compare equal, so a satisfiable
// DOUBLE pair (f = 0e0 AND f = -0e0, with or without a BETWEEN) must keep a
// real predicate, while genuinely distinct double values still fold. Keying
// by the raw marshaled bits would turn the satisfiable pair into a
// constant-false filter and discard its matching rows.
func TestSignedZeroPointDomainSQLValueEquality(t *testing.T) {
	plusZero := makePlan2Float64ConstExprWithType(0)
	minusZero := makePlan2Float64ConstExprWithType(math.Copysign(0, -1))
	plusZeroKey, okPlus := constLiteralKey(plusZero)
	minusZeroKey, okMinus := constLiteralKey(minusZero)
	require.True(t, okPlus && okMinus)
	require.Equal(t, plusZeroKey, minusZeroKey)

	plusF32Key, okF32 := constLiteralKey(makePlan2Float32ConstExprWithType(0))
	minusF32Key, okF32Minus := constLiteralKey(makePlan2Float32ConstExprWithType(float32(math.Copysign(0, -1))))
	require.True(t, okF32 && okF32Minus)
	require.Equal(t, plusF32Key, minusF32Key)

	negOne := makePlan2Float64ConstExprWithType(-1)
	one := makePlan2Float64ConstExprWithType(1)
	negThreeHalves := makePlan2Float64ConstExprWithType(-1.5)
	floatCol := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_float64)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "f"}},
		}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	normalize := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"the SQL-equal signed-zero or control filters must not fold to constant false")
		}
		return out
	}
	eq := func(lit *planpb.Expr) *planpb.Expr {
		return bind("=", floatCol(), lit)
	}
	between := func(lo, hi *planpb.Expr) *planpb.Expr {
		return bind("between", floatCol(), lo, hi)
	}

	// Satisfiable signed-zero pair with and without a range: not falsified;
	// without a range it merges to the one surviving duplicate equality.
	withRange := normalize(eq(plusZero), eq(minusZero), between(negOne, one))
	require.Len(t, withRange, 3)
	noRange := normalize(eq(plusZero), eq(minusZero))
	require.Len(t, noRange, 1)

	// A satisfiable single equality plus the range keeps both conjuncts.
	singleEq := normalize(eq(plusZero), between(negOne, one))
	require.Len(t, singleEq, 2)

	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}
	// Genuinely distinct double values still fold, with and without a range.
	folded(eq(plusZero), eq(negThreeHalves), between(negOne, one))
	folded(eq(plusZero), eq(negThreeHalves))
}

// TestUnicodeCollationPointDomainSQLValueEquality pins the SQL value semantics
// of the domain keys for text literals: under a native Unicode collation,
// case- and accent-equivalent literals compare equal (never a distinct-literal
// contradiction), a binary collation stays byte-wise, CHAR trailing-space
// equivalence is respected, and genuinely distinct values still fold.
func TestUnicodeCollationPointDomainSQLValueEquality(t *testing.T) {
	ciType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4UnicodeCI)
	ciPlanType := makePlan2Type(&ciType)
	binType := types.NewWithCharset(types.T_varchar, 20, 0, types.CharsetUTF8MB4Bin)
	binPlanType := makePlan2Type(&binType)
	charType := types.NewWithCharset(types.T_char, 10, 0, types.CharsetLegacy)
	charPlanType := makePlan2Type(&charType)

	col := func(ptype planpb.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  ptype,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "s"}},
		}
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	eq := func(ptype planpb.Type, lit string) *planpb.Expr {
		return bind("=", col(ptype), makePlan2StringConstExprWithType(lit))
	}
	between := func(ptype planpb.Type, lo, hi string) *planpb.Expr {
		return bind("between", col(ptype), makePlan2StringConstExprWithType(lo), makePlan2StringConstExprWithType(hi))
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal text literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// Satisfiable ci pair with a covering BETWEEN keeps its real predicates.
	require.Len(t, keepSatisfiable(eq(ciPlanType, "a"), eq(ciPlanType, "A"), between(ciPlanType, "a", "z")), 3)
	// Without a range the SQL-equal pair merges like a duplicate equality.
	require.Len(t, keepSatisfiable(eq(ciPlanType, "a"), eq(ciPlanType, "A")), 1)
	// A satisfiable single equality plus range keeps both conjuncts.
	require.Len(t, keepSatisfiable(eq(ciPlanType, "a"), between(ciPlanType, "a", "z")), 2)
	// Accent-equivalent literals behave like the case pair.
	require.Len(t, keepSatisfiable(eq(ciPlanType, "e"), eq(ciPlanType, "é"), between(ciPlanType, "a", "z")), 3)

	// Genuine distinct ci values still fold, with and without a range.
	folded(eq(ciPlanType, "a"), eq(ciPlanType, "z"), between(ciPlanType, "a", "z"))
	folded(eq(ciPlanType, "a"), eq(ciPlanType, "z"))

	// Binary collation stays byte-wise: 'a' and 'A' are a real contradiction.
	folded(eq(binPlanType, "a"), eq(binPlanType, "A"), between(binPlanType, "a", "z"))
	folded(eq(binPlanType, "a"), eq(binPlanType, "A"))

	// CHAR trailing-space equivalence is satisfiable; distinct CHAR values fold.
	require.Len(t, keepSatisfiable(eq(charPlanType, "a "), eq(charPlanType, "a"), between(charPlanType, "a", "z")), 3)
	folded(eq(charPlanType, "a"), eq(charPlanType, "c"), between(charPlanType, "a", "z"))

	// The key comparison itself follows each bound comparison domain.
	typedLit := func(ptype planpb.Type, s string) *planpb.Expr {
		e := makePlan2StringConstExprWithType(s)
		e.Typ = ptype
		return e
	}
	key := func(ptype planpb.Type, s string) (string, bool) {
		return constLiteralKey(typedLit(ptype, s))
	}
	ciLowerA, okLower := key(ciPlanType, "a")
	ciUpperA, okUpper := key(ciPlanType, "A")
	require.True(t, okLower && okUpper)
	require.Equal(t, ciLowerA, ciUpperA)
	ciAccented, okAccented := key(ciPlanType, "é")
	ciPlainE, okPlainE := key(ciPlanType, "e")
	require.True(t, okAccented && okPlainE)
	require.Equal(t, ciAccented, ciPlainE)
	ciZed, okZed := key(ciPlanType, "z")
	require.True(t, okZed)
	require.NotEqual(t, ciLowerA, ciZed)

	binLowerA, okBinLower := key(binPlanType, "a")
	binUpperA, okBinUpper := key(binPlanType, "A")
	require.True(t, okBinLower && okBinUpper)
	require.NotEqual(t, binLowerA, binUpperA)

	charPadded, okPadded := key(charPlanType, "a ")
	charPlain, okPlain := key(charPlanType, "a")
	require.True(t, okPadded && okPlain)
	require.Equal(t, charPadded, charPlain)
}

// TestTemporalPrecisionPointDomainSQLValueEquality pins the SQL value semantics
// of the domain keys for temporal literals: runtime temporal comparison tests
// the materialized payload only, so the same clock value spelled at
// DATETIME(0) and DATETIME(6) compares equal and is never a distinct-literal
// contradiction, while genuinely distinct values (including scale-rounded
// spellings that materialize different payloads) still fold.
func TestTemporalPrecisionPointDomainSQLValueEquality(t *testing.T) {
	dtColType := types.New(types.T_datetime, 26, 6)
	dtColPlanType := makePlan2Type(&dtColType)
	dtType := func(scale int32) planpb.Type {
		width := int32(19)
		if scale > 0 {
			width = 19 + scale + 1
		}
		tt := types.New(types.T_datetime, width, scale)
		return makePlan2Type(&tt)
	}
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	col := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  dtColPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "d"}},
		}
	}
	dtLiteral := func(scale int32, v types.Datetime) *planpb.Expr {
		e := makePlan2DateTimeConstExprWithType(int64(v))
		e.Typ = dtType(scale)
		return e
	}
	eqAt := func(scale int32, v types.Datetime) *planpb.Expr {
		return bind("=", col(), dtLiteral(scale, v))
	}
	between := func(lo, hi types.Datetime) *planpb.Expr {
		return bind("between", col(), dtLiteral(6, lo), dtLiteral(6, hi))
	}
	parse := func(s string) types.Datetime {
		v, err := types.ParseDatetime(s, 6)
		require.NoError(t, err)
		return v
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal temporal literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	sameZero, err := types.ParseDatetime("2024-01-01 00:00:00", 0)
	require.NoError(t, err)
	sameFull, err := types.ParseDatetime("2024-01-01 00:00:00", 6)
	require.NoError(t, err)
	require.Equal(t, sameZero, sameFull)
	lo := parse("2023-01-01 00:00:00")
	hi := parse("2025-01-01 00:00:00")

	// The same value spelled at DATETIME(0) and DATETIME(6) plus a covering
	// BETWEEN is satisfiable: the matching row survives with its real
	// predicates.
	require.Len(t, keepSatisfiable(eqAt(0, sameZero), eqAt(6, sameFull), between(lo, hi)), 3)
	// Without a range the SQL-equal pair merges like a duplicate equality.
	require.Len(t, keepSatisfiable(eqAt(0, sameZero), eqAt(6, sameFull)), 1)
	// A satisfiable single equality plus the range keeps both conjuncts.
	require.Len(t, keepSatisfiable(eqAt(6, sameFull), between(lo, hi)), 2)

	// Genuinely distinct temporal values still fold, with and without a range.
	otherDay := parse("2024-01-03 00:00:00")
	folded(eqAt(0, sameZero), eqAt(6, otherDay), between(lo, hi))
	folded(eqAt(0, sameZero), eqAt(6, otherDay))
	// Scale-rounded spellings materialize different runtime values and fold.
	rounded, err := types.ParseDatetime("2024-01-01 00:00:00.5", 0)
	require.NoError(t, err)
	kept, err := types.ParseDatetime("2024-01-01 00:00:00.5", 6)
	require.NoError(t, err)
	require.NotEqual(t, rounded, kept)
	folded(eqAt(0, rounded), eqAt(6, kept), between(lo, hi))
	folded(eqAt(0, rounded), eqAt(6, kept))

	// The key comparison itself follows the materialized temporal value.
	key := func(scale int32, v types.Datetime) (string, bool) {
		return constLiteralKey(dtLiteral(scale, v))
	}
	kZero, okZero := key(0, sameZero)
	kFull, okFull := key(6, sameFull)
	require.True(t, okZero && okFull)
	require.Equal(t, kZero, kFull)
	kOther, okOther := key(6, otherDay)
	require.True(t, okOther)
	require.NotEqual(t, kZero, kOther)
	kRounded, okRounded := key(0, rounded)
	kKept, okKept := key(6, kept)
	require.True(t, okRounded && okKept)
	require.NotEqual(t, kRounded, kKept)

	// TIME and TIMESTAMP share the materialized-payload comparison contract:
	// equal clock values at different scales key equal.
	timeZero, err := types.ParseTime("11:11:11", 0)
	require.NoError(t, err)
	timeFull, err := types.ParseTime("11:11:11", 6)
	require.NoError(t, err)
	require.Equal(t, timeZero, timeFull)
	timeZeroType := types.New(types.T_time, 10, 0)
	timeFullType := types.New(types.T_time, 15, 6)
	timeZeroExpr := makePlan2TimeConstExprWithType(int64(timeZero))
	timeZeroExpr.Typ = makePlan2Type(&timeZeroType)
	timeFullExpr := makePlan2TimeConstExprWithType(int64(timeFull))
	timeFullExpr.Typ = makePlan2Type(&timeFullType)
	tkZero, okTkZero := constLiteralKey(timeZeroExpr)
	tkFull, okTkFull := constLiteralKey(timeFullExpr)
	require.True(t, okTkZero && okTkFull)
	require.Equal(t, tkZero, tkFull)

	tsZero, err := types.ParseTimestamp(time.Local, "2024-01-01 00:00:00", 0)
	require.NoError(t, err)
	tsFull, err := types.ParseTimestamp(time.Local, "2024-01-01 00:00:00", 6)
	require.NoError(t, err)
	require.Equal(t, tsZero, tsFull)
	tsZeroType := types.New(types.T_timestamp, 19, 0)
	tsFullType := types.New(types.T_timestamp, 26, 6)
	tsZeroExpr := makePlan2TimestampConstExprWithType(int64(tsZero))
	tsZeroExpr.Typ = makePlan2Type(&tsZeroType)
	tsFullExpr := makePlan2TimestampConstExprWithType(int64(tsFull))
	tsFullExpr.Typ = makePlan2Type(&tsFullType)
	skZero, okSkZero := constLiteralKey(tsZeroExpr)
	skFull, okSkFull := constLiteralKey(tsFullExpr)
	require.True(t, okSkZero && okSkFull)
	require.Equal(t, skZero, skFull)
}

// TestDecimalWidthPointDomainSQLValueEquality pins the SQL value semantics of
// the domain keys for decimal literals: runtime decimal comparison rescales
// coefficient/scale pairs by operand scale and never consults the declared
// width, so the same value spelled at DECIMAL(8,2) and DECIMAL(12,2) (and two
// DECIMAL128 widths) is never a distinct-literal contradiction, while
// genuinely distinct coefficient/scale pairs still fold.
func TestDecimalWidthPointDomainSQLValueEquality(t *testing.T) {
	colType := types.New(types.T_decimal64, 16, 2)
	colPlanType := makePlan2Type(&colType)
	bigColType := types.New(types.T_decimal128, 38, 2)
	bigColPlanType := makePlan2Type(&bigColType)
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	col := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  colPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "d"}},
		}
	}
	bigCol := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  bigColPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "e"}},
		}
	}
	// 19.00 is coefficient 1900 at scale 2; 81.25 is coefficient 8125.
	decLit := func(width, scale int32, coeff int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Width: width, Scale: scale, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: coeff}},
			}},
		}
	}
	dec128Lit := func(width int32, coeff int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal128), Width: width, Scale: 2, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Decimal128Val{Decimal128Val: &planpb.Decimal128{A: coeff}},
			}},
		}
	}
	eqAt := func(width int32, coeff int64) *planpb.Expr {
		return bind("=", col(), decLit(width, 2, coeff))
	}
	between := func(lo, hi int64) *planpb.Expr {
		return bind("between", col(), decLit(16, 2, lo), decLit(16, 2, hi))
	}
	eq128At := func(width int32, coeff int64) *planpb.Expr {
		return bind("=", bigCol(), dec128Lit(width, coeff))
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal decimal literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// The same value spelled at DECIMAL(8,2) and DECIMAL(12,2) plus a covering
	// BETWEEN is satisfiable: the matching row survives with its real
	// predicates.
	require.Len(t, keepSatisfiable(eqAt(8, 1900), eqAt(12, 1900), between(0, 10000)), 3)
	// Without a range the SQL-equal pair merges like a duplicate equality.
	require.Len(t, keepSatisfiable(eqAt(8, 1900), eqAt(12, 1900)), 1)
	// A satisfiable single equality plus the range keeps both conjuncts.
	require.Len(t, keepSatisfiable(eqAt(12, 1900), between(0, 10000)), 2)

	// Genuinely distinct decimal values still fold, with and without a range.
	folded(eqAt(8, 1900), eqAt(12, 8125), between(0, 10000))
	folded(eqAt(12, 1900), eqAt(12, 8125))

	// The key comparison itself follows the SQL value: width metadata is not
	// part of identity, but the coefficient/scale pair stays distinct.
	k8, ok8 := constLiteralKey(decLit(8, 2, 1900))
	k12, ok12 := constLiteralKey(decLit(12, 2, 1900))
	require.True(t, ok8 && ok12)
	require.Equal(t, k8, k12)
	kOther, okOther := constLiteralKey(decLit(12, 2, 8125))
	require.True(t, okOther)
	require.NotEqual(t, k8, kOther)
	kScaleOne, okScaleOne := constLiteralKey(decLit(8, 1, 190))
	require.True(t, okScaleOne)
	require.NotEqual(t, k8, kScaleOne)

	// DECIMAL128 shares the value comparison contract: the same value typed
	// at two widths is one domain point; a distinct value still folds.
	require.Len(t, keepSatisfiable(eq128At(18, 1900), eq128At(30, 1900)), 1)
	folded(eq128At(18, 1900), eq128At(30, 8125))
	k128a, okA := constLiteralKey(dec128Lit(18, 1900))
	k128b, okB := constLiteralKey(dec128Lit(30, 1900))
	require.True(t, okA && okB)
	require.Equal(t, k128a, k128b)
	k128Other, okC := constLiteralKey(dec128Lit(30, 8125))
	require.True(t, okC)
	require.NotEqual(t, k128a, k128Other)
}

// TestDecimalProvenancePointDomainSQLValueEquality pins that binding
// provenance on decimal literals never decides SQL-value identity: the binder
// marks a literal whose canonical spelling differs from its source (for
// example '019.00' canonicalizes to '19.00') with the
// DecimalLiteralRequiresV82 protocol marker, which survives on the executable
// expression, but the marked and the ordinary spelling of the same value are
// SQL-equal and must never falsify a satisfiable point domain; genuinely
// distinct values still fold.
func TestDecimalProvenancePointDomainSQLValueEquality(t *testing.T) {
	colType := types.New(types.T_decimal64, 16, 2)
	colPlanType := makePlan2Type(&colType)
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	col := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  colPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "d"}},
		}
	}
	// The nonce literal mirrors the binder's plain decimal64 shape (width 18);
	// 19.00 is coefficient 1900 at scale 2 and 81.25 is coefficient 8125.
	plainLit := func(width int32, coeff int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Width: width, Scale: 2, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: coeff}},
			}},
		}
	}
	markedLit := func(width int32, coeff int64) *planpb.Expr {
		e := plainLit(width, coeff)
		e.GetLit().DecimalLiteralRequiresV82 = true
		return e
	}
	eqAt := func(width int32, coeff int64) *planpb.Expr {
		return bind("=", col(), plainLit(width, coeff))
	}
	eqMarkedAt := func(width int32, coeff int64) *planpb.Expr {
		return bind("=", col(), markedLit(width, coeff))
	}
	between := func(lo, hi int64) *planpb.Expr {
		return bind("between", col(), plainLit(16, lo), plainLit(16, hi))
	}
	require.True(t, markedLit(18, 1900).GetLit().GetDecimalLiteralRequiresV82())
	require.False(t, plainLit(18, 1900).GetLit().GetDecimalLiteralRequiresV82())

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal provenance-marked decimal literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// The marked spelling of the same value is satisfiable next to the
	// ordinary spelling, in both orders, with and without a covering BETWEEN;
	// binder-shaped same-width (18) and cross-width (8/12) spellings behave
	// alike because width is already value-irrelevant at the seam.
	require.Len(t, keepSatisfiable(eqMarkedAt(18, 1900), eqAt(18, 1900), between(0, 10000)), 3)
	require.Len(t, keepSatisfiable(eqAt(18, 1900), eqMarkedAt(18, 1900), between(0, 10000)), 3)
	require.Len(t, keepSatisfiable(eqMarkedAt(12, 1900), eqAt(8, 1900), between(0, 10000)), 3)
	require.Len(t, keepSatisfiable(eqMarkedAt(12, 1900), eqAt(8, 1900)), 1)
	require.Len(t, keepSatisfiable(eqAt(8, 1900), eqMarkedAt(12, 1900)), 1)

	// Genuinely distinct values still fold with the marked spelling on either
	// side, with and without the range.
	folded(eqMarkedAt(18, 1900), eqAt(18, 8125), between(0, 10000))
	folded(eqAt(18, 1900), eqMarkedAt(18, 8125), between(0, 10000))
	folded(eqMarkedAt(18, 1900), eqAt(18, 8125))

	// The value key ignores the provenance marker; executable identity
	// elsewhere explicitly preserves it.
	kMarked, okM := constLiteralKey(markedLit(12, 1900))
	kPlain, okP := constLiteralKey(plainLit(12, 1900))
	require.True(t, okM && okP)
	require.Equal(t, kMarked, kPlain)
	kPlain8, okP8 := constLiteralKey(plainLit(8, 1900))
	require.True(t, okP8)
	require.Equal(t, kMarked, kPlain8)
	kOther, okO := constLiteralKey(markedLit(12, 8125))
	require.True(t, okO)
	require.NotEqual(t, kMarked, kOther)

	// The IN-list value-set key makes the same provenance decision.
	bkMarked, okBM := blockFilterLiteralKey(markedLit(12, 1900).GetLit(), markedLit(12, 1900).Typ)
	bkPlain, okBP := blockFilterLiteralKey(plainLit(12, 1900).GetLit(), plainLit(12, 1900).Typ)
	require.True(t, okBM && okBP)
	require.Equal(t, bkMarked, bkPlain)
	bkOther, okBO := blockFilterLiteralKey(markedLit(12, 8125).GetLit(), markedLit(12, 8125).Typ)
	require.True(t, okBO)
	require.NotEqual(t, bkMarked, bkOther)
}

// TestIntegerCastPointDomainSQLValueEquality pins that declaration metadata on
// integral literals never decides SQL-value identity: runtime integer equality
// compares only the materialized payload (function/func_compare.go), and
// integral Type.Eq ignores Width/Scale (pkg/container/types/types.go), so the
// plain literal 19 (Width/Scale 0/0) and the folded CAST('19' AS SIGNED)
// literal (Width 64/Scale -1) are SQL-equal domain points and must never
// falsify a satisfiable point domain, in either argument order and with or
// without a covering BETWEEN; genuinely distinct values still fold.
func TestIntegerCastPointDomainSQLValueEquality(t *testing.T) {
	colType := types.New(types.T_int64, 0, 0)
	colPlanType := makePlan2Type(&colType)
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	col := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  colPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "v"}},
		}
	}
	// The plain token mirrors the binder's makePlan2Int64ConstExprWithType
	// shape (Width/Scale 0/0); the cast spelling mirrors the folded
	// CAST('19' AS SIGNED) literal (Width 64/Scale -1). Both carry I64Val.
	intLit := func(width, scale int32, v int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int64), Width: width, Scale: scale, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_I64Val{I64Val: v},
			}},
		}
	}
	eqAt := func(width, scale int32, v int64) *planpb.Expr {
		return bind("=", col(), intLit(width, scale, v))
	}
	between := func(lo, hi int64) *planpb.Expr {
		return bind("between", col(), intLit(64, -1, lo), intLit(64, -1, hi))
	}

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal integer literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// The plain and SIGNED-cast spellings of 19 are satisfiable together, in
	// both orders, with and without a covering BETWEEN.
	require.Len(t, keepSatisfiable(eqAt(0, 0, 19), eqAt(64, -1, 19), between(0, 100)), 3)
	require.Len(t, keepSatisfiable(eqAt(64, -1, 19), eqAt(0, 0, 19), between(0, 100)), 3)
	require.Len(t, keepSatisfiable(eqAt(0, 0, 19), eqAt(64, -1, 19)), 1)
	require.Len(t, keepSatisfiable(eqAt(64, -1, 19), eqAt(0, 0, 19)), 1)

	// Genuinely distinct values still fold, with and without the range.
	folded(eqAt(0, 0, 19), eqAt(64, -1, 20), between(0, 100))
	folded(eqAt(64, -1, 19), eqAt(0, 0, 20))

	// The value key ignores the declaration metadata; the payload stays part
	// of the key.
	kPlain, okP := constLiteralKey(intLit(0, 0, 19))
	kCast, okC := constLiteralKey(intLit(64, -1, 19))
	require.True(t, okP && okC)
	require.Equal(t, kPlain, kCast)
	kOther, okO := constLiteralKey(intLit(64, -1, 20))
	require.True(t, okO)
	require.NotEqual(t, kPlain, kOther)

	// The IN-list value-set key makes the same decision.
	bkPlain, okBP := blockFilterLiteralKey(intLit(0, 0, 19).GetLit(), intLit(0, 0, 19).Typ)
	bkCast, okBC := blockFilterLiteralKey(intLit(64, -1, 19).GetLit(), intLit(64, -1, 19).Typ)
	require.True(t, okBP && okBC)
	require.Equal(t, bkPlain, bkCast)
	bkOther, okBO := blockFilterLiteralKey(intLit(64, -1, 20).GetLit(), intLit(64, -1, 20).Typ)
	require.True(t, okBO)
	require.NotEqual(t, bkPlain, bkOther)
}

// TestDoubleCastPointDomainSQLValueEquality pins that materialized floating
// declaration metadata never decides SQL-value identity at the domain seam
// (round-9): runtime DOUBLE comparison tests the materialized payload only
// (function/func_compare.go), so the DOUBLE(M,D) width/scale an explicit cast
// carries cannot make two spellings of one value a distinct-literal
// contradiction. Runtime FLOAT comparison additionally normalizes each
// operand by its own declared scale (float32ComparisonNormalizers ->
// types.Float32ScaleNormalizer), so the value key must follow that effective
// comparison value: cross-width or cross-scale spellings of one value share
// one point, while scale-rounded spellings that test different values stay
// distinct points and still fold. Comparison binding converges floating
// literal shapes on bare columns (checkNoNeedCast admits any float literal
// against a float column and recasts both operands to the column shape), so
// these filters deliberately carry the declaration shapes the parser
// produces for double(M,D)/float(M,D) casts and exercise the seam directly,
// guarding any path that delivers those shapes to domain normalization.
func TestDoubleCastPointDomainSQLValueEquality(t *testing.T) {
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	eqNode := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: name},
				Args: args,
			}},
		}
	}
	dblCol := func() *planpb.Expr {
		f64 := types.New(types.T_float64, 0, 0)
		return &planpb.Expr{
			Typ:  makePlan2Type(&f64),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "f"}},
		}
	}
	f32Col := func() *planpb.Expr {
		f32 := types.New(types.T_float32, 0, 0)
		return &planpb.Expr{
			Typ:  makePlan2Type(&f32),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 2, Name: "g"}},
		}
	}
	dblLit := func(width, scale int32, v float64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_float64), Width: width, Scale: scale, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Dval{Dval: v},
			}},
		}
	}
	f32Lit := func(width, scale int32, v float64) *planpb.Expr {
		f32 := types.New(types.T_float32, width, scale)
		return &planpb.Expr{
			Typ: makePlan2Type(&f32),
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Fval{Fval: float32(v)},
			}},
		}
	}
	dblEq := func(width, scale int32, v float64) *planpb.Expr {
		return eqNode("=", dblCol(), dblLit(width, scale, v))
	}
	dblBetween := func(lo, hi float64) *planpb.Expr {
		return bind("between", dblCol(), dblLit(0, 0, lo), dblLit(0, 0, hi))
	}
	f32Eq := func(width, scale int32, v float64) *planpb.Expr {
		return eqNode("=", f32Col(), f32Lit(width, scale, v))
	}
	f32Between := func(lo, hi float64) *planpb.Expr {
		return bind("between", f32Col(), f32Lit(0, 0, lo), f32Lit(0, 0, hi))
	}
	// The float32 literal payload is stored as a float32 (mirroring
	// makePlan2Float32ConstExprWithType) and widened to its exact float64
	// value at runtime comparisons.
	f32Payload := func(v float64) float64 { return float64(float32(v)) }
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal floating literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// DOUBLE: cross-width and cross-scale spellings of one payload value are
	// one SQL point in both orders, with or without a covering BETWEEN.
	require.Len(t, keepSatisfiable(dblEq(8, 2, 19), dblEq(12, 2, 19), dblBetween(0, 100)), 3)
	require.Len(t, keepSatisfiable(dblEq(12, 2, 19), dblEq(8, 2, 19), dblBetween(0, 100)), 3)
	require.Len(t, keepSatisfiable(dblEq(8, 2, 19), dblEq(12, 2, 19)), 1)
	require.Len(t, keepSatisfiable(dblEq(12, 2, 19), dblEq(8, 2, 19)), 1)
	require.Len(t, keepSatisfiable(dblEq(8, 1, 19.5), dblEq(12, 2, 19.5), dblBetween(0, 100)), 3)
	require.Len(t, keepSatisfiable(dblEq(12, 2, 19.5), dblEq(8, 1, 19.5)), 1)

	// Genuinely distinct double payloads still fold, with and without a range,
	// and cross-width spellings do not mask a payload difference.
	folded(dblEq(8, 2, 19), dblEq(12, 2, 81), dblBetween(0, 100))
	folded(dblEq(12, 2, 19), dblEq(8, 2, 81))
	folded(dblEq(0, 0, 19), dblEq(8, 2, 19.5))

	// FLOAT: cross-width spellings of one value (19.00 at (8,2) and (12,4))
	// and runtime-equal cross-scale spellings (19.5 at (8,2) and (12,3)) are
	// one SQL point, because each side normalizes to the same comparison value.
	require.Len(t, keepSatisfiable(f32Eq(8, 2, f32Payload(19)), f32Eq(12, 4, f32Payload(19)), f32Between(0, 100)), 3)
	require.Len(t, keepSatisfiable(f32Eq(12, 4, f32Payload(19)), f32Eq(8, 2, f32Payload(19)), f32Between(0, 100)), 3)
	require.Len(t, keepSatisfiable(f32Eq(8, 2, f32Payload(19)), f32Eq(12, 4, f32Payload(19))), 1)
	require.Len(t, keepSatisfiable(f32Eq(8, 2, f32Payload(19.5)), f32Eq(12, 3, f32Payload(19.5)), f32Between(0, 100)), 3)

	// Scale-rounded float32 spellings whose comparison values differ
	// (19.004: round(_,2) is 19.00 while round(_,3) is 19.004) stay distinct
	// points in both orders, with and without the range.
	folded(f32Eq(8, 2, f32Payload(19.004)), f32Eq(12, 3, f32Payload(19.004)), f32Between(0, 100))
	folded(f32Eq(12, 3, f32Payload(19.004)), f32Eq(8, 2, f32Payload(19.004)))
	folded(f32Eq(8, 2, f32Payload(19)), f32Eq(12, 4, f32Payload(81)))

	// The value key decides by payload (after the float32 scale
	// normalization mirrors the runtime comparison value).
	dblKeyA, okDA := constLiteralKey(dblLit(8, 2, 19))
	dblKeyB, okDB := constLiteralKey(dblLit(12, 2, 19))
	require.True(t, okDA && okDB)
	require.Equal(t, dblKeyA, dblKeyB)
	dblKeyC, okDC := constLiteralKey(dblLit(0, 0, 19))
	require.True(t, okDC)
	require.Equal(t, dblKeyA, dblKeyC)
	dblKeyOther, okDO := constLiteralKey(dblLit(12, 2, 81))
	require.True(t, okDO)
	require.NotEqual(t, dblKeyA, dblKeyOther)

	f32KeyA, okFA := constLiteralKey(f32Lit(8, 2, f32Payload(19)))
	f32KeyB, okFB := constLiteralKey(f32Lit(12, 4, f32Payload(19)))
	require.True(t, okFA && okFB)
	require.Equal(t, f32KeyA, f32KeyB)
	f32KeyScaleA, okSA := constLiteralKey(f32Lit(8, 2, f32Payload(19.004)))
	f32KeyScaleB, okSB := constLiteralKey(f32Lit(12, 3, f32Payload(19.004)))
	require.True(t, okSA && okSB)
	require.NotEqual(t, f32KeyScaleA, f32KeyScaleB)
	f32KeyRounded, okSR := constLiteralKey(f32Lit(8, 2, f32Payload(19)))
	require.True(t, okSR)
	// 19.004 at scale 2 tests the value 19.00 at runtime, so its key is the
	// plain 19.00 key, while the scale-3 spelling (which tests 19.004) keeps
	// a distinct key.
	require.Equal(t, f32KeyScaleA, f32KeyRounded)
	require.NotEqual(t, f32KeyScaleB, f32KeyRounded)

	// The IN-list value-set key makes the same decision.
	bkDblA, okBA := blockFilterLiteralKey(dblLit(8, 2, 19).GetLit(), dblLit(8, 2, 19).Typ)
	bkDblB, okBB := blockFilterLiteralKey(dblLit(12, 2, 19).GetLit(), dblLit(12, 2, 19).Typ)
	require.True(t, okBA && okBB)
	require.Equal(t, bkDblA, bkDblB)
	bkDblOther, okBO := blockFilterLiteralKey(dblLit(12, 2, 81).GetLit(), dblLit(12, 2, 81).Typ)
	require.True(t, okBO)
	require.NotEqual(t, bkDblA, bkDblOther)

	bkF32A, okBFA := blockFilterLiteralKey(f32Lit(8, 2, f32Payload(19)).GetLit(), f32Lit(8, 2, f32Payload(19)).Typ)
	bkF32B, okBFB := blockFilterLiteralKey(f32Lit(12, 4, f32Payload(19)).GetLit(), f32Lit(12, 4, f32Payload(19)).Typ)
	require.True(t, okBFA && okBFB)
	require.Equal(t, bkF32A, bkF32B)
	bkF32Rounded, okBFR := blockFilterLiteralKey(f32Lit(12, 3, f32Payload(19.004)).GetLit(), f32Lit(12, 3, f32Payload(19.004)).Typ)
	require.True(t, okBFR)
	require.NotEqual(t, bkF32A, bkF32Rounded)
}

// TestFoldedSourcePointDomainSQLValueEquality pins that a materialized
// literal and the bind-time source retained in Literal.Src stay one SQL
// value (round-9): binding folds CAST('19.00' AS DECIMAL(8,2)) DIV 1 into
// an int64 literal and attaches the folded DIV expression as Literal.Src
// (the integer-division provenance in constant folding). The point-domain key
// must decide by the materialized payload, not by the marshaled source
// bytes, while a source that embeds execution-dependent state (parameter
// references) cannot prove the payload's value identity and must decline
// instead of falsifying a satisfiable filter. Executable-expression identity
// elsewhere keeps Src.
func TestFoldedSourcePointDomainSQLValueEquality(t *testing.T) {
	colType := types.New(types.T_int64, 0, 0)
	colPlanType := makePlan2Type(&colType)
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
		require.NoError(t, err)
		return bound
	}
	col := func() *planpb.Expr {
		return &planpb.Expr{
			Typ:  colPlanType,
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "v"}},
		}
	}
	intLit := func(v int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int64), Width: 0, Scale: 0, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_I64Val{I64Val: v},
			}},
		}
	}
	// The decimal dividend mirrors the folded CAST('19.00' AS DECIMAL(8,2))
	// literal: coefficient 1900 at scale 2.
	decLit := func(coeff int64) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 8, Scale: 2, NotNullable: true},
			Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{
				Value: &planpb.Literal_Decimal64Val{Decimal64Val: &planpb.Decimal64{A: coeff}},
			}},
		}
	}
	// A real bind-time DIV fold: the result materializes an int64 literal and
	// retains the folded source expression in Literal.Src.
	proc := newPlanTestProcess(t)
	divFold := func(coeff int64) *planpb.Expr {
		bound := bind("div", decLit(coeff), intLit(1))
		folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
		require.NoError(t, err)
		return folded
	}
	eqInt := func(v int64) *planpb.Expr {
		return bind("=", col(), intLit(v))
	}
	eqExpr := func(e *planpb.Expr) *planpb.Expr {
		return bind("=", col(), e)
	}
	between := func(lo, hi int64) *planpb.Expr {
		return bind("between", col(), intLit(lo), intLit(hi))
	}

	div19 := divFold(1900)
	require.NotNil(t, div19.GetLit())
	require.Equal(t, int64(19), div19.GetLit().GetI64Val())
	require.NotNil(t, div19.GetLit().Src, "integer-division fold must retain its source")
	require.True(t, staticIntegerComparisonPeer(div19.GetLit().Src))

	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, false)
	keepSatisfiable := func(filters ...*planpb.Expr) []*planpb.Expr {
		out := builder.normalizeColumnDomain(filters)
		for _, f := range out {
			require.False(t, IsFalseExpr(f) || f.GetLit() != nil && f.GetLit().GetIsnull(),
				"SQL-equal folded-source literal filters must not fold to constant false")
		}
		return out
	}
	folded := func(filters ...*planpb.Expr) {
		out := builder.normalizeColumnDomain(filters)
		require.Len(t, out, 1)
		require.True(t, IsFalseExpr(out[0]))
	}

	// The plain literal and the DIV-folded materialization of the same value
	// are one SQL point in both orders, with and without a covering BETWEEN.
	require.Len(t, keepSatisfiable(eqInt(19), eqExpr(div19), between(0, 100)), 3)
	require.Len(t, keepSatisfiable(eqExpr(div19), eqInt(19), between(0, 100)), 3)
	require.Len(t, keepSatisfiable(eqInt(19), eqExpr(div19)), 1)
	require.Len(t, keepSatisfiable(eqExpr(div19), eqInt(19)), 1)

	// A genuinely distinct DIV-folded value still folds, with and without the
	// range.
	folded(eqInt(19), eqExpr(divFold(8100)), between(0, 100))
	folded(eqExpr(divFold(8100)), eqInt(19))

	// A source that embeds execution-dependent state cannot prove the
	// materialized payload: the key declines rather than deciding, and the
	// saturated pair keeps its real filters instead of falsifying.
	paramSrc := intLit(19)
	paramSrc.GetLit().Src = &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64), Width: 0, Scale: 0},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}},
	}
	_, okKey := constLiteralKey(paramSrc)
	require.False(t, okKey, "an execution-dependent source must decline the value key")
	_, okBlock := blockFilterLiteralKey(paramSrc.GetLit(), paramSrc.Typ)
	require.False(t, okBlock, "an execution-dependent source must decline the IN-list value key")
	require.Len(t, keepSatisfiable(eqInt(19), eqExpr(paramSrc), between(0, 100)), 3)
	require.Len(t, keepSatisfiable(eqExpr(paramSrc), eqInt(19)), 2)

	// The value key decides by the materialized payload once the retained
	// source is proven statically evaluable.
	kPlain, okP := constLiteralKey(intLit(19))
	kFolded, okF := constLiteralKey(div19)
	require.True(t, okP && okF)
	require.Equal(t, kPlain, kFolded)
	kOther, okO := constLiteralKey(divFold(8100))
	require.True(t, okO)
	require.NotEqual(t, kPlain, kOther)
	bkPlain, okBP := blockFilterLiteralKey(intLit(19).GetLit(), intLit(19).Typ)
	bkFolded, okBF := blockFilterLiteralKey(div19.GetLit(), div19.Typ)
	require.True(t, okBP && okBF)
	require.Equal(t, bkPlain, bkFolded)
	bkOther, okBO := blockFilterLiteralKey(divFold(8100).GetLit(), divFold(8100).Typ)
	require.True(t, okBO)
	require.NotEqual(t, bkPlain, bkOther)
}

// TestFoldedBooleanLiteralSelectivityBounds pins the selectivity of folded
// constant boolean filters (#32068): TRUE keeps every row, FALSE and NULL keep
// none, and boolean composition keeps honoring AND/OR/NOT bounds.
func TestFoldedBooleanLiteralSelectivityBounds(t *testing.T) {
	builder := newStatsTestBuilderWithNDV("d", 10)
	wrapper := builder.compCtx.GetStatsCache().Get(1)
	stats := wrapper.GetStats()

	boolFn := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_bool)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: name},
				Args: args,
			}},
		}
	}

	falseExpr := MakeFalseExpr()
	require.Equal(t, 0.00000001, estimateExprSelectivity(falseExpr, builder, stats))
	// The estimate is cached on the expression like every other filter estimate.
	require.Equal(t, 0.00000001, falseExpr.Selectivity)
	require.Equal(t, 0.00000001, estimateExprSelectivity(falseExpr, builder, stats))

	require.Equal(t, 1.0, estimateExprSelectivity(makePlan2BoolConstExprWithType(true), builder, stats))

	nullBool := &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_bool)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Isnull: true}},
	}
	require.Equal(t, 0.00000001, estimateExprSelectivity(nullBool, builder, stats))

	require.Equal(t, 0.00000001,
		estimateExprSelectivity(boolFn("and", MakeFalseExpr(), makePlan2BoolConstExprWithType(true)), builder, stats))
	require.Equal(t, 1.0,
		estimateExprSelectivity(boolFn("or", MakeFalseExpr(), makePlan2BoolConstExprWithType(true)), builder, stats))
	require.InDelta(t, 1.0,
		estimateExprSelectivity(boolFn("not", MakeFalseExpr()), builder, stats), 1e-7)

	// Non-boolean literals are not filter predicates; keep their legacy estimate.
	require.Equal(t, 1.0, estimateExprSelectivity(makePlan2Int64ConstExprWithType(10), builder, stats))
}

// TestConsolidatedOrAndInListSelectivityControls executes the OR/IN controls
// #32068 owns after #32059 was consolidated into it: on one column with many
// distinct values each single equality keeps the 1/NDV point estimate, and a
// disjoint OR (and its equivalent IN list) must never estimate below either
// per-value operand.
func TestConsolidatedOrAndInListSelectivityControls(t *testing.T) {
	mock := NewMockCompilerContext(false, newPlanTestProcess(t))
	part := mock.tables["part"]
	require.NotNil(t, part)

	statsCache := NewStatsCache()
	partStats := NewStatsInfo()
	partStats.TableName = "part"
	partStats.TableCnt = 1_000_000
	partStats.DataTypeMap["p_retailprice"] = uint64(types.T_decimal64)
	partStats.MinValMap["p_retailprice"] = 900
	partStats.MaxValMap["p_retailprice"] = 1100
	partStats.NdvMap["p_retailprice"] = 1000
	statsCache.Set(part.TblId, partStats)
	ctx := &fixedStatsCompilerContext{statsCacheCompilerContext: &statsCacheCompilerContext{
		MockCompilerContext: mock, statsCache: statsCache,
	}}

	buildScan := func(sql string) *planpb.Node {
		stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
		require.NoError(t, err)
		require.Len(t, stmts, 1)
		defer stmts[0].Free()
		built, err := BuildPlan(ctx, stmts[0], false, nil)
		require.NoError(t, err)
		var scan *planpb.Node
		for _, node := range built.GetQuery().Nodes {
			if node.NodeType == planpb.Node_TABLE_SCAN {
				scan = node
			}
		}
		require.NotNil(t, scan)
		return scan
	}

	// Per-value control: one equality equals the 1/NDV point estimate.
	for _, sql := range []string{
		"select p_partkey from part where p_retailprice = 1000.40",
		"select p_partkey from part where p_retailprice = 1000.50",
	} {
		require.Equal(t, 0.001, buildScan(sql).Stats.Selectivity, sql)
	}

	// Consolidated OR invariant: disjoint same-column alternatives keep a
	// union estimate, never one below either operand or above the full table.
	orScan := buildScan("select p_partkey from part where p_retailprice = 1000.40 or p_retailprice = 1000.50")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(orScan.FilterList)))
	require.GreaterOrEqual(t, orScan.Stats.Selectivity, 0.001-1e-12)
	require.LessOrEqual(t, orScan.Stats.Selectivity, 1.0)

	// The equivalent IN list keeps the same per-value floor.
	inScan := buildScan("select p_partkey from part where p_retailprice in (1000.40, 1000.50)")
	require.False(t, IsFalseExpr(colexec.RewriteFilterExprList(inScan.FilterList)))
	require.GreaterOrEqual(t, inScan.Stats.Selectivity, 0.001-1e-12)
	require.LessOrEqual(t, inScan.Stats.Selectivity, 1.0)

	t.Logf("consolidated controls: per-value=0.001000, or=%.6f, in=%.6f",
		orScan.Stats.Selectivity, inScan.Stats.Selectivity)
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

type fixedStatsCompilerContext struct {
	*statsCacheCompilerContext
}

func (ctx *fixedStatsCompilerContext) Stats(
	obj *planpb.ObjectRef,
	_ *planpb.Snapshot,
) (*pb.StatsInfo, error) {
	wrapper := ctx.statsCache.Get(uint64(obj.Obj))
	return wrapper.GetStats(), nil
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

func TestGetExecType_IndexSearchScanUsesMultiCN(t *testing.T) {
	query := &planpb.Query{
		Steps: []int32{0},
		Nodes: []*planpb.Node{{
			NodeId:          0,
			NodeType:        planpb.Node_INDEX_SEARCH_SCAN,
			Stats:           &planpb.Stats{BlockNum: 1, Cost: 1, Outcnt: 1},
			IndexSearchScan: &planpb.IndexSearchScan{Index: testIvfIndex},
		}},
	}
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(query, false, false))
}

func TestGetExecType_IndexSearchScanRespectsOneCNAndDDL(t *testing.T) {
	makeQuery := func(force bool) *planpb.Query {
		return &planpb.Query{
			Steps: []int32{0},
			Nodes: []*planpb.Node{{
				NodeId:          0,
				NodeType:        planpb.Node_INDEX_SEARCH_SCAN,
				Stats:           &planpb.Stats{BlockNum: 1, Cost: 1, Outcnt: 1, ForceOneCN: force},
				IndexSearchScan: &planpb.IndexSearchScan{Index: testIvfIndex},
			}},
		}
	}
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(makeQuery(true), false, false))
	require.Equal(t, ExecTypeAP_ONECN, GetExecType(makeQuery(false), true, false))
}

func TestDetermineBuildSidePreservesDeclaredRuntimeFilterDependency(t *testing.T) {
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, newPlanTestProcess(t)), false, true)
	builder.qry.Nodes = []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_INDEX_SEARCH_SCAN, Stats: &planpb.Stats{Outcnt: 2}},
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

func TestGuardedDiagnosticKeepsLoopJoinSupportedOrientation(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	intType := planpb.Type{Id: int32(types.T_int64)}
	timeType := planpb.Type{Id: int32(types.T_time)}
	leftTag, rightTag := int32(10), int32(20)
	bind := func(name string, args ...*planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), name, args)
		require.NoError(t, err)
		return expr
	}
	key := bind("=", GetColExpr(intType, leftTag, 0), GetColExpr(intType, rightTag, 0))
	invalidTime := bind("time", makePlan2StringConstExprWithType("900:00:00"))
	guard := bind("case",
		bind("=", GetColExpr(intType, leftTag, 0), makePlan2Int64ConstExprWithType(1)),
		bind(">", GetColExpr(timeType, leftTag, 1), invalidTime),
		makePlan2BoolConstExprWithType(true))
	require.True(t, ContainsGuardedJoinDiagnostic(ctx.GetProcess(), guard))

	for _, joinType := range []planpb.Node_JoinType{
		planpb.Node_LEFT, planpb.Node_SEMI, planpb.Node_ANTI, planpb.Node_SINGLE,
	} {
		t.Run(joinType.String(), func(t *testing.T) {
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
			builder.qry.Nodes = []*planpb.Node{
				{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{leftTag}, Stats: &planpb.Stats{Outcnt: 1}},
				{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{rightTag}, Stats: &planpb.Stats{Outcnt: 100}},
				{NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: joinType, Children: []int32{0, 1},
					OnList: []*planpb.Expr{key}, Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}}},
			}
			builder.determineBuildAndProbeSide(2, false)
			require.True(t, builder.qry.Nodes[2].IsRightJoin, "ordinary equijoin retains its cost choice")

			builder.qry.Nodes[2].OnList = []*planpb.Expr{key, guard}
			builder.determineBuildAndProbeSide(2, false)
			require.False(t, builder.qry.Nodes[2].IsRightJoin)
			builder.swapJoinChildren(2)
			require.Equal(t, []int32{0, 1}, builder.qry.Nodes[2].Children)
			require.Equal(t, joinType, builder.qry.Nodes[2].JoinType)
		})
	}

	param := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_P{
		P: &planpb.ParamRef{Pos: 0},
	}}
	params := vector.NewVec(types.T_text.ToType())
	proc := ctx.GetProcess()
	defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
	require.NoError(t, vector.AppendBytes(params, []byte("00:00:01"), false, proc.Mp()))
	proc.SetPrepareParams(params)
	preparedGuard := bind("case",
		bind("=", GetColExpr(intType, leftTag, 0), makePlan2Int64ConstExprWithType(1)),
		bind(">", GetColExpr(timeType, leftTag, 1), bind("time", param)),
		makePlan2BoolConstExprWithType(true))
	require.True(t, ContainsGuardedJoinDiagnostic(proc, preparedGuard))
	for _, joinType := range []planpb.Node_JoinType{
		planpb.Node_LEFT, planpb.Node_SEMI, planpb.Node_ANTI, planpb.Node_SINGLE,
	} {
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
		builder.qry.Nodes = []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{leftTag}, Stats: &planpb.Stats{Outcnt: 1}},
			{NodeId: 1, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{rightTag}, Stats: &planpb.Stats{Outcnt: 100}},
			{NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: joinType, Children: []int32{0, 1},
				OnList: []*planpb.Expr{key, preparedGuard}, Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{}}},
		}
		builder.qry.Steps = []int32{2}
		safe, err := ProbePreparedJoinParameterDiagnostics(proc, &Plan{Plan: &planpb.Plan_Query{Query: builder.qry}})
		require.NoError(t, err)
		require.True(t, safe)
		builder.preparedBindingProof = &safe
		builder.determineBuildAndProbeSide(2, false)
		require.True(t, builder.qry.Nodes[2].IsRightJoin, "%s safe prepared operand restores cost choice", joinType)
	}
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
			MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)),
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
	builder := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, nil), false, true)
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
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
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
	t.Run("marked SINGLE build remains logical right", func(t *testing.T) {
		builder := makeBuilder(materialized.CTEHashBuildScanOption)
		builder.qry.Nodes[2].JoinType = planpb.Node_SINGLE
		builder.determineBuildAndProbeSide(2, false)
		require.False(t, builder.qry.Nodes[2].IsRightJoin)
	})
}

func TestDetermineInnerBuildSidePreservesCTEHashBuildDrainProof(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
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

	t.Run("registered synthetic column carries estimate", func(t *testing.T) {
		col := &planpb.ColRef{RelPos: 999, ColPos: 3}
		expr := &planpb.Expr{Ndv: 73, Expr: &planpb.Expr_Col{Col: col}}
		require.Equal(t, -1.0, getExprNdv(expr, builder))

		builder.syntheticNDVCols = map[[2]int32]struct{}{{col.RelPos, col.ColPos}: {}}
		require.Equal(t, 73.0, getExprNdv(expr, builder))
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
	xs := make([]*Stats, 0, 44)
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

func TestStatsInfoCacheCompletion(t *testing.T) {
	for _, tc := range []struct {
		stats *pb.StatsInfo
		want  bool
	}{
		{nil, false},
		{&pb.StatsInfo{TableCnt: 5}, false},
		{&pb.StatsInfo{AccurateObjectNumber: 1, TableCnt: 5}, false},
		{&pb.StatsInfo{TableName: "t"}, true},
		{&pb.StatsInfo{TableName: "t", TableCnt: 5}, true},
		{&pb.StatsInfo{TableName: "t", TableCnt: math.NaN()}, false},
	} {
		require.Equal(t, tc.want, StatsInfoUsableForCache(tc.stats))
	}
}

func TestEstimatedRowsInt64(t *testing.T) {
	for _, tc := range []struct {
		rows float64
		want int64
	}{
		{0, 0}, {5.75, 5}, {-1, 0}, {math.NaN(), 0},
		{float64(^uint64(0)), math.MaxInt64}, {math.Inf(1), math.MaxInt64},
	} {
		require.Equal(t, tc.want, EstimatedRowsInt64(tc.rows))
	}
}

func TestCachedPlanStatsChanged(t *testing.T) {
	ctx := &tableDefStatsTestCompilerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t))}
	scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN,
		ObjRef: &planpb.ObjectRef{Obj: 42}, TableDef: &planpb.TableDef{Name: "events", Version: 7},
		Stats: &planpb.Stats{TableCnt: 5, Outcnt: 1},
	}
	p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{Nodes: []*planpb.Node{scan}, Steps: []int32{0}}}}
	for _, tc := range []struct {
		name    string
		stats   *pb.StatsInfo
		cached  float64
		changed bool
	}{
		{"stable despite runtime filter", &pb.StatsInfo{TableCnt: 5}, 5, false},
		{"growth", &pb.StatsInfo{TableCnt: 5000}, 5, true},
		{"minor growth", &pb.StatsInfo{TableCnt: 148}, 128, false},
		{"below doubling", &pb.StatsInfo{TableCnt: 255}, 128, false},
		{"doubling", &pb.StatsInfo{TableCnt: 256}, 128, true},
		{"above halving", &pb.StatsInfo{TableCnt: 129}, 256, false},
		{"halving after rollback", &pb.StatsInfo{TableCnt: 128}, 256, true},
		{"observed empty to one", &pb.StatsInfo{TableCnt: 1}, 0, true},
		{"stable empty", &pb.StatsInfo{TableName: "events"}, 0, false},
		{"missing despite nearby default", nil, 1001, true},
		{"invalid captured", &pb.StatsInfo{TableCnt: 128}, math.NaN(), true},
		{"invalid observation", &pb.StatsInfo{TableCnt: math.Inf(1)}, 128, true},
		{"conservative max growth", &pb.StatsInfo{TableCnt: float64(^uint64(0))}, float64(^uint64(0)) / 1.5, true},
		{"conservative max recovery", &pb.StatsInfo{TableCnt: float64(^uint64(0)) / 1.5}, float64(^uint64(0)), true},
		{"stable conservative max", &pb.StatsInfo{TableCnt: float64(^uint64(0))}, float64(^uint64(0)), false},
		{"same-count object transition", &pb.StatsInfo{TableName: "events", AccurateObjectNumber: 1, TableCnt: 5}, 5, false},
		{"lost observation", nil, 5, true},
		{"stable unknown default", nil, 1000, false},
		{"named empty", &pb.StatsInfo{TableName: "events"}, 5, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx.stats, scan.Stats.TableCnt = tc.stats, tc.cached
			changed, err := CachedPlanStatsChanged(p, ctx)
			require.NoError(t, err)
			require.Equal(t, tc.changed, changed)
			require.Same(t, scan.TableDef, ctx.gotTableDef)
			require.Same(t, scan.ObjRef, ctx.gotObject)
			require.Same(t, scan.ScanSnapshot, ctx.gotSnapshot)
			require.Equal(t, float64(1), scan.Stats.Outcnt, "admission never patches a borrowed plan")
		})
	}
	ctx.err = fmt.Errorf("stats unavailable")
	changed, err := CachedPlanStatsChanged(p, ctx)
	require.ErrorIs(t, err, ctx.err)
	require.False(t, changed, "do not execute or rebuild using a failed observation")
	scan.Limit = MakePlan2Uint64ConstExprWithType(0)
	changed, err = CachedPlanStatsChanged(p, ctx)
	require.NoError(t, err, "minimal LIMIT0 scan model does not read storage statistics")
	require.False(t, changed)
}

func TestCachedLimitZeroFinalPlan(t *testing.T) {
	ctx := &tableDefStatsTestCompilerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t))}
	statements, err := mysql.Parse(ctx.GetContext(), "select n_nationkey from nation limit 0", 1)
	require.NoError(t, err)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false, nil)
	require.NoError(t, err)
	qry := p.GetQuery()
	require.NotNil(t, qry)
	root := qry.Nodes[qry.Steps[0]]
	require.Equal(t, planpb.Node_PROJECT, root.NodeType)
	require.Nil(t, root.Limit, "optimizer moves pagination while retaining output projection")
	scan := qry.Nodes[root.Children[0]]
	require.Equal(t, planpb.Node_TABLE_SCAN, scan.NodeType)
	require.NotNil(t, scan.Limit)
	ctx.stats, ctx.err = &pb.StatsInfo{TableCnt: 5}, fmt.Errorf("unexpected stats read")
	changed, err := CachedPlanStatsChanged(p, ctx)
	require.NoError(t, err)
	require.False(t, changed)
	scan.Limit = MakePlan2Uint64ConstExprWithType(1)
	_, err = CachedPlanStatsChanged(p, ctx)
	require.ErrorIs(t, err, ctx.err, "positive LIMIT still validates the normal scan model")
}

func TestAggregateBlockEstimateThroughL2(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(t, false)
	builder.qry.Nodes[0].Stats.Outcnt = float64(^uint64(0))
	builder.qry.Nodes = append(builder.qry.Nodes,
		&planpb.Node{NodeType: planpb.Node_AGG, Children: []int32{0}, Stats: DefaultStats()},
		&planpb.Node{NodeType: planpb.Node_PROJECT, Children: []int32{3}, Stats: DefaultStats()},
		&planpb.Node{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_L2, Children: []int32{4, 1}, Stats: DefaultStats()},
	)
	ReCalcNodeStats(5, builder, true, false, false)
	require.Equal(t, int32(math.MaxInt32), builder.qry.Nodes[5].Stats.BlockNum)
	for _, tc := range []struct{ blocks, want int32 }{
		{2, 16}, {268435455, 2147483640}, {268435456, math.MaxInt32},
	} {
		builder.qry.Nodes[4].Stats.BlockNum = tc.blocks
		ReCalcNodeStats(5, builder, false, false, false)
		require.Equal(t, tc.want, builder.qry.Nodes[5].Stats.BlockNum)
	}
}

func TestValueScanCardinality(t *testing.T) {
	for _, tc := range []struct {
		name   string
		rowset *planpb.RowsetData
		table  *planpb.TableDef
		limit  *Expr
		want   float64
	}{
		{name: "dummy", want: 1},
		{name: "dummy limit zero", limit: MakePlan2Uint64ConstExprWithType(0)},
		{name: "empty values", rowset: &planpb.RowsetData{}},
		{name: "one value", rowset: &planpb.RowsetData{RowCount: 1}, want: 1},
		{name: "multiple values", rowset: &planpb.RowsetData{RowCount: 3}, want: 3},
		{name: "table control", table: &planpb.TableDef{}, want: 1000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewQueryBuilder(planpb.Query_SELECT, NewMockCompilerContext(false, nil), false, false)
			n := &planpb.Node{NodeType: planpb.Node_VALUE_SCAN, RowsetData: tc.rowset, TableDef: tc.table, Limit: tc.limit, Stats: DefaultStats()}
			b.qry.Nodes = []*planpb.Node{n, {NodeType: planpb.Node_PROJECT, Children: []int32{0}, Stats: DefaultStats()},
				{NodeType: planpb.Node_VALUE_SCAN, RowsetData: &planpb.RowsetData{RowCount: 10}, Stats: DefaultStats()},
				{NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER, Children: []int32{2, 1}, Stats: DefaultStats()}}
			for range 2 {
				ReCalcNodeStats(3, b, true, true, true)
				require.Equal(t, tc.want, n.Stats.Outcnt)
				require.Equal(t, tc.want, b.qry.Nodes[1].Stats.Outcnt)
				require.Equal(t, tc.want, b.qry.Nodes[3].Stats.HashmapStats.HashmapSize)
				require.Equal(t, 10*tc.want, b.qry.Nodes[3].Stats.Outcnt)
			}
		})
	}
}

func TestCachedPlanStatsAdmissionQueryShapes(t *testing.T) {
	for _, tc := range []struct {
		sql    string
		strict bool
	}{
		{"select n_name from nation where n_nationkey=1", false},
		{"update nation set n_regionkey=2 where n_nationkey=1", false},
		{"select n_name from nation where n_nationkey=1 for update", false},
		{"delete from nation where n_nationkey=1", false},
		{"select sum(n_regionkey) from nation where n_nationkey>1", false},
		{"select n_name from nation where n_nationkey>1 order by n_nationkey", false},
		{"select n_name from nation where n_nationkey>1", false},
		{"select n_name from nation cross join region", true},
		{"update nation set n_regionkey=2 where n_nationkey>1", false},
		{"select n_name from nation where cast(n_nationkey as double)=1", false},
		{"select n_name from nation limit 1", false},
		{"select ps_availqty from partsupp where ps_partkey=1 and ps_suppkey=2", false},
		{"select ps_availqty from partsupp where ps_partkey=1", false},
		{"select n_name from nation where cast(n_nationkey as bigint)=1", false},
		{"select n_name from nation where n_nationkey in (1,2)", false},
		{"select n_name from nation where n_nationkey=1 or n_nationkey=2", false},
		{"select n_name from nation join region on n_regionkey=r_regionkey where n_nationkey=1", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ctx := &tableDefStatsTestCompilerContext{MockCompilerContext: NewMockCompilerContext(true, newPlanTestProcess(t))}
			// The legacy TPCH mock uses cluster-key metadata with empty key
			// names. Supply a real composite primary-key catalog for this test.
			table := ctx.tables["partsupp"]
			table.Pkey = &planpb.PrimaryKeyDef{PkeyColName: catalog.CPrimaryKeyColName, Names: []string{"ps_partkey", "ps_suppkey"}}
			hidden := MakeHiddenColDefByName(catalog.CPrimaryKeyColName)
			table.Pkey.CompPkeyCol = hidden
			table.Cols = append(table.Cols, hidden)

			stmts, err := mysql.Parse(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmts[0].Free()
			p, err := BuildPlan(ctx, stmts[0], false, nil)
			require.NoError(t, err)
			require.Equal(t, tc.strict, cachedPlanRequiresExactStats(p.GetQuery()), "%s", p.String())
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_TABLE_SCAN {
					node.Stats.TableCnt = 128
				}
			}
			for _, rows := range []float64{129, 148, 255, 128, 256} {
				ctx.stats = &pb.StatsInfo{TableCnt: rows}
				changed, err := CachedPlanStatsChanged(p, ctx)
				require.NoError(t, err)
				require.Equal(t, rows == 256 || tc.strict && rows != 128, changed)
			}
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == planpb.Node_TABLE_SCAN {
					require.Equal(t, float64(128), node.Stats.TableCnt)
				}
			}
		})
	}
}

func BenchmarkCachedPointPlanAdmission(b *testing.B) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(b))
	stmts, err := mysql.Parse(ctx.GetContext(), "select n_name from nation where n_nationkey=1", 1)
	require.NoError(b, err)
	defer stmts[0].Free()
	p, err := BuildPlan(ctx, stmts[0], false, nil)
	require.NoError(b, err)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		changed, err := CachedPlanStatsChanged(p, ctx)
		if err != nil || changed {
			b.Fatal("point admission failed", err)
		}
	}
}

func TestCachedPlanStrictStatsReachability(t *testing.T) {
	for _, tc := range []struct {
		name     string
		joinType planpb.Node_JoinType
		right    bool
	}{
		{"cartesian", planpb.Node_INNER, false}, {"resident dedup", planpb.Node_DEDUP, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scan := &planpb.Node{NodeType: planpb.Node_TABLE_SCAN, TableDef: &planpb.TableDef{Name: "events"}, ObjRef: &planpb.ObjectRef{Obj: 42}, Stats: &planpb.Stats{TableCnt: 128}}
			q := &planpb.Query{Nodes: []*planpb.Node{scan,
				{NodeType: planpb.Node_JOIN, JoinType: tc.joinType, IsRightJoin: tc.right, Children: []int32{0, 0}}}, Steps: []int32{0}}
			p := &planpb.Plan{Plan: &planpb.Plan_Query{Query: q}}
			ctx := &tableDefStatsTestCompilerContext{MockCompilerContext: NewMockCompilerContext(false, newPlanTestProcess(t)), stats: &pb.StatsInfo{TableCnt: 129}}
			changed, err := CachedPlanStatsChanged(p, ctx)
			require.NoError(t, err)
			require.False(t, changed, "abandoned construction joins are not executable")
			q.Steps = append(q.Steps, 1)
			changed, err = CachedPlanStatsChanged(p, ctx)
			require.NoError(t, err)
			require.True(t, changed, "every executable step contributes its contract")
			ctx.stats.TableCnt = 128
			changed, err = CachedPlanStatsChanged(p, ctx)
			require.NoError(t, err)
			require.False(t, changed, "strict admission still allows stable reuse")
			q.Steps = []int32{0}
			q.BackgroundQueries = []*planpb.Query{{}}
			ctx.stats.TableCnt = 129
			changed, err = CachedPlanStatsChanged(p, ctx)
			require.NoError(t, err)
			require.True(t, changed, "background contracts retain exact admission")
		})
	}
}

func TestGetExecType_UnpartitionedIndexSearchScanPlansLikeATableFunction(t *testing.T) {
	query := func(stats *planpb.Stats) *planpb.Query {
		return &planpb.Query{
			Steps: []int32{0},
			Nodes: []*planpb.Node{{
				NodeId:          0,
				NodeType:        planpb.Node_INDEX_SEARCH_SCAN,
				Stats:           stats,
				IndexSearchScan: &planpb.IndexSearchScan{Index: &planpb.IndexDef{IndexAlgo: "hnsw"}},
				RuntimeFilterProbeList: []*planpb.RuntimeFilterSpec{
					{MustApply: true, UseMembershipFilter: true},
				},
			}},
		}
	}
	require.Equal(t, ExecTypeTP, GetExecType(query(&planpb.Stats{BlockNum: 1, Cost: 1, Outcnt: 1}), false, false))
	require.Equal(t, ExecTypeAP_MULTICN, GetExecType(query(&planpb.Stats{
		BlockNum: int32(BlockThresholdForOneCN) + 1, Cost: float64(costThresholdForOneCN) + 1,
	}), false, false), "a large query can still use every CN")
}
