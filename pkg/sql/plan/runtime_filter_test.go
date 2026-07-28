// Copyright 2026 Matrix Origin
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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func makeRuntimeFilterTestEq(typ planpb.Type, leftTag, rightTag, leftPos, rightPos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{
				GetColExpr(typ, leftTag, leftPos),
				GetColExpr(typ, rightTag, rightPos),
			},
		}},
	}
}

func makeMixedSideRuntimeFilterResidual(typ planpb.Type, leftTag, rightTag int32) *planpb.Expr {
	mixedArg := &planpb.Expr{
		Typ: typ,
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.PLUS), 0), "+"),
			Args: []*planpb.Expr{
				GetColExpr(typ, leftTag, 0),
				GetColExpr(typ, rightTag, 0),
			},
		}},
	}
	return &planpb.Expr{
		Typ: planpb.Type{Id: int32(types.T_bool), NotNullable: true},
		Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: getFunctionObjRef(function.EncodeOverloadID(int32(function.EQUAL), 0), "="),
			Args: []*planpb.Expr{mixedArg, GetColExpr(typ, rightTag, 0)},
		}},
	}
}

func newRuntimeFilterSingleTestBuilder(rightSingle bool) *QueryBuilder {
	pkType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	return &QueryBuilder{
		compCtx: NewMockCompilerContext(true),
		qry: &planpb.Query{Nodes: []*planpb.Node{
			{
				NodeType:    planpb.Node_TABLE_SCAN,
				NodeId:      0,
				BindingTags: []int32{1},
				TableDef: &planpb.TableDef{
					Name:          "discardable_probe",
					Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
					Name2ColIndex: map[string]int32{"id": 0},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats: &planpb.Stats{Cost: 1_000_000, Outcnt: 1_000_000, TableCnt: 1_000_000, BlockNum: 123},
			},
			{
				NodeType:    planpb.Node_TABLE_SCAN,
				NodeId:      1,
				BindingTags: []int32{2},
				TableDef: &planpb.TableDef{
					Name:          "preserved_build",
					Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
					Name2ColIndex: map[string]int32{"id": 0},
					Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
				},
				Stats: &planpb.Stats{Cost: 3, Outcnt: 3, TableCnt: 3, BlockNum: 1, Selectivity: 1},
			},
			{
				NodeType:    planpb.Node_JOIN,
				NodeId:      2,
				Children:    []int32{0, 1},
				JoinType:    planpb.Node_SINGLE,
				IsRightJoin: rightSingle,
				OnList:      []*planpb.Expr{makeRuntimeFilterTestEq(pkType, 1, 2, 0, 0)},
				Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{
					HashmapSize: 3,
					HashOnPK:    true,
				}},
			},
		}},
	}
}

func configureRuntimeFilterCompositePK(builder *QueryBuilder) (*planpb.Node, *planpb.Node) {
	probe := builder.qry.Nodes[0]
	build := builder.qry.Nodes[1]
	pkType := probe.TableDef.Cols[0].Typ
	cpType := planpb.Type{Id: int32(types.T_varchar), NotNullable: true}
	probe.TableDef.Cols = []*planpb.ColDef{
		{Name: "a", Typ: pkType},
		{Name: "b", Typ: pkType},
		{Name: catalog.CPrimaryKeyColName, Typ: cpType},
	}
	probe.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1, catalog.CPrimaryKeyColName: 2}
	probe.TableDef.Pkey = &planpb.PrimaryKeyDef{
		PkeyColName: catalog.CPrimaryKeyColName,
		Names:       []string{"a", "b"},
	}
	build.TableDef.Cols = []*planpb.ColDef{{Name: "a", Typ: pkType}, {Name: "b", Typ: pkType}}
	build.TableDef.Name2ColIndex = map[string]int32{"a": 0, "b": 1}
	build.TableDef.Pkey = &planpb.PrimaryKeyDef{PkeyColName: "a", Names: []string{"a"}}
	builder.qry.Nodes[2].OnList = []*planpb.Expr{
		makeRuntimeFilterTestEq(pkType, 1, 2, 0, 0),
		makeRuntimeFilterTestEq(pkType, 1, 2, 1, 1),
	}
	return probe, build
}

func TestRightSingleRuntimeFilterSemanticAndDeliveryContract(t *testing.T) {
	t.Run("right single filters only the discardable probe and is colocated", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probeStatsBefore := DeepCopyStats(builder.qry.Nodes[0].Stats)

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		join := builder.qry.Nodes[2]
		probe := builder.qry.Nodes[0]
		build := builder.qry.Nodes[1]
		require.Len(t, join.RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.Equal(t, join.RuntimeFilterBuildList[0].Tag, probe.RuntimeFilterProbeList[0].Tag)
		require.Empty(t, build.RuntimeFilterProbeList)
		require.True(t, probe.Stats.ForceOneCN)
		require.True(t, build.Stats.ForceOneCN)
		require.Equal(t, probeStatsBefore.Cost, probe.Stats.Cost)
		require.Equal(t, probeStatsBefore.Outcnt, probe.Stats.Outcnt)
		require.Equal(t, probeStatsBefore.BlockNum, probe.Stats.BlockNum)
	})

	t.Run("left single preserves unmatched probe rows", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(false)
		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		require.False(t, builder.qry.Nodes[0].Stats.ForceOneCN)
	})

	t.Run("residual condition remains on the join", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes[2].OnList = append(builder.qry.Nodes[2].OnList, MakeFalseExpr())

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[2].OnList, 2)
		require.True(t, IsFalseExpr(builder.qry.Nodes[2].OnList[1]))
	})

	t.Run("mixed-side equality remains residual and is not used as an RF key", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		pkType := builder.qry.Nodes[0].TableDef.Cols[0].Typ
		mixed := makeMixedSideRuntimeFilterResidual(pkType, 1, 2)
		builder.qry.Nodes[2].OnList = append(builder.qry.Nodes[2].OnList, mixed)

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, builder.qry.Nodes[2].OnList, 2)
		require.Same(t, mixed, builder.qry.Nodes[2].OnList[1])
	})
}

func TestSingleJoinStatsUseSemanticPreservedSide(t *testing.T) {
	t.Run("left SINGLE preserves physical left stats", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(false)

		reCalcNodeStatsAfterSwap(2, builder, false, false, false)

		join := builder.qry.Nodes[2]
		require.Equal(t, builder.qry.Nodes[0].Stats.Outcnt, join.Stats.Outcnt)
		require.Equal(t, builder.qry.Nodes[0].Stats.BlockNum, join.Stats.BlockNum)
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.HashmapStats.HashmapSize)
	})

	t.Run("right SINGLE preserves physical right stats after child swap", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{
			NodeType: planpb.Node_PROJECT,
			NodeId:   3,
			Children: []int32{2},
			Stats:    DefaultStats(),
		})

		reCalcNodeStatsAfterSwap(3, builder, true, false, false)

		join := builder.qry.Nodes[2]
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.Outcnt)
		require.Equal(t, builder.qry.Nodes[1].Stats.BlockNum, join.Stats.BlockNum)
		require.Equal(t, builder.qry.Nodes[1].Stats.Selectivity, join.Stats.Selectivity)
		require.Equal(t, builder.qry.Nodes[1].Stats.Outcnt, join.Stats.HashmapStats.HashmapSize)
		require.Equal(t, join.Stats.Outcnt, builder.qry.Nodes[3].Stats.Outcnt)
	})

	t.Run("right SINGLE cardinality sizes a downstream join build side", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		pkType := builder.qry.Nodes[0].TableDef.Cols[0].Typ
		downstreamProbe := &planpb.Node{
			NodeType:    planpb.Node_TABLE_SCAN,
			NodeId:      3,
			BindingTags: []int32{3},
			TableDef: &planpb.TableDef{
				Name:          "downstream_probe",
				Cols:          []*planpb.ColDef{{Name: "id", Typ: pkType}},
				Name2ColIndex: map[string]int32{"id": 0},
				Pkey:          &planpb.PrimaryKeyDef{PkeyColName: "id", Names: []string{"id"}},
			},
			Stats: &planpb.Stats{
				Cost:        1_000,
				Outcnt:      1_000,
				TableCnt:    1_000,
				BlockNum:    10,
				Selectivity: 1,
			},
		}
		downstreamJoin := &planpb.Node{
			NodeType: planpb.Node_JOIN,
			NodeId:   4,
			Children: []int32{3, 2},
			JoinType: planpb.Node_INNER,
			OnList: []*planpb.Expr{
				makeRuntimeFilterTestEq(pkType, 3, 2, 0, 0),
			},
			Stats: DefaultStats(),
		}
		downstreamJoin.OnList[0].Ndv = 3
		builder.qry.Nodes = append(builder.qry.Nodes, downstreamProbe, downstreamJoin)

		reCalcNodeStatsAfterSwap(4, builder, true, false, false)

		rightSingle := builder.qry.Nodes[2]
		require.Equal(t, float64(3), rightSingle.Stats.Outcnt)
		require.Equal(t, rightSingle.Stats.Outcnt, downstreamJoin.Stats.HashmapStats.HashmapSize)
		require.Equal(t, float64(1_000), downstreamJoin.Stats.Outcnt)
	})

	t.Run("right SINGLE applies limit after selecting the preserved side", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		builder.qry.Nodes[2].Limit = MakePlan2Uint64ConstExprWithType(1)

		reCalcNodeStatsAfterSwap(2, builder, false, false, false)

		require.Equal(t, float64(1), builder.qry.Nodes[2].Stats.Outcnt)
	})
}

func TestRightSingleRuntimeFilterConservativeEligibility(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*QueryBuilder)
	}{
		{
			name: "probe is not a direct table scan",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].NodeType = planpb.Node_PROJECT
			},
		},
		{
			name: "probe scan has limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Limit = MakePlan2Int64ConstExprWithType(1)
			},
		},
		{
			name: "probe scan has offset",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Offset = MakePlan2Uint64ConstExprWithType(1)
			},
		},
		{
			name: "probe metadata has no primary key",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].TableDef.Pkey = nil
			},
		},
		{
			name: "build exceeds planner limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].Stats.Outcnt = 5_000_001
			},
		},
		{
			name: "preserved build is not a direct table scan",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].NodeType = planpb.Node_AGG
			},
		},
		{
			name: "build estimate exceeds exact IN limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Stats.TableCnt = 100_000
				builder.qry.Nodes[1].Stats.Outcnt = 50_000
			},
		},
		{
			name: "small filtered output still scans a build table above exact IN limit",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[0].Stats.TableCnt = 100_000
				builder.qry.Nodes[1].Stats.Outcnt = 3
				builder.qry.Nodes[1].Stats.TableCnt = 50_000
			},
		},
		{
			name: "unavailable build statistics are not an exact size bound",
			mutate: func(builder *QueryBuilder) {
				builder.qry.Nodes[1].Stats = DefaultStats()
			},
		},
		{
			name: "local placement override is active",
			mutate: func(builder *QueryBuilder) {
				builder.optimizerHints = &OptimizerHints{forceOneCN: 1}
			},
		},
		{
			name: "right single RF feature gate is disabled",
			mutate: func(builder *QueryBuilder) {
				builder.optimizerHints = &OptimizerHints{disableRightSingleRF: 1}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(true)
			test.mutate(builder)

			builder.generateRuntimeFilters(2)

			require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
			require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
		})
	}

	t.Run("leading cluster key remains prunable with non-PK row filtering", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe := builder.qry.Nodes[0]
		probeType := probe.TableDef.Cols[0].Typ
		probe.TableDef.Cols = append(probe.TableDef.Cols, &planpb.ColDef{Name: "cluster_key", Typ: probeType})
		probe.TableDef.Name2ColIndex["cluster_key"] = 1
		probe.TableDef.ClusterBy = &planpb.ClusterByDef{Name: "cluster_key"}
		joinKey := builder.qry.Nodes[2].OnList[0].GetF().Args[0]
		joinKey.GetCol().ColPos = 1
		joinKey.GetCol().Name = "cluster_key"

		builder.generateRuntimeFilters(2)
		builder.forceJoinOnOneCN(2, false)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.True(t, probe.RuntimeFilterProbeList[0].NotOnPk)
		require.Equal(t, int32(1), probe.RuntimeFilterProbeList[0].Expr.GetCol().ColPos)
		require.True(t, probe.Stats.ForceOneCN)
		require.True(t, builder.qry.Nodes[1].Stats.ForceOneCN)
	})

	t.Run("non-leading composite key does not sacrifice multi-CN scan", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe := builder.qry.Nodes[0]
		pkType := probe.TableDef.Cols[0].Typ
		probe.TableDef.Cols = []*planpb.ColDef{
			{Name: "leading", Typ: pkType},
			{Name: "lookup", Typ: pkType},
			{Name: catalog.CPrimaryKeyColName, Typ: planpb.Type{Id: int32(types.T_varchar)}},
		}
		probe.TableDef.Name2ColIndex = map[string]int32{
			"leading":                  0,
			"lookup":                   1,
			catalog.CPrimaryKeyColName: 2,
		}
		probe.TableDef.Pkey = &planpb.PrimaryKeyDef{
			PkeyColName: catalog.CPrimaryKeyColName,
			Names:       []string{"leading", "lookup"},
		}
		joinKey := builder.qry.Nodes[2].OnList[0].GetF().Args[0]
		joinKey.GetCol().ColPos = 1
		joinKey.GetCol().Name = "lookup"

		builder.generateRuntimeFilters(2)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
	})

	t.Run("full composite prefix remains exact", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.Equal(t, int32(2), probe.RuntimeFilterProbeList[0].Expr.GetCol().ColPos)
	})

	t.Run("leading composite prefix remains prunable", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)
		builder.qry.Nodes[2].OnList = builder.qry.Nodes[2].OnList[:1]

		builder.generateRuntimeFilters(2)

		require.Len(t, builder.qry.Nodes[2].RuntimeFilterBuildList, 1)
		require.Len(t, probe.RuntimeFilterProbeList, 1)
		require.True(t, probe.RuntimeFilterProbeList[0].MatchPrefix)
		require.Equal(t, int32(2), probe.RuntimeFilterProbeList[0].Expr.GetCol().ColPos)
	})

	t.Run("missing hidden composite key metadata leaves no partial runtime filter", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)
		builder.qry.Nodes[2].OnList = builder.qry.Nodes[2].OnList[:1]
		delete(probe.TableDef.Name2ColIndex, catalog.CPrimaryKeyColName)

		builder.generateRuntimeFilters(2)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
	})

	t.Run("composite probe preserves existing runtime filters", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, _ := configureRuntimeFilterCompositePK(builder)
		existing := &planpb.RuntimeFilterSpec{Tag: 99, Expr: GetColExpr(probe.TableDef.Cols[0].Typ, 1, 0)}
		probe.RuntimeFilterProbeList = []*planpb.RuntimeFilterSpec{existing}

		builder.generateRuntimeFilters(2)

		require.Len(t, probe.RuntimeFilterProbeList, 2)
		require.Same(t, existing, probe.RuntimeFilterProbeList[0])
		require.Equal(t, builder.qry.Nodes[2].RuntimeFilterBuildList[0].Tag, probe.RuntimeFilterProbeList[1].Tag)
	})

	t.Run("composite build estimate must fit exact IN limit", func(t *testing.T) {
		builder := newRuntimeFilterSingleTestBuilder(true)
		probe, build := configureRuntimeFilterCompositePK(builder)
		probe.Stats.TableCnt = 100_000
		build.Stats.Outcnt = 50_000

		builder.generateRuntimeFilters(2)

		require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
		require.Empty(t, probe.RuntimeFilterProbeList)
	})
}

func TestAnalyzeRuntimeFilterJoinPolicy(t *testing.T) {
	tests := []struct {
		name      string
		joinType  planpb.Node_JoinType
		right     bool
		eligible  bool
		localOnly bool
	}{
		{name: "inner", joinType: planpb.Node_INNER, eligible: true},
		{name: "left", joinType: planpb.Node_LEFT},
		{name: "left single", joinType: planpb.Node_SINGLE},
		{name: "right single", joinType: planpb.Node_SINGLE, right: true, eligible: true, localOnly: true},
		{name: "right", joinType: planpb.Node_RIGHT, eligible: true, localOnly: true},
		{name: "outer", joinType: planpb.Node_OUTER},
		{name: "left semi", joinType: planpb.Node_SEMI, eligible: true},
		{name: "right semi", joinType: planpb.Node_SEMI, right: true, eligible: true, localOnly: true},
		{name: "left anti", joinType: planpb.Node_ANTI},
		{name: "right anti", joinType: planpb.Node_ANTI, right: true, eligible: true, localOnly: true},
		{name: "left dedup", joinType: planpb.Node_DEDUP, eligible: true},
		{name: "right dedup", joinType: planpb.Node_DEDUP, right: true},
		{name: "index", joinType: planpb.Node_INDEX, eligible: true, localOnly: true},
		{name: "mark", joinType: planpb.Node_MARK},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			policy := analyzeRuntimeFilterJoinPolicy(&planpb.Node{
				NodeType:    planpb.Node_JOIN,
				JoinType:    test.joinType,
				IsRightJoin: test.right,
			})
			require.Equal(t, test.eligible, policy.eligible)
			require.Equal(t, test.localOnly, policy.requiresLocalDelivery)
		})
	}
}

func TestForceJoinOnOneCNRuntimeFilterPolicy(t *testing.T) {
	tests := []struct {
		name     string
		joinType planpb.Node_JoinType
		right    bool
		shuffle  bool
		force    bool
	}{
		{name: "right join", joinType: planpb.Node_RIGHT, force: true},
		{name: "shuffle right join", joinType: planpb.Node_RIGHT, shuffle: true},
		{name: "left semi", joinType: planpb.Node_SEMI},
		{name: "right semi", joinType: planpb.Node_SEMI, right: true, force: true},
		{name: "shuffle right semi", joinType: planpb.Node_SEMI, right: true, shuffle: true},
		{name: "left anti", joinType: planpb.Node_ANTI},
		{name: "right anti", joinType: planpb.Node_ANTI, right: true, force: true},
		{name: "shuffle right anti", joinType: planpb.Node_ANTI, right: true, shuffle: true},
		{name: "right single", joinType: planpb.Node_SINGLE, right: true, force: true},
		{name: "shuffle right single", joinType: planpb.Node_SINGLE, right: true, shuffle: true},
		{name: "left dedup", joinType: planpb.Node_DEDUP, force: true},
		{name: "right dedup", joinType: planpb.Node_DEDUP, right: true, force: true},
		{name: "shuffle dedup", joinType: planpb.Node_DEDUP, shuffle: true},
		{name: "mark", joinType: planpb.Node_MARK},
		{name: "outer", joinType: planpb.Node_OUTER},
		{name: "shuffle index remains local", joinType: planpb.Node_INDEX, shuffle: true, force: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := newRuntimeFilterSingleTestBuilder(test.right)
			join := builder.qry.Nodes[2]
			join.JoinType = test.joinType
			join.IsRightJoin = test.right
			join.Stats.HashmapStats.Shuffle = test.shuffle
			join.RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{{Tag: 1}}

			builder.forceJoinOnOneCN(2, false)

			require.Equal(t, test.force, builder.qry.Nodes[0].Stats.ForceOneCN)
			require.Equal(t, test.force, builder.qry.Nodes[1].Stats.ForceOneCN)
		})
	}
}

func TestDisableRightSingleRuntimeFilterHint(t *testing.T) {
	builder := newRuntimeFilterSingleTestBuilder(true)
	builder.optimizerHints = &OptimizerHints{}

	handleOptimizerHints("disableRightSingleRF=1", builder)
	builder.generateRuntimeFilters(2)

	require.Equal(t, 1, builder.optimizerHints.disableRightSingleRF)
	require.Empty(t, builder.qry.Nodes[2].RuntimeFilterBuildList)
	require.Empty(t, builder.qry.Nodes[0].RuntimeFilterProbeList)
}
