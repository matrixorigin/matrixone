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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	statsinfo "github.com/matrixorigin/matrixone/pkg/pb/statsinfo"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func useLegacyGroupingSetPlan(t *testing.T, mock *MockOptimizer) {
	t.Helper()
	setGroupingSetTestProtocol(t, mock.CurrentContext(), defines.MORPCVersion47)
}

func buildSharedGroupingSetPlan(t *testing.T, sql string) *planpb.Query {
	t.Helper()
	return buildSharedGroupingSetPlanWithContext(t, sql, NewMockCompilerContext(true, newPlanTestProcess(t)))
}

func setGroupingSetTestProtocol(t *testing.T, ctx CompilerContext, version int64) {
	t.Helper()
	setPlanTestGlobalVariable(t, ctx.GetProcess().GetService(), moruntime.MOProtocolVersion, version)
}

func buildSharedGroupingSetPlanWithContext(t *testing.T, sql string, ctx CompilerContext) *planpb.Query {
	t.Helper()
	setGroupingSetTestProtocol(t, ctx, defines.MORPCVersion49)
	setPlanTestGlobalVariable(t, ctx.GetProcess().GetService(), "optimizer_hints", "")

	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	built, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	return built.GetQuery()
}

type groupingSetStatsCompilerContext struct {
	*MockCompilerContext
	statsCache *StatsCache
}

func (ctx *groupingSetStatsCompilerContext) GetStatsCache() *StatsCache {
	return ctx.statsCache
}

func (ctx *groupingSetStatsCompilerContext) Stats(obj *planpb.ObjectRef, _ *planpb.Snapshot) (*statsinfo.StatsInfo, error) {
	stats := ctx.statsCache.Get(uint64(obj.Obj))
	return stats.GetStats(), nil
}

func TestGroupingSetInputSharingProtocolGate(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus,
		grouping(l_returnflag), grouping(l_linestatus), count(*)
		from lineitem
		group by l_returnflag, l_linestatus with rollup`

	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))

	build := func(version int64) *planpb.Query {
		t.Helper()
		setPlanTestGlobalVariable(t, ctx.GetProcess().GetService(), moruntime.MOProtocolVersion, version)
		stmt, err := mysql.ParseOne(context.Background(), sql, 1)
		require.NoError(t, err)
		defer stmt.Free()
		built, err := BuildPlan(ctx, stmt, false)
		require.NoError(t, err)
		return built.GetQuery()
	}

	legacy := reachableGroupingSetShape(build(defines.MORPCVersion48))
	require.Equal(t, 3, legacy.tableScans)
	require.Zero(t, legacy.expandProjects)

	shared := reachableGroupingSetShape(build(defines.MORPCVersion49))
	require.Equal(t, 1, shared.tableScans)
	require.Equal(t, 1, shared.aggregates)
	require.Equal(t, 1, shared.expandProjects)
	require.Equal(t, 1, shared.aggregatesOnExpand)
	require.Equal(t, 3, shared.sinkScans)
	require.Equal(t, 1, shared.materializedSinks)
	require.Equal(t, []bool{true, true, true, false, false, false}, shared.flags)
	require.True(t, shared.hasEmptyRowMarker)

	setPlanTestGlobalVariable(t, ctx.GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
	rolledBack := reachableGroupingSetShape(build(defines.MORPCVersion49))
	require.Equal(t, 3, rolledBack.tableScans)
	require.Zero(t, rolledBack.expandProjects)
}

func TestRollupEmptyInputGuard(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		guard     bool
	}{
		{"with rollup", "select l_returnflag,sum(l_quantity) from lineitem group by l_returnflag with rollup", true},
		{"functional", "select l_returnflag,count(l_quantity) from lineitem group by rollup(l_returnflag)", true},
		{"window", "select l_returnflag,sum(l_quantity),row_number() over(order by sum(l_quantity)) from lineitem group by l_returnflag with rollup", true},
		{"explicit sets", "select l_returnflag,sum(l_quantity) from lineitem group by grouping sets ((l_returnflag),())", false},
		{"cube", "select l_returnflag,sum(l_quantity) from lineitem group by cube(l_returnflag)", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, _, branches := bindEmptyRollupBranches(t, test.sql)
			require.Len(t, branches, 2)
			for i, branch := range branches {
				require.Len(t, branch.agg.Children, 1)
				if i > 0 {
					require.True(t, sameGroupingSetAggregateShape(branches[0].agg, branch.agg))
				}
				filters := 0
				for _, node := range builder.qry.Nodes {
					if node.NodeType == planpb.Node_FILTER && len(node.Children) == 1 && node.Children[0] == branch.aggID {
						require.Len(t, node.FilterList, 1)
						require.True(t, groupingSetPositiveCountFilter(node.FilterList[0], branch.agg))
						filters++
					}
				}
				if test.guard {
					require.Equal(t, 1, filters)
				} else {
					require.Zero(t, filters)
				}
			}
		})
	}
}

func bindEmptyRollupBranches(t *testing.T, sql string) (*QueryBuilder, int32, []groupingSetBranch) {
	t.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	setGroupingSetTestProtocol(t, ctx, defines.MORPCVersion49)
	stmt, err := mysql.ParseOne(context.Background(), sql, 1)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)
	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	root, err := builder.bindSelect(stmt.(*tree.Select), NewBindContext(builder, nil), true)
	require.NoError(t, err)
	require.Len(t, builder.groupingSetCandidates, 1)
	candidate := builder.groupingSetCandidates[0]
	branches := make([]groupingSetBranch, len(candidate.nodes))
	for i, node := range candidate.nodes {
		aggID, ok := builder.findGroupingSetAggregate(node, candidate.contexts[i])
		require.True(t, ok)
		branches[i] = groupingSetBranch{rootID: node, aggID: aggID, agg: builder.qry.Nodes[aggID], ctx: candidate.contexts[i]}
	}
	return builder, root, branches
}

func TestDirectGroupingSetCountFilter(t *testing.T) {
	const sql = "select l_returnflag,sum(l_quantity) from lineitem group by l_returnflag with rollup"
	for _, test := range []struct {
		name   string
		change func(*planpb.Node, []groupingSetBranch)
		want   bool
	}{
		{"common count guard", func(*planpb.Node, []groupingSetBranch) {}, true},
		{"different threshold", func(filter *planpb.Node, _ []groupingSetBranch) {
			filter.FilterList[0].GetF().Args[1] = MakePlan2Int64ConstExprWithType(1)
		}, false},
		{"additional having", func(filter *planpb.Node, _ []groupingSetBranch) {
			filter.FilterList = append(filter.FilterList, MakePlan2BoolConstExprWithType(false))
		}, false},
		{"limit", func(filter *planpb.Node, _ []groupingSetBranch) {
			filter.Limit = MakePlan2Int64ConstExprWithType(1)
		}, false},
		{"nullable count", func(filter *planpb.Node, branches []groupingSetBranch) {
			pos := filter.FilterList[0].GetF().Args[0].GetCol().ColPos
			branches[1].agg.AggList[pos].GetF().Func.ObjName = "count"
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder, _, branches := bindEmptyRollupBranches(t, sql)
			nodes := builder.groupingSetCandidates[0].nodes
			filter := builder.qry.Nodes[builder.qry.Nodes[branches[1].rootID].Children[0]]
			test.change(filter, branches)
			output, ok := builder.directGroupingSetOutput(nodes, branches)
			require.Equal(t, test.want, ok)
			if ok {
				require.NotNil(t, output.filter)
			}
		})
	}
}

func TestDirectGroupingSetSplitCountFilter(t *testing.T) {
	builder, _, branches := bindEmptyRollupBranches(t,
		`select l_returnflag,l_linestatus,l_shipmode,l_orderkey,sum(l_quantity)
		from lineitem group by rollup(l_returnflag,l_linestatus,l_shipmode,l_orderkey)`)
	output, ok := builder.directGroupingSetOutput(builder.groupingSetCandidates[0].nodes, branches)
	require.True(t, ok)
	branches[0].agg.Stats = &planpb.Stats{Outcnt: 6_000_000}
	branches[0].agg.GroupBy[3].Ndv = 1_500_000
	require.True(t, builder.shareDirectGroupingSetSplit(branches, output, &planpb.Stats{Outcnt: 6_000_000}, 200))
	union := builder.qry.Nodes[output.unionRootID]
	require.Equal(t, branches[0].rootID, union.Children[0])
	coarse := builder.qry.Nodes[union.Children[1]]
	filter := builder.qry.Nodes[coarse.Children[0]]
	require.Equal(t, planpb.Node_FILTER, filter.NodeType)
	require.Len(t, filter.FilterList, 1)
	countCol := filter.FilterList[0].GetF().Args[0].GetCol()
	shared := builder.qry.Nodes[filter.Children[0]]
	require.Equal(t, shared.BindingTags[0], countCol.RelPos)
	countOutput := shared.ProjectList[countCol.ColPos].GetCol()
	agg := builder.qry.Nodes[shared.Children[0]]
	require.Equal(t, agg.BindingTags[1], countOutput.RelPos)
	require.Equal(t, "starcount", agg.AggList[countOutput.ColPos].GetF().Func.ObjName)
}

func TestGroupingSetInputSharingKeepsDecimalSumOnRawInput(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus, l_shipmode,
		grouping(l_returnflag, l_linestatus, l_shipmode), sum(l_extendedprice)
		from lineitem
		group by rollup(l_returnflag, l_linestatus, l_shipmode)`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	// Checked fixed-width SUM is not associative in error semantics. Every
	// grouping set must therefore aggregate the raw rows in their original
	// order; SUM(SUM(x)) prefix reuse could introduce an intermediate overflow.
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.aggregates)
	require.Equal(t, 1, shape.expandProjects)
	require.Equal(t, 1, shape.aggregatesOnExpand)
	require.Equal(t, 4, shape.sinkScans)
	require.Equal(t, 1, shape.materializedSinks)
}

func TestGroupingSetInputSharingStreamsEquivalentBranchProjects(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus, sum(l_extendedprice)
		from lineitem
		group by rollup(l_returnflag, l_linestatus)
		order by l_returnflag, l_linestatus`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.aggregates)
	require.Equal(t, 1, shape.expandProjects)
	require.Equal(t, 1, shape.aggregatesOnExpand)
	require.Zero(t, shape.sinkScans)
	require.Zero(t, shape.materializedSinks)
}

func TestSplitGroupingSetCoarseDisablesShuffle(t *testing.T) {
	child := &planpb.Node{NodeType: planpb.Node_PROJECT}
	agg := &planpb.Node{
		NodeType: planpb.Node_AGG,
		Children: []int32{0},
		GroupBy:  make([]*planpb.Expr, 2),
		Stats: &planpb.Stats{HashmapStats: &planpb.HashMapStats{
			Shuffle:       true,
			ShuffleColIdx: 0,
		}},
	}
	builder := &QueryBuilder{
		qry:                        &planpb.Query{Nodes: []*planpb.Node{child, agg}},
		splitGroupingSetCoarseAggs: map[*planpb.Node]struct{}{agg: {}},
	}
	determineShuffleForGroupBy(agg, builder)
	require.False(t, agg.Stats.HashmapStats.Shuffle)
	require.Equal(t, int32(-1), agg.Stats.HashmapStats.ShuffleColIdx)
}

func TestDirectGroupingSetSplitFits(t *testing.T) {
	newBranches := func() []groupingSetBranch {
		branches := make([]groupingSetBranch, 5)
		for i := range branches {
			groups := make([]*planpb.Expr, 4)
			flags := make([]bool, 4)
			for j := range groups {
				groups[j] = groupingSetCol(planpb.Type{Id: int32(types.T_int64)}, 1, int32(j))
				flags[j] = j < 4-i
			}
			groups[3].Ndv = 1000
			branches[i].agg = &planpb.Node{GroupBy: groups, GroupingFlag: flags,
				Stats: &planpb.Stats{Outcnt: 800_000}}
		}
		return branches
	}
	producer := &planpb.Stats{Outcnt: 1_000_000}
	for _, tc := range []struct {
		name   string
		change func([]groupingSetBranch, *planpb.Stats)
		want   bool
	}{
		{name: "large strict prefix", want: true},
		{name: "small input", change: func(_ []groupingSetBranch, s *planpb.Stats) { s.Outcnt = 1_000 }},
		{name: "low trailing NDV", change: func(b []groupingSetBranch, _ *planpb.Stats) { b[0].agg.GroupBy[3].Ndv = 10 }},
		{name: "finest reduces input", change: func(b []groupingSetBranch, _ *planpb.Stats) { b[0].agg.Stats.Outcnt = 100_000 }},
		{name: "not a prefix", change: func(b []groupingSetBranch, _ *planpb.Stats) { b[2].agg.GroupingFlag[0] = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			branches, stats := newBranches(), *producer
			if tc.change != nil {
				tc.change(branches, &stats)
			}
			require.Equal(t, tc.want, directGroupingSetSplitFits(branches, &stats, 200))
		})
	}
}

func TestDirectGroupingSetSplitPlan(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus, l_shipmode, l_orderkey,
		sum(l_extendedprice) from lineitem
		group by rollup(l_returnflag, l_linestatus, l_shipmode, l_orderkey)`
	// The mock compiler has no column stats, so the opportunity gate fails
	// closed rather than duplicating the scan speculatively.
	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.aggregates)
	require.Equal(t, 1, shape.expandProjects)
	require.Equal(t, 1, shape.aggregatesOnExpand)
	require.Zero(t, shape.sinkScans)

	mock := NewMockCompilerContext(true, newPlanTestProcess(t))
	cache := NewStatsCache()
	cache.Set(mock.tables["lineitem"].TblId, &statsinfo.StatsInfo{
		TableCnt: 6_000_000,
		NdvMap: map[string]float64{
			"l_returnflag": 3, "l_linestatus": 2, "l_shipmode": 7,
			"l_orderkey": 1_500_000,
		},
	})
	withStats := &groupingSetStatsCompilerContext{MockCompilerContext: mock, statsCache: cache}
	shape = reachableGroupingSetShape(buildSharedGroupingSetPlanWithContext(t, sql, withStats))
	require.Equal(t, 2, shape.tableScans)
	require.Equal(t, 2, shape.aggregates)
	require.Equal(t, 1, shape.expandProjects)
	require.Equal(t, 1, shape.aggregatesOnExpand)
	require.Zero(t, shape.sinkScans)

	orderedSQL := sql + ` order by l_returnflag, l_linestatus, l_shipmode, l_orderkey`
	shape = reachableGroupingSetShape(buildSharedGroupingSetPlanWithContext(t, orderedSQL, withStats))
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.aggregates)

	cache.Set(mock.tables["orders"].TblId, &statsinfo.StatsInfo{
		TableCnt: 1_500_000,
		NdvMap:   map[string]float64{"o_orderkey": 1_500_000},
	})
	for _, tc := range []struct {
		name       string
		from       string
		scans      int
		aggregates int
	}{
		{name: "where join", from: "lineitem, orders where l_orderkey = o_orderkey", scans: 4, aggregates: 2},
		{name: "on join", from: "lineitem join orders on l_orderkey = o_orderkey", scans: 4, aggregates: 2},
		{name: "cartesian join", from: "lineitem cross join orders", scans: 2, aggregates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			joinedSQL := `select l_returnflag, l_linestatus, l_shipmode, l_orderkey,
				sum(l_extendedprice) from ` + tc.from + `
				group by rollup(l_returnflag, l_linestatus, l_shipmode, l_orderkey)`
			shape := reachableGroupingSetShape(buildSharedGroupingSetPlanWithContext(t, joinedSQL, withStats))
			// Equivalent WHERE/ON joins must cost the joined input; a genuine
			// Cartesian product must retain its multiplicative cardinality.
			require.Equal(t, tc.scans, shape.tableScans)
			require.Equal(t, tc.aggregates, shape.aggregates)
			require.Equal(t, 1, shape.expandProjects)
			require.Zero(t, shape.sinkScans)
		})
	}
}

func TestGroupingSetInputSharingKeepsVolatileBranchProjects(t *testing.T) {
	const sql = `select l_returnflag, l_linestatus, rand(), sum(l_extendedprice)
		from lineitem
		group by rollup(l_returnflag, l_linestatus)`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.expandProjects)
	require.NotZero(t, shape.sinkScans)
	require.NotZero(t, shape.materializedSinks)
}

func TestGroupingSetInputSharingKeepsHashBuildDrainMarker(t *testing.T) {
	const sql = `select count(*)
		from (
			select l_returnflag, l_linestatus, sum(l_extendedprice) as revenue
			from lineitem
			group by rollup(l_returnflag, l_linestatus)
		) g
		join nation n on g.l_returnflag = n.n_name`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Equal(t, 1, shape.expandProjects)
	require.NotZero(t, shape.sinkScans)
	require.NotZero(t, shape.materializedSinks)
}

func TestGroupingSetInputSharingRejectsInheritedGroupingSentinel(t *testing.T) {
	const sql = `select d.l_returnflag, grouping(d.l_returnflag), count(*)
		from (
			select l_returnflag
			from lineitem
			group by rollup(l_returnflag)
		) d
		group by rollup(d.l_returnflag)`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	// Query outputs normalize inner sentinels. Nested sharing nevertheless
	// retains its conservative admission policy until its cost and error-order
	// contract is proved independently of output normalization.
	require.Zero(t, shape.expandProjects)
	require.Zero(t, shape.sinkScans)
}

func TestGroupingSetInputSharingRequiresLegacyDrainWitness(t *testing.T) {
	const sql = `select cast('' as varchar) as g, cast(0 as unsigned) as n
		union all
		select l_returnflag, count(*)
		from lineitem
		group by rollup(l_returnflag)
		limit 1`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Zero(t, shape.expandProjects)
	require.Zero(t, shape.sinkScans)
}

func TestGroupingSetInputSharingAllowsFallibleKeyWhenAllBranchesDrain(t *testing.T) {
	const sql = `select cast(l_returnflag as bigint),
		grouping(cast(l_returnflag as bigint)), count(*)
		from lineitem
		group by rollup(cast(l_returnflag as bigint))`

	shape := reachableGroupingSetShape(buildSharedGroupingSetPlan(t, sql))
	require.Equal(t, 1, shape.tableScans)
	require.Equal(t, 1, shape.expandProjects)
	require.Equal(t, 2, shape.sinkScans)
}

func TestGroupingSetSentinelDetectionFollowsMaterializedSource(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{
		Nodes: []*planpb.Node{
			{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}},
			{NodeType: planpb.Node_SINK, Children: []int32{2}},
			{NodeType: planpb.Node_PROJECT, Children: []int32{3}},
			{
				NodeType:     planpb.Node_PROJECT,
				GroupingFlag: []bool{true, false},
				ExtraOptions: groupingSetExpandOptionPrefix + "2",
			},
			{NodeType: planpb.Node_TABLE_SCAN},
		},
		Steps: []int32{1},
	}}

	require.True(t, builder.subtreeMayExposeGroupingSentinel(0, make(map[int32]bool)))
	require.False(t, builder.subtreeMayExposeGroupingSentinel(4, make(map[int32]bool)))
}

func TestRewriteGroupingSetExprPreservesSQLBitOrder(t *testing.T) {
	groupingExpr := func() *planpb.Expr {
		args := make([]*planpb.Expr, 3)
		for i := range args {
			args[i] = groupingSetCol(
				planpb.Type{Id: int32(types.T_int64)}, 7, int32(i))
		}
		return &planpb.Expr{
			Typ: planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_F{F: &planpb.Function{
				Func: &planpb.ObjectRef{ObjName: "grouping"},
				Args: args,
			}},
		}
	}

	for _, test := range []struct {
		name  string
		flags []bool
		want  int64
	}{
		{name: "all active", flags: []bool{true, true, true}, want: 0},
		{name: "rightmost inactive", flags: []bool{true, true, false}, want: 1},
		{name: "middle inactive", flags: []bool{true, false, true}, want: 2},
		{name: "leftmost inactive", flags: []bool{false, true, true}, want: 4},
		{name: "outer inactive", flags: []bool{false, true, false}, want: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			expr := groupingExpr()
			agg := &planpb.Node{
				BindingTags:  []int32{7, 8},
				GroupingFlag: test.flags,
			}

			rewriteGroupingSetExpr(expr, agg, 9, len(test.flags))

			require.Equal(t, test.want, expr.GetLit().GetI64Val())
		})
	}
}

func TestGroupingSetDeclaredRowSize(t *testing.T) {
	for _, test := range []struct {
		name string
		typs []planpb.Type
		want float64
		ok   bool
	}{
		{
			name: "fixed and nullable",
			typs: []planpb.Type{
				{Id: int32(types.T_int64), NotNullable: true},
				{Id: int32(types.T_int64)},
			},
			want: 17,
			ok:   true,
		},
		{
			name: "varchar declared characters",
			typs: []planpb.Type{{Id: int32(types.T_varchar), Width: 10}},
			want: float64(types.VarlenaSize + 40 + 1),
			ok:   true,
		},
		{
			name: "array declared elements",
			typs: []planpb.Type{{
				Id: int32(types.T_array_float32), Width: 3, NotNullable: true,
			}},
			want: float64(types.VarlenaSize + 12),
			ok:   true,
		},
		{name: "missing variable capacity", typs: []planpb.Type{{Id: int32(types.T_text)}}, ok: false},
		{name: "unsupported tuple", typs: []planpb.Type{{Id: int32(types.T_tuple), Width: 8}}, ok: false},
		{name: "future type", typs: []planpb.Type{{Id: 256, Width: 8}}, ok: false},
		{name: "empty schema", ok: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := materializedDeclaredRowSize(test.typs)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, got)
		})
	}
}

func TestGroupingSetSharingFitsCostAndStorage(t *testing.T) {
	for _, test := range []struct {
		name                                   string
		producerCost, inputSize, rows, outSize float64
		branches                               int
		want                                   bool
	}{
		{name: "clear byte-work win", producerCost: 1000, inputSize: 8, rows: 10, outSize: 16, branches: 3, want: true},
		{name: "wide aggregate output loses", producerCost: 1000, inputSize: 8, rows: 10, outSize: 1000, branches: 3},
		{name: "single output row exceeds record safety bound", producerCost: 1e15, inputSize: 8, rows: 1, outSize: float64(materialized.MaxSpillBatchBytes)/2 + 1, branches: 3},
		{name: "branch scan traffic loses", producerCost: 40, inputSize: 8, rows: 10, outSize: 16, branches: 20},
		{name: "spill ceiling", producerCost: math.MaxFloat64 / 16, inputSize: 8, rows: groupingSetEstimatedSpillBytesLimit/8 + 1, outSize: 8, branches: 2},
		{name: "wide high-cardinality result exceeds spill ceiling", producerCost: 300_000_000, inputSize: 1000, rows: 13_500_000, outSize: 1000, branches: 9},
		{name: "single branch", producerCost: 1000, inputSize: 8, rows: 10, outSize: 16, branches: 1},
		{name: "unknown input width", producerCost: 1000, rows: 10, outSize: 16, branches: 3},
		{name: "nan producer", producerCost: math.NaN(), inputSize: 8, rows: 10, outSize: 16, branches: 3},
		{name: "overflowed traffic", producerCost: math.MaxFloat64, inputSize: 2, rows: math.MaxFloat64, outSize: 2, branches: 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, groupingSetSharingFitsCostAndStorage(
				test.producerCost, test.inputSize, test.rows, test.outSize, test.branches))
		})
	}
}

func TestGroupingSetMaterializedRowsForAdmission(t *testing.T) {
	for _, test := range []struct {
		name                              string
		producerRows, aggregateRows, want float64
		branches                          int
		ok                                bool
	}{
		{name: "inflate optimistic groups", producerRows: 1_000_000, aggregateRows: 100, branches: 4, want: 3200, ok: true},
		{name: "cap at relational ceiling", producerRows: 100, aggregateRows: 90, branches: 3, want: 300, ok: true},
		{name: "keep larger aggregate estimate", producerRows: 10, aggregateRows: 40, branches: 3, want: 40, ok: true},
		{name: "invalid producer", aggregateRows: 1, branches: 2},
		{name: "single branch", producerRows: 10, aggregateRows: 1, branches: 1},
		{name: "overflow", producerRows: math.MaxFloat64, aggregateRows: 1, branches: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, ok := groupingSetMaterializedRowsForAdmission(
				test.producerRows, test.aggregateRows, test.branches)
			require.Equal(t, test.ok, ok)
			require.Equal(t, test.want, got)
		})
	}
}

type groupingSetShape struct {
	tableScans         int
	aggregates         int
	expandProjects     int
	aggregatesOnExpand int
	sinkScans          int
	materializedSinks  int
	flags              []bool
	hasEmptyRowMarker  bool
}

func TestGroupingSetExpandKeepsOutputFilterAboveInput(t *testing.T) {
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	typ := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	filter, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "isnull", []*planpb.Expr{
		GetColExpr(typ, 20, 0),
	})
	require.NoError(t, err)
	equal, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(typ, 20, 0), MakePlan2Int64ConstExprWithType(10),
	})
	require.NoError(t, err)
	for _, expands := range []bool{false, true} {
		for _, filter := range []*planpb.Expr{filter, equal} {
			builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
			project := &planpb.Node{
				NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0},
				BindingTags: []int32{20}, ProjectList: []*planpb.Expr{GetColExpr(typ, 10, 0)},
			}
			if expands {
				project.ExtraOptions = groupingSetExpandOptionPrefix + "2"
				project.GroupingFlag = []bool{true, false}
			}
			scan := &planpb.Node{NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{10}}
			builder.qry.Nodes = []*planpb.Node{scan, project}
			_, remaining := builder.pushdownFilters(1, []*planpb.Expr{DeepCopyExpr(filter)}, false)
			if expands {
				require.Len(t, remaining, 1)
				require.Empty(t, scan.FilterList, "rolled-up NULLs do not exist in the original input")
				scan.NodeType = planpb.Node_VALUE_SCAN
				_, remaining = builder.pushdownFilters(1, []*planpb.Expr{DeepCopyExpr(filter)}, false)
				require.Len(t, remaining, 1, "a dummy input must not discard the expansion barrier")
			} else {
				require.Empty(t, remaining)
				require.Len(t, scan.FilterList, 1, "ordinary projection pushdown remains available")
			}
		}
	}
}

func reachableGroupingSetShape(query *planpb.Query) groupingSetShape {
	shape := groupingSetShape{}
	seen := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) || seen[nodeID] {
			return
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			shape.tableScans++
		case planpb.Node_AGG:
			shape.aggregates++
			if len(node.Children) == 1 {
				if _, ok := DecodeGroupingSetExpandOption(query.Nodes[node.Children[0]].ExtraOptions); ok {
					shape.aggregatesOnExpand++
				}
			}
		case planpb.Node_SINK_SCAN:
			shape.sinkScans++
		case planpb.Node_SINK:
			if node.ExtraOptions == materialized.CTESinkOption {
				shape.materializedSinks++
			}
		case planpb.Node_PROJECT:
			if _, ok := DecodeGroupingSetExpandOption(node.ExtraOptions); ok {
				shape.expandProjects++
				shape.flags = append([]bool(nil), node.GroupingFlag...)
				if len(node.ProjectList) >= 2 {
					marker := node.ProjectList[len(node.ProjectList)-2]
					setID := node.ProjectList[len(node.ProjectList)-1]
					shape.hasEmptyRowMarker = types.T(marker.Typ.Id) == types.T_bool &&
						types.T(setID.Typ.Id) == types.T_int64
				}
			}
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, stepID := range query.Steps {
		visit(stepID)
	}
	return shape
}
