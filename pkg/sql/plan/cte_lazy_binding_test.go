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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

type cteViewTrackingContext struct {
	CompilerContext
	views []string
}

func (c *cteViewTrackingContext) GetViews() []string {
	return c.views
}

func (c *cteViewTrackingContext) SetViews(views []string) {
	c.views = append([]string(nil), views...)
}

type cteViewTrackingOptimizer struct {
	ctx CompilerContext
}

func (o *cteViewTrackingOptimizer) CurrentContext() CompilerContext {
	return o.ctx
}

func (o *cteViewTrackingOptimizer) Optimize(stmt tree.Statement) (*Query, error) {
	logicPlan, err := BuildPlan(o.ctx, stmt, false)
	if err != nil {
		return nil, err
	}
	return logicPlan.GetQuery(), nil
}

func collectGroupingFlags(query *Query, rootIDs ...int32) [][]bool {
	seen := make(map[int32]bool)
	groupingFlags := make([][]bool, 0)
	var visit func(int32)
	visit = func(nodeID int32) {
		if seen[nodeID] {
			return
		}
		seen[nodeID] = true
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_AGG {
			groupingFlags = append(groupingFlags, append([]bool(nil), node.GroupingFlag...))
		}
		for _, childID := range node.Children {
			visit(childID)
		}
	}
	for _, rootID := range rootIDs {
		visit(rootID)
	}
	return groupingFlags
}

func cteReachablePlanNodes(query *Query) map[int32]bool {
	reachable := make(map[int32]bool)
	var visit func(int32)
	visit = func(nodeID int32) {
		if reachable[nodeID] {
			return
		}
		reachable[nodeID] = true
		for _, childID := range query.Nodes[nodeID].Children {
			visit(childID)
		}
	}
	for _, stepID := range query.Steps {
		visit(stepID)
	}
	return reachable
}

func countReachableNodeType(query *Query, nodeType planpb.Node_NodeType) int {
	count := 0
	for nodeID := range cteReachablePlanNodes(query) {
		if query.Nodes[nodeID].NodeType == nodeType {
			count++
		}
	}
	return count
}

func requireSharedCTEGroupingFlags(t *testing.T, logicPlan *Plan, expected [][]bool) {
	t.Helper()
	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.ElementsMatch(t, expected, collectGroupingFlags(query, query.Steps...), "shared CTE producer grouping variants")
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	sources := make(map[int32]int)
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_SINK_SCAN {
			for _, sourceStep := range node.SourceStep {
				sources[sourceStep]++
			}
		}
	}
	require.Len(t, sources, 1)
	for _, consumers := range sources {
		require.Equal(t, 2, consumers)
	}
}

func TestCTELazyBindingDeclarationScope(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("unused invalid body is not bound", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with bad as (select missing_column from nation)
			select 1`)
		require.NoError(t, err)
	})

	t.Run("referenced invalid body is bound", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with bad as (select missing_column from nation)
			select * from bad`)
		require.ErrorContains(t, err, "missing_column")
	})

	t.Run("body cannot capture declaring block FROM", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn as (select * from bvt_test2.t2 where t2.b = t3.a)
			select * from bvt_test2.t3 where exists (select * from qn)`)
		require.ErrorContains(t, err, "missing FROM-clause entry for table 't3'")
	})

	t.Run("body can correlate to an outer query block", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			select (
				with qn as (select t2.a * t1.a as a from cte_test.t1),
				     qn2 as (select 3 * a as b from qn)
				select * from qn2 limit 1
			)
			from bvt_test2.t2`)
		require.NoError(t, err)
	})
}

func TestCTELazyBindingRollupSingleExpansion(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with totals as (
			select n_regionkey, count(*) as n
			from nation
			group by n_regionkey with rollup
		)
		select * from totals`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.ElementsMatch(t, [][]bool{{true}, {false}}, collectGroupingFlags(query, query.Steps...))
}

func TestCTELazyBindingRepeatedGroupingSets(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("rollup keeps both variants for each reference", func(t *testing.T) {
		logicPlan, err := runOneStmt(mock, t, `
			with totals as (
				select n_regionkey, count(*) as n
				from nation
				group by n_regionkey with rollup
			)
			select *
			from totals a join totals b on a.n_regionkey = b.n_regionkey`)
		require.NoError(t, err)
		requireSharedCTEGroupingFlags(t, logicPlan, [][]bool{{true}, {false}})
	})

	t.Run("cube keeps all variants for each reference", func(t *testing.T) {
		logicPlan, err := runOneStmt(mock, t, `
			with totals as (
				select count(*) as n
				from nation
				group by cube(n_regionkey, n_nationkey)
			)
			select *
			from totals a join totals b on a.n = b.n`)
		require.NoError(t, err)
		requireSharedCTEGroupingFlags(t, logicPlan, [][]bool{
			{false, false},
			{true, false},
			{true, true},
			{false, true},
		})
	})
}

func TestCTELazyBindingVisibilityGuards(t *testing.T) {
	mock := NewMockOptimizer(false)

	t.Run("forward reference stays rejected", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn2 as (select a from qn),
			     qn as (select a from cte_test.t1)
			select * from qn2`)
		require.Error(t, err)
	})

	t.Run("self reference stays rejected", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with qn as (select * from qn)
			select * from qn`)
		require.ErrorContains(t, err, "recursive table must be referenced only once")
	})

	t.Run("recursive reference stays accepted", func(t *testing.T) {
		_, err := runOneStmt(mock, t, `
			with recursive c as (
				select a from cte_test.t1
				union all
				select a + 1 from c where a < 3
			)
			select * from c`)
		require.NoError(t, err)
	})
}

func TestCTELazyBindingKeepsRootContextOwnership(t *testing.T) {
	ctx := &cteViewTrackingContext{CompilerContext: NewMockCompilerContext(false)}
	mock := &cteViewTrackingOptimizer{ctx: ctx}

	_, err := runOneStmt(mock, t, `
	with qn as (select * from cte_test.v2)
		select * from qn`)
	require.NoError(t, err)
	require.Len(t, ctx.views, 1)
	databaseName, viewName, snapshot, err := ParseViewDependencyKey(ctx.views[0])
	require.NoError(t, err)
	require.Equal(t, "cte_test", databaseName)
	require.Equal(t, "v2", viewName)
	require.Nil(t, snapshot)
}

func TestCTEMultiReferenceReusesExpensiveProducer(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with q15_revenue0 as (
			select l_suppkey as supplier_no,
			       sum(l_extendedprice * (1 - l_discount)) as total_revenue
			from lineitem
			where l_shipdate >= date '1995-12-01'
			  and l_shipdate < date '1995-12-01' + interval '3' month
			group by l_suppkey
		)
		select s_suppkey, s_name, s_address, s_phone, total_revenue
		from supplier, q15_revenue0
		where s_suppkey = supplier_no
		  and total_revenue = (select max(total_revenue) from q15_revenue0)
		order by s_suppkey`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	lineitemScans := 0
	groupedRevenueAggs := 0
	sinkScans := 0
	sinks := 0
	scalarAggConsumers := 0
	sourceSteps := make(map[int32]int)
	reachable := cteReachablePlanNodes(query)
	var containsSinkScan func(int32) bool
	containsSinkScan = func(nodeID int32) bool {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_SINK_SCAN {
			return true
		}
		for _, childID := range node.Children {
			if containsSinkScan(childID) {
				return true
			}
		}
		return false
	}
	for nodeID := range reachable {
		node := query.Nodes[nodeID]
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			if node.TableDef != nil && node.TableDef.Name == "lineitem" {
				lineitemScans++
			}
		case planpb.Node_AGG:
			if len(node.GroupBy) == 1 {
				groupedRevenueAggs++
			} else if len(node.GroupBy) == 0 && containsSinkScan(nodeID) {
				scalarAggConsumers++
			}
		case planpb.Node_SINK:
			sinks++
			require.Equal(t, materialized.CTESinkOption, node.ExtraOptions)
		case planpb.Node_SINK_SCAN:
			sinkScans++
			for _, sourceStep := range node.SourceStep {
				sourceSteps[sourceStep]++
			}
		}
	}
	require.Equal(t, 1, lineitemScans)
	require.Equal(t, 1, groupedRevenueAggs)
	require.Equal(t, 1, sinks)
	require.Equal(t, 2, sinkScans)
	require.Equal(t, 1, scalarAggConsumers, "the scalar MAX consumer must remain above one shared scan")
	require.Len(t, sourceSteps, 1)
	for _, consumers := range sourceSteps {
		require.Equal(t, 2, consumers)
	}
}

func TestCTEMultiReferenceReuseGuards(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "single reference",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c`,
		},
		{
			name: "cost does not win",
			sql: `with c as (select * from nation)
				select a.n_nationkey from c a join c b on a.n_nationkey = b.n_nationkey`,
		},
		{
			name: "volatile producer",
			sql: `with c as (
				select n_regionkey, max(rand()) as r from nation group by n_regionkey
			) select a.n_regionkey from c a join c b on a.n_regionkey = b.n_regionkey`,
		},
		{
			name: "real time producer",
			sql: `with c as (
				select n_regionkey, max(now()) as ts from nation group by n_regionkey
			) select a.n_regionkey from c a join c b on a.n_regionkey = b.n_regionkey`,
		},
		{
			name: "sample producer",
			sql: `with c as (
				select sample(total, 2 rows) as n from (
					select l_suppkey, sum(l_extendedprice) as total
					from lineitem group by l_suppkey
				) grouped
			) select a.n from c a join c b on a.n = b.n`,
		},
		{
			name: "limit consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select a.n_regionkey from c a join (select * from c limit 1) b
				on a.n_regionkey = b.n_regionkey`,
		},
		{
			name: "offset consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select a.n_regionkey from c a join (select * from c limit 10 offset 1) b
				on a.n_regionkey = b.n_regionkey`,
		},
		{
			name: "limit above scalar aggregate",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n = (
				select max(b.n) from c b limit 1
			)`,
		},
		{
			name: "exists consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where exists (
				select 1 from c b where a.n_regionkey = b.n_regionkey
			)`,
		},
		{
			name: "in consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n_regionkey in (
				select b.n_regionkey from c b
			)`,
		},
		{
			name: "any consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n_regionkey = any (
				select b.n_regionkey from c b
			)`,
		},
		{
			name: "all consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n_regionkey >= all (
				select b.n_regionkey from c b
			)`,
		},
		{
			name: "outer for update",
			sql: `with c as (
				select l_suppkey, sum(l_extendedprice) as total from lineitem group by l_suppkey
			) select s.* from supplier s
				join c a on s.s_suppkey = a.l_suppkey
				join c b on s.s_suppkey = b.l_suppkey
				for update`,
		},
		{
			name: "correlated nested cte",
			sql: `select (
				with c as (
					select t1.a * t2.a as a from cte_test.t1
				) select max(x.a + y.a) from c x join c y on x.a = y.a
			) from bvt_test2.t2`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			require.Equal(t, 0, countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN))
		})
	}
}

func TestCTEMultiReferenceReuseRespectsNestedShadowing(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `with c as (
		select n_regionkey, count(*) as n from nation group by n_regionkey
	) select outer_c.n_regionkey
		from c outer_c join (
			with c as (
				select r_regionkey, count(*) as n from region group by r_regionkey
			) select x.r_regionkey from c x join c y on x.r_regionkey = y.r_regionkey
		) inner_c on outer_c.n_regionkey = inner_c.r_regionkey`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN), "only the twice-referenced inner CTE should be shared")
	tableScans := make(map[string]int)
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil {
			tableScans[node.TableDef.Name]++
		}
	}
	require.Equal(t, 1, tableScans["nation"], "the outer single reference must remain independent")
	require.Equal(t, 1, tableScans["region"], "the shadowing inner CTE must have one producer")
}

func TestCTEReuseCostGuard(t *testing.T) {
	tests := []struct {
		name                         string
		producerCost, outcnt, refcnt float64
		want                         bool
	}{
		{name: "profitable", producerCost: 1000, outcnt: 1, refcnt: 2, want: true},
		{name: "equal cost", producerCost: 2, outcnt: 1, refcnt: 2},
		{name: "single reference", producerCost: 1000, outcnt: 1, refcnt: 1},
		{name: "missing cost", outcnt: 1, refcnt: 2},
		{name: "missing outcnt", producerCost: 1000, refcnt: 2},
		{name: "nan cost", producerCost: math.NaN(), outcnt: 1, refcnt: 2},
		{name: "infinite outcnt", producerCost: 1000, outcnt: math.Inf(1), refcnt: 2},
		{name: "inline cost overflow", producerCost: math.MaxFloat64, outcnt: 1, refcnt: 2},
		{name: "consumer cost overflow", producerCost: math.MaxFloat64, outcnt: math.MaxFloat64, refcnt: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cteReuseIsProfitable(test.producerCost, test.outcnt, test.refcnt))
		})
	}
}

func TestCTEReuseMemoryGuard(t *testing.T) {
	fixed := []planpb.Type{{Id: int32(types.T_int64)}}
	variable := []planpb.Type{{Id: int32(types.T_varchar)}}
	tests := []struct {
		name  string
		stats *planpb.Stats
		typs  []planpb.Type
		want  bool
	}{
		{name: "below limit", stats: &planpb.Stats{Outcnt: 1024, Rowsize: 8}, typs: fixed, want: true},
		{name: "exact limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedMaterializedBytesLimit}, typs: fixed, want: true},
		{name: "above limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedMaterializedBytesLimit + 1}, typs: fixed},
		{name: "variable width", stats: &planpb.Stats{Outcnt: 1, Rowsize: 8}, typs: variable},
		{name: "missing rowsize", stats: &planpb.Stats{Outcnt: 1}, typs: fixed},
		{name: "nan rowsize", stats: &planpb.Stats{Outcnt: 1, Rowsize: math.NaN()}, typs: fixed},
		{name: "overflow", stats: &planpb.Stats{Outcnt: math.MaxFloat64, Rowsize: 2}, typs: fixed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cteReuseFitsMemory(test.stats, test.typs))
		})
	}
}

func TestCTEReuseRejectsExternalAndSideEffectingNodes(t *testing.T) {
	for _, nodeType := range []planpb.Node_NodeType{
		planpb.Node_FUNCTION_SCAN,
		planpb.Node_EXTERNAL_SCAN,
		planpb.Node_EXTERNAL_FUNCTION,
		planpb.Node_LOCK_OP,
		planpb.Node_INSERT,
		planpb.Node_DELETE,
		planpb.Node_MULTI_UPDATE,
		planpb.Node_POSTDML,
		planpb.Node_RECURSIVE_CTE,
		planpb.Node_RECURSIVE_SCAN,
		planpb.Node_SAMPLE,
	} {
		t.Run(nodeType.String(), func(t *testing.T) {
			builder := &QueryBuilder{qry: &Query{Nodes: []*Node{{NodeType: nodeType}}}}
			require.False(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))
		})
	}
}

func TestCTEMultiReferenceReusePreservesConsumerBindings(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with totals(region_key, total) as (
			select n_regionkey, count(*) from nation group by n_regionkey
		)
		select a.region_key, b.total
		from totals a join totals b on a.region_key = b.region_key`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	projectedCols := make(map[int32]bool)
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType != planpb.Node_SINK_SCAN {
			continue
		}
		require.Equal(t, []string{"region_key", "total"}, []string{node.TableDef.Cols[0].Name, node.TableDef.Cols[1].Name})
		for _, expr := range node.ProjectList {
			require.NotNil(t, expr.GetCol())
			projectedCols[expr.GetCol().ColPos] = true
		}
	}
	require.Equal(t, map[int32]bool{0: true, 1: true}, projectedCols)
}
