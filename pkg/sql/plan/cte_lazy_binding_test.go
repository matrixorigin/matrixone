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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/util/sysview"
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

func countReachableTableFunction(query *Query, name string) int {
	count := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_FUNCTION_SCAN && node.TableDef != nil &&
			node.TableDef.TblFunc != nil && node.TableDef.TblFunc.Name == name {
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
	useLegacyGroupingSetPlan(t, mock)
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
	useLegacyGroupingSetPlan(t, mock)

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

func TestRecursiveCTEQueryBlockReferenceScope(t *testing.T) {
	mock := NewMockOptimizer(false)

	for _, test := range []struct {
		name string
		sql  string
	}{
		{
			name: "exists subquery cannot add a recursive source",
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select n + 1
					from r
					where exists (select 1 from r nested)
					  and n < 3
				)
				select * from r`,
		},
		{
			name: "scalar subquery cannot add a recursive source",
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select (
						select nested.n + 1
						from r nested
						where nested.n = r.n
					)
					from r
					where n < 3
				)
				select * from r`,
		},
		{
			name: "nested recursive cte cannot add an outer recursive source",
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select n + 1
					from r
					where n < 3
					  and exists (
						with recursive x(m) as (
							select 1
							union all
							select x.m + 1
							from x join r z on false
						)
						select 1 from x
					  )
				)
				select * from r`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := runOneStmt(mock, t, test.sql)
			require.ErrorContains(t, err, "recursive table must be referenced only once, and not in any subquery")
		})
	}

	for _, test := range []struct {
		name           string
		sql            string
		recursiveScans int
	}{
		{
			name:           "top-level recursive source in join remains valid",
			recursiveScans: 1,
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select r.n + 1
					from r join cte_test.t1 on true
					where r.n < 3
				)
				select * from r`,
		},
		{
			name:           "correlated subquery without recursive source remains valid",
			recursiveScans: 1,
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select n + 1
					from r
					where exists (select 1 where r.n < 3)
				)
				select * from r`,
		},
		{
			name:           "nested recursive cte without outer recursive source remains valid",
			recursiveScans: 2,
			sql: `
				with recursive r(n) as (
					select 1
					union all
					select n + 1
					from r
					where n < 3
					  and exists (
						with recursive x(m) as (
							select 1
							union all
							select m + 1 from x where m < 1
						)
						select 1 from x where r.n > 0
					  )
				)
				select * from r`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			recursiveScans := 0
			for _, node := range logicPlan.GetQuery().Nodes {
				if node.NodeType == planpb.Node_RECURSIVE_SCAN {
					recursiveScans++
				}
			}
			require.Equal(t, test.recursiveScans, recursiveScans)
		})
	}
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

func TestCTEReuseHonorsPostOptimizerSQLSelectLimit(t *testing.T) {
	const sql = `
		with q15_revenue0 as (
			select l_suppkey as supplier_no,
			       sum(l_extendedprice * (1 - l_discount)) as total_revenue
			from lineitem
			where l_shipdate >= date '1995-12-01'
			  and l_shipdate < date '1995-12-01' + interval '3' month
			group by l_suppkey
		)
		select supplier_no from q15_revenue0
		union all
		select supplier_no from q15_revenue0`

	for _, test := range []struct {
		name     string
		limit    uint64
		prepare  bool
		wantScan int
	}{
		{name: "ordinary unlimited", limit: ^uint64(0), wantScan: 2},
		{name: "ordinary finite", limit: 1},
		{name: "prepared dynamic", limit: ^uint64(0), prepare: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(false)
			ctx := mock.CurrentContext()
			resolver := func(name string, _, _ bool) (interface{}, error) {
				if name == SQLSelectLimitVariable {
					return test.limit, nil
				}
				return nil, nil
			}
			mock.ctxt.ResolveVariableFunc = resolver
			proc := ctx.GetProcess()
			proc.Base.SessionInfo.ApplySQLSelectLimit = true
			proc.SetResolveVariableFunc(resolver)

			statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			t.Cleanup(func() { statements[0].Free() })
			logicPlan, err := BuildPlan(ctx, statements[0], test.prepare)
			require.NoError(t, err)
			require.Equal(t, test.wantScan,
				countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN))
		})
	}
}

func TestCTEMultiReferenceReusesProducerContainingCTE(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with base_rows as (
			select l_suppkey, l_extendedprice from lineitem
		), supplier_totals as (
			select l_suppkey, sum(l_extendedprice) as total
			from base_rows group by l_suppkey
		)
		select a.l_suppkey, a.total, b.total
		from supplier_totals a join supplier_totals b
			on a.l_suppkey = b.l_suppkey`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	lineitemScans := 0
	groupedAggs := 0
	sinks := 0
	sinkScans := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		switch node.NodeType {
		case planpb.Node_TABLE_SCAN:
			if node.TableDef != nil && node.TableDef.Name == "lineitem" {
				lineitemScans++
			}
		case planpb.Node_AGG:
			if len(node.GroupBy) == 1 {
				groupedAggs++
			}
		case planpb.Node_SINK:
			sinks++
			require.Equal(t, materialized.CTESinkOption, node.ExtraOptions)
		case planpb.Node_SINK_SCAN:
			sinkScans++
		}
	}
	require.Equal(t, 1, lineitemScans)
	require.Equal(t, 1, groupedAggs)
	require.Equal(t, 1, sinks)
	require.Equal(t, 2, sinkScans)
}

func TestCTEMultiReferenceMergesLocalConsumerPredicates(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with customer_totals as (
			select o_custkey, max(c_name) as customer_name,
			       sum(o_totalprice) as total
			from orders join customer on o_custkey = c_custkey
			group by o_custkey
		)
		select a.o_custkey, a.customer_name, b.total
		from customer_totals a join customer_totals b
		  on a.o_custkey = b.o_custkey
		where a.o_custkey between 1 and 100
		  and b.o_custkey between 50 and 150
		  and a.total > 0
		order by a.o_custkey
		limit 100`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	tableScans := make(map[string]int)
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil {
			tableScans[node.TableDef.Name]++
		}
	}
	require.Equal(t, 1, tableScans["orders"])
	require.Equal(t, 1, tableScans["customer"])
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	require.Equal(t, 1, countReachableNodeType(query, planpb.Node_SINK))
}

func TestCTEMultiReferenceReusesHashSemiBuildConsumers(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with expensive_keys as (
			select l_suppkey, sum(l_extendedprice) as total
			from lineitem group by l_suppkey
		)
		select o_orderkey from orders
		where o_custkey in (select l_suppkey from expensive_keys)
		union all
		select c_custkey from customer
		where c_custkey in (select l_suppkey from expensive_keys)`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	require.Equal(t, 1, countReachableNodeType(query, planpb.Node_SINK))

	lineitemScans := 0
	markedScans := 0
	markedSemis := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil &&
			node.TableDef.Name == "lineitem" {
			lineitemScans++
		}
		if node.NodeType == planpb.Node_SINK_SCAN {
			require.Equal(t, materialized.CTEHashBuildScanOption, node.ExtraOptions)
			require.Len(t, node.ProjectList, 1,
				"the shared membership source should retain only its consumed key")
			markedScans++
		}
		if node.NodeType == planpb.Node_JOIN && node.JoinType == planpb.Node_SEMI &&
			subtreeHasNodeOption(query, node.Children[1], materialized.CTEHashBuildScanOption) {
			require.False(t, node.IsRightJoin)
			markedSemis++
		}
	}
	require.Equal(t, 1, lineitemScans)
	require.Equal(t, 2, markedScans)
	require.Equal(t, 2, markedSemis,
		"each marked CTE reader must remain the physical hash-build input")
}

func TestCTEMultiReferencePrunesUnusedVariableWidthPayload(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey, max(l_comment) as comment
			from lineitem group by l_suppkey
		)
		select a.l_suppkey from c a join c b
			on a.l_suppkey = b.l_suppkey
		where a.l_suppkey < 10`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	for nodeID := range cteReachablePlanNodes(query) {
		if node := query.Nodes[nodeID]; node.NodeType == planpb.Node_SINK_SCAN {
			require.Len(t, node.ProjectList, 1,
				"an unused variable-width payload must not inflate the shared source")
		}
	}
}

func TestCTEMultiReferenceRejectsExpandedUnsafeOutputEvaluation(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_suppkey
		)
		select sum(risky) from c where k = 1
		union all
		select sum(length(payload)) from c where k = 2`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"sharing must not evaluate a fallible output for a consumer that did not request it")
}

func TestCTEMultiReferenceRejectsExpandedUnsafeOutputRowDomain(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k, l_orderkey as x,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_suppkey, l_orderkey
		)
		select sum(risky), max(length(payload))
		from c where k = 1 and x = 1
		union all
		select sum(risky), max(length(payload))
		from c where k = 2`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"sharing must not evaluate a fallible output on rows admitted only by a weakened shared predicate")
}

func TestCTEMultiReferenceRejectsHiddenDerivedTablePredicate(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k, l_orderkey as x,
			       cast(max(l_comment) as bigint) as risky
			from lineitem group by l_suppkey, l_orderkey
		)
		select sum(risky) from (select * from c) d where k = 1 and x = 1
		union all
		select sum(risky) from (select * from c) e where k = 2 and x = 1`)
	require.NoError(t, err)

	require.Equal(t, 0,
		countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN),
		"a projection boundary must not disguise a filtered fallible row domain as complete")
}

func TestCTEMultiReferenceRejectsExpandedFallibleProducerPredicate(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_orderkey as k
			from lineitem where cast(l_comment as bigint) > 0
		)
		(select count(*) from (select * from c) d where k = 1)
		union all
		(select k from c limit 0)`)
	require.NoError(t, err)

	require.Equal(t, 0,
		countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN),
		"sharing must not expand the input domain of a fallible producer predicate")
}

func TestCTEMultiReferenceRejectsExpandedFallibleHaving(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k from lineitem group by l_suppkey
			having cast(max(l_comment) as bigint) > 0
		)
		(select count(*) from c where k = 1)
		union all
		(select k from c limit 0)`)
	require.NoError(t, err)

	require.Equal(t, 0,
		countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN),
		"sharing must not expand the group domain of a fallible HAVING predicate")
}

func TestCTEMultiReferenceRejectsExpandedFallibleGroupingKey(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k from lineitem
			group by l_suppkey, cast(l_comment as bigint)
		)
		(select count(*) from c where k = 1)
		union all
		(select k from c limit 0)`)
	require.NoError(t, err)

	require.Equal(t, 0,
		countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN),
		"sharing must not expand the row domain of a fallible grouping key")
}

func TestCTEMultiReferenceRejectsOmittedFallibleConsumerPredicate(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as k, l_shipmode as x,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_suppkey, l_shipmode
		)
		select sum(risky), max(length(payload))
		from c where k = 1 and cast(x as bigint) > 0
		union all
		select sum(risky), max(length(payload))
		from c where k = 2`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"an omitted fallible consumer predicate must make the shared row domain inexact")
}

func TestCTEMultiReferenceRejectsFallibleOutputBeforeConsumerJoin(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_shipmode as region, l_suppkey as k,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_shipmode, l_suppkey
		)
		select sum(c.risky), max(length(c.payload))
		from c join supplier d1 on c.k = d1.s_suppkey
		where c.region = 'AIR'
		union all
		select sum(c.risky), max(length(c.payload))
		from c join supplier d2 on c.k = d2.s_suppkey
		where c.region = 'SHIP'`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"consumer joins must not expand evaluation of a fallible shared output")
}

func TestCTEMultiReferenceRejectsFallibleOutputBeforeConsumerTopN(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_shipmode as region, l_suppkey as k,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_shipmode, l_suppkey
		)
		select sum(risky), max(length(payload)) from (
			select risky, payload from c
			where region = 'AIR' order by k limit 1
		) a
		union all
		select sum(risky), max(length(payload)) from (
			select risky, payload from c
			where region = 'SHIP' order by k limit 1
		) b`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"consumer Top-N must not expand evaluation of a fallible shared output")
}

func TestCTEMultiReferenceRejectsOmittedTagFreeConsumerPredicate(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey as region,
			       cast(max(l_comment) as bigint) as risky,
			       max(l_comment) as payload
			from lineitem group by l_suppkey
		)
		select coalesce(sum(risky), 0), coalesce(max(length(payload)), 0)
		from c where region = 1 and rand() < 0
		union all
		select coalesce(sum(risky), 0), coalesce(max(length(payload)), 0)
		from c where region = 2 and rand() < 0`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 0, countReachableNodeType(query, planpb.Node_SINK_SCAN),
		"an omitted tag-free predicate must make the shared row domain inexact")
}

func TestCTEMultiReferenceReusesRobustPredicateFreeSpillProducer(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey, max(l_comment) as comment
			from lineitem group by l_suppkey
		)
		select a.l_suppkey, a.comment
		from c a join c b on a.l_suppkey = b.l_suppkey
		join c d on a.l_suppkey = d.l_suppkey`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.Equal(t, 3, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	require.Equal(t, 1, countReachableNodeType(query, planpb.Node_SINK))
	lineitemScans := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil &&
			node.TableDef.Name == "lineitem" {
			lineitemScans++
		}
	}
	require.Equal(t, 1, lineitemScans)
}

func TestCTEReuseRollbackHintKeepsConsumersInline(t *testing.T) {
	mock := NewMockOptimizer(false)
	rt := moruntime.ServiceRuntime(mock.CurrentContext().GetProcess().GetService())
	oldHints, hadHints := rt.GetGlobalVariables("optimizer_hints")
	t.Cleanup(func() {
		if hadHints {
			rt.SetGlobalVariables("optimizer_hints", oldHints)
		} else {
			rt.SetGlobalVariables("optimizer_hints", "")
		}
	})
	rt.SetGlobalVariables("optimizer_hints", "sharedComputation=1")

	logicPlan, err := runOneStmt(mock, t, `
		with c as (
			select l_suppkey, max(l_comment) as comment
			from lineitem group by l_suppkey
		)
		select a.l_suppkey, a.comment
		from c a join c b on a.l_suppkey = b.l_suppkey
		join c d on a.l_suppkey = d.l_suppkey`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.Zero(t, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	require.Zero(t, countReachableNodeType(query, planpb.Node_SINK))
	lineitemScans := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN && node.TableDef != nil &&
			node.TableDef.Name == "lineitem" {
			lineitemScans++
		}
	}
	require.Equal(t, 3, lineitemScans)
}

func TestCTEMultiReferenceRejectsNonHashBuildConsumers(t *testing.T) {
	mock := NewMockOptimizer(false)
	for _, test := range []struct {
		name      string
		predicate string
	}{
		{name: "non equality any", predicate: "> any"},
		{name: "null aware not in", predicate: "not in"},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, `
				with expensive_keys as (
					select l_suppkey, sum(l_extendedprice) as total
					from lineitem group by l_suppkey
				)
				select o_orderkey from orders
				where o_custkey `+test.predicate+` (select l_suppkey from expensive_keys)
				union all
				select c_custkey from customer
				where c_custkey `+test.predicate+` (select l_suppkey from expensive_keys)`)
			require.NoError(t, err)
			require.Equal(t, 0,
				countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN))
		})
	}
}

func subtreeHasNodeOption(query *planpb.Query, nodeID int32, option string) bool {
	node := query.Nodes[nodeID]
	if node.ExtraOptions == option {
		return true
	}
	for _, childID := range node.Children {
		if subtreeHasNodeOption(query, childID, option) {
			return true
		}
	}
	return false
}

func TestCTEReuseRewritesConsumersInsideInlineCTE(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `
		with supplier_totals as (
			select l_suppkey, sum(l_extendedprice) as total
			from lineitem group by l_suppkey
		), combined as (
			select a.l_suppkey, a.total as a_total, b.total as b_total
			from supplier_totals a join supplier_totals b
				on a.l_suppkey = b.l_suppkey
		)
		select * from combined`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	require.Equal(t, 2, countReachableNodeType(query, planpb.Node_SINK_SCAN))
	lineitemScans := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_TABLE_SCAN &&
			node.TableDef != nil && node.TableDef.Name == "lineitem" {
			lineitemScans++
		}
	}
	require.Equal(t, 1, lineitemScans)
}

func TestCTEReuseRejectsProducerContainingRecursiveCTE(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t, `with recursive r(n) as (
		select 1
		union all
		select n + 1 from r where n < 3
	), c as (
		select n from r
	) select a.n from c a join c b on a.n = b.n`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	cteSinks := 0
	for nodeID := range cteReachablePlanNodes(query) {
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_SINK && node.ExtraOptions == materialized.CTESinkOption {
			cteSinks++
		}
	}
	require.Equal(t, 0, cteSinks)
}

func TestCTEMultiReferenceReuseGuards(t *testing.T) {
	mock := NewMockOptimizer(false)
	tests := []struct {
		name          string
		sql           string
		wantSinkScans int
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
			wantSinkScans: 2,
		},
		{
			name: "offset consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select a.n_regionkey from c a join (select * from c limit 10 offset 1) b
				on a.n_regionkey = b.n_regionkey`,
			wantSinkScans: 2,
		},
		{
			name: "exists consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where exists (
				select 1 from c b where a.n_regionkey = b.n_regionkey
			)`,
			wantSinkScans: 2,
		},
		{
			name: "in consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n_regionkey in (
				select b.n_regionkey from c b
			)`,
			wantSinkScans: 2,
		},
		{
			name: "any consumer",
			sql: `with c as (
				select n_regionkey, count(*) as n from nation group by n_regionkey
			) select * from c a where a.n_regionkey = any (
				select b.n_regionkey from c b
			)`,
			wantSinkScans: 2,
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
		{
			name: "one predicate-free variable-width consumer",
			sql: `with c as (
				select l_suppkey, max(l_comment) as comment
				from lineitem group by l_suppkey
			) select a.l_suppkey, a.comment from c a join c b
				on a.l_suppkey = b.l_suppkey
				where a.l_suppkey < 10`,
			wantSinkScans: 2,
		},
		{
			name: "volatile consumer predicates",
			sql: `with c as (
				select l_suppkey, max(l_comment) as comment
				from lineitem group by l_suppkey
			) select a.l_suppkey, a.comment from c a join c b
				on a.l_suppkey = b.l_suppkey
				where a.l_suppkey < rand() and b.l_suppkey < rand()`,
			wantSinkScans: 2,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)
			require.Equal(t, test.wantSinkScans,
				countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK_SCAN))
		})
	}
}

func TestCTEReuseRejectsOccurrenceOutsideRewriteRoot(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeType: planpb.Node_VALUE_SCAN},
		{NodeType: planpb.Node_PROJECT, Children: []int32{0}},
		{NodeType: planpb.Node_VALUE_SCAN},
	}}}

	reusable := builder.cteHasDrainWitness(1, []cteOccurrence{
		{rootID: 0},
		{rootID: 2},
	})
	require.False(t, reusable, "a rewrite rooted at node 1 cannot replace occurrence node 2")
}

func TestCTEDrainProofRejectsProbeBehindDrainingOperator(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	joinCond, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(intType, 10, 0),
		GetColExpr(intType, 20, 0),
	})
	require.NoError(t, err)

	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	builder.optimizerHints = &OptimizerHints{joinOrdering: 1}
	builder.qry.Nodes = []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{10}},
		{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0}},
		{NodeId: 2, NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{20}},
		{
			NodeId: 3, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{1, 2}, OnList: []*planpb.Expr{joinCond},
		},
	}

	_, ok := builder.cteConsumerDrainRequirements(3, []cteOccurrence{{rootID: 0}})
	require.False(t, ok,
		"an empty hash build can skip the entire aggregate/probe subtree")
}

func TestCTEDrainProofRejectsCrossJoinConsumer(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN},
		{NodeId: 1, NodeType: planpb.Node_VALUE_SCAN},
		{NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{0, 1}},
	}}}

	_, ok := builder.cteConsumerDrainRequirements(2, []cteOccurrence{{rootID: 0}})
	require.False(t, ok, "a CROSS input has no preserved hash-build contract")
}

func TestCTEDrainProofRejectsPinnedInnerProbe(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	joinCond, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(intType, 10, 0),
		GetColExpr(intType, 20, 0),
	})
	require.NoError(t, err)

	makeBuilder := func(rightType planpb.Node_NodeType, runtimeFilter bool) *QueryBuilder {
		join := &planpb.Node{
			NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{0, 1}, OnList: []*planpb.Expr{joinCond},
		}
		if runtimeFilter {
			join.RuntimeFilterBuildList = []*planpb.RuntimeFilterSpec{{Tag: 1}}
		}
		return &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
			{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{10}},
			{NodeId: 1, NodeType: rightType, BindingTags: []int32{20}},
			join,
		}}}
	}

	for _, builder := range []*QueryBuilder{
		makeBuilder(planpb.Node_FUNCTION_SCAN, false),
		makeBuilder(planpb.Node_VALUE_SCAN, true),
	} {
		_, ok := builder.cteConsumerDrainRequirements(2, []cteOccurrence{{rootID: 0}})
		require.False(t, ok, "a pinned logical-left probe cannot become the physical build")
	}
}

func TestCTEDrainProofRejectsZeroAndStreamingLimits(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN},
		{NodeId: 1, NodeType: planpb.Node_AGG, Children: []int32{0},
			Limit: MakePlan2Uint64ConstExprWithType(0)},
	}}}

	_, ok := builder.cteConsumerDrainRequirements(1, []cteOccurrence{{rootID: 0}})
	require.False(t, ok, "literal LIMIT 0 skips the aggregate input entirely")

	builder.qry.Nodes[1].Limit = MakePlan2Uint64ConstExprWithType(1)
	_, ok = builder.cteConsumerDrainRequirements(1, []cteOccurrence{{rootID: 0}})
	require.True(t, ok, "a positive limit cannot short-circuit blocking aggregation")

	builder.qry.Nodes[1].NodeType = planpb.Node_DISTINCT
	_, ok = builder.cteConsumerDrainRequirements(1, []cteOccurrence{{rootID: 0}})
	require.False(t, ok, "streaming distinct is not a full-input witness under LIMIT")
}

func TestCTEDrainProofRejectsSamplingConsumer(t *testing.T) {
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN},
		{NodeId: 1, NodeType: planpb.Node_SAMPLE, Children: []int32{0}},
	}}}

	_, ok := builder.cteConsumerDrainRequirements(1, []cteOccurrence{{rootID: 0}})
	require.False(t, ok, "block sampling may stop after consuming only part of its input")
}

func TestCTEDrainProofMarksExactInnerHashBuild(t *testing.T) {
	ctx := NewMockCompilerContext(false)
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	joinCond, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
		GetColExpr(intType, 10, 0),
		GetColExpr(intType, 20, 0),
	})
	require.NoError(t, err)

	builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, true)
	builder.qry.Nodes = []*planpb.Node{
		{NodeId: 0, NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{20}},
		{NodeId: 1, NodeType: planpb.Node_VALUE_SCAN, BindingTags: []int32{10}},
		{
			NodeId: 2, NodeType: planpb.Node_JOIN, JoinType: planpb.Node_INNER,
			Children: []int32{1, 0}, OnList: []*planpb.Expr{joinCond},
		},
	}

	requirements, ok := builder.cteConsumerDrainRequirements(
		2, []cteOccurrence{{rootID: 0}})
	require.True(t, ok)
	require.True(t, requirements[0])
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
		{name: "equal cost", producerCost: 3, outcnt: 1, refcnt: 2},
		{name: "write cost does not win", producerCost: 2.5, outcnt: 1, refcnt: 2},
		{name: "single reference", producerCost: 1000, outcnt: 1, refcnt: 1},
		{name: "missing cost", outcnt: 1, refcnt: 2},
		{name: "missing outcnt", producerCost: 1000, refcnt: 2},
		{name: "nan cost", producerCost: math.NaN(), outcnt: 1, refcnt: 2},
		{name: "infinite outcnt", producerCost: 1000, outcnt: math.Inf(1), refcnt: 2},
		{name: "inline cost overflow", producerCost: math.MaxFloat64, outcnt: 1, refcnt: 2},
		{name: "materialization cost overflow", producerCost: math.MaxFloat64, outcnt: math.MaxFloat64, refcnt: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cteReuseIsProfitable(test.producerCost, test.outcnt, test.refcnt))
		})
	}
}

func TestCTEReuseSpillCostSafetyFactor(t *testing.T) {
	for _, test := range []struct {
		name                         string
		producerCost, outcnt, refcnt float64
		factor                       float64
		want                         bool
	}{
		{name: "wide win", producerCost: 100, outcnt: 1, refcnt: 3, factor: 2, want: true},
		{name: "ordinary win lacks margin", producerCost: 3, outcnt: 1, refcnt: 3, factor: 2},
		{name: "invalid factor", producerCost: 100, outcnt: 1, refcnt: 3, factor: 0},
		{name: "infinite factor", producerCost: 100, outcnt: 1, refcnt: 3, factor: math.Inf(1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cteReuseIsProfitableWithSafetyFactor(
				test.producerCost, test.outcnt, test.refcnt, test.factor))
		})
	}
}

func TestCTEReuseMemoryGuard(t *testing.T) {
	fixed := []planpb.Type{{Id: int32(types.T_int64)}}
	variable := []planpb.Type{{Id: int32(types.T_varchar), Width: 1024}}
	unboundedVariable := []planpb.Type{{Id: int32(types.T_varchar)}}
	oversizedVariable := []planpb.Type{{
		Id: int32(types.T_varchar), Width: int32(materialized.MaxSpillBatchBytes / 4),
	}}
	tests := []struct {
		name           string
		stats          *planpb.Stats
		typs           []planpb.Type
		predicateAware bool
		want           bool
	}{
		{name: "below limit", stats: &planpb.Stats{Outcnt: 1024, Rowsize: 8}, typs: fixed, want: true},
		{name: "exact limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedMaterializedBytesLimit}, typs: fixed, want: true},
		{name: "above limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedMaterializedBytesLimit + 1}, typs: fixed},
		{name: "variable width", stats: &planpb.Stats{Outcnt: 1, Rowsize: 8}, typs: variable},
		{name: "predicate-aware variable width", stats: &planpb.Stats{Outcnt: 1, Rowsize: 8}, typs: variable, predicateAware: true, want: true},
		{name: "missing variable capacity", stats: &planpb.Stats{Outcnt: 1, Rowsize: 8}, typs: unboundedVariable, predicateAware: true},
		{name: "single declared row exceeds record safety bound", stats: &planpb.Stats{Outcnt: 1, Rowsize: 8}, typs: oversizedVariable, predicateAware: true},
		{name: "predicate-aware spill", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedMaterializedBytesLimit + 1}, typs: fixed, predicateAware: true, want: true},
		{name: "exact spill limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedSpillBytesLimit}, typs: variable, predicateAware: true, want: true},
		{name: "above spill limit", stats: &planpb.Stats{Outcnt: 1, Rowsize: cteReuseEstimatedSpillBytesLimit + 1}, typs: variable, predicateAware: true},
		{name: "missing rowsize", stats: &planpb.Stats{Outcnt: 1}, typs: fixed},
		{name: "nan rowsize", stats: &planpb.Stats{Outcnt: 1, Rowsize: math.NaN()}, typs: fixed},
		{name: "overflow", stats: &planpb.Stats{Outcnt: math.MaxFloat64, Rowsize: 2}, typs: fixed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, cteReuseFitsStorage(test.stats, test.typs, test.predicateAware))
		})
	}
}

func TestSharedMaterializationRespectsCumulativeProcessCaps(t *testing.T) {
	mock := NewMockOptimizer(false)
	proc := mock.CurrentContext().GetProcess()
	require.NotNil(t, proc)
	require.NotNil(t, proc.Base)
	proc.Base.Lim.Size = 160 * mpool.MB
	proc.Base.Lim.SpillSize = 180 * mpool.MB
	builder := &QueryBuilder{compCtx: mock.CurrentContext()}

	types := []planpb.Type{{Id: int32(types.T_int64), NotNullable: true}}
	spill80MB, ok := estimatedSharedMaterializationSpillBytes(80*mpool.MB, 1, types)
	require.True(t, ok)
	require.True(t, builder.reserveSharedMaterialization(80*mpool.MB, 1, types))
	require.Equal(t, float64(64*mpool.MB), builder.sharedMaterializationMemoryBytes)
	require.Equal(t, spill80MB, builder.sharedMaterializationSpillBytes)
	spill32MB, ok := estimatedSharedMaterializationSpillBytes(32*mpool.MB, 1, types)
	require.True(t, ok)
	require.True(t, builder.reserveSharedMaterialization(32*mpool.MB, 1, types))
	require.False(t, builder.reserveSharedMaterialization(80*mpool.MB, 1, types),
		"planner-introduced sources must not jointly exceed the explicit spill cap")
	require.Equal(t, float64(96*mpool.MB), builder.sharedMaterializationMemoryBytes,
		"a rejected reservation must not mutate the cumulative ledger")
	require.Equal(t, spill80MB+spill32MB, builder.sharedMaterializationSpillBytes)

	proc.Base.Lim.SpillSize = 1
	smallSpillCap := &QueryBuilder{compCtx: mock.CurrentContext()}
	require.False(t, smallSpillCap.reserveSharedMaterialization(32*mpool.KB, 1, types),
		"a byte-small source may still spill after the in-memory batch-count bound")

	proc.Base.Lim.SpillSize = mpool.GB
	memoryBound := &QueryBuilder{compCtx: mock.CurrentContext()}
	require.True(t, memoryBound.reserveSharedMaterialization(64*mpool.MB, 1, types))
	require.True(t, memoryBound.reserveSharedMaterialization(64*mpool.MB, 1, types))
	require.False(t, memoryBound.reserveSharedMaterialization(64*mpool.MB, 1, types),
		"in-memory sources must also respect the cumulative query memory cap")
}

func TestSharedMaterializationAccountsForPerRecordSpillFraming(t *testing.T) {
	outputTypes := []planpb.Type{{Id: int32(types.T_int64), NotNullable: true}}
	const rows = 5000
	const payloadBytes = rows * 8
	spillBytes, ok := estimatedSharedMaterializationSpillBytes(payloadBytes, rows, outputTypes)
	require.True(t, ok)
	recordBytes := sharedMaterializationSpillRecordFixedBytes +
		sharedMaterializationSpillVectorFixedBytes +
		sharedMaterializationSpillVectorMetadataBytes +
		sharedMaterializationSpillGroupingBytes
	require.Equal(t, float64(payloadBytes+rows*recordBytes), spillBytes,
		"the worst-case one-row record must include grouping provenance")

	mock := NewMockOptimizer(false)
	proc := mock.CurrentContext().GetProcess()
	proc.Base.Lim.Size = mpool.GB
	proc.Base.Lim.SpillSize = payloadBytes
	builder := &QueryBuilder{compCtx: mock.CurrentContext()}
	require.False(t, builder.reserveSharedMaterialization(payloadBytes, rows, outputTypes),
		"an exact payload-only cap cannot cover legal one-row spill records")

	proc.Base.Lim.SpillSize = int64(spillBytes)
	withFraming := &QueryBuilder{compCtx: mock.CurrentContext()}
	require.True(t, withFraming.reserveSharedMaterialization(payloadBytes, rows, outputTypes))
}

func TestCTEMaterializedEstimateUsesDeclaredVariableCapacity(t *testing.T) {
	estimated, fixed, ok := cteEstimatedMaterializedBytes(
		&planpb.Stats{Outcnt: 2, Rowsize: 8},
		[]planpb.Type{{Id: int32(types.T_varchar), Width: 10}},
	)
	require.True(t, ok)
	require.False(t, fixed)
	require.Equal(t, float64(2*(types.VarlenaSize+4*10+1)), estimated)
}

func TestCTEReuseStorageEstimateUsesConsumerProjection(t *testing.T) {
	keyType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	payloadType := planpb.Type{Id: int32(types.T_varchar), Width: 1024}
	builder := &QueryBuilder{qry: &planpb.Query{Nodes: []*planpb.Node{
		{
			NodeType: planpb.Node_PROJECT, BindingTags: []int32{10},
			ProjectList: []*planpb.Expr{
				GetColExpr(keyType, 1, 0), GetColExpr(payloadType, 1, 1),
			},
			FilterList: []*planpb.Expr{GetColExpr(payloadType, 10, 1)},
		},
		{
			NodeType: planpb.Node_PROJECT, Children: []int32{0},
			ProjectList: []*planpb.Expr{GetColExpr(keyType, 10, 0)},
		},
		{
			NodeType: planpb.Node_PROJECT, BindingTags: []int32{20},
			ProjectList: []*planpb.Expr{
				GetColExpr(keyType, 2, 0), GetColExpr(payloadType, 2, 1),
			},
		},
		{
			NodeType: planpb.Node_PROJECT, Children: []int32{2},
			ProjectList: []*planpb.Expr{GetColExpr(keyType, 20, 0)},
		},
		{NodeType: planpb.Node_UNION_ALL, Children: []int32{1, 3}},
	}}}

	outputTypes, narrowed := builder.cteStorageOutputTypes(4, []cteOccurrence{
		{rootID: 0, rootTag: 10, types: []planpb.Type{keyType, payloadType}},
		{rootID: 2, rootTag: 20, types: []planpb.Type{keyType, payloadType}},
	})
	require.True(t, narrowed)
	require.Equal(t, []planpb.Type{keyType}, outputTypes)
	rowSize, fixed := fixedOutputRowSize(outputTypes)
	require.True(t, fixed)
	require.Equal(t, float64(types.T_int64.TypeLen()), rowSize)
}

func TestCTEOutputDemandRequiresTotalExtraColumns(t *testing.T) {
	intType := planpb.Type{Id: int32(types.T_int64), NotNullable: true}
	stringType := planpb.Type{Id: int32(types.T_varchar)}
	targetType := types.T_int64.ToType()
	ctx := NewMockCompilerContext(true)
	castExpr, err := makePlan2CastExpr(
		ctx.GetContext(),
		GetColExpr(stringType, 1, 1),
		makePlan2Type(&targetType),
	)
	require.NoError(t, err)
	secondCastExpr, err := makePlan2CastExpr(
		ctx.GetContext(),
		GetColExpr(stringType, 2, 1),
		makePlan2Type(&targetType),
	)
	require.NoError(t, err)

	builder := &QueryBuilder{
		compCtx: ctx,
		qry: &planpb.Query{Nodes: []*planpb.Node{
			{
				NodeId: 0, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{1},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "k", Typ: intType}, {Name: "raw", Typ: stringType},
				}},
			},
			{
				NodeId: 1, NodeType: planpb.Node_PROJECT, Children: []int32{0}, BindingTags: []int32{10},
				ProjectList: []*planpb.Expr{GetColExpr(intType, 1, 0), castExpr},
			},
			{
				NodeId: 2, NodeType: planpb.Node_PROJECT, Children: []int32{1},
				ProjectList: []*planpb.Expr{GetColExpr(intType, 10, 0), GetColExpr(castExpr.Typ, 10, 1)},
			},
			{
				NodeId: 3, NodeType: planpb.Node_TABLE_SCAN, BindingTags: []int32{2},
				TableDef: &planpb.TableDef{Cols: []*planpb.ColDef{
					{Name: "k", Typ: intType}, {Name: "raw", Typ: stringType},
				}},
			},
			{
				NodeId: 4, NodeType: planpb.Node_PROJECT, Children: []int32{3}, BindingTags: []int32{20},
				ProjectList: []*planpb.Expr{
					GetColExpr(intType, 2, 0),
					secondCastExpr,
				},
			},
			{
				NodeId: 5, NodeType: planpb.Node_PROJECT, Children: []int32{4},
				ProjectList: []*planpb.Expr{GetColExpr(intType, 20, 0)},
			},
			{NodeId: 6, NodeType: planpb.Node_UNION_ALL, Children: []int32{2, 5}},
		}},
	}

	require.False(t, builder.cteOutputDemandPreservesEvaluation(6, []cteOccurrence{
		{rootID: 1, rootTag: 10, types: []planpb.Type{intType, castExpr.Typ}},
		{rootID: 4, rootTag: 20, types: []planpb.Type{intType, castExpr.Typ}},
	}, true), "a consumer-only fallible cast must keep the CTE inline")
}

func TestCTEReuseRejectsExternalAndSideEffectingNodes(t *testing.T) {
	for _, nodeType := range []planpb.Node_NodeType{
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

func TestCTEReuseRejectsVolatileValueScanExpressions(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		select * from (values row(rand())) v(x)`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)
	valueScanID := int32(-1)
	for nodeID, node := range query.Nodes {
		if node.NodeType == planpb.Node_VALUE_SCAN {
			valueScanID = int32(nodeID)
			break
		}
	}
	require.NotEqual(t, int32(-1), valueScanID)
	builder := &QueryBuilder{qry: query}
	require.False(t, builder.cteSubtreeIsDeterministic(valueScanID, make(map[int32]bool)),
		"VALUES expressions execute inside RowsetData and must participate in volatility checks")
	require.False(t, builder.cteProducerEvaluationIsTotal(
		valueScanID, valueScanID, nil, make(map[int32]bool),
	), "VALUES expressions must also participate in expanded-domain safety checks")
}

func TestCTEReuseRejectsVolatileAuxiliaryNodeExpressions(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `select rand()`)
	require.NoError(t, err)
	var volatile *planpb.Expr
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == planpb.Node_PROJECT && len(node.ProjectList) == 1 &&
			node.ProjectList[0].GetF() != nil {
			volatile = node.ProjectList[0]
			break
		}
	}
	require.NotNil(t, volatile)

	tests := []struct {
		name string
		node *planpb.Node
	}{
		{name: "physical equality key", node: &planpb.Node{
			NodeType: planpb.Node_UNION, PhysicalEqualityKeyList: []*planpb.Expr{volatile},
		}},
		{name: "gap fill bound", node: &planpb.Node{
			NodeType: planpb.Node_TIME_WINDOW, GapFillStart: volatile,
		}},
		{name: "index reader limit", node: &planpb.Node{
			NodeType:         planpb.Node_TABLE_SCAN,
			IndexReaderParam: &planpb.IndexReaderParam{Limit: volatile},
		}},
		{name: "vector index query", node: &planpb.Node{
			NodeType:        planpb.Node_TABLE_SCAN,
			VectorIndexScan: &planpb.VectorIndexScan{QueryVector: volatile},
		}},
		{name: "dedup update expression", node: &planpb.Node{
			NodeType:     planpb.Node_JOIN,
			DedupJoinCtx: &planpb.DedupJoinCtx{UpdateColExprList: []*planpb.Expr{volatile}},
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := &QueryBuilder{qry: &Query{Nodes: []*Node{test.node}}}
			require.False(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))
			require.False(t, builder.cteProducerEvaluationIsTotal(
				0, 0, nil, make(map[int32]bool),
			))
		})
	}
}

func TestCTEReuseSharesOnlyStatementStableCurrentRolesFunction(t *testing.T) {
	currentRoles := func() *planpb.Node {
		return &planpb.Node{
			NodeType: planpb.Node_FUNCTION_SCAN,
			TableDef: &planpb.TableDef{TblFunc: &planpb.TableFunction{Name: "mo_current_roles"}},
		}
	}

	builder := &QueryBuilder{qry: &Query{Nodes: []*Node{currentRoles()}}}
	require.True(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))
	require.True(t, builder.cteSubtreeIsCurrentRoleClosure(0, make(map[int32]bool)))
	require.True(t, currentRoleClosureOutput([]planpb.Type{{Id: int32(types.T_int64)}}))
	require.False(t, currentRoleClosureOutput([]planpb.Type{{Id: int32(types.T_varchar)}}))

	builder.qry.Nodes[0].TableDef.TblFunc.Name = "generate_series"
	require.False(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))

	builder.qry.Nodes[0] = currentRoles()
	builder.qry.Nodes[0].TblFuncExprList = []*planpb.Expr{{}}
	require.False(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))

	builder.qry.Nodes = append(builder.qry.Nodes, &planpb.Node{NodeType: planpb.Node_VALUE_SCAN})
	builder.qry.Nodes[0] = currentRoles()
	builder.qry.Nodes[0].Children = []int32{1}
	require.False(t, builder.cteSubtreeIsDeterministic(0, make(map[int32]bool)))

	builder.qry.Nodes = []*Node{
		currentRoles(),
		{
			NodeType: planpb.Node_PROJECT,
			Children: []int32{0},
			ProjectList: []*planpb.Expr{GetColExpr(
				planpb.Type{Id: int32(types.T_int64)}, 0, 0)},
		},
	}
	require.True(t, builder.cteSubtreeIsCurrentRoleClosure(1, make(map[int32]bool)))
	builder.qry.Nodes[1].ProjectList[0].Typ.Id = int32(types.T_varchar)
	require.False(t, builder.cteSubtreeIsCurrentRoleClosure(1, make(map[int32]bool)))
	builder.qry.Nodes[1].ProjectList[0] = MakePlan2Int64ConstExprWithType(1)
	require.False(t, builder.cteSubtreeIsCurrentRoleClosure(1, make(map[int32]bool)))
}

func TestCTEReuseCurrentRolesExemptionRejectsAmplifyingSubtree(t *testing.T) {
	purePlan, err := runOneStmt(NewMockOptimizer(false), t, `
		WITH c AS (SELECT role_id FROM mo_current_roles() role_closure)
		SELECT a.role_id FROM c a JOIN c b ON a.role_id = b.role_id LIMIT 1`)
	require.NoError(t, err)
	require.Equal(t, 1, countReachableNodeType(purePlan.GetQuery(), planpb.Node_SINK))
	require.Equal(t, 1, countReachableTableFunction(purePlan.GetQuery(), "mo_current_roles"))

	earlyStopQueries := []struct {
		name string
		sql  string
	}{
		{
			name: "limit union",
			sql: `
				WITH c AS (
					SELECT l.l_comment, r.role_id
					FROM lineitem l CROSS JOIN mo_current_roles() r
				)
				(SELECT role_id FROM c LIMIT 1)
				UNION ALL
				(SELECT role_id FROM c LIMIT 1)`,
		},
		{
			name: "semi join",
			sql: `
				WITH c AS (
					SELECT l.l_comment, r.role_id
					FROM lineitem l CROSS JOIN mo_current_roles() r
				)
				SELECT role_id FROM c a
				WHERE EXISTS (SELECT 1 FROM c b WHERE a.role_id = b.role_id)`,
		},
	}
	for _, test := range earlyStopQueries {
		t.Run(test.name, func(t *testing.T) {
			amplifiedPlan, err := runOneStmt(NewMockOptimizer(false), t, test.sql)
			require.NoError(t, err)
			require.Zero(t, countReachableNodeType(amplifiedPlan.GetQuery(), planpb.Node_SINK),
				"an early-terminating amplifying subtree must retain the guarded inline plan")
			require.Equal(t, 2, countReachableTableFunction(amplifiedPlan.GetQuery(), "mo_current_roles"))
		})
	}

	variableWidthPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		WITH c AS (
			SELECT l.l_comment, r.role_id
			FROM lineitem l CROSS JOIN mo_current_roles() r
		)
		SELECT count(*) FROM c a JOIN c b ON a.role_id = b.role_id`)
	require.NoError(t, err)
	require.Zero(t, countReachableNodeType(variableWidthPlan.GetQuery(), planpb.Node_SINK),
		"a full-drain variable-width subtree must retain the materialization memory guard")
}

func TestCTEReuseCurrentRolesExemptionRejectsFallibleProjection(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(false), t, `
		WITH c AS (
			SELECT CAST(CONCAT('not-an-integer-', role_id) AS BIGINT) AS role_id
			FROM mo_current_roles() role_closure
		)
		(SELECT role_id FROM c LIMIT 0)
		UNION ALL
		(SELECT role_id FROM c LIMIT 0)`)
	require.NoError(t, err)
	require.Zero(t, countReachableNodeType(logicPlan.GetQuery(), planpb.Node_SINK),
		"the no-witness exemption must not eagerly evaluate a fallible projection")
	require.Equal(t, 2,
		countReachableTableFunction(logicPlan.GetQuery(), "mo_current_roles"))
}

func TestInformationSchemaMetadataPlansShareCurrentRolesOnce(t *testing.T) {
	views := []struct {
		name string
		ddl  string
	}{
		{name: "TABLES", ddl: sysview.InformationSchemaTablesV41DDL},
		{name: "COLUMNS", ddl: sysview.InformationSchemaColumnsV41DDL},
		{name: "STATISTICS", ddl: sysview.InformationSchemaStatisticsDDL},
		{name: "CHECK_CONSTRAINTS", ddl: sysview.InformationSchemaCheckConstraintsDDL},
		{name: "VIEWS", ddl: sysview.InformationSchemaViewsDDL},
		{name: "SCHEMATA", ddl: sysview.InformationSchemaSchemataDDL},
	}
	for _, view := range views {
		t.Run(view.name, func(t *testing.T) {
			as := strings.Index(view.ddl, " AS ")
			require.Greater(t, as, 0)
			logicPlan, err := runOneStmt(NewMockOptimizer(false), t, view.ddl[as+4:])
			require.NoError(t, err)
			query := logicPlan.GetQuery()
			require.Equal(t, 1, countReachableTableFunction(query, "mo_current_roles"))
			require.Equal(t, 1, countReachableNodeType(query, planpb.Node_SINK),
				"the role closure must have one statement-local producer")
		})
	}
}

func BenchmarkInformationSchemaSchemataPlanSharesCurrentRoles(b *testing.B) {
	as := strings.Index(sysview.InformationSchemaSchemataDDL, " AS ")
	if as <= 0 {
		b.Fatal("SCHEMATA DDL has no AS clause")
	}
	sql := sysview.InformationSchemaSchemataDDL[as+4:]
	ctx := NewMockOptimizer(false).CurrentContext()

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
		if err != nil {
			b.Fatal(err)
		}
		logicPlan, err := BuildPlan(ctx, statements[0], false)
		statements[0].Free()
		if err != nil {
			b.Fatal(err)
		}
		if scans := countReachableTableFunction(logicPlan.GetQuery(), "mo_current_roles"); scans != 1 {
			b.Fatalf("expected one reachable mo_current_roles scan, got %d", scans)
		}
	}
}

func TestCTEReuseAcceptsGuardedNestedMaterializedSource(t *testing.T) {
	builder := &QueryBuilder{qry: &Query{Nodes: []*Node{
		{NodeType: planpb.Node_VALUE_SCAN},
		{
			NodeType: planpb.Node_SINK, Children: []int32{0},
			ExtraOptions: materialized.CTESinkOption,
		},
		{NodeType: planpb.Node_SINK_SCAN, SourceStep: []int32{0}},
		{NodeType: planpb.Node_PROJECT, Children: []int32{2}},
	}, Steps: []int32{1}}}
	require.True(t, builder.cteSubtreeIsDeterministic(3, make(map[int32]bool)))

	builder.qry.Nodes[1].ExtraOptions = ""
	require.False(t, builder.cteSubtreeIsDeterministic(3, make(map[int32]bool)),
		"an unguarded sink dependency must still fail closed")
}

func TestCTEReuseRecognizesGuardedRuntimeFilterExpression(t *testing.T) {
	col := func(pos int32) *planpb.Expr {
		return &planpb.Expr{
			Typ:  planpb.Type{Id: int32(types.T_int64)},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}},
		}
	}
	for _, test := range []struct {
		name string
		spec *planpb.RuntimeFilterSpec
		want bool
	}{
		{
			name: "probe or legacy expression",
			spec: &planpb.RuntimeFilterSpec{Expr: col(0)},
			want: true,
		},
		{
			name: "guarded build expression",
			spec: &planpb.RuntimeFilterSpec{BuildExpr: col(0)},
			want: true,
		},
		{
			name: "rolling upgrade raw hybrid",
			spec: &planpb.RuntimeFilterSpec{
				Expr: col(0), BuildExpr: col(0),
			},
			want: true,
		},
		{
			name: "contradictory dual expression",
			spec: &planpb.RuntimeFilterSpec{
				Expr: col(0), BuildExpr: col(1),
			},
		},
		{
			name: "missing expression",
			spec: &planpb.RuntimeFilterSpec{},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			builder := &QueryBuilder{qry: &Query{Nodes: []*Node{{
				NodeType:               planpb.Node_PROJECT,
				RuntimeFilterBuildList: []*planpb.RuntimeFilterSpec{test.spec},
			}}}}
			require.Equal(t, test.want,
				builder.cteSubtreeIsDeterministic(
					0, make(map[int32]bool)))
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
