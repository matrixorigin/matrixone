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

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIssue28158CorrelatedScalarAggregateUsesOuterKeyDomain(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		SELECT o.o_orderkey,
		       CASE WHEN o.o_orderstatus IN ('O', 'F') THEN
		         (SELECT MAX(l.l_quantity)
		            FROM lineitem l
		           WHERE l.l_orderkey = o.o_orderkey
		             AND l.l_partkey = o.o_custkey
		             AND l.l_returnflag = 'A')
		       ELSE NULL END AS latest_seq
		  FROM orders o
		  LEFT JOIN lineitem next_event
		    ON next_event.l_orderkey = o.o_orderkey
		   AND next_event.l_partkey = o.o_custkey
		 WHERE o.o_orderkey = 1
		   AND o.o_custkey = 2
		 ORDER BY next_event.l_linenumber`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	agg := issue28158FindAggregate(query, "max")
	require.NotNil(t, agg)
	require.Len(t, agg.Children, 1)

	join := query.Nodes[agg.Children[0]]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	// Keep the duplicate-safe SEMI shape even when the outer domain is a
	// primary-key point.  The point constants are copied to the aggregate scan
	// separately so index selection does not depend on join reordering.
	require.Equal(t, plan.Node_SEMI, join.JoinType)
	require.Len(t, join.Children, 2)
	require.Len(t, join.OnList, 2)

	var domain *plan.Node
	for _, childID := range join.Children {
		child := query.Nodes[childID]
		if child.NodeType == plan.Node_TABLE_SCAN && child.TableDef != nil && child.TableDef.Name == "orders" {
			domain = child
			break
		}
	}
	require.NotNil(t, domain)
	require.Equal(t, plan.Node_TABLE_SCAN, domain.NodeType)
	require.Equal(t, "orders", domain.TableDef.Name)
	require.Len(t, domain.FilterList, 2)
	aggregateScan := issue28158FindTableScanUnder(query, agg.Children[0], "lineitem")
	require.NotNil(t, aggregateScan)
	// lineitem already has the local returnflag predicate.  The additional
	// orderkey predicate is the proof-backed point filter copied from orders;
	// it protects the physical index path, not just the logical SEMI shape.
	require.Len(t, aggregateScan.FilterList, 2)
	foundOrderKey := false
	for _, filter := range aggregateScan.FilterList {
		foundOrderKey = foundOrderKey || exprContainsColName(filter, "l_orderkey")
	}
	require.True(t, foundOrderKey)
}

func TestIssue28158PointPushdownIsOrderIndependent(t *testing.T) {
	whereClauses := []string{
		"o.o_orderkey = 1 AND o.o_custkey = 2",
		"o.o_custkey = 2 AND o.o_orderkey = 1",
	}
	for _, where := range whereClauses {
		logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
			SELECT o.o_orderkey,
			       (SELECT MAX(l.l_quantity)
			          FROM lineitem l
			         WHERE l.l_orderkey = o.o_orderkey)
			  FROM orders o
			 WHERE `+where)
		require.NoError(t, err)

		agg := issue28158FindAggregate(logicPlan.GetQuery(), "max")
		require.NotNil(t, agg)
		aggregateScan := issue28158FindTableScanUnder(logicPlan.GetQuery(), agg.Children[0], "lineitem")
		require.NotNil(t, aggregateScan)
		require.True(t, issue28158HasColumnFilter(aggregateScan.FilterList, "l_orderkey"))
	}
}

func TestIssue28158WhereScalarPredicateOrderIsIndependent(t *testing.T) {
	whereClauses := []string{
		`(SELECT MAX(l.l_quantity)
		    FROM lineitem l
		   WHERE l.l_orderkey = o.o_orderkey) > 0
		AND o.o_orderkey = 1
		AND o.o_custkey = 2`,
		`o.o_orderkey = 1
		AND o.o_custkey = 2
		AND (SELECT MAX(l.l_quantity)
		       FROM lineitem l
		      WHERE l.l_orderkey = o.o_orderkey) > 0`,
	}
	for _, where := range whereClauses {
		logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
			SELECT o.o_orderkey
			  FROM orders o
			 WHERE `+where)
		require.NoError(t, err)

		agg := issue28158FindAggregate(logicPlan.GetQuery(), "max")
		require.NotNil(t, agg)
		require.Len(t, agg.Children, 1)
		join := logicPlan.GetQuery().Nodes[agg.Children[0]]
		require.Equal(t, plan.Node_JOIN, join.NodeType)
		require.Equal(t, plan.Node_SEMI, join.JoinType)
		aggregateScan := issue28158FindTableScanUnder(logicPlan.GetQuery(), agg.Children[0], "lineitem")
		require.NotNil(t, aggregateScan)
		require.True(t, issue28158HasColumnFilter(aggregateScan.FilterList, "l_orderkey"))
	}
}

func TestIssue28158SkipsNonPointOuterDomains(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		SELECT n.n_nationkey,
		       (SELECT MAX(l.l_quantity)
		          FROM lineitem l
		         WHERE l.l_partkey = n.n_regionkey)
		  FROM nation n
		 WHERE n.n_regionkey = 1`)
	require.NoError(t, err)

	agg := issue28158FindAggregate(logicPlan.GetQuery(), "max")
	require.NotNil(t, agg)
	require.Len(t, agg.Children, 1)
	child := logicPlan.GetQuery().Nodes[agg.Children[0]]
	// A non-key/range-like domain would add work without the point-domain
	// index payoff; keep the existing plan until admission is cost based.
	require.NotEqual(t, plan.Node_JOIN, child.NodeType)
}

func TestIssue28158SkipsCompositeNonPointDomain(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		SELECT ps.ps_partkey,
		       (SELECT MAX(l.l_quantity)
		          FROM lineitem l
		         WHERE l.l_partkey = ps.ps_partkey
		           AND l.l_suppkey = ps.ps_suppkey)
		  FROM partsupp ps
		 WHERE ps.ps_partkey = 1`)
	require.NoError(t, err)

	agg := issue28158FindAggregate(logicPlan.GetQuery(), "max")
	require.NotNil(t, agg)
	require.Len(t, agg.Children, 1)
	child := logicPlan.GetQuery().Nodes[agg.Children[0]]
	// Only one component of partsupp's composite primary key is fixed, so this
	// is not a point domain and must not add a broad SEMI scan.
	require.NotEqual(t, plan.Node_JOIN, child.NodeType)
}

func TestIssue28158CompositePrimaryKeyProofRequiresAllKeyParts(t *testing.T) {
	binding := &Binding{
		tag:         7,
		cols:        []string{"workspace_id", "task_id"},
		colIdByName: map[string]int32{"workspace_id": 0, "task_id": 1},
	}
	scan := &plan.Node{TableDef: &plan.TableDef{Pkey: &plan.PrimaryKeyDef{
		Names: []string{"workspace_id", "task_id"},
	}}}
	filters := []*plan.Expr{
		issue28158EqualityFilter(binding.tag, 0, 1),
		issue28158EqualityFilter(binding.tag, 1, 2),
	}
	require.True(t, scalarAggregateDomainHasPrimaryKeyPointFilter(binding, scan, filters))
	require.False(t, scalarAggregateDomainHasPrimaryKeyPointFilter(binding, scan, filters[:1]))
	orFilter := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "or"},
		Args: []*plan.Expr{filters[0], filters[1]},
	}}}
	require.False(t, scalarAggregateDomainHasPrimaryKeyPointFilter(binding, scan, []*plan.Expr{orFilter}))
}

func issue28158EqualityFilter(tag, colPos int32, value int64) *plan.Expr {
	return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "="},
		Args: []*plan.Expr{
			{Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: tag, ColPos: colPos}}},
			{Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: value}}}},
		},
	}}}
}

func TestIssue28158KeepsUnsafeOrUnfilteredShapesUnchanged(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "unfiltered outer relation",
			sql: `
			SELECT o.o_orderkey,
			       (SELECT MAX(l.l_quantity)
			          FROM lineitem l
			         WHERE l.l_orderkey = o.o_orderkey)
			  FROM orders o`,
		},
		{
			name: "volatile outer predicate",
			sql: `
			SELECT o.o_orderkey,
			       (SELECT MAX(l.l_quantity)
			          FROM lineitem l
			         WHERE l.l_orderkey = o.o_orderkey)
			  FROM orders o
			 WHERE o.o_orderkey > FLOOR(RAND() * 100)`,
		},
		{
			name: "computed inner key",
			sql: `
			SELECT o.o_orderkey,
			       (SELECT MAX(l.l_quantity)
			          FROM lineitem l
			         WHERE l.l_orderkey + 1 = o.o_orderkey)
			  FROM orders o
			 WHERE o.o_orderkey = 1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, test.sql)
			require.NoError(t, err)
			agg := issue28158FindAggregate(logicPlan.GetQuery(), "max")
			require.NotNil(t, agg)
			require.Len(t, agg.Children, 1)
			child := logicPlan.GetQuery().Nodes[agg.Children[0]]
			// No safe point-domain rewrite exists in these cases, so no key-domain
			// join may be introduced.
			require.NotEqual(t, plan.Node_JOIN, child.NodeType)
		})
	}
}

func TestIssue28158CopiesOnlyDeterministicOuterConjunct(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `
		SELECT o.o_orderkey,
		       (SELECT MAX(l.l_quantity)
		          FROM lineitem l
		         WHERE l.l_orderkey = o.o_orderkey)
		  FROM orders o
		 WHERE o.o_orderkey = 1
		   AND o.o_custkey > FLOOR(RAND() * 100)`)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	agg := issue28158FindAggregate(query, "max")
	require.NotNil(t, agg)
	require.Len(t, agg.Children, 1)
	join := query.Nodes[agg.Children[0]]
	require.Equal(t, plan.Node_JOIN, join.NodeType)
	require.Equal(t, plan.Node_SEMI, join.JoinType)

	var domain *plan.Node
	for _, childID := range join.Children {
		child := query.Nodes[childID]
		if child.NodeType == plan.Node_TABLE_SCAN && child.TableDef != nil && child.TableDef.Name == "orders" {
			domain = child
			break
		}
	}
	require.NotNil(t, domain)
	require.Len(t, domain.FilterList, 1)
	require.False(t, exprContainsFuncName(domain.FilterList[0], "rand"))
	aggregateScan := issue28158FindTableScanUnder(query, agg.Children[0], "lineitem")
	require.NotNil(t, aggregateScan)
	require.Len(t, aggregateScan.FilterList, 1)
	require.False(t, exprContainsFuncName(aggregateScan.FilterList[0], "rand"))
	require.True(t, exprContainsColName(aggregateScan.FilterList[0], "l_orderkey"))
}

func issue28158FindAggregate(query *plan.Query, functionName string) *plan.Node {
	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_AGG {
			continue
		}
		for _, expr := range node.AggList {
			if exprContainsFuncName(expr, functionName) {
				return node
			}
		}
	}
	return nil
}

func issue28158FindTableScanUnder(query *plan.Query, nodeID int32, tableName string) *plan.Node {
	return issue28158FindTableScanUnderSeen(query, nodeID, tableName, make(map[int32]struct{}))
}

func issue28158FindTableScanUnderSeen(
	query *plan.Query,
	nodeID int32,
	tableName string,
	seen map[int32]struct{},
) *plan.Node {
	if query == nil || nodeID < 0 || int(nodeID) >= len(query.Nodes) {
		return nil
	}
	if _, ok := seen[nodeID]; ok {
		return nil
	}
	seen[nodeID] = struct{}{}
	node := query.Nodes[nodeID]
	if node.NodeType == plan.Node_TABLE_SCAN && node.TableDef != nil && node.TableDef.Name == tableName {
		return node
	}
	for _, childID := range node.Children {
		if scan := issue28158FindTableScanUnderSeen(query, childID, tableName, seen); scan != nil {
			return scan
		}
	}
	return nil
}

func issue28158HasColumnFilter(filters []*plan.Expr, columnName string) bool {
	for _, filter := range filters {
		if exprContainsColName(filter, columnName) {
			return true
		}
	}
	return false
}
