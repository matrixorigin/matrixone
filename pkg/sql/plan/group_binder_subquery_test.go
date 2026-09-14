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
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestBuildPlanFlattensUncorrelatedScalarGroupKeys(t *testing.T) {
	for _, test := range []struct {
		name     string
		sql      string
		wantJoin bool
	}{
		{
			name: "literal scalar",
			sql:  "select (select 1), count(*) from tpch.nation group by (select 1)",
		},
		{
			name:     "aggregate scalar",
			wantJoin: true,
			sql: "select (select max(r_regionkey) from tpch.region), count(*) " +
				"from tpch.nation group by (select max(r_regionkey) from tpch.region)",
		},
		{
			name:     "alias",
			wantJoin: true,
			sql: "select (select max(r_regionkey) from tpch.region) as k, count(*) " +
				"from tpch.nation group by k",
		},
		{
			name:     "ordinal",
			wantJoin: true,
			sql: "select (select max(r_regionkey) from tpch.region) as k, count(*) " +
				"from tpch.nation group by 1",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, test.sql)
			require.NoError(t, err)
			assertScalarGroupKeysFlattened(t, logicPlan.GetQuery(), test.wantJoin)
		})
	}
}

func TestBuildPreparedPlanFlattensUncorrelatedScalarGroupKey(t *testing.T) {
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL,
		"select (select max(r_regionkey) from tpch.region where r_regionkey <= ?), count(*) "+
			"from tpch.nation group by (select max(r_regionkey) from tpch.region where r_regionkey <= ?)", 1)
	require.NoError(t, err)
	defer stmt.Free()

	logicPlan, err := BuildPlan(NewMockCompilerContext(true), stmt, true)
	require.NoError(t, err)
	assertScalarGroupKeysFlattened(t, logicPlan.GetQuery(), true)
}

func TestBuildPlanKeepsUnsupportedGroupSubqueriesFailClosed(t *testing.T) {
	for _, test := range []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name: "correlated scalar",
			sql: "select count(*) from tpch.nation n group by " +
				"(select max(r.r_regionkey) from tpch.region r where r.r_regionkey = n.n_regionkey)",
			wantErr: "correlated subquery in GROUP BY clause",
		},
		{
			name: "correlated group expression",
			sql: "select n.n_nationkey, " +
				"(select count(*) from tpch.region r group by n.n_nationkey) from tpch.nation n",
			wantErr: "correlated columns in GROUP BY clause",
		},
		{
			name: "multiple columns",
			sql: "select count(*) from tpch.nation group by " +
				"(select r_regionkey, r_name from tpch.region limit 1)",
			wantErr: "subquery returns more than 1 column",
		},
		{
			name:    "existential",
			sql:     "select count(*) from tpch.nation group by exists(select 1 from tpch.region)",
			wantErr: "subquery in GROUP BY clause",
		},
		{
			name: "rollup",
			sql: "select (select 1), count(*) from tpch.nation " +
				"group by (select 1) with rollup",
			wantErr: "subquery in GROUP BY clause",
		},
		{
			name: "sample",
			sql: "select (select 1), sample(n_regionkey, 1 rows) from tpch.nation " +
				"group by (select 1)",
			wantErr: "subquery in GROUP BY with SAMPLE",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, test.sql)
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func assertScalarGroupKeysFlattened(t *testing.T, query *planpb.Query, wantJoin bool) {
	t.Helper()
	require.NotNil(t, query)
	foundAggregate := false
	foundJoinBelowAggregate := false
	var subtreeContainsJoin func(int32, map[int32]struct{}) bool
	subtreeContainsJoin = func(nodeID int32, visited map[int32]struct{}) bool {
		if nodeID < 0 || int(nodeID) >= len(query.Nodes) {
			return false
		}
		if _, ok := visited[nodeID]; ok {
			return false
		}
		visited[nodeID] = struct{}{}
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_JOIN {
			return true
		}
		for _, child := range node.Children {
			if subtreeContainsJoin(child, visited) {
				return true
			}
		}
		return false
	}
	visited := make(map[int32]struct{})
	var visit func(int32)
	visit = func(nodeID int32) {
		if _, ok := visited[nodeID]; ok {
			return
		}
		visited[nodeID] = struct{}{}
		require.GreaterOrEqual(t, nodeID, int32(0))
		require.Less(t, int(nodeID), len(query.Nodes))
		node := query.Nodes[nodeID]
		if node.NodeType == planpb.Node_AGG && len(node.GroupBy) > 0 {
			foundAggregate = true
			for _, child := range node.Children {
				foundJoinBelowAggregate = foundJoinBelowAggregate ||
					subtreeContainsJoin(child, make(map[int32]struct{}))
			}
		}
		for _, group := range node.GroupBy {
			require.False(t, hasSubquery(group), "reachable aggregate retains a scalar subquery")
			require.False(t, hasCorrCol(group), "reachable aggregate retains a correlated group key")
		}
		for _, child := range node.Children {
			visit(child)
		}
	}
	for _, step := range query.Steps {
		visit(step)
	}
	require.True(t, foundAggregate)
	require.Equal(t, wantJoin, foundJoinBelowAggregate)
}
