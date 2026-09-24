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
	"fmt"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestCorrelatedLocalCTEAggregateReaggregates(t *testing.T) {
	for _, tc := range []struct {
		name, aggregate, selected, op, wantFunction string
	}{
		{"max inclusive", "MAX(i.N_NATIONKEY) AS m", "m", "<=", "max"},
		{"min strict", "MIN(i.N_NATIONKEY) AS m", "m", "<", "min"},
		{"sum greater", "SUM(i.N_NATIONKEY) AS m", "m", ">", "sum"},
		{"count star greater inclusive", "COUNT(*) AS m", "m", ">=", "count"},
		{"count column", "COUNT(i.N_NATIONKEY) AS m", "m", "<=", "count"},
		{"second aggregate", "COUNT(*) AS c, SUM(i.N_NATIONKEY) AS m", "m", "<=", "sum"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sql := fmt.Sprintf(`SELECT o.N_NATIONKEY,
				(WITH x AS (SELECT %s FROM NATION i WHERE i.N_NATIONKEY %s o.N_NATIONKEY)
				 SELECT %s FROM x) FROM NATION o`, tc.aggregate, tc.op, tc.selected)
			logicPlan, err := runSelectWithValidator(NewMockOptimizer(true), t, sql, func(query *planpb.Query) error {
				var postJoinAgg *planpb.Node
				for _, node := range query.Nodes {
					if node.NodeType != planpb.Node_AGG || len(node.Children) != 1 {
						continue
					}
					join := query.Nodes[node.Children[0]]
					if join.NodeType == planpb.Node_JOIN && join.JoinType == planpb.Node_LEFT {
						postJoinAgg = node
						require.Len(t, join.Children, 2)
						require.Equal(t, planpb.Node_TABLE_SCAN, query.Nodes[join.Children[1]].NodeType)
						break
					}
				}
				require.NotNil(t, postJoinAgg, "scalar aggregate must run after the correlated LEFT JOIN")
				require.Len(t, postJoinAgg.AggList, 1)
				require.Equal(t, tc.wantFunction, postJoinAgg.AggList[0].GetF().Func.ObjName)
				return nil
			})
			require.NoError(t, err)
			assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
		})
	}
}

func TestCorrelatedLocalCTENonEqAggregateRejectsUnsafeShapes(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"synthetic row argument", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT SUM(1) AS m FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY)
			 SELECT m FROM x) FROM NATION o`},
		{"computed output", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY)
			 SELECT m + 1 FROM x) FROM NATION o`},
		{"grouped aggregate", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY GROUP BY i.N_REGIONKEY)
			 SELECT m FROM x) FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.Error(t, err)
		})
	}
}

func TestCorrelatedLocalCTEAggregateSiblingOutputsSurviveRemapping(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"two siblings", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY) SELECT m FROM x),
			(WITH x AS (SELECT SUM(i.N_NATIONKEY) AS s FROM NATION i WHERE i.N_NATIONKEY < o.N_NATIONKEY) SELECT s FROM x)
			FROM NATION o`},
		{"three siblings reversed", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY >= o.N_NATIONKEY) SELECT c FROM x),
			(WITH x AS (SELECT SUM(i.N_NATIONKEY) AS s FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY) SELECT s FROM x),
			(WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY > o.N_NATIONKEY) SELECT m FROM x)
			FROM NATION o`},
		{"same expression", `SELECT o.N_NATIONKEY,
			(WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT c FROM x) +
			(WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY) SELECT c FROM x)
			FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
		})
	}
}

func TestCorrelatedAggregatePreservesExistentialOuterOutputs(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"direct after exists filter", `SELECT o.N_NATIONKEY,
			(SELECT MAX(i.N_NATIONKEY) FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY)
			FROM NATION o WHERE EXISTS
			(SELECT 1 FROM NATION e WHERE e.N_NATIONKEY = o.N_NATIONKEY)`},
		{"wrapped after exists projection", `SELECT o.N_NATIONKEY,
			EXISTS(SELECT 1 FROM NATION e WHERE e.N_NATIONKEY = o.N_NATIONKEY),
			(WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i
			 WHERE i.N_NATIONKEY <= o.N_NATIONKEY) SELECT m FROM x)
			FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
		})
	}
}

func TestCorrelatedLocalCTEEqualityCountAllowsMultipleOuterBindings(t *testing.T) {
	logicPlan, err := runOneStmt(NewMockOptimizer(true), t, `SELECT o.N_NATIONKEY,
		(WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY)
		 SELECT c FROM x)
		FROM NATION o CROSS JOIN NATION p`)
	require.NoError(t, err)
	assertReachablePlanHasNoCorrelatedExpr(t, logicPlan.GetQuery())
}
