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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/aggexec"
	"github.com/stretchr/testify/require"
)

func TestCorrelatedLocalCTEImplicitAggregateSpine(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"computed count", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT c + 1 FROM x) FROM NATION o`},
		{"nested count star", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"nested count value", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT COUNT(m) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"nested computed sum", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT SUM(COALESCE(m, 7) + 1) AS s FROM a) SELECT s FROM x) FROM NATION o`},
		{"triple nesting", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT COUNT(m) AS c FROM a), y AS (SELECT SUM(c) AS s FROM x) SELECT s FROM y) FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			query := p.GetQuery()
			assertReachablePlanHasNoCorrelatedExpr(t, query)
			aggCount := 0
			for id := range reachablePlanNodes(query) {
				if query.Nodes[id].NodeType == plan.Node_AGG {
					aggCount++
				}
			}
			require.Equal(t, 1, aggCount, "upper singleton aggregates should not execute")
		})
	}
}

func TestCorrelatedLocalCTEImplicitAggregateSpineRejectsRowRemoval(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"having", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING MAX(i.N_NATIONKEY) > 0), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"grouped", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY GROUP BY i.N_REGIONKEY), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"non equality", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY <= o.N_NATIONKEY), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"or equalities", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY OR i.N_NATIONKEY = o.N_NATIONKEY + 1), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"like", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NAME LIKE o.N_NAME), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"empty retaining having", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING COUNT(*) >= 0) SELECT c FROM x) FROM NATION o`},
		{"retaining null then count", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING MAX(i.N_NATIONKEY) IS NULL) SELECT COUNT(*) FROM x) FROM NATION o`},
		{"volatile filter", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT m FROM x WHERE RAND() > 0.5) FROM NATION o`},
		{"grouped count nullif limit", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY GROUP BY i.N_NATIONKEY) SELECT NULLIF(COUNT(*), 0) FROM x LIMIT 1) FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			want := "correlated aggregate spine"
			switch tc.name {
			case "non equality", "or equalities", "like":
				want = "nested non-equality correlated aggregate"
			}
			require.ErrorContains(t, err, want)
		})
	}
	_, err := runOneStmt(NewMockOptimizer(true), t,
		`SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT AVG(c) FROM x) FROM NATION o`)
	require.ErrorContains(t, err, "unsupported singleton aggregate")
}

func TestCorrelatedLocalCTENonEqualityMaxUsesRawRows(t *testing.T) {
	for _, sql := range []string{
		`SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i
		 WHERE i.N_NATIONKEY > o.N_NATIONKEY) SELECT m FROM x) FROM NATION o`,
		`SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i
		 WHERE i.N_NATIONKEY = o.N_NATIONKEY OR i.N_NATIONKEY = o.N_NATIONKEY + 1)
		 SELECT m FROM x) FROM NATION o`,
	} {
		p, err := runOneStmt(NewMockOptimizer(true), t, sql)
		require.NoError(t, err)
		assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
	}
}

func TestCorrelatedLocalCTEPreservesExistingScalarBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"grouped", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY GROUP BY i.N_NATIONKEY) SELECT c FROM x) FROM NATION o`},
		{"rejecting having max", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING MAX(i.N_NATIONKEY) > 0) SELECT m FROM x) FROM NATION o`},
		{"rejecting having count", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING COUNT(*) > 0) SELECT c FROM x) FROM NATION o`},
		{"rejecting projected filter", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), y AS (SELECT m FROM x WHERE m > 0) SELECT m FROM y) FROM NATION o`},
		{"zero limit", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT m FROM x LIMIT 0) FROM NATION o`},
		{"retaining null having", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING MAX(i.N_NATIONKEY) IS NULL) SELECT m FROM x) FROM NATION o`},
		{"one limit", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT m FROM x LIMIT 1) FROM NATION o`},
		{"one row order", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT m FROM x ORDER BY m) FROM NATION o`},
		{"nested avg null", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT AVG(m) FROM x) FROM NATION o`},
		{"nested widened sum null", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT SUM(m) AS s FROM a) SELECT s FROM x) FROM NATION o`},
		{"grouped lower max", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY GROUP BY i.N_NATIONKEY) SELECT MAX(m) FROM x) FROM NATION o`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.NoError(t, err)
			assertReachablePlanHasNoCorrelatedExpr(t, p.GetQuery())
		})
	}
}

func TestCorrelatedLocalCTEPreparedNullableFilter(t *testing.T) {
	for _, sql := range []string{
		`PREPARE ps_max_filter FROM 'SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT m FROM x WHERE m > ?) FROM NATION o'`,
		`PREPARE ps_max_having FROM 'SELECT o.N_NATIONKEY, (WITH x AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY HAVING MAX(i.N_NATIONKEY) IS NULL OR ? = 1) SELECT m FROM x) FROM NATION o'`,
	} {
		_, err := runOneStmt(NewMockOptimizer(true), t, sql)
		require.NoError(t, err)
	}
}

func TestCorrelatedLocalCTEPreservationProofDepth(t *testing.T) {
	nodes := make([]*plan.Node, 34)
	for i := 0; i < 31; i++ {
		nodes[i] = &plan.Node{NodeId: int32(i), NodeType: plan.Node_FILTER, Children: []int32{int32(i + 1)}}
	}
	nodes[31] = &plan.Node{
		NodeId: 31, NodeType: plan.Node_AGG, Children: []int32{32}, BindingTags: []int32{1, 2},
		AggList: []*plan.Expr{{Typ: makePlan2Int64ConstExprWithType(0).Typ,
			Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: aggexec.AggIdOfMax}}}}},
	}
	nodes[32] = &plan.Node{NodeId: 32, NodeType: plan.Node_AGG, Children: []int32{33}}
	nodes[33] = &plan.Node{NodeId: 33, NodeType: plan.Node_TABLE_SCAN}
	builder := &QueryBuilder{qry: &plan.Query{Nodes: nodes}}
	require.False(t, builder.aggregateSpineLegacySafe(0, &BindContext{results: []*plan.Expr{{}}}))
}
