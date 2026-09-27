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

func TestCorrelatedLocalCTEImplicitAggregateSpine(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"computed count", `SELECT o.N_NATIONKEY, (WITH x AS (SELECT COUNT(*) AS c FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY) SELECT c + 1 FROM x) FROM NATION o`},
		{"nested count star", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT COUNT(*) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"nested count value", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT COUNT(m) AS c FROM a) SELECT c FROM x) FROM NATION o`},
		{"nested widened sum", `SELECT o.N_NATIONKEY, (WITH a AS (SELECT MAX(i.N_NATIONKEY) AS m FROM NATION i WHERE i.N_NATIONKEY = o.N_NATIONKEY), x AS (SELECT SUM(m) AS s FROM a) SELECT s FROM x) FROM NATION o`},
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(NewMockOptimizer(true), t, tc.sql)
			require.ErrorContains(t, err, "correlated aggregate spine")
		})
	}
}
