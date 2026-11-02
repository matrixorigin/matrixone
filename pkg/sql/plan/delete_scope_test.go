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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIsUnrestrictedDelete(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		targetCount int
		want        bool
	}{
		{name: "plain", sql: "delete from t", targetCount: 1, want: true},
		{name: "order only", sql: "delete from t order by a", targetCount: 1, want: true},
		{name: "where", sql: "delete from t where true", targetCount: 1},
		{name: "limit zero", sql: "delete from t limit 0", targetCount: 1},
		{name: "join without where", sql: "delete t from t join s on t.a = s.a", targetCount: 1},
		{name: "using without where", sql: "delete from t using t join s on t.a = s.a", targetCount: 1},
		{name: "partition", sql: "delete from t partition (p0)", targetCount: 1},
		{name: "multiple targets", sql: "delete t, s from t join s on t.a = s.a", targetCount: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			deleteStmt, ok := stmt.(*tree.Delete)
			require.True(t, ok)
			require.Equal(t, test.want, isUnrestrictedDelete(deleteStmt, test.targetCount))
		})
	}

	require.False(t, isUnrestrictedDelete(nil, 1))
}

func TestDeleteCTETargetScope(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		for _, tc := range []struct {
			name, sql string
			wantError string
		}{
			{"alias shadows CTE", "with x as (select 1 as id) delete x from nation as x where x.n_nationkey=1", ""},
			{"joined source without WHERE", "with x as (select 99 as id), q as (select 1 as id) delete x from nation as x join q on x.n_nationkey=q.id", ""},
			{"apply source", "delete nation from nation cross apply generate_series(nation.n_nationkey,nation.n_nationkey) as g where nation.n_nationkey=1", ""},
			{"qualified explicit alias baseline", "delete ignored.x from nation as x where x.n_nationkey=1", ""},
			{"qualified explicit alias with CTE", "with x as (select 99 as id) delete ignored.x from nation as x where x.n_nationkey=1", ""},
			{"qualified explicit alias same spelling baseline", "delete ignored.nation from nation as nation where nation.n_nationkey=1", ""},
			{"qualified explicit alias same spelling with CTE", "with nation as (select 99 as id) delete ignored.nation from tpch.nation as nation where nation.n_nationkey=1", ""},
			{"unrelated CTE", "with q as (select 1 as id) delete x from nation as x where x.n_nationkey=1", ""},
			{"qualified source shadows CTE", "with nation as (select 1 as id) delete x from tpch.nation as x where x.n_nationkey=1", ""},
			{"qualified unaliased source", "with nation as (select 1 as id) delete nation from tpch.nation where nation.n_nationkey=1", ""},
			{"qualified target", "with nation as (select 1 as id) delete from tpch.nation where n_nationkey=1", ""},
			{"CTE target", "with nation as (select 1 as n_nationkey) delete from nation where n_nationkey=1", "cannot delete from CTE"},
			{"aliased CTE target", "with nation as (select 1 as n_nationkey) delete x from nation as x where x.n_nationkey=1", "cannot delete from CTE"},
			{"qualified target mismatches source", "with q as (select 1) delete other.nation from tpch.nation where nation.n_nationkey=1", "does not match its source"},
			{"qualified target reads CTE", "with nation as (select 1 as n_nationkey) delete tpch.nation from nation where nation.n_nationkey=1", "cannot delete from CTE"},
			{"target missing from USING", "delete from nation using region", "missing FROM-clause entry for table 'nation'"},
			{"target hidden by alias", "delete nation from nation as x where x.n_nationkey=1", "missing FROM-clause entry for table 'nation'"},
			{"second target missing", "delete x, missing from nation as x join region as r on x.n_regionkey=r.r_regionkey", "missing FROM-clause entry for table 'missing'"},
			{"derived source", "delete nation from (select 1 as n_nationkey) as nation", "missing FROM-clause entry for table 'nation'"},
		} {
			route := "modern/"
			if legacy {
				route = "legacy/"
			}
			t.Run(route+tc.name, func(t *testing.T) {
				mock := NewMockOptimizer(true)
				stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
				require.NoError(t, err)
				defer stmt.Free()
				var p *Plan
				if legacy {
					p, err = buildDelete(stmt.(*tree.Delete), mock.CurrentContext(), false)
				} else {
					p, err = BuildPlan(mock.CurrentContext(), stmt, false)
				}
				if tc.wantError != "" {
					require.ErrorContains(t, err, tc.wantError)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
					return
				}
				require.NoError(t, err)
				var targets []string
				for _, node := range p.GetQuery().Nodes {
					if node.NodeType == planpb.Node_DELETE {
						require.False(t, node.DeleteCtx.CanTruncate, "a filtered source must not truncate the target")
						targets = append(targets, node.DeleteCtx.TableDef.Name)
					}
					for _, update := range node.UpdateCtxList {
						targets = append(targets, update.TableDef.Name)
					}
				}
				require.Equal(t, []string{"nation"}, targets)
			})
		}
	}
}
