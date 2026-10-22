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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/internal/materialized"
	"github.com/stretchr/testify/require"
)

func TestCTEIdentityPublication(t *testing.T) {
	for _, tc := range []struct {
		name, sql string
		readers   int
		recursive bool
	}{
		{"singleton", "with r as(select uuid()x) select a.x,b.x from r a join r b on a.x=b.x", 2, false},
		{"three readers", "with r as(select rand()x) select a.x,b.x,c.x from r a,r b,r c", 3, false},
		{"predicate", "with r as(select n_nationkey k from nation where rand()>0.5) select a.k,b.k from r a,r b", 2, false},
		{"ordering", "with r as(select n_nationkey k from nation order by rand() limit 2) select a.k,b.k from r a,r b", 2, false},
		{"recursive", "with recursive r(n) as(select 1 union all select n+1 from r where n<3) select a.n,b.n from r a,r b", 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			setPlanTestGlobalVariable(t, mock.CurrentContext().GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
			stmt, err := mysql.ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			checked := false
			_, err = bindAndOptimizeSelectQueryWithValidator(planpb.Query_SELECT, mock.CurrentContext(), stmt.(*tree.Select), false, false, func(q *Query) error {
				checked = true
				var sources []int32
				for step, id := range q.Steps {
					if q.Nodes[id].ExtraOptions == materialized.CTESinkOption {
						sources = append(sources, int32(step))
						require.False(t, q.Nodes[id].RecursiveSink)
						require.False(t, q.Nodes[id].RecursiveCte)
					}
				}
				require.Len(t, sources, 1, "semantic publication is independent of optional reuse")
				tags := make(map[int32]bool)
				for id := range cteReachablePlanNodes(q) {
					n := q.Nodes[id]
					if n.NodeType == planpb.Node_SINK_SCAN && len(n.SourceStep) == 1 && n.SourceStep[0] == sources[0] {
						require.Len(t, n.BindingTags, 1)
						require.False(t, tags[n.BindingTags[0]], "each consumer needs an independent binding")
						tags[n.BindingTags[0]] = true
					}
				}
				require.Len(t, tags, tc.readers)
				if tc.recursive {
					require.Equal(t, 1, countReachableNodeType(q, planpb.Node_RECURSIVE_CTE))
					require.Equal(t, 1, countReachableNodeType(q, planpb.Node_RECURSIVE_SCAN))
				}
				return nil
			})
			require.NoError(t, err)
			require.True(t, checked)
		})
	}
}

func TestCTEIdentityStableControls(t *testing.T) {
	for _, sql := range []string{
		"with r as(select now()x) select a.x,b.x from r a,r b",
		"with r as(select cast(n_name as integer)x from nation) select a.x,b.x from r a,r b",
		"with unused as(select rand(),missing_column from nation) select 1",
	} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		setPlanTestGlobalVariable(t, mock.CurrentContext().GetProcess().GetService(), "optimizer_hints", "sharedComputation=1")
		logic, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, sql)
		require.Zero(t, countReachableNodeType(logic.GetQuery(), planpb.Node_SINK_SCAN), sql)
	}
}

func TestCTEIdentityInvocationOwnership(t *testing.T) {
	for _, tc := range []struct{ name, sql, wantError string }{
		{"outer capture", `with r as(select uuid()x) select (select max(x) from r group by t2.a) from bvt_test2.t2`, "shared CTE capture across dependent invocations"},
		{"local declaration", `select (with r as(select uuid()x) select max(x) from r group by t2.a) from bvt_test2.t2`, ""},
		{"round capture", `select (with recursive base as(select uuid()x),r(n,x) as(select 1,x from base union all select n+1,base.x from r join base on r.x=base.x where n<t2.a) select count(*) from r) from bvt_test2.t2`, "shared CTE capture across recursive rounds"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			_, err := runOneStmt(mock, t, tc.sql)
			if tc.wantError != "" {
				require.ErrorContains(t, err, tc.wantError)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCTEIdentityCachedReaderLocking(t *testing.T) {
	for _, q := range []string{
		"with recursive r(n) as(select 1 union all select n+1 from r where n<3) select a.n from(select n from r)a join r b on a.n=b.n for update",
		"with r as(select n_nationkey k,uuid()x from nation) select a.k from(select k from r)a join r b on a.k=b.k for update",
	} {
		mock := NewMockOptimizer(false, newPlanTestProcess(t))
		_, err := runOneStmt(mock, t, q)
		require.ErrorContains(t, err, "FOR UPDATE on a materialized CTE", q)
	}
}
