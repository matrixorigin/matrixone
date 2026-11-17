// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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

func TestIndependentWindowFusion(t *testing.T) {
	for _, tc := range []struct {
		name, sql          string
		windows, functions int
	}{
		{"same layout", `select max(o_totalprice) over w, min(o_orderkey) over w from orders
window w as (partition by o_custkey order by o_orderdate rows unbounded preceding)`, 1, 2},
		{"three outputs and post filter", `select x.b, x.a, x.c from
(select o_custkey, max(o_totalprice) over w a, min(o_orderkey) over w b,
max(o_orderkey) over w c from orders
window w as (partition by o_custkey order by o_orderdate rows unbounded preceding)) x
where x.c > x.b`, 1, 3},
		{"intermediate filter barrier", `select x.c from
(select max(o_totalprice) over w a, min(o_orderkey) over w b,
max(o_orderkey) over w c from orders
window w as (partition by o_custkey order by o_orderdate rows unbounded preceding)) x
where x.a > x.b`, 2, 3},
		{"different order", `select max(o_totalprice) over (partition by o_custkey order by o_orderdate rows unbounded preceding),
min(o_orderkey) over (partition by o_custkey order by o_orderkey rows unbounded preceding) from orders`, 2, 2},
		{"fallible aggregate unchanged", `select max(o_totalprice) over w, sum(o_totalprice) over w from orders
window w as (partition by o_custkey order by o_orderdate rows unbounded preceding)`, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := NewMockOptimizer(false, newPlanTestProcess(t))
			logic, err := runOneStmt(opt, t, tc.sql)
			require.NoError(t, err)
			query := logic.GetQuery()
			windows, functions := 0, 0
			seen := make(map[int32]bool)
			var walk func(int32)
			walk = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				node := query.Nodes[id]
				if node.NodeType == plan.Node_WINDOW {
					windows++
					functions += len(node.WinSpecList)
					inputWidth := len(query.Nodes[node.Children[0]].ProjectList)
					for _, expr := range node.ProjectList {
						col := expr.GetCol()
						require.NotNil(t, col)
						require.GreaterOrEqual(t, col.ColPos, int32(0))
						require.Less(t, int(col.ColPos), inputWidth+len(node.WinSpecList))
					}
				}
				for _, child := range node.Children {
					walk(child)
				}
			}
			for _, root := range query.Steps {
				walk(root)
			}
			require.Equal(t, tc.windows, windows)
			require.Equal(t, tc.functions, functions)
		})
	}
}
