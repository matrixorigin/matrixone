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

// The scan a DEDUP join checks keys against is narrow, for the primary key and unique
// indexes of INSERT, UPDATE and REPLACE; every other scan, such as the source of an
// INSERT ... SELECT, is not.
func TestDedupTargetScanIsNarrow(t *testing.T) {
	for _, sql := range []string{
		"insert into constraint_test.emp values (1, 'a', 'b', 1, '2020-01-01', 1.0, 1.0, 1)",
		"insert into constraint_test.emp select * from constraint_test.emp",
		"update constraint_test.emp set empno = empno + 1",
		"update constraint_test.emp set ename = 'x'",
		"replace into constraint_test.emp values (1, 'a', 'b', 1, '2020-01-01', 1.0, 1.0, 1)",
	} {
		t.Run(sql, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, sql)
			require.NoError(t, err)
			query := p.GetQuery()
			target := make(map[int32]bool)
			dedups := 0
			for _, n := range query.Nodes {
				if n.NodeType == plan.Node_JOIN && n.JoinType == plan.Node_DEDUP {
					dedups++
					for _, c := range n.Children {
						if query.Nodes[c].NodeType == plan.Node_TABLE_SCAN && query.Nodes[c].NarrowScan {
							target[c] = true
						}
					}
				}
			}
			require.NotZero(t, dedups)
			require.NotEmpty(t, target, "each DEDUP join's target scan is narrow")
			for i, n := range query.Nodes {
				if n.NodeType == plan.Node_TABLE_SCAN && !target[int32(i)] {
					require.False(t, n.NarrowScan, "scan of %s outside a DEDUP join", n.TableDef.GetName())
				}
			}
		})
	}
}
