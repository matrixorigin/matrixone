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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestUnionAllFilteredConstantBranchKeepsRightColumn(t *testing.T) {
	mock := NewMockOptimizer(false, newPlanTestProcess(t))
	for _, test := range []struct {
		name       string
		sql        string
		wantNumber int64
		wantString string
		wantNull   bool
	}{
		{name: "aggregate", sql: "SELECT SUM(eid) FROM (SELECT 1 AS point, 10 AS eid UNION ALL SELECT 2, 20) u WHERE point > 0", wantNumber: 20},
		{name: "projection", sql: "SELECT eid FROM (SELECT 1 AS point, 10 AS eid UNION ALL SELECT 2, 20) u WHERE point > 0", wantNumber: 20},
		{name: "selective filter", sql: "SELECT SUM(eid) FROM (SELECT 1 AS point, 10 AS eid UNION ALL SELECT 2, 20) u WHERE point > 1", wantNumber: 20},
		{name: "string", sql: "SELECT v FROM (SELECT 1 AS k, 'ten' AS v UNION ALL SELECT 2, 'twenty') u WHERE k > 0", wantString: "twenty"},
		{name: "null", sql: "SELECT v FROM (SELECT 1 AS k, 10 AS v UNION ALL SELECT 2, NULL) u WHERE k > 0", wantNull: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			logicPlan, err := runOneStmt(mock, t, test.sql)
			require.NoError(t, err)

			nodes := logicPlan.GetQuery().Nodes
			for _, node := range nodes {
				if node.NodeType != plan.Node_UNION_ALL {
					continue
				}
				require.Len(t, node.Children, 2)
				require.Len(t, node.ProjectList, 1)

				rightID := node.Children[1]
				for {
					right := nodes[rightID]
					if right.NodeType == plan.Node_PROJECT {
						require.Len(t, right.ProjectList, 1)
						literal := right.ProjectList[0].GetLit()
						require.NotNil(t, literal)
						switch {
						case test.wantNull:
							require.True(t, literal.GetIsnull())
						case test.wantString != "":
							require.Equal(t, test.wantString, literal.GetSval())
						default:
							require.Equal(t, test.wantNumber, literal.GetI64Val())
						}
						return
					}
					require.Len(t, right.Children, 1, "right set-operation branch has no project")
					rightID = right.Children[0]
				}
			}
			t.Fatal("plan has no UNION ALL")
		})
	}
}
