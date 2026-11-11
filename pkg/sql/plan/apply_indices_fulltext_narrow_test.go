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

// The base table scan joined back to a fulltext search reads only the candidate rows;
// the scan copied from it to filter the whole table does not.
func TestFullTextJoinBackScanIsNarrow(t *testing.T) {
	for _, algo := range []struct {
		name string
		new  func(testing.TB) *issue24822Optimizer
	}{
		{"fulltext", newIssue24822Optimizer},
		{"fulltext2", newIssue24822FullText2Optimizer},
	} {
		for _, tc := range []struct {
			name     string
			residual string
			pushdown int8
			narrow   int
			whole    int
		}{
			{name: "no filter", narrow: 1},
			{name: "residual filter", residual: " AND title <> 'excluded'", narrow: 1},
			{name: "residual filter with membership", residual: " AND title <> 'excluded'", pushdown: 1, narrow: 1, whole: 1},
		} {
			t.Run(algo.name+"/"+tc.name, func(t *testing.T) {
				optimizer := algo.new(t)
				optimizer.ctx.fulltextBloomFilterPushdown = tc.pushdown
				p, err := runOneStmt(optimizer, t, `SELECT ft.id, ft.title FROM ft
					WHERE MATCH(title, body) AGAINST('hello' IN BOOLEAN MODE)`+tc.residual+" LIMIT 2")
				require.NoError(t, err)
				query := p.GetQuery()
				joined := make(map[int32]bool)
				for _, n := range query.Nodes {
					if n.NodeType == plan.Node_JOIN {
						joined[n.Children[0]] = true
					}
				}
				narrow, whole := 0, 0
				for _, n := range query.Nodes {
					if n.NodeType != plan.Node_TABLE_SCAN || n.TableDef.GetName() != "ft" {
						continue
					}
					if n.NarrowScan {
						require.True(t, joined[n.NodeId], "a narrow scan is the row fetch of a join")
						narrow++
					} else {
						whole++
					}
				}
				require.Equal(t, tc.narrow, narrow)
				require.Equal(t, tc.whole, whole)
			})
		}
	}
}
