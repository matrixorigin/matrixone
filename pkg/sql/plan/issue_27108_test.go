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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestIssue27108UnionAllKeepsConstantFalseAboveGlobalAgg(t *testing.T) {
	stmt, err := mysql.ParseOne(t.Context(), `
		SELECT check_id, status, check_id <> 'excluded' AS should_block
		FROM (
			SELECT
				'bronze' AS gate_name,
				'keep' AS check_id,
				CASE WHEN COUNT(*) > 0 THEN 'PASS' ELSE 'FAIL' END AS status
			FROM (SELECT 1 AS n) t
			UNION ALL
			SELECT
				'bronze',
				'excluded',
				CASE WHEN COUNT(*) >= 2 THEN 'PASS' ELSE 'FAIL' END
			FROM (SELECT 1 AS n) t
		) v
		WHERE status = 'FAIL'
		  AND check_id <> 'excluded'`, 1)
	require.NoError(t, err)

	query, err := NewBaseOptimizer(NewMockCompilerContext(true)).Optimize(stmt, false)
	require.NoError(t, err)

	var union *planpb.Node
	for _, node := range query.Nodes {
		if node.NodeType == planpb.Node_UNION_ALL {
			union = node
			break
		}
	}
	require.NotNil(t, union)

	rightAgg := issue27108FindNode(query, union.Children[1], planpb.Node_AGG)
	require.NotNil(t, rightAgg)

	hasFalseFilter := false
	for _, filter := range rightAgg.FilterList {
		hasFalseFilter = hasFalseFilter || IsFalseExpr(filter)
	}
	require.True(t, hasFalseFilter,
		"the branch-local false predicate must filter the global aggregate result")
}

func issue27108FindNode(query *planpb.Query, nodeID int32, nodeType planpb.Node_NodeType) *planpb.Node {
	node := query.Nodes[nodeID]
	if node.NodeType == nodeType {
		return node
	}
	for _, childID := range node.Children {
		if found := issue27108FindNode(query, childID, nodeType); found != nil {
			return found
		}
	}
	return nil
}
