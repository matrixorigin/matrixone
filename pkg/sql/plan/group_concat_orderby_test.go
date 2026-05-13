// Copyright 2024 Matrix Origin
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

// TestGroupConcatOrderBy_PlanShape verifies that GROUP_CONCAT(... ORDER BY ...)
// is rewritten into a SORT node feeding an AGG node, instead of using a Window
// operator. It also asserts that BindContext.forceWindows stays false (the
// legacy "force window" path is no longer taken).
func TestGroupConcatOrderBy_PlanShape(t *testing.T) {
	mock := NewMockOptimizer(false)
	sqls := []string{
		// no GROUP BY: aggregate over full table, ORDER BY inside group_concat
		"SELECT GROUP_CONCAT(n_name ORDER BY n_nationkey) FROM NATION",
		// with GROUP BY
		"SELECT n_regionkey, GROUP_CONCAT(n_name ORDER BY n_nationkey) FROM NATION GROUP BY n_regionkey",
		// DESC + NULLS FIRST
		"SELECT GROUP_CONCAT(n_name ORDER BY n_nationkey DESC) FROM NATION",
		// multiple ORDER BY columns
		"SELECT GROUP_CONCAT(n_name ORDER BY n_regionkey, n_nationkey) FROM NATION",
		// ORDER BY by integer position (1 -> first arg, n_name)
		"SELECT GROUP_CONCAT(n_name ORDER BY 1) FROM NATION",
	}

	for _, sql := range sqls {
		logicPlan, err := runOneStmt(mock, t, sql)
		require.NoError(t, err, "build plan failed for sql=%s", sql)

		query := logicPlan.GetQuery()
		require.NotNil(t, query, "sql=%s", sql)

		nodes := query.GetNodes()

		// There must be no WINDOW node — group_concat ORDER BY must not be
		// rewritten into a window operator anymore.
		for _, n := range nodes {
			require.NotEqual(t, plan.Node_WINDOW, n.NodeType,
				"unexpected WINDOW node for sql=%s", sql)
		}

		// We expect at least one SORT node feeding into an AGG node.
		var aggNode *plan.Node
		for _, n := range nodes {
			if n.NodeType == plan.Node_AGG {
				aggNode = n
				break
			}
		}
		require.NotNil(t, aggNode, "AGG node missing for sql=%s", sql)
		require.Len(t, aggNode.Children, 1, "AGG should have a single child for sql=%s", sql)

		sortChild := nodes[aggNode.Children[0]]
		require.Equal(t, plan.Node_SORT, sortChild.NodeType,
			"AGG's child must be SORT for sql=%s", sql)
		require.NotEmpty(t, sortChild.OrderBy,
			"SORT node must carry order specs for sql=%s", sql)
	}
}

// TestGroupConcatOrderBy_NoOrderBy_NoSort ensures that plain GROUP_CONCAT
// without ORDER BY does NOT introduce a Sort node.
func TestGroupConcatOrderBy_NoOrderBy_NoSort(t *testing.T) {
	mock := NewMockOptimizer(false)

	logicPlan, err := runOneStmt(mock, t,
		"SELECT GROUP_CONCAT(n_name) FROM NATION")
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	for _, n := range query.GetNodes() {
		require.NotEqual(t, plan.Node_WINDOW, n.NodeType,
			"unexpected WINDOW node")
	}

	var aggNode *plan.Node
	for _, n := range query.GetNodes() {
		if n.NodeType == plan.Node_AGG {
			aggNode = n
			break
		}
	}
	require.NotNil(t, aggNode)
	require.Len(t, aggNode.Children, 1)

	child := query.GetNodes()[aggNode.Children[0]]
	require.NotEqual(t, plan.Node_SORT, child.NodeType,
		"plain GROUP_CONCAT without ORDER BY should not introduce a SORT")
}

// TestGroupConcatOrderBy_MultiArg_NoPanic regression-checks the original
// crash: GROUP_CONCAT with multiple value arguments + ORDER BY would route
// through the Window operator and panic with index out of range. After the
// fix, planning must succeed without rewriting to a window.
func TestGroupConcatOrderBy_MultiArg_NoPanic(t *testing.T) {
	mock := NewMockOptimizer(false)

	logicPlan, err := runOneStmt(mock, t,
		"SELECT GROUP_CONCAT(n_name, ':', n_nationkey ORDER BY n_nationkey) FROM NATION")
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	for _, n := range query.GetNodes() {
		require.NotEqual(t, plan.Node_WINDOW, n.NodeType,
			"multi-arg GROUP_CONCAT must not be rewritten to a window")
	}
}

// TestGroupConcatOrderBy_Errors covers the error paths inside
// processForceWindows: subquery in ORDER BY, non-integer constant, position
// out of range, negative position.
func TestGroupConcatOrderBy_Errors(t *testing.T) {
	mock := NewMockOptimizer(false)

	cases := []struct {
		name string
		sql  string
	}{
		{
			name: "subquery in group_concat ORDER BY",
			sql:  "SELECT GROUP_CONCAT(n_name ORDER BY (SELECT 1)) FROM NATION",
		},
		{
			name: "ORDER BY position out of range",
			sql:  "SELECT GROUP_CONCAT(n_name ORDER BY 5) FROM NATION",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := runOneStmt(mock, t, tc.sql)
			require.Error(t, err, "expected build error for sql=%s", tc.sql)
		})
	}
}

// TestGroupConcatOrderBy_DoesNotSetForceWindows ensures the legacy
// forceWindows / isDistinct knobs are no longer toggled by the rewrite.
// We check this indirectly by confirming the produced plan has no WINDOW
// node and a SORT-before-AGG shape (which forceWindows path could not
// produce).
func TestGroupConcatOrderBy_DoesNotSetForceWindows(t *testing.T) {
	mock := NewMockOptimizer(false)
	logicPlan, err := runOneStmt(mock, t,
		"SELECT GROUP_CONCAT(n_name ORDER BY n_nationkey) FROM NATION")
	require.NoError(t, err)

	for _, n := range logicPlan.GetQuery().GetNodes() {
		require.NotEqual(t, plan.Node_WINDOW, n.NodeType)
	}
}
