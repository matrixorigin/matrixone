// Copyright 2023 Matrix Origin
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

func findWindowNodeWithFilter(t *testing.T, query *plan.Query, name string) *plan.Node {
	t.Helper()

	for _, node := range query.Nodes {
		if node.NodeType != plan.Node_WINDOW || len(node.FilterList) == 0 {
			continue
		}

		filterFn := node.FilterList[0].GetF()
		if filterFn == nil || len(filterFn.Args) == 0 {
			continue
		}

		filterCol := filterFn.Args[0].GetCol()
		if filterCol == nil {
			continue
		}

		if name == "" || filterCol.Name == name || containsIgnoreCase(filterCol.Name, name) {
			return node
		}
	}

	return nil
}

func findProjectedColByName(t *testing.T, node *plan.Node, name string) *plan.ColRef {
	t.Helper()

	for _, expr := range node.ProjectList {
		col := expr.GetCol()
		if col == nil {
			continue
		}
		if col.Name == name || containsIgnoreCase(col.Name, name) {
			return col
		}
	}

	return nil
}

func findChildProjectedColByName(t *testing.T, query *plan.Query, node *plan.Node, name string) *plan.ColRef {
	t.Helper()

	if len(node.Children) == 0 {
		return nil
	}

	child := query.Nodes[node.Children[0]]
	for _, expr := range child.ProjectList {
		col := expr.GetCol()
		if col == nil {
			continue
		}
		if col.Name == name || containsIgnoreCase(col.Name, name) {
			return col
		}
	}

	return nil
}

func containsIgnoreCase(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	if len(s) < len(sub) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if equalFoldASCII(s[i:i+len(sub)], sub) {
			return true
		}
	}
	return false
}

func equalFoldASCII(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ai := a[i]
		bi := b[i]
		if 'A' <= ai && ai <= 'Z' {
			ai += 'a' - 'A'
		}
		if 'A' <= bi && bi <= 'Z' {
			bi += 'a' - 'A'
		}
		if ai != bi {
			return false
		}
	}
	return true
}

// Regression coverage for issue #23882.
func TestStackedWindowFilterKeepsPriorWindowSlot(t *testing.T) {
	opt := NewMockOptimizer(false)
	sql := `
SELECT x.o_custkey, x.second_cum_totalprice
FROM (
    SELECT o_custkey,
           ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn,
           SUM(o_totalprice) OVER (
               PARTITION BY o_custkey
               ORDER BY o_orderdate
               ROWS UNBOUNDED PRECEDING
           ) AS second_cum_totalprice
    FROM orders
    WHERE YEAR(o_orderdate) = 1994
) AS x
WHERE x.rn = 2;
`

	logicPlan, err := runOneStmt(opt, t, sql)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	target := findWindowNodeWithFilter(t, query, "ROW_NUMBER()")
	require.NotNil(t, target)
	require.Equal(t, int32(1), target.WindowIdx)

	filterFn := target.FilterList[0].GetF()
	require.NotNil(t, filterFn)
	filterCol := filterFn.Args[0].GetCol()
	require.NotNil(t, filterCol)
	require.Contains(t, filterCol.Name, "ROW_NUMBER()")

	projectCol := findChildProjectedColByName(t, query, target, "ROW_NUMBER()")
	require.NotNil(t, projectCol)
	require.Equal(t, projectCol.ColPos, filterCol.ColPos)
}

// Regression coverage for issue #23882.
func TestCurrentWindowFilterUsesProjectedSlot(t *testing.T) {
	opt := NewMockOptimizer(false)
	sql := `
SELECT x.o_custkey, x.second_cum_totalprice
FROM (
    SELECT o_custkey,
           ROW_NUMBER() OVER (PARTITION BY o_custkey ORDER BY o_orderdate) AS rn,
           SUM(o_totalprice) OVER (
               PARTITION BY o_custkey
               ORDER BY o_orderdate
               ROWS UNBOUNDED PRECEDING
           ) AS second_cum_totalprice
    FROM orders
    WHERE YEAR(o_orderdate) = 1994
) AS x
WHERE x.second_cum_totalprice > 0;
`

	logicPlan, err := runOneStmt(opt, t, sql)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	target := findWindowNodeWithFilter(t, query, "SUM(")
	require.NotNil(t, target)
	require.Equal(t, int32(1), target.WindowIdx)

	filterFn := target.FilterList[0].GetF()
	require.NotNil(t, filterFn)
	filterCol := filterFn.Args[0].GetCol()
	require.NotNil(t, filterCol)
	require.Contains(t, filterCol.Name, "SUM(")

	projectCol := findProjectedColByName(t, target, "SUM(")
	require.NotNil(t, projectCol)
	require.Equal(t, projectCol.ColPos, filterCol.ColPos)
}
