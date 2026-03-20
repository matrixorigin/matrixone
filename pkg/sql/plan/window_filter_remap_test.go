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

func findWindowNodeWithFilter(t *testing.T, query *plan.Query) *plan.Node {
	t.Helper()

	for _, node := range query.Nodes {
		if node.NodeType == plan.Node_WINDOW && len(node.FilterList) > 0 {
			return node
		}
	}

	return nil
}

func findFilterColRef(expr *plan.Expr, pred func(*plan.ColRef) bool) *plan.ColRef {
	switch exprImpl := expr.Expr.(type) {
	case *plan.Expr_Col:
		if pred(exprImpl.Col) {
			return exprImpl.Col
		}
	case *plan.Expr_F:
		for _, arg := range exprImpl.F.Args {
			if col := findFilterColRef(arg, pred); col != nil {
				return col
			}
		}
	}
	return nil
}

func lastProjectedCol(t *testing.T, node *plan.Node) *plan.ColRef {
	t.Helper()

	require.NotEmpty(t, node.ProjectList)
	col := node.ProjectList[len(node.ProjectList)-1].GetCol()
	require.NotNil(t, col)
	return col
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

	target := findWindowNodeWithFilter(t, query)
	require.NotNil(t, target)
	require.Equal(t, int32(1), target.WindowIdx)

	filterFn := target.FilterList[0].GetF()
	require.NotNil(t, filterFn)
	filterCol := findFilterColRef(target.FilterList[0], func(col *plan.ColRef) bool {
		return col.ColPos == int32(len(query.Nodes[target.Children[0]].ProjectList)-1)
	})
	require.NotNil(t, filterCol)
	require.NotEmpty(t, target.Children)
	child := query.Nodes[target.Children[0]]
	require.Equal(t, int32(len(child.ProjectList)-1), filterCol.ColPos)
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

	target := findWindowNodeWithFilter(t, query)
	require.NotNil(t, target)
	require.Equal(t, int32(1), target.WindowIdx)

	filterFn := target.FilterList[0].GetF()
	require.NotNil(t, filterFn)
	projectCol := lastProjectedCol(t, target)
	filterCol := findFilterColRef(target.FilterList[0], func(col *plan.ColRef) bool {
		return col.ColPos == projectCol.ColPos
	})
	require.NotNil(t, filterCol)
	require.Equal(t, projectCol.ColPos, filterCol.ColPos)
}
