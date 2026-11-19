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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestOnDupArbitrationRestoresSourceOrder(t *testing.T) {
	for _, source := range []string{
		"values (1,'Sales','NY'),(2,'Sales','LA')",
		"select deptno,dname,loc from dept order by deptno",
	} {
		t.Run(source, func(t *testing.T) {
			mock := NewMockOptimizer(true, newPlanTestProcess(t))
			statements, err := mysql.Parse(mock.CurrentContext().GetContext(), "insert into dept "+source+" on duplicate key update loc=values(loc)", 1)
			require.NoError(t, err)
			builder := NewQueryBuilder(planpb.Query_INSERT, mock.CurrentContext(), false, true)
			_, err = builder.bindInsert(statements[0].(*tree.Insert), NewBindContext(builder, nil))
			require.NoError(t, err)
			q := builder.qry
			found := false
			for _, n := range q.Nodes {
				if n.NodeType != planpb.Node_PRE_INSERT_UK || !n.PreInsertUkCtx.GetOdkuTargetArbitration() {
					continue
				}
				found = true
				require.Equal(t, int32(len(n.ProjectList)-1), n.PreInsertUkCtx.OutputColumns)
				projection := q.Nodes[n.Children[0]]
				require.Equal(t, planpb.Node_PROJECT, projection.NodeType)
				sort := q.Nodes[projection.Children[0]]
				require.Equal(t, planpb.Node_SORT, sort.NodeType)
				require.Len(t, sort.OrderBy, 1)
				require.Equal(t, planpb.OrderBySpec_ASC, sort.OrderBy[0].Flag)
				require.NotNil(t, sort.OrderBy[0].Expr.GetCol())
				node := q.Nodes[sort.Children[0]]
				probes := 0
				for node.NodeType == planpb.Node_JOIN {
					require.Equal(t, planpb.Node_LEFT, node.JoinType)
					probes++
					node = q.Nodes[node.Children[0]]
				}
				require.GreaterOrEqual(t, probes, 2)
				require.Equal(t, planpb.Node_WINDOW, node.NodeType)
				require.Equal(t, "row_number", node.WinSpecList[0].GetW().Name)
				require.Empty(t, node.WinSpecList[0].GetW().PartitionBy)
				require.Equal(t, node.BindingTags[0], sort.OrderBy[0].Expr.GetCol().RelPos)
			}
			require.True(t, found)
		})
	}
}

func TestOnDupSourceOrderDoesNotChangeOtherInsertPaths(t *testing.T) {
	for _, statement := range []string{
		"insert into dept values (1,'Sales','NY')",
		"insert ignore into dept values (1,'Sales','NY')",
		"insert into constraint_test.t1(a,b) values (1,'x') on duplicate key update b=values(b)",
	} {
		t.Run(statement, func(t *testing.T) {
			p, err := runOneStmt(NewMockOptimizer(true, newPlanTestProcess(t)), t, statement)
			require.NoError(t, err)
			for _, n := range p.GetQuery().Nodes {
				require.NotEqual(t, planpb.Node_WINDOW, n.NodeType)
			}
		})
	}
}
