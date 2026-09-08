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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestIsUnrestrictedDelete(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		targetCount int
		want        bool
	}{
		{name: "plain", sql: "delete from t", targetCount: 1, want: true},
		{name: "order only", sql: "delete from t order by a", targetCount: 1, want: true},
		{name: "where", sql: "delete from t where true", targetCount: 1},
		{name: "limit zero", sql: "delete from t limit 0", targetCount: 1},
		{name: "join without where", sql: "delete t from t join s on t.a = s.a", targetCount: 1},
		{name: "using without where", sql: "delete from t using t join s on t.a = s.a", targetCount: 1},
		{name: "partition", sql: "delete from t partition (p0)", targetCount: 1},
		{name: "multiple targets", sql: "delete t, s from t join s on t.a = s.a", targetCount: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(t.Context(), test.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			deleteStmt, ok := stmt.(*tree.Delete)
			require.True(t, ok)
			require.Equal(t, test.want, isUnrestrictedDelete(deleteStmt, test.targetCount))
		})
	}

	require.False(t, isUnrestrictedDelete(nil, 1))
}

func TestIrregularIndexDeleteJoinIsRowScoped(t *testing.T) {
	logicPlan, err := runOneStmt(
		NewMockOptimizer(true),
		t,
		"delete d from constraint_test.docs_ft d join constraint_test.dept s on d.id = s.deptno",
	)
	require.NoError(t, err)

	query := logicPlan.GetQuery()
	require.NotNil(t, query)

	var originDelete *planpb.Node
	fulltextDeleteCount := 0
	for _, node := range query.Nodes {
		if node.NodeType != planpb.Node_DELETE || node.DeleteCtx == nil || node.DeleteCtx.TableDef == nil {
			continue
		}
		switch node.DeleteCtx.TableDef.Name {
		case "docs_ft":
			originDelete = node
		case catalog.FullTextIndexTableNamePrefix + "docs_ft_body":
			fulltextDeleteCount++
		}
	}

	require.NotNil(t, originDelete, "missing origin-table delete")
	require.False(t, originDelete.DeleteCtx.CanTruncate, "a join must be evaluated before deleting target rows")
	require.Equal(t, 1, fulltextDeleteCount, "row-scoped fulltext maintenance must not be skipped")
}
