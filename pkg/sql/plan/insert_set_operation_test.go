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
	"context"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDirectInsertSetOperationSources(t *testing.T) {
	tests := []struct {
		name     string
		operator string
		nodeType planpb.Node_NodeType
	}{
		{name: "union all", operator: "union all", nodeType: planpb.Node_UNION_ALL},
		{name: "union distinct", operator: "union", nodeType: planpb.Node_UNION},
		{name: "intersect", operator: "intersect", nodeType: planpb.Node_INTERSECT},
		{name: "intersect all", operator: "intersect all", nodeType: planpb.Node_INTERSECT_ALL},
		{name: "except", operator: "except", nodeType: planpb.Node_MINUS},
		{name: "minus", operator: "minus", nodeType: planpb.Node_MINUS},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mock := NewMockOptimizer(true)
			stmt, err := parsers.ParseOne(
				context.Background(),
				dialect.MYSQL,
				"insert into nation select * from nation "+test.operator+" select * from nation2",
				1,
			)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)

			insertStmt, ok := stmt.(*tree.Insert)
			require.True(t, ok)
			require.IsType(t, &tree.UnionClause{}, insertStmt.Rows.Select)

			logicPlan, err := BuildPlan(mock.CurrentContext(), stmt, false)
			require.NoError(t, err)
			require.True(t, insertPlanHasNodeType(logicPlan, test.nodeType))
		})
	}
}

func TestParenthesizedInsertSetOperationSourceRemainsSupported(t *testing.T) {
	mock := NewMockOptimizer(true)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		"insert into nation (select * from nation union all select * from nation2)",
		1,
	)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)

	insertStmt, ok := stmt.(*tree.Insert)
	require.True(t, ok)
	require.IsType(t, &tree.ParenSelect{}, insertStmt.Rows.Select)

	logicPlan, err := BuildPlan(mock.CurrentContext(), stmt, false)
	require.NoError(t, err)
	require.True(t, insertPlanHasNodeType(logicPlan, planpb.Node_UNION_ALL))
}

func TestLegacyDirectInsertSetOperationSource(t *testing.T) {
	mock := NewMockOptimizer(true)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		"insert into nation select * from nation union all select * from nation2",
		1,
	)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)

	logicPlan, err := buildInsert(stmt.(*tree.Insert), mock.CurrentContext(), false, false)
	require.NoError(t, err)
	require.True(t, insertPlanHasNodeType(logicPlan, planpb.Node_UNION_ALL))
}

func TestDirectInsertSetOperationReportsBranchWidthError(t *testing.T) {
	mock := NewMockOptimizer(true)
	stmt, err := parsers.ParseOne(
		context.Background(),
		dialect.MYSQL,
		"insert into nation select * from nation union all select n_nationkey from nation2",
		1,
	)
	require.NoError(t, err)
	t.Cleanup(stmt.Free)

	_, err = BuildPlan(mock.CurrentContext(), stmt, false)
	require.ErrorContains(t, err, "SELECT statements have different number of columns")
}

func insertPlanHasNodeType(logicPlan *Plan, nodeType planpb.Node_NodeType) bool {
	if logicPlan == nil || logicPlan.GetQuery() == nil {
		return false
	}
	for _, node := range logicPlan.GetQuery().Nodes {
		if node.NodeType == nodeType {
			return true
		}
	}
	return false
}
