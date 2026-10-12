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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestBindUserVariableAssignmentPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select @rank := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	require.NotNil(t, p.GetQuery())

	found := false
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			assignment := expr.GetUserVarAssignment()
			if assignment == nil {
				continue
			}
			found = true
			require.Equal(t, "rank", assignment.GetName())
			require.NotNil(t, assignment.GetValue())
		}
	}
	require.True(t, found, "the bound projection must retain the user-variable assignment")
}

func TestBindNestedUserVariableAssignmentPlan(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select @a := @b := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	p, err := BuildPlan(ctx, stmt, false)
	require.NoError(t, err)
	var outer, inner *Expr
	for _, node := range p.GetQuery().Nodes {
		for _, expr := range node.ProjectList {
			if expr.GetUserVarAssignment() != nil {
				outer = expr
				inner = expr.GetUserVarAssignment().GetValue()
			}
		}
	}
	require.NotNil(t, outer)
	require.Equal(t, "a", outer.GetUserVarAssignment().GetName())
	require.NotNil(t, inner)
	require.Equal(t, "b", inner.GetUserVarAssignment().GetName())
}

func TestUserVariableAssignmentRejectsUnsafeFilterScope(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	stmt, err := mysql.ParseOne(ctx.GetContext(), "select 1 where @rank := 1", 1)
	require.NoError(t, err)
	defer stmt.Free()

	_, err = BuildPlan(ctx, stmt, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "WHERE is not supported")
}

func TestUserVariableAssignmentRejectsAmbiguousOrderAndLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want string
	}{
		{name: "order", sql: "select @rank := 1 order by 1", want: "ORDER BY with user-variable assignment"},
		{name: "limit", sql: "select @rank := 1 limit 1", want: "LIMIT with user-variable assignment"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(ctx.GetContext(), tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestUserVariableAssignmentRejectsMultipleProjectionAssignments(t *testing.T) {
	for _, sql := range []string{
		"select @a := 1, @b := 2",
		"select @a := 1, @a := 2",
	} {
		t.Run(sql, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			stmt, err := mysql.ParseOne(ctx.GetContext(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()

			_, err = BuildPlan(ctx, stmt, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "multiple user-variable assignment projections")
		})
	}
}
