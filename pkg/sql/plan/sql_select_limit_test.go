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

func TestSQLSelectLimitIsMarkedOnTopLevelSelect(t *testing.T) {
	query := buildSQLSelectLimitTestQuery(t, "select n_name from nation order by n_name")
	require.True(t, query.ApplySqlSelectLimit)
}

func TestExplicitLimitTakesPrecedenceOverSQLSelectLimit(t *testing.T) {
	tests := []string{
		"select n_name from nation order by n_name limit 5",
		"((select n_name from nation order by n_name limit 5))",
	}

	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			query := buildSQLSelectLimitTestQuery(t, sql)
			require.False(t, query.ApplySqlSelectLimit)
		})
	}
}

func TestSQLSelectLimitCapsWholeUnion(t *testing.T) {
	query := buildSQLSelectLimitTestQuery(t,
		"select n_name from nation union all select r_name from region")
	require.True(t, query.ApplySqlSelectLimit)
}

func TestOffsetOnlyDoesNotDisableSQLSelectLimit(t *testing.T) {
	stmt := &tree.Select{Limit: &tree.Limit{
		Offset: tree.NewNumVal(int64(1), "1", false, tree.P_int64),
	}}
	require.False(t, selectHasExplicitTopLevelLimit(stmt))
}

func buildSQLSelectLimitTestQuery(t *testing.T, sql string) *planpb.Query {
	t.Helper()
	ctx := NewMockCompilerContext(true)
	stmts, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	queryPlan, err := BuildPlan(ctx, stmts[0], false)
	require.NoError(t, err)
	require.NotNil(t, queryPlan.GetQuery())
	return queryPlan.GetQuery()
}
