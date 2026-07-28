// Copyright 2021 - 2026 Matrix Origin
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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// findFullTextMatch drills into a select statement (descending through derived
// tables) and returns the first MATCH ... AGAINST expression found in a WHERE
// clause.
func findFullTextMatch(t *testing.T, stmt tree.SelectStatement) *tree.FullTextMatchExpr {
	t.Helper()
	clause, ok := stmt.(*tree.SelectClause)
	require.True(t, ok, "expected *tree.SelectClause, got %T", stmt)
	if clause.Where != nil {
		if m, ok := clause.Where.Expr.(*tree.FullTextMatchExpr); ok {
			return m
		}
	}
	require.NotEmpty(t, clause.From.Tables)
	te := clause.From.Tables[0]
	for {
		switch v := te.(type) {
		case *tree.JoinTableExpr:
			te = v.Left
		case *tree.AliasedTableExpr:
			te = v.Expr
		case *tree.ParenTableExpr:
			te = v.Expr
		case *tree.Select:
			return findFullTextMatch(t, v.Select)
		case *tree.Subquery:
			sel, ok := v.Select.(*tree.Select)
			require.True(t, ok, "expected *tree.Select subquery, got %T", v.Select)
			return findFullTextMatch(t, sel.Select)
		default:
			t.Fatalf("unexpected table expr %T while searching for MATCH", te)
			return nil
		}
	}
}

// TestCTASFullTextPatternSurvivesInternalReparse: the CTAS follow-up
// INSERT ... SELECT is executed by the internal SQL executor, which parses in
// DEFAULT sql_mode regardless of the session's mode (parsers.Parse in
// pkg/sql/compile/sql_executor.go passes an empty mode). So CreateAsSelectSql
// must be default-escaped. This test builds the CTAS plan under a session
// mode, reparses the generated SQL exactly as the executor will, and asserts
// the MATCH pattern the follow-up query actually searches equals the pattern
// the user wrote (#24823 review follow-up).
func TestCTASFullTextPatternSurvivesInternalReparse(t *testing.T) {
	sql := `CREATE TABLE ctas_ft AS SELECT N_NAME FROM NATION WHERE MATCH(N_NAME) AGAINST('a\nb' IN BOOLEAN MODE)`

	// buildWithMode parses+plans the CTAS under the given session sql_mode and
	// returns the generated follow-up SQL plus the pattern the user wrote (as
	// the session-mode parse understood it).
	buildWithMode := func(t *testing.T, mode string) (generated string, want string) {
		t.Helper()
		mock := NewMockOptimizer(false)
		mock.ctxt.SetSqlModeOverride(mode)
		ctx := mock.CurrentContext()
		stmts, err := mysql.ParseWithSQLMode(ctx.GetContext(), sql, 1, mode)
		require.NoError(t, err)
		ct, ok := stmts[0].(*tree.CreateTable)
		require.True(t, ok)
		want = findFullTextMatch(t, ct.AsSource.Select).Pattern.(*tree.NumVal).String()
		logicPlan, err := BuildPlan(ctx, stmts[0], false)
		require.NoError(t, err)
		createTable := logicPlan.GetDdl().GetCreateTable()
		require.NotNil(t, createTable)
		return createTable.GetCreateAsSelectSql(), want
	}

	// reparse the generated SQL the way the internal executor does: default mode.
	executorPattern := func(t *testing.T, generated string) string {
		t.Helper()
		stmts, err := mysql.ParseWithSQLMode(context.Background(), generated, 1, "")
		require.NoError(t, err)
		ins, ok := stmts[0].(*tree.Insert)
		require.True(t, ok, "generated CTAS SQL must be an INSERT, got %T", stmts[0])
		return findFullTextMatch(t, ins.Rows.Select).Pattern.(*tree.NumVal).String()
	}

	// NO_BACKSLASH_ESCAPES session: the stored pattern is literally a\nb
	// (backslash + 'n'). The formatter must double the backslash so the
	// executor's default-mode parse reduces it back to the literal.
	t.Run("no_backslash_escapes session", func(t *testing.T) {
		generated, want := buildWithMode(t, "NO_BACKSLASH_ESCAPES")
		require.Equal(t, `a\nb`, want)
		require.Contains(t, generated, `a\\nb`,
			"backslash must be doubled for the default-mode reparse, got: "+generated)
		require.Equal(t, want, executorPattern(t, generated))
	})

	// Default session: '\n' parses to a newline; the round-trip through the
	// executor's default-mode parse must preserve it.
	t.Run("default session", func(t *testing.T) {
		generated, want := buildWithMode(t, "")
		require.Equal(t, "a\nb", want)
		require.Equal(t, want, executorPattern(t, generated))
	})
}
