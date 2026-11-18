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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func parseEndOwnerSyntax(t *testing.T, sql string) tree.Statement {
	t.Helper()
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	formatted := tree.String(stmt, dialect.MYSQL)
	_, err = parsers.ParseOne(t.Context(), dialect.MYSQL, formatted, 1)
	require.NoError(t, err, sql)
	return stmt
}

func assertEndSelect(t *testing.T, stmt tree.Statement, sql string) {
	t.Helper()
	sel, ok := stmt.(*tree.Select)
	require.True(t, ok, sql)
	clause, ok := sel.Select.(*tree.SelectClause)
	require.True(t, ok, sql)
	require.Len(t, clause.Exprs, 1, sql)
	name, ok := clause.Exprs[0].Expr.(*tree.UnresolvedName)
	require.True(t, ok, sql)
	require.Equal(t, "end", name.ColName(), sql)
	require.Equal(t, 1, name.NumParts, sql)
	require.Empty(t, name.TblName(), sql)
}

func TestBareEndColumnOwnerParsesFormatsAndBuildsPlan(t *testing.T) {
	t.Run("CreateTable AST and real BuildPlan consumer", func(t *testing.T) {
		sql := "create table end_t (id int, end varbinary(16))"
		stmt := parseEndOwnerSyntax(t, sql)
		create, ok := stmt.(*tree.CreateTable)
		require.True(t, ok, sql)
		require.False(t, create.IsAsSelect, sql)
		require.Len(t, create.Defs, 2, sql)
		end, ok := create.Defs[1].(*tree.ColumnTableDef)
		require.True(t, ok, sql)
		require.Equal(t, "end", end.Name.ColName(), sql)

		p, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false, nil)
		require.NoError(t, err, sql)
		ddl := p.GetDdl().GetCreateTable()
		require.NotNil(t, ddl, sql)
		cols := ddl.GetTableDef().GetCols()
		require.GreaterOrEqual(t, len(cols), 2, sql)
		require.Equal(t, "id", cols[0].GetName(), sql)
		require.Equal(t, "end", cols[1].GetName(), sql)
		var _ *planpb.CreateTable = ddl
	})

	t.Run("DML and expression owner forms survive parse-format-reparse", func(t *testing.T) {
		cases := []struct{ sql, owner string }{
			{"insert into end_t (id, end) values (1, 'x')", "insert"},
			{"update end_t set end = 'x' where id = 1", "update"},
			{"delete from end_t where end = 'x'", "delete"},
			{"select end from end_t", "select"},
			{"select end_t.end from end_t", "select"},
			{"select case when end then 1 else 2 end from end_t", "select"},
		}
		for _, tc := range cases {
			t.Run(tc.sql, func(t *testing.T) {
				stmt := parseEndOwnerSyntax(t, tc.sql)
				require.NotEmpty(t, tc.owner, tc.sql)
				switch tc.owner {
				case "insert":
					insert, ok := stmt.(*tree.Insert)
					require.True(t, ok, tc.sql)
					require.NotEmpty(t, insert.ColumnNames, tc.sql)
					require.Equal(t, "end", insert.ColumnNames[1].ColName(), tc.sql)
				case "update":
					update, ok := stmt.(*tree.Update)
					require.True(t, ok, tc.sql)
					require.NotEmpty(t, update.Exprs, tc.sql)
					require.Equal(t, "end", update.Exprs[0].Names[0].ColName(), tc.sql)
				case "delete":
					del, ok := stmt.(*tree.Delete)
					require.True(t, ok, tc.sql)
					require.NotNil(t, del.Where, tc.sql)
				case "select":
					selectStmt, ok := stmt.(*tree.Select)
					require.True(t, ok, tc.sql)
					clause, ok := selectStmt.Select.(*tree.SelectClause)
					require.True(t, ok, tc.sql)
					require.Len(t, clause.Exprs, 1, tc.sql)
					switch selected := clause.Exprs[0].Expr.(type) {
					case *tree.UnresolvedName:
						require.Equal(t, "end", selected.ColName(), tc.sql)
						if tc.sql == "select end_t.end from end_t" {
							require.Equal(t, 2, selected.NumParts, tc.sql)
							require.Equal(t, "end_t", selected.TblName(), tc.sql)
						} else {
							require.Equal(t, 1, selected.NumParts, tc.sql)
							require.Empty(t, selected.TblName(), tc.sql)
						}
					case *tree.CaseExpr:
						require.Len(t, selected.Whens, 1, tc.sql)
						cond, ok := selected.Whens[0].Cond.(*tree.UnresolvedName)
						require.True(t, ok, tc.sql)
						require.Equal(t, "end", cond.ColName(), tc.sql)
						require.Equal(t, 1, cond.NumParts, tc.sql)
					default:
						t.Fatalf("unhandled select expression type: %T", selected)
					}
				default:
					t.Fatalf("unhandled owner: %q", tc.owner)
				}
			})
		}
	})
}

func TestBareEndOwnerPreservesLegalBoundaries(t *testing.T) {
	t.Run("compound END closes a nested CASE and does not swallow the next statement", func(t *testing.T) {
		sql := "begin select case when end then 1 else 2 end from end_t; end; select 2"
		stmts, err := parsers.Parse(t.Context(), dialect.MYSQL, sql, 1)
		require.NoError(t, err, sql)
		require.Len(t, stmts, 2, sql)
		compound, ok := stmts[0].(*tree.CompoundStmt)
		require.True(t, ok, sql)
		require.Len(t, compound.Stmts, 2, sql)
		first, ok := compound.Stmts[0].(*tree.Select)
		require.True(t, ok, sql)
		require.NotNil(t, first.Select, sql)
		require.Nil(t, compound.Stmts[1], sql)
		sel, ok := stmts[1].(*tree.Select)
		require.True(t, ok, sql)
		require.NotNil(t, sel.Select, sql)
	})

	t.Run("procedure STRING body parses back to the end owner", func(t *testing.T) {
		sql := `create procedure end_proc (in id int) 'select end from end_t'`
		stmt := parseEndOwnerSyntax(t, sql)
		procedure, ok := stmt.(*tree.CreateProcedure)
		require.True(t, ok, sql)
		require.NotNil(t, procedure, sql)
		require.Equal(t, "select end from end_t", procedure.Body, sql)
		bodyStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, procedure.Body, 1)

		require.NoError(t, err, sql)
		assertEndSelect(t, bodyStmt, procedure.Body)
	})

	t.Run("SQL-task block body keeps END and still closes", func(t *testing.T) {
		sql := `create task task_end as begin select end from end_t; end`
		stmt := parseEndOwnerSyntax(t, sql)
		task, ok := stmt.(*tree.CreateSQLTask)
		require.True(t, ok, sql)
		require.Equal(t, "select `end` from `end_t`", task.SQLBody, sql)
		bodyStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, task.SQLBody, 1)
		require.NoError(t, err, sql)
		assertEndSelect(t, bodyStmt, task.SQLBody)
	})

	t.Run("baseline compound closing END is not swallowed as a describe column", func(t *testing.T) {
		sql := "begin describe t end;"
		stmt := parseEndOwnerSyntax(t, sql)
		compound, ok := stmt.(*tree.CompoundStmt)
		require.True(t, ok, sql)
		require.Len(t, compound.Stmts, 1, sql)
		show, ok := compound.Stmts[0].(*tree.ShowColumns)
		require.True(t, ok, sql)
		require.Nil(t, show.ColName, sql)
	})
}
