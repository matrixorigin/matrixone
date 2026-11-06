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

package mysql

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDigestAlterDefaultQueryEnvelope(t *testing.T) {
	for _, tc := range []struct {
		expression string
		valid      bool
	}{
		{"((SELECT 1))", true},
		{"((SELECT 1) ORDER BY 1)", false},
		{"((SELECT 1) LIMIT 1)", false},
		{"((SELECT 1) ORDER BY 1 LIMIT 1)", false},
		{"(((SELECT 1) ORDER BY 1 LIMIT 1))", true},
		{"(WITH c AS (SELECT 1) (SELECT 1))", false},
		{"((WITH c AS (SELECT 1) (SELECT 1)))", true},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			statements, err := parseWithSQLMode(t.Context(), "ALTER TABLE t ALTER COLUMN a SET DEFAULT "+tc.expression, 0, "", &statementDigestSyntax{nulls: make(map[int]struct{})})
			require.NoError(t, err)
			defer func() {
				for _, s := range statements {
					s.Free()
				}
			}()
			expression := statements[0].(*tree.AlterTable).Options[0].(*tree.AlterTableAlterColumnClause).DefaultExpr.Expr
			query := expression.(*tree.Subquery).Select.(*tree.ParenSelect).Select
			require.IsType(t, &tree.ParenSelect{}, query.Select)
			require.Equal(t, tc.valid, digestAlterDefaultValid(expression))
		})
	}
}

func TestParenthesizedQueryLayerForms(t *testing.T) {
	for _, sql := range []string{
		"WITH c AS (SELECT 1) (SELECT 1)",
		"(SELECT 1) FOR UPDATE",
		"((SELECT 1) FOR UPDATE)",
	} {
		t.Run(sql, func(t *testing.T) {
			statement, err := ParseOneWithSQLMode(t.Context(), sql, 0, "")
			require.NoError(t, err)
			defer statement.Free()
			formatted := tree.String(statement, dialect.MYSQL)
			roundTrip, err := ParseOneWithSQLMode(t.Context(), formatted, 0, "")
			require.NoError(t, err)
			defer roundTrip.Free()
			require.Equal(t, formatted, tree.String(roundTrip, dialect.MYSQL))
		})
	}
}

func TestDigestCurrentRoleLexicalIdentity(t *testing.T) {
	for _, digestMode := range []bool{true, false, true} {
		for _, expression := range []string{"CURRENT_ROLE", "CURRENT_ROLE()", "CURRENT_ROLE ()", "CURRENT_ROLE/**/()"} {
			func() {
				var syntax *statementDigestSyntax
				if digestMode {
					syntax = &statementDigestSyntax{nulls: make(map[int]struct{})}
				}
				statements, err := parseWithSQLMode(t.Context(), "SELECT "+expression, 0, "", syntax)
				require.NoError(t, err)
				defer func() {
					for _, s := range statements {
						s.Free()
					}
				}()
				expr := statements[0].(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
				if digestMode && expression == "CURRENT_ROLE" {
					require.IsType(t, &tree.UnresolvedName{}, expr)
				} else {
					require.IsType(t, &tree.FuncExpr{}, expr)
				}
			}()
		}
	}
}
