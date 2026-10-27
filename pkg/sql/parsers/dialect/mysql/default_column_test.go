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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestDefaultColumnSyntax(t *testing.T) {
	for _, sql := range []string{"SELECT DEFAULT(a)", "SELECT DEFAULT(t.a)", "SELECT DEFAULT(db.t.a)"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			require.IsType(t, &tree.DefaultColExpr{}, expr)
			formatted := tree.String(stmt, dialect.MYSQL)
			parsed, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err)
			defer parsed.Free()
			require.Equal(t, formatted, tree.String(parsed, dialect.MYSQL))
		})
	}
	for _, sql := range []string{"SELECT DEFAULT()", "SELECT DEFAULT(1)", "SELECT DEFAULT(a+1)", "SELECT DEFAULT(*)", "SELECT DEFAULT(a,b)"} {
		stmt, err := ParseOne(t.Context(), sql, 1)
		if stmt != nil {
			stmt.Free()
		}
		require.Error(t, err, sql)
	}
}

func TestDefaultColumnCapturedSQLMode(t *testing.T) {
	for _, mode := range []string{"", "STRICT_TRANS_TABLES", "STRICT_ALL_TABLES", "TRADITIONAL"} {
		statement, err := ParseOneWithSQLMode(t.Context(), "SELECT DEFAULT(x)", 1, mode)
		require.NoError(t, err)
		defer statement.Free()
		expr := statement.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.DefaultColExpr)
		require.Equal(t, mode != "", expr.StrictMode)
	}
}

func TestDefaultColumnTemporalLiteralSyntax(t *testing.T) {
	for _, tc := range []struct {
		sql     string
		literal bool
	}{
		{"SELECT DATE '2020-01-01'", true},
		{"SELECT TIME '12:34:56'", true},
		{"SELECT TIMESTAMP '2020-01-01 00:00:00'", true},
		{"SELECT DATE('2020-01-01')", false},
		{"SELECT TIME('12:34:56')", false},
		{"SELECT TIMESTAMP('2020-01-01 00:00:00')", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			statement, err := ParseOne(t.Context(), tc.sql, 1)
			require.NoError(t, err)
			defer statement.Free()
			expr := statement.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			require.Equal(t, tc.literal, expr.IsTemporalLiteral)
			formatted := tree.StringWithOpts(statement, dialect.MYSQL, tree.WithQuoteString(true))
			reparsed, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err)
			defer reparsed.Free()
			expr = reparsed.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			require.Equal(t, tc.literal, expr.IsTemporalLiteral)
		})
	}
}

type defaultColumnVisitor struct{ skip, stop, replace bool }

func (v defaultColumnVisitor) Enter(expr tree.Expr) (tree.Expr, bool) {
	if _, column := expr.(*tree.UnresolvedName); column && v.replace {
		return tree.NewUnresolvedColName("renamed"), true
	}
	return expr, v.skip
}
func (v defaultColumnVisitor) Exit(expr tree.Expr) (tree.Expr, bool) { return expr, !v.stop }

func TestDefaultColumnVisitor(t *testing.T) {
	for _, visitor := range []defaultColumnVisitor{{}, {skip: true}, {stop: true}, {replace: true}} {
		expr := &tree.DefaultColExpr{Column: tree.NewUnresolvedColName("a")}
		visited, ok := expr.Accept(visitor)
		require.Equal(t, !visitor.stop, ok)
		want := "default(a)"
		if visitor.replace {
			want = "default(renamed)"
		}
		require.Equal(t, want, tree.String(visited, dialect.MYSQL))
	}
}
