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

func TestRowExpression(t *testing.T) {
	for _, tc := range []struct{ name, row, tuple string }{
		{"strings", "select row('a', 'b', 'c') = row('a', 'b', 'c')", "select ('a', 'b', 'c') = ('a', 'b', 'c')"},
		{"null and parameter", "select row(?, null) = row(1, null)", "select (?, null) = (1, null)"},
		{"nested", "select row((1 + 2), row(3, 4)) = row(3, (3, 4))", "select ((1 + 2), (3, 4)) = (3, (3, 4))"},
		{"json value", "select row(json_value(doc, ? returning char null on empty error on error), 1) = row(?, 2)", "select (json_value(doc, ? returning char null on empty error on error), 1) = (?, 2)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			control, err := ParseOne(t.Context(), tc.tuple, 1)
			require.NoError(t, err)
			defer control.Free()
			stmt, err := ParseOne(t.Context(), tc.row, 1)
			require.NoError(t, err)
			defer stmt.Free()
			expr := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.ComparisonExpr)
			require.IsType(t, &tree.Tuple{}, expr.Left)
			require.IsType(t, &tree.Tuple{}, expr.Right)
			formatted := tree.String(stmt, dialect.MYSQL)
			require.Equal(t, tree.String(control, dialect.MYSQL), formatted)
			reparsed, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err)
			defer reparsed.Free()
			require.Equal(t, formatted, tree.String(reparsed, dialect.MYSQL))
		})
	}
	for _, sql := range []string{"select row()", "select row(1)"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), sql, 1)
			if stmt != nil {
				defer stmt.Free()
			}
			require.Error(t, err)
		})
	}
	for _, sql := range []string{
		"values row(1), row(2)",
		"insert into t values (1, 2), (3, 4)",
		"select row_number() over (order by a rows between current row and current row) from t",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(t.Context(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := ParseOne(t.Context(), formatted, 1)
			require.NoError(t, err)
			defer again.Free()
		})
	}
	t.Run("quoted identifiers", func(t *testing.T) {
		stmt, err := ParseOne(t.Context(), "select `row`, `row_number` from t", 1)
		require.NoError(t, err)
		defer stmt.Free()
	})

}
