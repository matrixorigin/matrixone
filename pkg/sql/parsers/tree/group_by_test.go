// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree_test

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestGroupingQualifiedColumns(t *testing.T) {
	for _, columns := range []string{"k, r", "t.k, t.r", "db.t.k, db.t.r", "`t`.`k`, `t`.`r`"} {
		t.Run(columns, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), "select grouping("+columns+") from db.t group by k,r with rollup", 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			formatted := tree.String(stmt, dialect.MYSQL)
			again, err := mysql.ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err)
			t.Cleanup(again.Free)
			require.Equal(t, formatted, tree.String(again, dialect.MYSQL))
			before := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			after := again.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(*tree.FuncExpr)
			require.Equal(t, before.Exprs, after.Exprs, "qualification must survive the round trip")
		})
	}
}

func TestGroupByFormatRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name, by string
	}{
		{"ordinary", "group by k, r"},
		{"sets", "group by grouping sets ((k, r), (k), ())"},
		{"duplicate sets", "group by grouping sets ((k), (k), ())"},
		{"empty set", "group by grouping sets (())"},
		{"duplicate empty sets", "group by grouping sets ((), ())"},
		{"rollup", "group by k, r with rollup"},
		{"cube", "group by cube (k, r)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := mysql.ParseOne(context.Background(), "select k, count(*) from t "+tc.by, 1)
			require.NoError(t, err)
			t.Cleanup(stmt.Free)
			group := stmt.(*tree.Select).Select.(*tree.SelectClause).GroupBy
			formatted := tree.String(group, dialect.MYSQL)
			require.Equal(t, tc.by, formatted)
			again, err := mysql.ParseOne(context.Background(), "select k, count(*) from t "+formatted, 1)
			require.NoError(t, err)
			t.Cleanup(again.Free)
			require.Equal(t, group, again.(*tree.Select).Select.(*tree.SelectClause).GroupBy)
		})
	}
}
