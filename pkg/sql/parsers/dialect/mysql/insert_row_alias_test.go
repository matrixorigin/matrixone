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

package mysql

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestInsertRowAliasASTAndFormat(t *testing.T) {
	tests := []struct {
		sql       string
		row       string
		cols      tree.IdentifierList
		formatted string
	}{
		{
			sql:       "insert into t values (1, 2), (3, 4) as n(x, y) on duplicate key update a = n.x",
			row:       "n",
			cols:      tree.IdentifierList{"x", "y"},
			formatted: "insert into t values (1, 2), (3, 4) as n(x, y) on duplicate key update a = n.x",
		},
		{
			sql:       "insert into t values (1, 2) AS Incoming(X, Y)",
			row:       "Incoming",
			cols:      tree.IdentifierList{"X", "Y"},
			formatted: "insert into t values (1, 2) as Incoming(X, Y)",
		},
		{
			sql:       "insert into t (b, a) values (2, 1) as incoming",
			row:       "incoming",
			formatted: "insert into t (b, a) values (2, 1) as incoming",
		},
		{
			sql:       "insert into t set a = 1, b = 2 as incoming(a1, b1)",
			row:       "incoming",
			cols:      tree.IdentifierList{"a1", "b1"},
			formatted: "insert into t (a, b) values (1, 2) as incoming(a1, b1)",
		},
	}

	for _, tt := range tests {
		ast, err := ParseOne(context.Background(), tt.sql, 1)
		require.NoError(t, err, tt.sql)
		insertStmt, ok := ast.(*tree.Insert)
		require.True(t, ok, tt.sql)
		require.NotNil(t, insertStmt.RowAlias, tt.sql)
		require.Equal(t, tt.row, string(insertStmt.RowAlias.Alias), tt.sql)
		require.Equal(t, tt.cols, insertStmt.RowAlias.Cols, tt.sql)
		formatted := tree.String(ast, dialect.MYSQL)
		require.Equal(t, tt.formatted, formatted, tt.sql)

		roundTripped, err := ParseOne(context.Background(), formatted, 1)
		require.NoError(t, err, formatted)
		require.Equal(t, formatted, tree.String(roundTripped, dialect.MYSQL))
	}
}

func TestInsertRowAliasParserRejectsUnsupportedForms(t *testing.T) {
	for _, sql := range []string{
		"insert into t values row(1) as n",
		"insert into t values (1) n",
		"insert into t values (1) (x)",
		"insert overwrite t values (1) as n",
		"replace into t values (1) as n",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.Error(t, err, sql)
	}
}

func TestInsertRowAliasInsideSQLPrepare(t *testing.T) {
	stmt, err := ParseOne(context.Background(),
		"prepare s from insert into t values (?, ?) as n(x, y) on duplicate key update a = n.x", 1)
	require.NoError(t, err)
	prepared, ok := stmt.(*tree.PrepareStmt)
	require.True(t, ok)
	insertStmt, ok := prepared.Stmt.(*tree.Insert)
	require.True(t, ok)
	require.Equal(t, tree.Identifier("n"), insertStmt.RowAlias.Alias)
	require.Equal(t, tree.IdentifierList{"x", "y"}, insertStmt.RowAlias.Cols)
}

func TestInsertRowAliasSubqueryAndExpressionFormsParse(t *testing.T) {
	for _, sql := range []string{
		"insert into t values (1, 5, 0) as n(x, y, z) on duplicate key update b = (select n.y + t.a from s as n where n.x = t.id)",
		"insert into t values (1, 6, 0) as n on duplicate key update b = case when n.a > 5 then n.a else null end",
		"insert into t values (1, 5, 0) as n(x, y, z) on duplicate key update b = (select n.y + s.y from s as s where s.x = n.x)",
	} {
		_, err := ParseOne(context.Background(), sql, 1)
		require.NoError(t, err, sql)
	}
}
