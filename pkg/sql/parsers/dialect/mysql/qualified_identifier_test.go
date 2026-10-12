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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func requireQualifiedName(t *testing.T, expr tree.Expr, parts ...string) {
	t.Helper()
	require.IsType(t, &tree.UnresolvedName{}, expr)
	name := expr.(*tree.UnresolvedName)
	require.Equal(t, len(parts), name.NumParts)
	require.False(t, name.Star)
	for i, part := range parts {
		require.Equal(t, part, name.CStrParts[len(parts)-1-i].Origin())
	}
}

func TestQualifiedKeywordsAreIdentifiers(t *testing.T) {
	// The keyword vocabulary defines inputs, while requested spelling and typed
	// AST components supply the oracle independently of token classification.
	for word := range keywords {
		t.Run(word, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), "select src."+word+" from tbl src", 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.IsType(t, &tree.Select{}, stmt)
			selectStmt := stmt.(*tree.Select)
			require.IsType(t, &tree.SelectClause{}, selectStmt.Select)
			clause := selectStmt.Select.(*tree.SelectClause)
			require.Len(t, clause.Exprs, 1)
			requireQualifiedName(t, clause.Exprs[0].Expr, "src", word)
		})
	}
}

func TestQualifiedIdentifierContexts(t *testing.T) {
	tests := []struct {
		name, sql, mode string
		parts           []string
	}{
		{"mixed case", "select t.RaNk from src t", "", []string{"t", "RaNk"}},
		{"quoted control", "select t.`rank` from src t", "", []string{"t", "rank"}},
		{"three parts", "select db.select.rank from db.select", "", []string{"db", "select", "rank"}},
		{"whitespace and comments", "select t /* before */ . /* after */ rank from src t", "", []string{"t", "rank"}},
		{"executable comment", "select t./*! rank */ from src t", "", []string{"t", "rank"}},
		{"BEGIN before comma", "select t.begin, t.rank from src t", "", []string{"t", "begin"}},
		{"HIGH_NOT_PRECEDENCE", "select t.not from src t", "HIGH_NOT_PRECEDENCE", []string{"t", "not"}},
		{"IGNORE_SPACE", "select t.rank from src t", "IGNORE_SPACE", []string{"t", "rank"}},
		{"ANSI_QUOTES", "select t.\"rank\" from src t", "ANSI_QUOTES", []string{"t", "rank"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), tt.sql, 1, tt.mode)
			require.NoError(t, err)
			defer stmt.Free()
			clause := stmt.(*tree.Select).Select.(*tree.SelectClause)
			requireQualifiedName(t, clause.Exprs[0].Expr, tt.parts...)
		})
	}
	t.Run("bulk update targets and values", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "UPDATE rank_source AS t1, rank_stage AS t2 SET t1.rank = t2.rank, t1.global_rank = t2.global_rank WHERE t1.id = t2.id", 1)
		require.NoError(t, err)
		defer stmt.Free()
		require.IsType(t, &tree.Update{}, stmt)
		update := stmt.(*tree.Update)
		require.True(t, update.MultiTable)
		require.Len(t, update.Exprs, 2)
		require.Len(t, update.Exprs[0].Names, 1)
		requireQualifiedName(t, update.Exprs[0].Names[0], "t1", "rank")
		requireQualifiedName(t, update.Exprs[0].Expr, "t2", "rank")
		requireQualifiedName(t, update.Exprs[1].Names[0], "t1", "global_rank")
		requireQualifiedName(t, update.Exprs[1].Expr, "t2", "global_rank")
	})
	for _, name := range []string{"select", "begin", "count"} {
		t.Run("qualified table "+name, func(t *testing.T) {
			stmt, err := ParseOneWithSQLMode(context.Background(), "create table db."+name+"(id int)", 1, "IGNORE_SPACE")
			require.NoError(t, err)
			defer stmt.Free()
			table := stmt.(*tree.CreateTable).Table
			require.True(t, table.ExplicitSchema)
			require.Equal(t, tree.Identifier("db"), table.SchemaName)
			require.Equal(t, tree.Identifier(name), table.ObjectName)
		})
	}
	t.Run("ASOF table followed by JOIN", func(t *testing.T) {
		stmt, err := ParseOne(context.Background(), "select t.rank from db.asof join src t on t.id = db.asof.id", 1)
		require.NoError(t, err)
		defer stmt.Free()
	})
	t.Run("FOR does not combine with ICEBERG", func(t *testing.T) {
		lexer := NewLexer(dialect.MYSQL, "t.for iceberg snapshot 1", 1)
		for _, token := range []int{ID, int('.'), ID, ICEBERG, SNAPSHOT, INTEGRAL} {
			var val yySymType
			require.Equal(t, token, lexer.Lex(&val))
		}
	})
}

func TestQualifiedIdentifierBoundariesAndReuse(t *testing.T) {
	for _, sql := range []string{
		"select rank from src", "create table select(id int)",
		"select t.'rank' from src t", "select t.\"rank\" from src t",
		"select t._utf8mb4'rank' from src t", "select t.x'AB' from src t",
		"select t.b'01' from src t", "select t.? from src t",
		"select t.1 from src t", "select t.", "select t.`rank",
		"select t./* unterminated", "select t.😀 from src t",
	} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			if stmt != nil {
				defer stmt.Free()
			}
			require.Error(t, err)
		})
	}
	for _, sql := range []string{"select rank() over (order by id) from src", "select 1.25, .5, t.* from src t"} {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
		})
	}
	t.Run("parser reuse after success and failure", func(t *testing.T) {
		var parser MySQLParser
		for _, sql := range []string{"select t.rank from src t", "select t."} {
			stmts, err := parser.Parse(context.Background(), sql, 1)
			for _, stmt := range stmts {
				stmt.Free()
			}
			require.Equal(t, sql == "select t.", err != nil)
			stmts, err = parser.Parse(context.Background(), "select rank from src", 1)
			for _, stmt := range stmts {
				stmt.Free()
			}
			require.Error(t, err)
			require.False(t, parser.scanner.identifierAfterDot)
		}
	})
	t.Run("scanner context is scoped and reset", func(t *testing.T) {
		scanner := NewScanner(dialect.MYSQL, "rank rank")
		defer PutScanner(scanner)
		token, word := scanner.ScanAfterDot()
		require.Equal(t, ID, token)
		require.Equal(t, "rank", word)
		require.False(t, scanner.identifierAfterDot)
		token, _ = scanner.Scan()
		require.Equal(t, RANK, token)
		scanner.identifierAfterDot = true
		scanner.setSql("rank")
		require.False(t, scanner.identifierAfterDot)
		token, _ = scanner.Scan()
		require.Equal(t, RANK, token)
	})
	t.Run("comment-aware qualification is scoped", func(t *testing.T) {
		scanner := NewScanner(dialect.MYSQL, "/* gap */ begin, rank")
		defer PutScanner(scanner)
		token, text := scanner.ScanWithCommentsAfterDot()
		require.Equal(t, COMMENT, token)
		require.Equal(t, "/* gap */", text)
		require.False(t, scanner.identifierAfterDot)
		token, text = scanner.ScanWithCommentsAfterDot()
		require.Equal(t, ID, token)
		require.Equal(t, "begin", text)
		require.False(t, scanner.identifierAfterDot)
		token, _ = scanner.ScanWithComments()
		require.Equal(t, int(','), token)
		token, _ = scanner.ScanWithComments()
		require.Equal(t, RANK, token)

		scanner.setSql("'unterminated")
		token, _ = scanner.ScanWithCommentsAfterDot()
		require.Equal(t, LEX_ERROR, token)
		require.False(t, scanner.identifierAfterDot)
		scanner.setSql("rank")
		token, _ = scanner.ScanWithComments()
		require.Equal(t, RANK, token)
	})
}
