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

package compile

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestCTASRewriteOptionCarriesEffectiveAST(t *testing.T) {
	ctx := context.Background()
	outerSQL := `/*+ {"rewrites":{"db1.t1":"select * from db1.t1 where note = 'a\\b'"},"remapdb":{"db1":"db2"}} */ create table db1.copy as select * from db1.t1`
	stmts, err := parsers.ParseWithSQLMode(ctx, dialect.MYSQL, outerSQL, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.NoError(t, parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(
		ctx, stmts, outerSQL, "NO_BACKSLASH_ESCAPES", 1))
	createTable, ok := stmts[0].(*tree.CreateTable)
	require.True(t, ok)
	require.NotNil(t, createTable.AsSource.RewriteOption)
	option := createTable.AsSource.RewriteOption
	require.Equal(t, map[string]string{"db1": "db2"}, option.RemapDb)

	generated := "insert into `db2`.`copy` select * from db2.t1"
	inner, err := parsers.Parse(ctx, dialect.MYSQL, generated, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range inner {
			stmt.Free()
		}
	}()
	attachRewriteOptionToStatement(inner[0], option)
	insert, ok := inner[0].(*tree.Insert)
	require.True(t, ok)
	require.Same(t, option, insert.Rows.RewriteOption)
	require.Contains(t, option.Rewrites, "db1.t1")
}

func TestLeadingRewriteHintValidation(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
		ok   bool
	}{
		{
			name: "bang hint",
			sql:  `/*!+ {"rewrites":{"db1.t1":"select 1"}} */ select 1`,
			want: `/*!+ {"rewrites":{"db1.t1":"select 1"}} */`,
			ok:   true,
		},
		{name: "non-json hint", sql: "/*+ select 1", ok: false},
		{name: "unterminated hint", sql: "/*+ {\"rewrites\":{}} select 1", ok: false},
		{name: "ordinary sql", sql: "select 1", ok: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := leadingRewriteHint(tt.sql)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestInternalCTASRewriteHintAttachesToSource(t *testing.T) {
	sql := `/*+    {"rewrites":{"db1.t1":"select * from db1.t1 where tenant_id = 1"}} */ insert into db1.copy select * from db1.t1`
	stmts, err := parsers.Parse(context.Background(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.True(t, hasLeadingRewriteHint(sql))
	require.NoError(t, parsers.AddRewriteHintsWithSQLMode(context.Background(), stmts, sql, ""))
	insert, ok := stmts[0].(*tree.Insert)
	require.True(t, ok)
	require.NotNil(t, insert.Rows)
	require.NotNil(t, insert.Rows.RewriteOption)
	require.Contains(t, insert.Rows.RewriteOption.Rewrites, "db1.t1")
}

func TestAttachRewriteOptionToStatementVariants(t *testing.T) {
	option := &tree.RewriteOption{}

	selectStmt := &tree.Select{}
	attachRewriteOptionToStatement(selectStmt, option)
	require.Same(t, option, selectStmt.RewriteOption)

	paren := &tree.ParenSelect{Select: &tree.Select{}}
	paren.Select.Select = &tree.ParenSelect{Select: &tree.Select{}}
	attachRewriteOptionToStatement(paren, option)
	require.Same(t, option, paren.Select.RewriteOption)
	require.Same(t, option, paren.Select.Select.(*tree.ParenSelect).Select.RewriteOption)

	insert := &tree.Insert{Rows: &tree.Select{}}
	attachRewriteOptionToStatement(insert, option)
	require.Same(t, option, insert.Rows.RewriteOption)
	attachRewriteOptionToStatement(&tree.Insert{}, option)

	multiInsert := &tree.MultiInsert{Source: &tree.Select{}}
	attachRewriteOptionToStatement(multiInsert, option)
	require.Same(t, option, multiInsert.Source.RewriteOption)

	createTable := &tree.CreateTable{AsSource: &tree.Select{}}
	attachRewriteOptionToStatement(createTable, option)
	require.Same(t, option, createTable.AsSource.RewriteOption)

	attachRewriteOptionToStatement(nil, option)
	attachRewriteOptionToStatement(selectStmt, nil)
}

func TestCTASRewriteOption(t *testing.T) {
	option := &tree.RewriteOption{}
	c := &Compile{stmt: &tree.CreateTable{AsSource: &tree.Select{RewriteOption: option}}}
	require.Same(t, option, c.ctasRewriteOption())

	c.stmt = &tree.CreateTable{}
	require.Nil(t, c.ctasRewriteOption())
	c.stmt = &tree.Select{}
	require.Nil(t, c.ctasRewriteOption())
}
