// Copyright 2024 Matrix Origin
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

package frontend

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// parseWithRewriteHints parses sql and attaches the leading rewrite/remap hint,
// matching GetComputationWrapper's production order before applyRemapDb.
func parseWithRewriteHints(t *testing.T, sql string) []tree.Statement {
	t.Helper()
	ctx := context.Background()
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.NoError(t, parsers.AddRewriteHints(ctx, stmts, sql))
	return stmts
}

// Issue #29161: session remapdb must not drop a role rewrite keyed on the
// source database. The planner looks up rewrites by the post-remap
// schema+table, so both the key and the rule body must observe the target.
func TestApplyRemapDbKeepsRewriteKeysAndBodiesAligned(t *testing.T) {
	ctx := context.Background()

	t.Run("role rewrite key and body follow remap", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"src_db.t":"select id from src_db.t where tenant = 1"},"remapdb":{"src_db":"dst_db"}} */ select id from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		sel := stmts[0].(*tree.Select)
		require.NotNil(t, sel.RewriteOption)
		require.Contains(t, sel.RewriteOption.Rewrites, "src_db.t")

		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))

		out := tree.StringWithOpts(stmts[0], dialect.MYSQL, tree.WithSingleQuoteString())
		require.Contains(t, out, "dst_db.t")
		require.NotContains(t, out, "src_db.t")

		require.NotContains(t, sel.RewriteOption.Rewrites, "src_db.t")
		chain, ok := sel.RewriteOption.Rewrites["dst_db.t"]
		require.True(t, ok, "rewrites=%v", sel.RewriteOption.Rewrites)
		require.Len(t, chain, 1)
		require.Equal(t, "dst_db", chain[0].DbName)
		body := tree.StringWithOpts(chain[0].Stmt, dialect.MYSQL, tree.WithSingleQuoteString())
		require.Contains(t, body, "dst_db.t")
		require.NotContains(t, body, "src_db.t")
		require.Contains(t, body, "tenant = 1")
	})

	t.Run("unrelated rewrite key is left alone", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"other.t":"select 1"},"remapdb":{"src_db":"dst_db"}} */ select * from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		sel := stmts[0].(*tree.Select)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		require.Contains(t, sel.RewriteOption.Rewrites, "other.t")
		require.NotContains(t, sel.RewriteOption.Rewrites, "dst_db.t")
	})

	t.Run("remap-only statement has nothing to realign", func(t *testing.T) {
		sql := `/*+ {"remapdb":{"src_db":"dst_db"}} */ select * from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		sel := stmts[0].(*tree.Select)
		require.NotNil(t, sel.RewriteOption)
		require.Empty(t, sel.RewriteOption.Rewrites)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		out := tree.StringWithOpts(stmts[0], dialect.MYSQL, tree.WithSingleQuoteString())
		require.Contains(t, out, "dst_db.t")
	})

	t.Run("chain bodies all follow remap", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"src_db.t":["select id from src_db.t where tenant = 1","select id from src_db.t where id > 0"]},"remapdb":{"src_db":"dst_db"}} */ select id from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		sel := stmts[0].(*tree.Select)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		chain, ok := sel.RewriteOption.Rewrites["dst_db.t"]
		require.True(t, ok)
		require.Len(t, chain, 2)
		for _, rewrite := range chain {
			require.Equal(t, "dst_db", rewrite.DbName)
			body := tree.StringWithOpts(rewrite.Stmt, dialect.MYSQL, tree.WithSingleQuoteString())
			require.Contains(t, body, "dst_db.t")
			require.NotContains(t, body, "src_db.t")
		}
	})

	t.Run("two source keys colliding on one destination fail closed", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"a.t":"select 1","b.t":"select 2"},"remapdb":{"a":"z","b":"z"}} */ select * from a.t`
		stmts := parseWithRewriteHints(t, sql)
		sel := stmts[0].(*tree.Select)
		before := make(map[string][]*tree.Rewrite, len(sel.RewriteOption.Rewrites))
		for k, v := range sel.RewriteOption.Rewrites {
			before[k] = v
		}
		err := applyRemapDb(ctx, stmts, map[string]string{"a": "z", "b": "z"}, 1)
		require.Error(t, err)
		require.Contains(t, err.Error(), "rewrite key collision")
		// Fail closed before mutating the option.
		require.Len(t, sel.RewriteOption.Rewrites, 2)
		require.Contains(t, sel.RewriteOption.Rewrites, "a.t")
		require.Contains(t, sel.RewriteOption.Rewrites, "b.t")
		require.Equal(t, before, sel.RewriteOption.Rewrites)
	})

	t.Run("insert source rewrite follows remap", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"src_db.t":"select id from src_db.t where tenant = 1"},"remapdb":{"src_db":"dst_db"}} */ insert into dst_db.u select id from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		ins := stmts[0].(*tree.Insert)
		require.NotNil(t, ins.Rows)
		require.NotNil(t, ins.Rows.RewriteOption)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		require.NotContains(t, ins.Rows.RewriteOption.Rewrites, "src_db.t")
		require.Contains(t, ins.Rows.RewriteOption.Rewrites, "dst_db.t")
		body := tree.StringWithOpts(
			ins.Rows.RewriteOption.Rewrites["dst_db.t"][0].Stmt,
			dialect.MYSQL, tree.WithSingleQuoteString(),
		)
		require.Contains(t, body, "dst_db.t")
	})

	t.Run("empty rewrite map is a no-op", func(t *testing.T) {
		require.NoError(t, remapRewriteOption(nil, remapDbContext{}))
		require.NoError(t, remapRewriteOption(&tree.RewriteOption{}, remapDbContext{}))
	})
}

func TestApplyRemapDbRewriteKeysEdgePaths(t *testing.T) {
	ctx := context.Background()

	t.Run("paren select keeps RewriteOption on outer select", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"src_db.t":"select id from src_db.t where tenant = 1"},"remapdb":{"src_db":"dst_db"}} */ (select id from src_db.t)`
		stmts := parseWithRewriteHints(t, sql)
		sel, ok := stmts[0].(*tree.Select)
		require.True(t, ok, "got %T", stmts[0])
		ps, ok := sel.Select.(*tree.ParenSelect)
		require.True(t, ok, "got %T", sel.Select)
		require.NotNil(t, ps.Select)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		require.NotContains(t, sel.RewriteOption.Rewrites, "src_db.t")
		require.Contains(t, sel.RewriteOption.Rewrites, "dst_db.t")
		body := tree.StringWithOpts(
			sel.RewriteOption.Rewrites["dst_db.t"][0].Stmt,
			dialect.MYSQL, tree.WithSingleQuoteString(),
		)
		require.Contains(t, body, "dst_db.t")
		require.NotContains(t, body, "src_db.t")
	})

	t.Run("prepare statement body keeps RewriteOption", func(t *testing.T) {
		sql := `/*+ {"rewrites":{"src_db.t":"select id from src_db.t where tenant = 1"},"remapdb":{"src_db":"dst_db"}} */ prepare s from select id from src_db.t`
		stmts := parseWithRewriteHints(t, sql)
		ps, ok := stmts[0].(*tree.PrepareStmt)
		require.True(t, ok)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 1))
		sel, ok := ps.Stmt.(*tree.Select)
		require.True(t, ok)
		require.NotContains(t, sel.RewriteOption.Rewrites, "src_db.t")
		require.Contains(t, sel.RewriteOption.Rewrites, "dst_db.t")
	})

	t.Run("lower_case_table_names=2 comparison identity matches planner", func(t *testing.T) {
		// Planner builds: NewCStr(schema).Compare() + "." + NewCStr(table).Compare()
		// Mode 2 lowercases for Compare but preserves execution spelling in map keys
		// per NormalizeRewriteKey. Remap target is preserved (mode != 1).
		sql := `/*+ {"rewrites":{"Src_Db.T":"select 1"},"remapdb":{"src_db":"dst_db"}} */ select 1 from Src_Db.T`
		stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 2)
		require.NoError(t, err)
		require.NoError(t, parsers.AddRewriteHintsWithSQLModeAndLowerCaseTableNames(ctx, stmts, sql, "", 2))
		sel := stmts[0].(*tree.Select)
		require.NotEmpty(t, sel.RewriteOption)
		require.NoError(t, applyRemapDb(ctx, stmts, map[string]string{"src_db": "dst_db"}, 2))
		require.NotContains(t, sel.RewriteOption.Rewrites, "src_db.T")
		// find remapped key and assert planner-style lookup identity
		var got string
		for k := range sel.RewriteOption.Rewrites {
			got = k
		}
		require.Equal(t, tree.NewCStr("dst_db", 2).Compare()+"."+tree.NewCStr("T", 2).Compare(), got)
	})
}
