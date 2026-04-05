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
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIsGPUOffloadQuery(t *testing.T) {
	assert.True(t, isGPUOffloadQuery("/*+ GPU */ SELECT * FROM t"))
	assert.True(t, isGPUOffloadQuery("  /*+ GPU */ SELECT * FROM t"))
	assert.True(t, isGPUOffloadQuery("/*+ gpu */ SELECT * FROM t"))
	assert.False(t, isGPUOffloadQuery("SELECT * FROM t"))
	assert.False(t, isGPUOffloadQuery("/*+ HASH_JOIN */ SELECT * FROM t"))
	assert.False(t, isGPUOffloadQuery(""))
}

func TestStripGPUHint(t *testing.T) {
	assert.Equal(t, "SELECT * FROM t", stripGPUHint("/*+ GPU */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripGPUHint("  /*+ GPU */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripGPUHint("SELECT * FROM t"))
}

func TestGetManifestBaseURL(t *testing.T) {
	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()

	debugHTTPAddr = ""
	assert.Equal(t, "", getManifestBaseURL())

	debugHTTPAddr = ":6060"
	assert.Equal(t, "http://localhost:6060", getManifestBaseURL())

	debugHTTPAddr = "0.0.0.0:6060"
	assert.Equal(t, "http://localhost:6060", getManifestBaseURL())

	debugHTTPAddr = "10.0.1.5:6060"
	assert.Equal(t, "http://10.0.1.5:6060", getManifestBaseURL())
}

func TestRewriteTableExpr_SimpleTable(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT a, b FROM tpch.lineitem WHERE a > 1"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.Len(t, stmts, 1)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)
	require.NotNil(t, sc.From)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "default_db", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
	assert.Contains(t, result, "where a > 1")
}

func TestRewriteTableExpr_NoSchema(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT * FROM lineitem"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "mydb", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=mydb.lineitem')")
}

func TestRewriteTableExpr_Join(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT * FROM tpch.orders o INNER JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "default_db", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.orders')")
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
	// Aliases should be preserved
	assert.Contains(t, result, "as o")
	assert.Contains(t, result, "as l")
}

func TestRewriteTableExpr_Subquery(t *testing.T) {
	ctx := context.Background()
	// Inner table ref inside subquery should also be rewritten
	sql := "SELECT * FROM (SELECT * FROM tpch.lineitem) AS sub"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "default_db", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "as sub")
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
}

func TestRewriteTableExpr_WithAlias(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT l.l_orderkey FROM tpch.lineitem AS l WHERE l.l_shipdate >= '1994-01-01'"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "default_db", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
	assert.Contains(t, result, "as l")
}

func TestRewriteTableExpr_AggregateQuery(t *testing.T) {
	ctx := context.Background()
	sql := "SELECT l_orderkey, SUM(l_quantity) FROM tpch.lineitem WHERE l_shipdate >= '1994-01-01' GROUP BY l_orderkey ORDER BY l_orderkey"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	sc := sel.Select.(*tree.SelectClause)

	for i, te := range sc.From.Tables {
		sc.From.Tables[i] = rewriteTableExpr(te, "default_db", "http://mo:6060", nil)
	}

	result := tree.String(sel, dialect.MYSQL)
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
	assert.Contains(t, result, "group by")
	assert.Contains(t, result, "order by")
}

func TestRewriteTableExpr_CTE(t *testing.T) {
	ctx := context.Background()
	// CTE "recent" should NOT be rewritten; inner "tpch.lineitem" should be.
	sql := "WITH recent AS (SELECT * FROM tpch.lineitem WHERE l_shipdate >= '2024-01-01') SELECT * FROM recent"

	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	require.NoError(t, err)

	sel := stmts[0].(*tree.Select)
	cteNames := collectCTENames(sel.With)

	// Rewrite the main SELECT body
	rewriteSelectStmt(sel.Select, "default_db", "http://mo:6060", cteNames)

	// Rewrite inside CTE bodies
	for _, cte := range sel.With.CTEs {
		if inner, ok := cte.Stmt.(*tree.Select); ok {
			rewriteSelectStmt(inner.Select, "default_db", "http://mo:6060", cteNames)
		}
	}

	result := tree.String(sel, dialect.MYSQL)
	// "recent" in FROM recent should NOT become tae_scan
	assert.NotContains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=default_db.recent')")
	// Inner lineitem SHOULD be rewritten
	assert.Contains(t, result, "tae_scan('http://mo:6060/debug/tae/manifest?table=tpch.lineitem')")
	assert.Contains(t, result, "with recent as")
}

func TestTaeScanRefFormat(t *testing.T) {
	ref := &taeScanRef{url: "http://mo:6060/debug/tae/manifest?table=db.t"}
	ctx := tree.NewFmtCtx(dialect.MYSQL)
	ref.Format(ctx)
	assert.Equal(t, "tae_scan('http://mo:6060/debug/tae/manifest?table=db.t')", ctx.String())
}

func TestBuildGPUResultSet(t *testing.T) {
	result := &gpuSidecarResponse{
		Meta: []gpuColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "VARCHAR"},
		},
		Data: []json.RawMessage{
			json.RawMessage(`[1, "alice"]`),
			json.RawMessage(`[2, "bob"]`),
		},
		Rows: 2,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.NoError(t, err)

	assert.Equal(t, uint64(2), mrs.GetColumnCount())
	assert.Equal(t, uint64(2), mrs.GetRowCount())

	col1, _ := mrs.GetColumn(nil, 0)
	assert.Equal(t, "id", col1.Name())

	col2, _ := mrs.GetColumn(nil, 1)
	assert.Equal(t, "name", col2.Name())
}

func TestGpuTypeToMysql(t *testing.T) {
	assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, gpuTypeToMysql("INTEGER"))
	assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, gpuTypeToMysql("BIGINT"))
	assert.Equal(t, defines.MYSQL_TYPE_DOUBLE, gpuTypeToMysql("DOUBLE"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, gpuTypeToMysql("VARCHAR"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, gpuTypeToMysql("DATE"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, gpuTypeToMysql("unknown_type"))
}

func TestRewriteEndToEnd(t *testing.T) {
	// Test the full rewrite pipeline without a session (manually calling rewriteTableExpr)
	ctx := context.Background()

	cases := []struct {
		name     string
		sql      string
		tables   []string // expected table names in manifest URLs
		keywords []string // keywords that should appear in output
	}{
		{
			name:     "simple select",
			sql:      "SELECT * FROM tpch.lineitem",
			tables:   []string{"tpch.lineitem"},
			keywords: []string{"tae_scan("},
		},
		{
			name:     "two-table join",
			sql:      "SELECT * FROM tpch.orders o JOIN tpch.lineitem l ON o.o_orderkey = l.l_orderkey",
			tables:   []string{"tpch.orders", "tpch.lineitem"},
			keywords: []string{"join", "as o", "as l"},
		},
		{
			name:     "with WHERE and GROUP BY",
			sql:      "SELECT col1, COUNT(*) FROM db.t WHERE col1 > 0 GROUP BY col1",
			tables:   []string{"db.t"},
			keywords: []string{"where", "group by"},
		},
		{
			name:     "union",
			sql:      "SELECT a FROM tpch.orders UNION ALL SELECT a FROM tpch.lineitem",
			tables:   []string{"tpch.orders", "tpch.lineitem"},
			keywords: []string{"union all"},
		},
		{
			name:     "subquery in FROM",
			sql:      "SELECT * FROM (SELECT a FROM tpch.lineitem) AS sub",
			tables:   []string{"tpch.lineitem"},
			keywords: []string{"as sub"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			sel := stmts[0].(*tree.Select)

			rewriteSelectStmt(sel.Select, "default_db", "http://mo:6060", nil)

			result := tree.String(sel, dialect.MYSQL)
			for _, tbl := range tc.tables {
				expected := fmt.Sprintf("tae_scan('http://mo:6060/debug/tae/manifest?table=%s')", tbl)
				assert.Contains(t, result, expected, "should contain tae_scan for %s", tbl)
			}
			for _, kw := range tc.keywords {
				assert.Contains(t, result, kw, "should contain keyword %s", kw)
			}
		})
	}
}
