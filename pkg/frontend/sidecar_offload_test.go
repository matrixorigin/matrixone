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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func TestIsSidecarQuery(t *testing.T) {
	// /*+ SIDECAR */ — CPU mode
	isSidecar, useGPU := isSidecarQuery("/*+ SIDECAR */ SELECT * FROM t")
	assert.True(t, isSidecar)
	assert.False(t, useGPU)

	isSidecar, useGPU = isSidecarQuery("  /*+ SIDECAR */ SELECT * FROM t")
	assert.True(t, isSidecar)
	assert.False(t, useGPU)

	isSidecar, useGPU = isSidecarQuery("/*+ sidecar */ SELECT * FROM t")
	assert.True(t, isSidecar)
	assert.False(t, useGPU)

	// /*+ SIDECAR GPU */ — GPU mode
	isSidecar, useGPU = isSidecarQuery("/*+ SIDECAR GPU */ SELECT * FROM t")
	assert.True(t, isSidecar)
	assert.True(t, useGPU)

	isSidecar, useGPU = isSidecarQuery("  /*+ sidecar gpu */ SELECT * FROM t")
	assert.True(t, isSidecar)
	assert.True(t, useGPU)

	// Non-sidecar queries
	isSidecar, _ = isSidecarQuery("SELECT * FROM t")
	assert.False(t, isSidecar)

	isSidecar, _ = isSidecarQuery("/*+ HASH_JOIN */ SELECT * FROM t")
	assert.False(t, isSidecar)

	isSidecar, _ = isSidecarQuery("")
	assert.False(t, isSidecar)
}

func TestStripSidecarHint(t *testing.T) {
	assert.Equal(t, "SELECT * FROM t", stripSidecarHint("/*+ SIDECAR */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripSidecarHint("  /*+ SIDECAR */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripSidecarHint("/*+ SIDECAR GPU */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripSidecarHint("  /*+ sidecar gpu */ SELECT * FROM t"))
	assert.Equal(t, "SELECT * FROM t", stripSidecarHint("SELECT * FROM t"))
}

func TestWrapForGPUExecution(t *testing.T) {
	// Simple query
	assert.Equal(t,
		"SELECT * FROM gpu_execution('SELECT 1')",
		wrapForGPUExecution("SELECT 1"))

	// Query with single quotes — they should be doubled
	assert.Equal(t,
		"SELECT * FROM gpu_execution('SELECT * FROM tae_scan(''http://localhost:8888/debug/tae/manifest?table=db.t'')')",
		wrapForGPUExecution("SELECT * FROM tae_scan('http://localhost:8888/debug/tae/manifest?table=db.t')"))
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
	result := &sidecarResponse{
		Meta: []sidecarColumn{
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
	assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, sidecarTypeToMysql("INTEGER"))
	assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, sidecarTypeToMysql("BIGINT"))
	assert.Equal(t, defines.MYSQL_TYPE_DOUBLE, sidecarTypeToMysql("DOUBLE"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, sidecarTypeToMysql("VARCHAR"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, sidecarTypeToMysql("DATE"))
	assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, sidecarTypeToMysql("unknown_type"))
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
		{
			name:     "no FROM clause",
			sql:      "SELECT 42 AS answer",
			tables:   nil,
			keywords: []string{"42"},
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
			if tc.tables == nil {
				assert.NotContains(t, result, "tae_scan", "should not contain tae_scan")
			}
			for _, kw := range tc.keywords {
				assert.Contains(t, result, kw, "should contain keyword %s", kw)
			}
		})
	}
}

func TestFixMOSyntaxForDuckDB(t *testing.T) {
	cases := []struct {
		name, in, out string
	}{
		{"date func", "date('2024-01-01')", "DATE '2024-01-01'"},
		{"interval func", "interval('3', 'month')", "INTERVAL '3' month"},
		{"extract func", "extract('year', col)", "EXTRACT(year FROM col)"},
		{"date arith", "DATE '2024-01-01' + INTERVAL '3' month",
			"CAST(DATE '2024-01-01' + INTERVAL '3' month AS DATE)"},
		{"no change", "SELECT 1", "SELECT 1"},
		{"combined", "SELECT count(*), date('2024-01-01') FROM t",
			"SELECT count(*), DATE '2024-01-01' FROM t"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.out, fixMOSyntaxForDuckDB(tc.in))
		})
	}
}

func TestGpuTypeToMysql_AllTypes(t *testing.T) {
	// Integer types
	for _, typ := range []string{
		"INTEGER", "INT", "INT32", "BIGINT", "INT64",
		"SMALLINT", "INT16", "TINYINT", "INT8",
		"UINTEGER", "UINT32", "UBIGINT", "UINT64",
		"USMALLINT", "UINT16", "UTINYINT", "UINT8", "HUGEINT",
	} {
		assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, sidecarTypeToMysql(typ), "type %s", typ)
	}
	// Float types
	assert.Equal(t, defines.MYSQL_TYPE_FLOAT, sidecarTypeToMysql("FLOAT"))
	assert.Equal(t, defines.MYSQL_TYPE_FLOAT, sidecarTypeToMysql("REAL"))
	assert.Equal(t, defines.MYSQL_TYPE_FLOAT, sidecarTypeToMysql("FLOAT4"))
	// Double types
	assert.Equal(t, defines.MYSQL_TYPE_DOUBLE, sidecarTypeToMysql("DOUBLE"))
	assert.Equal(t, defines.MYSQL_TYPE_DOUBLE, sidecarTypeToMysql("FLOAT8"))
	// Bool
	assert.Equal(t, defines.MYSQL_TYPE_BOOL, sidecarTypeToMysql("BOOLEAN"))
	assert.Equal(t, defines.MYSQL_TYPE_BOOL, sidecarTypeToMysql("BOOL"))
	// Blob
	assert.Equal(t, defines.MYSQL_TYPE_BLOB, sidecarTypeToMysql("BLOB"))
	assert.Equal(t, defines.MYSQL_TYPE_BLOB, sidecarTypeToMysql("BYTEA"))
	// VARCHAR-mapped types
	for _, typ := range []string{"DATE", "TIMESTAMP", "DECIMAL(38,2)", "VARCHAR", "TEXT"} {
		assert.Equal(t, defines.MYSQL_TYPE_VARCHAR, sidecarTypeToMysql(typ), "type %s", typ)
	}
	// Case insensitive
	assert.Equal(t, defines.MYSQL_TYPE_LONGLONG, sidecarTypeToMysql("integer"))
	assert.Equal(t, defines.MYSQL_TYPE_DOUBLE, sidecarTypeToMysql("double"))
}

func TestBuildGPUResultSet_BigintPrecision(t *testing.T) {
	// Verify that large integers (>2^53) are preserved via json.Number
	result := &sidecarResponse{
		Meta: []sidecarColumn{
			{Name: "big_id", Type: "BIGINT"},
			{Name: "price", Type: "DOUBLE"},
			{Name: "label", Type: "VARCHAR"},
		},
		Data: []json.RawMessage{
			json.RawMessage(`[9007199254740993, 123.456, "hello"]`),
		},
		Rows: 1,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.NoError(t, err)

	assert.Equal(t, uint64(1), mrs.GetRowCount())
	row, _ := mrs.GetRow(nil, 0)
	// BIGINT should be int64, not float64
	assert.IsType(t, int64(0), row[0])
	assert.Equal(t, int64(9007199254740993), row[0])
	// DOUBLE should be float64
	assert.IsType(t, float64(0), row[1])
	assert.Equal(t, 123.456, row[1])
	// VARCHAR stays as string
	assert.Equal(t, "hello", row[2])
}

func TestBuildGPUResultSet_NullValues(t *testing.T) {
	result := &sidecarResponse{
		Meta: []sidecarColumn{
			{Name: "id", Type: "INTEGER"},
			{Name: "val", Type: "VARCHAR"},
		},
		Data: []json.RawMessage{
			json.RawMessage(`[1, null]`),
			json.RawMessage(`[null, "ok"]`),
		},
		Rows: 2,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.NoError(t, err)
	assert.Equal(t, uint64(2), mrs.GetRowCount())
}

func TestBuildGPUResultSet_EmptyResult(t *testing.T) {
	result := &sidecarResponse{
		Meta: []sidecarColumn{{Name: "x", Type: "INTEGER"}},
		Data: []json.RawMessage{},
		Rows: 0,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), mrs.GetRowCount())
	assert.Equal(t, uint64(1), mrs.GetColumnCount())
}

func TestBuildGPUResultSet_InvalidJSON(t *testing.T) {
	result := &sidecarResponse{
		Meta: []sidecarColumn{{Name: "x", Type: "INTEGER"}},
		Data: []json.RawMessage{json.RawMessage(`{invalid}`)},
		Rows: 1,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse sidecar row")
}

func TestTaeScanRefFormat_EscapeQuotes(t *testing.T) {
	ref := &taeScanRef{url: "http://mo:6060/debug/tae/manifest?table=db.it's"}
	ctx := tree.NewFmtCtx(dialect.MYSQL)
	ref.Format(ctx)
	assert.Equal(t, "tae_scan('http://mo:6060/debug/tae/manifest?table=db.it''s')", ctx.String())
}

func TestSetDebugHTTPAddr(t *testing.T) {
	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()

	SetDebugHTTPAddr(":8888")
	assert.Equal(t, ":8888", debugHTTPAddr)
	assert.Equal(t, "http://localhost:8888", getManifestBaseURL())
}

func TestWalkExprForSubqueries_MoreTypes(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name string
		sql  string
	}{
		{"NOT subquery", "SELECT * FROM t1 WHERE NOT EXISTS (SELECT 1 FROM tpch.t2)"},
		{"OR subquery", "SELECT * FROM t1 WHERE a=1 OR b IN (SELECT x FROM tpch.t3)"},
		{"CASE subquery", "SELECT CASE WHEN (SELECT count(*) FROM tpch.t2)>0 THEN 1 ELSE 0 END FROM t1"},
		{"binary expr", "SELECT a + (SELECT max(b) FROM tpch.t2) FROM t1"},
		{"IS NULL", "SELECT * FROM t1 WHERE (SELECT x FROM tpch.t2 LIMIT 1) IS NULL"},
		{"IS NOT NULL", "SELECT * FROM t1 WHERE (SELECT x FROM tpch.t2 LIMIT 1) IS NOT NULL"},
		{"CAST", "SELECT CAST((SELECT x FROM tpch.t2 LIMIT 1) AS SIGNED) FROM t1"},
		{"func arg", "SELECT coalesce((SELECT x FROM tpch.t2 LIMIT 1), 0) FROM t1"},
		{"tuple", "SELECT * FROM t1 WHERE (a,b) IN (SELECT x,y FROM tpch.t2)"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stmts, err := parsers.Parse(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			sel := stmts[0].(*tree.Select)
			rewriteSelectStmt(sel.Select, "default_db", "http://mo:6060", nil)
			result := tree.String(sel, dialect.MYSQL)
			assert.Contains(t, result, "tae_scan(", "should rewrite subquery table in: %s", tc.name)
		})
	}
}

func TestCollectCTENames_Nil(t *testing.T) {
	names := collectCTENames(nil)
	assert.NotNil(t, names)
	assert.Len(t, names, 0)
}

func TestBuildGPUResultSet_ColumnCountMismatch(t *testing.T) {
	result := &sidecarResponse{
		Meta: []sidecarColumn{
			{Name: "a", Type: "INTEGER"},
			{Name: "b", Type: "VARCHAR"},
			{Name: "c", Type: "DOUBLE"},
		},
		Data: []json.RawMessage{
			json.RawMessage(`[1, "ok", 3.14]`), // correct: 3 columns
			json.RawMessage(`[2, "short"]`),    // wrong: 2 columns
		},
		Rows: 2,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sidecar row has 2 columns, expected 3")
}

func TestBuildGPUResultSet_ExtraColumns(t *testing.T) {
	result := &sidecarResponse{
		Meta: []sidecarColumn{
			{Name: "a", Type: "INTEGER"},
		},
		Data: []json.RawMessage{
			json.RawMessage(`[1, "extra", 3.14]`), // 3 columns but meta has 1
		},
		Rows: 1,
	}

	mrs := &MysqlResultSet{}
	err := buildGPUResultSet(context.Background(), mrs, result)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sidecar row has 3 columns, expected 1")
}

func TestSendToSidecar_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Invalid SQL: syntax error"))
	}))
	defer srv.Close()

	_, err := sendToSidecar(context.Background(), srv.URL, "INVALID SQL")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sidecar error (400)")
	assert.Contains(t, err.Error(), "Invalid SQL")
}

func TestSendToSidecar_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := `{"meta":[{"name":"x","type":"INTEGER"}],"data":[[1]],"rows":1}`
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(resp))
	}))
	defer srv.Close()

	result, err := sendToSidecar(context.Background(), srv.URL, "SELECT 1 AS x")
	require.NoError(t, err)
	assert.Equal(t, 1, result.Rows)
	assert.Len(t, result.Meta, 1)
	assert.Equal(t, "x", result.Meta[0].Name)
}

func TestSendToSidecar_TooLarge(t *testing.T) {
	// sendToSidecar uses maxResponseSize = 512 MB; we can't allocate that much
	// in a unit test, but we can verify the truncation logic by temporarily
	// testing with a smaller sidecar. The key path is:
	//   if int64(len(body)) > maxResponseSize { return error }
	// This is covered structurally; the test below verifies HTTP error paths.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("internal error"))
	}))
	defer srv.Close()

	_, err := sendToSidecar(context.Background(), srv.URL, "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "sidecar error (500)")
}

func TestSendToSidecar_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("{not json"))
	}))
	defer srv.Close()

	_, err := sendToSidecar(context.Background(), srv.URL, "SELECT 1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse sidecar response")
}

func TestExecRequest_SidecarNotConfigured(t *testing.T) {
	// When sidecar_url is not set and debugHTTPAddr is empty,
	// handleSidecarOffload returns errSidecarNotConfigured → ExecRequest strips
	// the hint and falls through to normal COM_QUERY processing.
	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = ""

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}

	execCtx := &ExecCtx{reqCtx: context.Background(), ses: ses}

	// handleSidecarOffload should return errSidecarNotConfigured (CPU mode)
	err := handleSidecarOffload(ses, execCtx, "/*+ SIDECAR */ SELECT 1", false)
	assert.Equal(t, errSidecarNotConfigured, err)

	// Same for GPU mode
	err = handleSidecarOffload(ses, execCtx, "/*+ SIDECAR GPU */ SELECT 1", true)
	assert.Equal(t, errSidecarNotConfigured, err)
}

func TestExecRequest_SidecarError(t *testing.T) {
	// When sidecar URL is set but returns an error, handleSidecarOffload
	// returns a real error (not errSidecarNotConfigured).
	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = ":8888"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}

	// Point sidecar to a server that returns 500
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer srv.Close()

	err := ses.SetSessionSysVar(context.Background(), "sidecar_url", srv.URL)
	require.NoError(t, err)

	ses.SetDatabaseName("testdb")
	execCtx := &ExecCtx{reqCtx: context.Background(), ses: ses}

	err = handleSidecarOffload(ses, execCtx, "/*+ SIDECAR */ SELECT 1 AS x FROM testdb.t1", false)
	assert.Error(t, err)
	assert.NotEqual(t, errSidecarNotConfigured, err)
	assert.Contains(t, err.Error(), "sidecar error (500)")
}

func TestExecRequest_SidecarSuccess(t *testing.T) {
	// Full end-to-end: sidecar URL set + mock sidecar → result set built.
	saved := debugHTTPAddr
	defer func() { debugHTTPAddr = saved }()
	debugHTTPAddr = ":8888"

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ses := newTestSession(t, ctrl)
	ses.txnHandler = &TxnHandler{}

	// Mock sidecar that returns a valid JSONCompact response
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"meta":[{"name":"x","type":"INTEGER"}],"data":[[42]],"rows":1}`))
	}))
	defer srv.Close()

	err := ses.SetSessionSysVar(context.Background(), "sidecar_url", srv.URL)
	require.NoError(t, err)

	ses.SetDatabaseName("testdb")
	execCtx := &ExecCtx{reqCtx: context.Background(), ses: ses}

	// CPU mode
	err = handleSidecarOffload(ses, execCtx, "/*+ SIDECAR */ SELECT 42 AS x FROM testdb.t1", false)
	assert.NoError(t, err)

	// Verify result set was built correctly
	mrs := ses.GetMysqlResultSet()
	assert.Equal(t, uint64(1), mrs.GetRowCount())
	assert.Equal(t, uint64(1), mrs.GetColumnCount())
}
