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
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

const gpuHintPrefix = "/*+ GPU */"

// errGPUNotConfigured is a sentinel indicating GPU offload should fall back
// to normal MO execution (sidecar URL not set).
var errGPUNotConfigured = moerr.NewInternalErrorNoCtx("gpu offload not configured")

// Precompiled regexes for fixMOSyntaxForDuckDB (avoid recompiling per query).
var (
	reDateFunc     = regexp.MustCompile(`(?i)\bdate\('([^']+)'\)`)
	reIntervalFunc = regexp.MustCompile(`(?i)\binterval\('([^']+)',\s*'([^']+)'\)`)
	reExtractFunc  = regexp.MustCompile(`(?i)\bextract\('([^']+)',\s*`)
	reDateArith    = regexp.MustCompile(`(?i)(DATE\s+'[^']+'\s*[+-]\s*INTERVAL\s+'[^']+'\s*\w+)`)
)

// debugHTTPAddr stores the -debug-http listen address, set at startup.
var debugHTTPAddr string

// SetDebugHTTPAddr is called from cmd/mo-service/main.go to store the
// debug HTTP listen address for use by GPU offload manifest URL generation.
func SetDebugHTTPAddr(addr string) {
	debugHTTPAddr = addr
}

// getManifestBaseURL derives the manifest HTTP base URL from the debug
// HTTP listen address. Returns e.g. "http://localhost:6060".
func getManifestBaseURL() string {
	if debugHTTPAddr == "" {
		return ""
	}
	host := debugHTTPAddr
	// ":6060" or "0.0.0.0:6060" → "localhost:6060"
	if strings.HasPrefix(host, ":") {
		host = "localhost" + host
	} else if strings.HasPrefix(host, "0.0.0.0:") {
		host = "localhost" + host[len("0.0.0.0"):]
	}
	return "http://" + host
}

// isGPUOffloadQuery checks whether the SQL string starts with the /*+ GPU */ hint.
func isGPUOffloadQuery(sql string) bool {
	trimmed := strings.TrimSpace(sql)
	return strings.HasPrefix(strings.ToUpper(trimmed), gpuHintPrefix)
}

// stripGPUHint removes the /*+ GPU */ hint prefix from the SQL string.
func stripGPUHint(sql string) string {
	trimmed := strings.TrimSpace(sql)
	upper := strings.ToUpper(trimmed)
	if strings.HasPrefix(upper, gpuHintPrefix) {
		return strings.TrimSpace(trimmed[len(gpuHintPrefix):])
	}
	return sql
}

// (gpuSidecarRequest removed — httpserver accepts raw SQL body)

// httpserver JSONCompact response from DuckDB sidecar.
type gpuSidecarResponse struct {
	Meta       []gpuColumn       `json:"meta"`
	Data       []json.RawMessage `json:"data"`
	Rows       int               `json:"rows"`
	Statistics *gpuStatistics    `json:"statistics,omitempty"`
}

type gpuColumn struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type gpuStatistics struct {
	Elapsed  float64 `json:"elapsed"`
	RowsRead int     `json:"rows_read"`
	ByteRead int     `json:"bytes_read"`
}

// handleGPUOffload intercepts a /*+ GPU */ query:
//  1. Strips the hint.
//  2. Generates manifests for referenced tables via the debug HTTP endpoint.
//  3. Rewrites FROM clauses to use tae_scan(manifest_url).
//  4. Sends the rewritten SQL to the DuckDB sidecar.
//  5. Translates the response into a MySQL result set.
func handleGPUOffload(ses *Session, execCtx *ExecCtx, sql string) error {
	ctx := execCtx.reqCtx
	stripped := stripGPUHint(sql)

	sidecarURLVal, err := ses.GetSessionSysVar("gpu_sidecar_url")
	var sidecarURL string
	if err == nil && sidecarURLVal != nil {
		sidecarURL = sidecarURLVal.(string)
	}
	if sidecarURL == "" {
		// Fall back to TOML config value (frontend.gpuSidecarUrl).
		sidecarURL = getPu(ses.GetService()).SV.GPUSidecarURL
	}
	if sidecarURL == "" {
		return errGPUNotConfigured
	}

	manifestURL := getManifestBaseURL()
	if manifestURL == "" {
		return errGPUNotConfigured
	}

	t0 := time.Now()
	rewritten, err := rewriteTablesForGPU(ctx, ses, stripped, manifestURL)
	if err != nil {
		return err
	}
	t1 := time.Now()

	logutil.Infof("GPU offload: rewritten SQL: %s", rewritten)

	result, err := sendToSidecar(ctx, sidecarURL, rewritten)
	if err != nil {
		return err
	}
	t2 := time.Now()

	mrs := &MysqlResultSet{}
	if err := buildGPUResultSet(ctx, mrs, result); err != nil {
		return err
	}
	ses.SetMysqlResultSet(mrs)
	t3 := time.Now()

	err = trySaveQueryResult(ctx, ses, mrs)
	t4 := time.Now()

	logutil.Infof("GPU offload timing: rewrite=%v sidecar=%v buildResult=%v saveResult=%v total=%v rows=%d",
		t1.Sub(t0), t2.Sub(t1), t3.Sub(t2), t4.Sub(t3), t4.Sub(t0), result.Rows)

	return err
}

// taeScanRef is a synthetic AST node that formats as tae_scan('url').
// It replaces real TableName nodes in the AST during GPU offload rewriting.
type taeScanRef struct {
	tree.TableExpr
	url string
}

func (t *taeScanRef) Format(ctx *tree.FmtCtx) {
	escaped := strings.ReplaceAll(t.url, "'", "''")
	ctx.WriteString(fmt.Sprintf("tae_scan('%s')", escaped))
}

// rewriteTablesForGPU parses the SQL into an AST, walks the tree to find
// all table references in FROM/JOIN positions, replaces them with
// tae_scan(manifest_url) calls, and serializes the modified AST back to SQL.
//
// This handles:
//   - Simple FROM table / FROM db.table
//   - JOINs (INNER, LEFT, RIGHT, CROSS, etc.)
//   - Aliased tables (FROM table AS t)
//   - Parenthesized table expressions
//   - Subqueries are left unchanged (only real table names are rewritten)
func rewriteTablesForGPU(ctx context.Context, ses *Session, sql string, manifestBaseURL string) (string, error) {
	stmts, err := parsers.Parse(ctx, dialect.MYSQL, sql, 1)
	if err != nil {
		return "", moerr.NewInternalErrorf(ctx, "GPU offload: failed to parse SQL: %v", err)
	}
	if len(stmts) != 1 {
		return "", moerr.NewInternalErrorf(ctx, "GPU offload: expected single statement, got %d", len(stmts))
	}

	sel, ok := stmts[0].(*tree.Select)
	if !ok {
		return "", moerr.NewInternalErrorf(ctx, "GPU offload: only SELECT statements are supported")
	}

	// Collect CTE names so we don't rewrite references to them.
	cteNames := collectCTENames(sel.With)

	defaultDB := ses.GetDatabaseName()
	rewriteSelectStmt(sel.Select, defaultDB, manifestBaseURL, cteNames)

	// Also rewrite table refs inside CTE bodies themselves.
	if sel.With != nil {
		for _, cte := range sel.With.CTEs {
			if inner, ok := cte.Stmt.(*tree.Select); ok {
				rewriteSelectStmt(inner.Select, defaultDB, manifestBaseURL, cteNames)
			}
		}
	}

	raw := tree.StringWithOpts(sel, dialect.MYSQL, tree.WithSingleQuoteString())
	return fixMOSyntaxForDuckDB(raw), nil
}

// fixMOSyntaxForDuckDB translates MO-specific SQL serialization quirks
// into standard SQL that DuckDB can parse.
func fixMOSyntaxForDuckDB(sql string) string {
	// count('*') → count(*)
	sql = strings.ReplaceAll(sql, "count('*')", "count(*)")
	// date('...') → DATE '...'
	sql = reDateFunc.ReplaceAllString(sql, "DATE '$1'")
	// interval('N', 'unit') → INTERVAL 'N' unit
	sql = reIntervalFunc.ReplaceAllString(sql, "INTERVAL '$1' $2")
	// extract('field', expr) → EXTRACT(field FROM expr)
	sql = reExtractFunc.ReplaceAllString(sql, "EXTRACT($1 FROM ")
	// DATE 'x' +/- INTERVAL ... → CAST(DATE 'x' +/- INTERVAL ... AS DATE)
	sql = reDateArith.ReplaceAllString(sql, "CAST($1 AS DATE)")
	return sql
}

// collectCTENames returns the set of CTE names defined in a WITH clause.
func collectCTENames(with *tree.With) map[string]bool {
	names := make(map[string]bool)
	if with == nil {
		return names
	}
	for _, cte := range with.CTEs {
		if cte.Name != nil {
			names[strings.ToLower(string(cte.Name.Alias))] = true
		}
	}
	return names
}

// rewriteSelectStmt recursively walks a SelectStatement to rewrite all
// table references in FROM/JOIN positions and inside expression subqueries
// (WHERE, HAVING, SELECT list).
func rewriteSelectStmt(stmt tree.SelectStatement, defaultDB string, manifestBaseURL string, cteNames map[string]bool) {
	switch s := stmt.(type) {
	case *tree.SelectClause:
		if s.From != nil {
			for i, te := range s.From.Tables {
				s.From.Tables[i] = rewriteTableExpr(te, defaultDB, manifestBaseURL, cteNames)
			}
		}
		// Walk expressions that may contain subqueries
		if s.Where != nil {
			walkExprForSubqueries(s.Where.Expr, defaultDB, manifestBaseURL, cteNames)
		}
		if s.Having != nil {
			walkExprForSubqueries(s.Having.Expr, defaultDB, manifestBaseURL, cteNames)
		}
		for _, se := range s.Exprs {
			walkExprForSubqueries(se.Expr, defaultDB, manifestBaseURL, cteNames)
		}
	case *tree.UnionClause:
		rewriteSelectStmt(s.Left, defaultDB, manifestBaseURL, cteNames)
		rewriteSelectStmt(s.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.ParenSelect:
		if s.Select != nil {
			rewriteSelectStmt(s.Select.Select, defaultDB, manifestBaseURL, cteNames)
		}
	}
}

// walkExprForSubqueries recursively traverses an expression tree to find
// Subquery nodes and rewrite their inner table references.
func walkExprForSubqueries(expr tree.Expr, defaultDB string, manifestBaseURL string, cteNames map[string]bool) {
	if expr == nil {
		return
	}
	switch e := expr.(type) {
	case *tree.Subquery:
		rewriteSelectStmt(e.Select, defaultDB, manifestBaseURL, cteNames)
	case *tree.ComparisonExpr:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.AndExpr:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.OrExpr:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.XorExpr:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.NotExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.ParenExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.IsNullExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.IsNotNullExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.FuncExpr:
		for _, arg := range e.Exprs {
			walkExprForSubqueries(arg, defaultDB, manifestBaseURL, cteNames)
		}
	case *tree.CaseExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
		for _, w := range e.Whens {
			walkExprForSubqueries(w.Cond, defaultDB, manifestBaseURL, cteNames)
			walkExprForSubqueries(w.Val, defaultDB, manifestBaseURL, cteNames)
		}
		walkExprForSubqueries(e.Else, defaultDB, manifestBaseURL, cteNames)
	case *tree.RangeCond:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.From, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.To, defaultDB, manifestBaseURL, cteNames)
	case *tree.BinaryExpr:
		walkExprForSubqueries(e.Left, defaultDB, manifestBaseURL, cteNames)
		walkExprForSubqueries(e.Right, defaultDB, manifestBaseURL, cteNames)
	case *tree.UnaryExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.CastExpr:
		walkExprForSubqueries(e.Expr, defaultDB, manifestBaseURL, cteNames)
	case *tree.Tuple:
		for _, item := range e.Exprs {
			walkExprForSubqueries(item, defaultDB, manifestBaseURL, cteNames)
		}
	}
}

// rewriteTableExpr recursively walks a TableExpr and replaces TableName
// nodes with tae_scan(manifest_url) references. Recurses into subqueries.
// CTE names are skipped (they are virtual, not physical tables).
func rewriteTableExpr(te tree.TableExpr, defaultDB string, manifestBaseURL string, cteNames map[string]bool) tree.TableExpr {
	switch expr := te.(type) {
	case *tree.TableName:
		name := strings.ToLower(string(expr.ObjectName))
		if cteNames[name] {
			return te // CTE reference — leave unchanged
		}
		db := string(expr.SchemaName)
		if db == "" {
			db = defaultDB
		}
		tableParam := url.QueryEscape(db + "." + name)
		u := fmt.Sprintf("%s/debug/tae/manifest?table=%s", manifestBaseURL, tableParam)
		return &taeScanRef{url: u}

	case *tree.AliasedTableExpr:
		expr.Expr = rewriteTableExpr(expr.Expr, defaultDB, manifestBaseURL, cteNames)
		return expr

	case *tree.JoinTableExpr:
		expr.Left = rewriteTableExpr(expr.Left, defaultDB, manifestBaseURL, cteNames)
		expr.Right = rewriteTableExpr(expr.Right, defaultDB, manifestBaseURL, cteNames)
		return expr

	case *tree.ParenTableExpr:
		expr.Expr = rewriteTableExpr(expr.Expr, defaultDB, manifestBaseURL, cteNames)
		return expr

	case *tree.Subquery:
		rewriteSelectStmt(expr.Select, defaultDB, manifestBaseURL, cteNames)
		return expr

	case *tree.Select:
		rewriteSelectStmt(expr.Select, defaultDB, manifestBaseURL, cteNames)
		return expr

	default:
		return te
	}
}

// sendToSidecar sends the rewritten SQL to the DuckDB httpserver extension.
// httpserver expects raw SQL as the POST body at /?default_format=JSONCompact.
func sendToSidecar(ctx context.Context, sidecarURL string, sql string) (*gpuSidecarResponse, error) {
	client := &http.Client{Timeout: 5 * time.Minute}
	url := strings.TrimRight(sidecarURL, "/") + "/?default_format=JSONCompact"
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(sql))
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to create sidecar request: %v", err)
	}

	t0 := time.Now()
	resp, err := client.Do(req)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "sidecar request failed: %v", err)
	}
	defer resp.Body.Close()
	t1 := time.Now()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to read sidecar response: %v", err)
	}
	t2 := time.Now()

	if resp.StatusCode != http.StatusOK {
		return nil, moerr.NewInternalErrorf(ctx, "sidecar error (%d): %s", resp.StatusCode, string(body))
	}

	var result gpuSidecarResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, moerr.NewInternalErrorf(ctx, "failed to parse sidecar response: %v", err)
	}
	t3 := time.Now()

	logutil.Infof("GPU offload sendToSidecar: httpWait=%v readBody=%v (%d bytes) jsonUnmarshal=%v total=%v",
		t1.Sub(t0), t2.Sub(t1), len(body), t3.Sub(t2), t3.Sub(t0))

	return &result, nil
}

// buildGPUResultSet populates a MysqlResultSet from the httpserver JSONCompact response.
func buildGPUResultSet(ctx context.Context, mrs *MysqlResultSet, result *gpuSidecarResponse) error {

	// Define columns from "meta"
	for _, col := range result.Meta {
		mc := new(MysqlColumn)
		mc.SetName(col.Name)
		mc.SetColumnType(gpuTypeToMysql(col.Type))
		mrs.AddColumn(mc)
	}

	// Add rows from "data".
	// Use json.Number to preserve integer precision for values > 2^53.
	for _, rawRow := range result.Data {
		var row []interface{}
		dec := json.NewDecoder(strings.NewReader(string(rawRow)))
		dec.UseNumber()
		if err := dec.Decode(&row); err != nil {
			return moerr.NewInternalErrorf(ctx,
				"failed to parse sidecar row: %v", err)
		}
		// Convert json.Number to appropriate Go types based on column type.
		for i, val := range row {
			if num, ok := val.(json.Number); ok {
				if i < len(result.Meta) {
					colType := gpuTypeToMysql(result.Meta[i].Type)
					if colType == defines.MYSQL_TYPE_LONGLONG {
						if v, err := strconv.ParseInt(string(num), 10, 64); err == nil {
							row[i] = v
						} else {
							row[i] = string(num)
						}
					} else if colType == defines.MYSQL_TYPE_FLOAT || colType == defines.MYSQL_TYPE_DOUBLE {
						if v, err := strconv.ParseFloat(string(num), 64); err == nil {
							row[i] = v
						} else {
							row[i] = string(num)
						}
					} else {
						row[i] = string(num)
					}
				} else {
					row[i] = string(num)
				}
			}
		}
		mrs.AddRow(row)
	}

	return nil
}

// gpuTypeToMysql maps DuckDB type names to MySQL column types.
func gpuTypeToMysql(duckType string) defines.MysqlType {
	// Normalize: strip parameters like DECIMAL(38,4) → DECIMAL
	upper := strings.ToUpper(duckType)
	base := upper
	if idx := strings.IndexByte(upper, '('); idx >= 0 {
		base = strings.TrimSpace(upper[:idx])
	}
	switch base {
	case "INTEGER", "INT", "INT32", "BIGINT", "INT64", "SMALLINT", "INT16", "TINYINT", "INT8",
		"UINTEGER", "UINT32", "UBIGINT", "UINT64", "USMALLINT", "UINT16", "UTINYINT", "UINT8",
		"HUGEINT":
		return defines.MYSQL_TYPE_LONGLONG
	case "FLOAT", "REAL", "FLOAT4":
		return defines.MYSQL_TYPE_FLOAT
	case "DOUBLE", "FLOAT8":
		return defines.MYSQL_TYPE_DOUBLE
	case "BOOLEAN", "BOOL":
		return defines.MYSQL_TYPE_BOOL
	case "BLOB", "BYTEA":
		return defines.MYSQL_TYPE_BLOB
	default:
		// DATE, TIMESTAMP, DECIMAL, VARCHAR, etc. — send as VARCHAR.
		// DuckDB's JSON output represents these as strings/numbers that
		// MO's MySQL protocol can send as-is in text form.
		return defines.MYSQL_TYPE_VARCHAR
	}
}
