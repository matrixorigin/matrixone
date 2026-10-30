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

package issues

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/embed"
	"github.com/matrixorigin/matrixone/pkg/tests/testutils"
)

const issue30936ConnectTemplate = "dump:111@tcp(127.0.0.1:%d)/?interpolateParams=false"

func issue30936Open(t *testing.T, c embed.Cluster, cn embed.ServiceOperator) *sql.DB {
	t.Helper()
	db, err := sql.Open("mysql", fmt.Sprintf(
		issue30936ConnectTemplate, cn.GetServiceConfig().CN.Frontend.Port))
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

func issue30936NormalizeShowValue(value interface{}) string {
	switch typed := value.(type) {
	case nil:
		return "<nil>"
	case []byte:
		return string(typed)
	case string:
		return typed
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return fmt.Sprintf("%v", typed)
	}
}

// TestIssue30936IndexPrefixSubPartMetadata covers the consolidated #30935
// acceptance: retained prefix lengths report as numeric SUB_PART / Sub_part
// through information_schema.STATISTICS and SHOW INDEX on every CN, while
// whole-column index parts keep actual SQL NULL instead of the 'NULL' string
// the old facade emitted.
func TestIssue30936IndexPrefixSubPartMetadata(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbName := testutils.GetDatabaseName(t)
		schemaName := strings.ToLower(dbName)

		writer, err := c.GetCNService(0)
		require.NoError(t, err)
		stmt := issue30936Open(t, c, writer)
		defer stmt.Close()
		execSQLRequire(t, ctx, stmt, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, stmt, "drop database if exists `"+dbName+"`")

		// idx_v / idx_txt / idx_name retain prefixes; idx_full and the
		// PRIMARY KEY index whole columns. The multibyte name pins the
		// reported multibyte VARCHAR case against the prefix metadata.
		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936pi ("+
			"id int primary key,"+
			"v varchar(64) default 'value',"+
			"txt text,"+
			"name varchar(30) default '名字',"+
			"key idx_v (v(10)),"+
			"key idx_txt (txt(100)),"+
			"key idx_name (name(12)),"+
			"key idx_full (v, name))")
		execSQLRequire(t, ctx, stmt,
			"insert into `"+dbName+"`.t30936pi values (1, 'value', '中长文本值', '中中中中')")

		expect := map[[2]string]sql.NullInt64{
			{"PRIMARY", "id"}:    {},
			{"idx_v", "v"}:       {Int64: 10, Valid: true},
			{"idx_txt", "txt"}:   {Int64: 100, Valid: true},
			{"idx_name", "name"}: {Int64: 12, Valid: true},
			{"idx_full", "v"}:    {},
			{"idx_full", "name"}: {},
		}
		expectShow := map[[2]string]string{
			{"PRIMARY", "id"}:    "<nil>",
			{"idx_v", "v"}:       "10",
			{"idx_txt", "txt"}:   "100",
			{"idx_name", "name"}: "12",
			{"idx_full", "v"}:    "<nil>",
			{"idx_full", "name"}: "<nil>",
		}

		statisticsQuery := `select index_name, column_name, sub_part
from information_schema.statistics
where table_schema = ? and table_name = 't30936pi'
order by index_name, seq_in_index`
		showStatement := "show index from `" + dbName + "`.t30936pi"

		for _, cnIdx := range []int{0, 1} {
			cn, err := c.GetCNService(cnIdx)
			require.NoError(t, err)
			db := issue30936Open(t, c, cn)
			defer db.Close()

			rows, err := db.QueryContext(ctx, statisticsQuery, schemaName)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			got := make(map[[2]string]sql.NullInt64)
			for rows.Next() {
				var indexName, columnName string
				var subPart sql.NullInt64
				require.NoError(t, rows.Scan(&indexName, &columnName, &subPart))
				got[[2]string{indexName, columnName}] = subPart
			}
			require.NoError(t, rows.Err())
			require.Equal(t, expect, got,
				"CN %d: information_schema.STATISTICS SUB_PART must report retained prefix lengths and SQL NULL otherwise", cnIdx)

			showRows, err := db.QueryContext(ctx, showStatement)
			require.NoError(t, err)
			defer func() { require.NoError(t, showRows.Close()) }()
			shown := make(map[[2]string]string)
			for showRows.Next() {
				var table, keyName, columnName, collation, nullColumn,
					indexType, comment, indexComment, indexParams, visible, expression interface{}
				var nonUnique, seq, cardinality int
				var subPart, packed interface{}
				require.NoError(t, showRows.Scan(
					&table, &nonUnique, &keyName, &seq, &columnName,
					&collation, &cardinality, &subPart, &packed, &nullColumn,
					&indexType, &comment, &indexComment, &indexParams,
					&visible, &expression))
				shown[[2]string{
					issue30936NormalizeShowValue(keyName),
					issue30936NormalizeShowValue(columnName),
				}] = issue30936NormalizeShowValue(subPart)
			}
			require.NoError(t, showRows.Err())
			require.Equal(t, expectShow, shown,
				"CN %d: SHOW INDEX Sub_part must be numeric for retained prefixes and the typed NULL otherwise", cnIdx)
		}
	})
}

// TestIssue30936GeneratedExpressionMetadataLength covers the consolidated
// #30937 acceptance: information_schema.COLUMNS renders the full generated
// expression beyond 500 characters, in agreement with SHOW CREATE TABLE and
// the evaluated column value, while short expressions keep their form.
func TestIssue30936GeneratedExpressionMetadataLength(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbName := testutils.GetDatabaseName(t)
		schemaName := strings.ToLower(dbName)

		writer, err := c.GetCNService(0)
		require.NoError(t, err)
		stmt := issue30936Open(t, c, writer)
		defer stmt.Close()
		execSQLRequire(t, ctx, stmt, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, stmt, "drop database if exists `"+dbName+"`")

		blocks := make([]string, 12)
		for i := range blocks {
			blocks[i] = strings.Repeat(string(rune('a'+i)), 48)
		}
		literals := make([]string, len(blocks))
		for i, block := range blocks {
			literals[i] = "'" + block + "'"
		}
		longExpression := "concat(" + strings.Join(literals, ", ") + ")"
		require.Greater(t, len(longExpression), 500, "the reported expression must exceed the 500-character bound")
		headMarker := blocks[0]
		tailMarker := blocks[len(blocks)-1]

		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936gen ("+
			"id int primary key,"+
			"g varchar(2048) generated always as ("+longExpression+"),"+
			"short varchar(16) generated always as (concat('ab','cd')))")
		execSQLRequire(t, ctx, stmt,
			"insert into `"+dbName+"`.t30936gen (id) values (1)")

		// The evaluated value never depended on metadata rendering; keep it
		// as the control the rendered expression must agree with.
		var evaluated string
		require.NoError(t, stmt.QueryRowContext(ctx,
			"select g from `"+dbName+"`.t30936gen where id = 1").Scan(&evaluated))
		require.Equal(t, strings.Join(blocks, ""), evaluated, "evaluated generated value")

		var table interface{}
		var createSQL string
		require.NoError(t, stmt.QueryRowContext(ctx,
			"show create table `"+dbName+"`.t30936gen").Scan(&table, &createSQL))
		require.Contains(t, createSQL, headMarker, "SHOW CREATE TABLE keeps the head of the expression")
		require.Contains(t, createSQL, tailMarker, "SHOW CREATE TABLE keeps the tail of the expression")

		query := `select column_name, generation_expression
from information_schema.columns
where table_schema = ? and table_name = 't30936gen'
order by ordinal_position`
		for _, cnIdx := range []int{0, 1} {
			cn, err := c.GetCNService(cnIdx)
			require.NoError(t, err)
			db := issue30936Open(t, c, cn)
			defer db.Close()

			rows, err := db.QueryContext(ctx, query, schemaName)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			got := make(map[string]string)
			for rows.Next() {
				var columnName, generationExpression string
				require.NoError(t, rows.Scan(&columnName, &generationExpression))
				got[columnName] = generationExpression
			}
			require.NoError(t, rows.Err())

			long := got["g"]
			require.Greater(t, len(long), 500,
				"CN %d: the generated expression must render past the removed 500-character bound", cnIdx)
			require.Contains(t, long, headMarker,
				"CN %d: the generated expression head must survive rendering", cnIdx)
			require.Contains(t, long, tailMarker,
				"CN %d: the generated expression tail must not be truncated at 500 characters", cnIdx)

			// Both metadata surfaces carry the same tail literal: SHOW CREATE
			// TABLE renders the stored expression, and information_schema
			// must agree with it instead of a 500-character cut.
			require.Contains(t, createSQL, tailMarker,
				"CN %d: SHOW CREATE TABLE keeps the same expression tail", cnIdx)

			require.Contains(t, got["short"], "ab")
			require.Contains(t, got["short"], "cd",
				"CN %d: short generated expressions keep their rendering", cnIdx)
		}
	})
}

// TestIssue30936DeclaredLabelEncodingBoundary covers the section-2.3 encoding
// oracle from the approved design: the persisted attr_enum format cannot
// distinguish ENUM('a,b') from ENUM('a','b') (a pre-existing comma-escaping
// catalog defect with its own follow-up), so both colliding declarations
// deterministically report the boundary interpretation - the two-member
// lengths 1/4 with no numeric metadata - the same reading every existing
// attr_enum consumer (SHOW COLUMNS, SHOW CREATE TABLE) renders, while the
// unambiguous control ENUM('ab','c') reports exactly 2/8, and a comma inside
// a SET member is rejected at declaration, keeping the SET encoding
// unambiguous by construction.
func TestIssue30936DeclaredLabelEncodingBoundary(t *testing.T) {
	embed.RunBaseClusterTests(t, func(c embed.Cluster) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()

		dbName := testutils.GetDatabaseName(t)
		schemaName := strings.ToLower(dbName)

		writer, err := c.GetCNService(0)
		require.NoError(t, err)
		stmt := issue30936Open(t, c, writer)
		defer stmt.Close()
		execSQLRequire(t, ctx, stmt, "create database `"+dbName+"`")
		defer execSQLMaybe(t, ctx, stmt, "drop database if exists `"+dbName+"`")

		execSQLRequire(t, ctx, stmt, "create table `"+dbName+"`.t30936eb ("+
			"id int primary key,"+
			"e_collide enum('a,b'),"+
			"e_split enum('a','b'),"+
			"e_control enum('ab','c'))")

		// A comma inside a SET member is the one declaration the encoding
		// cannot survive; it must never reach the catalog.
		_, err = stmt.ExecContext(ctx, "create table `"+dbName+"`.t30936eb_bad (s set('a,b'))")
		require.Error(t, err, "SET('a,b') must be rejected at declaration")
		require.Contains(t, err.Error(), "set member",
			"the declaration rejection must name the offending set member")

		// The boundary values: both colliding declarations persist "a,b" and
		// both report the two-member interpretation; the control is exact.
		charMeta := map[string][2]sql.NullInt64{
			"e_collide": {{Int64: 1, Valid: true}, {Int64: 4, Valid: true}},
			"e_split":   {{Int64: 1, Valid: true}, {Int64: 4, Valid: true}},
			"e_control": {{Int64: 2, Valid: true}, {Int64: 8, Valid: true}},
		}
		query := `select column_name, character_maximum_length, character_octet_length,
coalesce(numeric_precision, -1), coalesce(numeric_scale, -1)
from information_schema.columns
where table_schema = ? and table_name = 't30936eb'
and column_name in ('e_collide', 'e_split', 'e_control')`
		for _, cnIdx := range []int{0, 1} {
			cn, err := c.GetCNService(cnIdx)
			require.NoError(t, err)
			db := issue30936Open(t, c, cn)
			defer db.Close()

			rows, err := db.QueryContext(ctx, query, schemaName)
			require.NoError(t, err)
			defer func() { require.NoError(t, rows.Close()) }()
			got := make(map[string][2]sql.NullInt64)
			for rows.Next() {
				var columnName string
				var charLen, octetLen sql.NullInt64
				var precision, scale int64
				require.NoError(t, rows.Scan(&columnName, &charLen, &octetLen, &precision, &scale))
				require.Equal(t, int64(-1), precision,
					"CN %d: column %s must not be misreported as numeric", cnIdx, columnName)
				require.Equal(t, int64(-1), scale,
					"CN %d: column %s must not be misreported as numeric", cnIdx, columnName)
				got[columnName] = [2]sql.NullInt64{charLen, octetLen}
			}
			require.NoError(t, rows.Err())
			require.Equal(t, charMeta, got,
				"CN %d: the colliding declarations keep the boundary interpretation and the control stays exact", cnIdx)
		}

		// The boundary agrees with what the system itself renders for the
		// ambiguous class: SHOW CREATE TABLE reads the same persisted string
		// through the same split interpretation.
		for _, cnIdx := range []int{0, 1} {
			cn, err := c.GetCNService(cnIdx)
			require.NoError(t, err)
			db := issue30936Open(t, c, cn)
			defer db.Close()

			showRows, err := db.QueryContext(ctx,
				"show create table `"+dbName+"`.t30936eb")
			require.NoError(t, err)
			defer func() { require.NoError(t, showRows.Close()) }()
			require.True(t, showRows.Next(), "CN %d: SHOW CREATE TABLE must return a row", cnIdx)
			var table, createSQL string
			require.NoError(t, showRows.Scan(&table, &createSQL))
			require.False(t, showRows.Next(), "CN %d: SHOW CREATE TABLE must return exactly one row", cnIdx)
			require.NoError(t, showRows.Err())
			require.Contains(t, strings.ToLower(createSQL), "enum('a','b')",
				"CN %d: the colliding and split columns render the split interpretation identically", cnIdx)
		}
	})
}
