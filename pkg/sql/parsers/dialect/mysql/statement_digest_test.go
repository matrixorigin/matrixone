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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeStatementDigest(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "issue example", sql: "SELECT 1", want: "SELECT ?"},
		{name: "comments and whitespace", sql: "  select 2 /* comment */ where 10=20; -- tail\n", want: "SELECT ? WHERE ? = ? ;"},
		{name: "identifiers", sql: "SELECT a + b, a - b FROM t1,t2,t3 WHERE a=c", want: "SELECT `a` + `b` , `a` - `b` FROM `t1` , `t2` , `t3` WHERE `a` = `c`"},
		{name: "value list", sql: "SELECT 1,2,3", want: "SELECT ?, ..."},
		{name: "row value list", sql: "INSERT INTO t VALUES (1,2),(3,4)", want: "INSERT INTO `t` VALUES (...) /* , ... */"},
		{name: "in lists", sql: "SELECT * FROM t WHERE a IN (1,2,3) AND b IN ((1,2),(3,4))", want: "SELECT * FROM `t` WHERE `a` IN (...) AND `b` IN ( (...) /* , ... */ )"},
		{name: "boolean and null values", sql: "SELECT TRUE,FALSE,NULL", want: "SELECT TRUE , FALSE , ?"},
		{name: "is null operator", sql: "SELECT * FROM t WHERE a IS NULL OR b IS NOT NULL", want: "SELECT * FROM `t` WHERE `a` IS NULL OR `b` IS NOT NULL"},
		{name: "unary signs", sql: "SELECT -1,+2,a-3", want: "SELECT ?, ... , `a` - ?"},
		{name: "qualified identifier and limit", sql: "SELECT db.t.a FROM db.t AS x LIMIT 10 OFFSET 2", want: "SELECT `db` . `t` . `a` FROM `db` . `t` AS `x` LIMIT ? OFFSET ?"},
		{name: "unicode identifier", sql: "SELECT 数量 FROM 订单 WHERE 编号 = 42", want: "SELECT `数量` FROM `订单` WHERE `编号` = ?"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeStatementDigest(context.Background(), test.sql, "", 1024)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestNormalizeStatementDigestSQLMode(t *testing.T) {
	got, err := NormalizeStatementDigest(context.Background(), `SELECT "column" FROM t WHERE a || b`, "ANSI_QUOTES,PIPES_AS_CONCAT", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT `column` FROM `t` WHERE `a` || `b`", got)

	got, err = NormalizeStatementDigest(context.Background(), `SELECT /*+ INDEX(t "idx") */ * FROM t`, "", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT /*+ INDEX ( `t` ? ) */ * FROM `t`", got)

	got, err = NormalizeStatementDigest(context.Background(), `SELECT /*+ INDEX(t "idx") */ * FROM t`, "ANSI_QUOTES", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT /*+ INDEX ( `t` `idx` ) */ * FROM `t`", got)
}

func TestNormalizeStatementDigestMySQLCounterexamples(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{name: "single value parentheses", sql: "SELECT (1), ((1)), ABS(1), COALESCE(1)", want: "SELECT (?) , ( (?) ) , `ABS` (?) , COALESCE (?)"},
		{name: "single column row list", sql: "INSERT INTO t(a) VALUES (1),(2),(3)", want: "INSERT INTO `t` ( `a` ) VALUES (?) /* , ... */"},
		{name: "operator aliases", sql: "SELECT a<>b, c REGEXP 'x+'", want: "SELECT `a` != `b` , `c` RLIKE ?"},
		{name: "charset introducers", sql: "SELECT _utf8mb4'你好', _binary'abc', N'hello'", want: "SELECT (_charset) ? , (_charset) ?, ..."},
		{name: "charset introducer variants", sql: "SELECT _utf8'a', _utf8mb3'b', _latin1'c', _ascii'd', _ucs2'e', _utf16'f', _utf32'g', _foo'h'", want: "SELECT (_charset) ? , (_charset) ? , (_charset) ? , (_charset) ? , (_charset) ? , (_charset) ? , (_charset) ? , `_foo` ?"},
		{name: "charset introducer comments", sql: "SELECT _utf8/*c*/'a', _latin1 -- c\n'b', _ascii#c\n'c'", want: "SELECT (_charset) ? , (_charset) ? , (_charset) ?"},
		{name: "scoped system variables", sql: "SELECT @@sql_mode, @@session.sql_mode, @@global.max_connections", want: "SELECT @@`sql_mode` , @@SESSION . `sql_mode` , @@GLOBAL . `max_connections`"},
		{name: "unary chains and not2", sql: "SELECT --1, +-2, -+3, !!a, ! !a, !!!!a", want: "SELECT ?, ... , ! `a` , ! ! `a` , ! ! `a`"},
		{name: "unary versus binary context", sql: "SELECT a=-1, (-2), 1+-3, 1=-4", want: "SELECT `a` = - ? , (?) , ? + ?, ... = - ?"},
		{name: "trailing delimiter", sql: "SELECT 1;", want: "SELECT ? ;"},
		{name: "keyword canonicalization", sql: "CREATE TABLE t(a INT, b INT1, c INT2, d INT3, e INT4, f MEDIUMINT, g BIGINT, h FLOAT, i DOUBLE, j CHAR(1), k VARCHAR(2))", want: "CREATE TABLE `t` ( `a` INTEGER , `b` TINYINT , `c` SMALLINT , `d` MIDDLEINT , `e` INTEGER , `f` MIDDLEINT , `g` INT8 , `h` FLOAT4 , `i` FLOAT8 , `j` CHARACTER (?) , `k` VARCHARACTER (?) )"},
		{name: "statement keyword aliases", sql: "CREATE DATABASE d", want: "CREATE SCHEMA `d`"},
		{name: "describe alias", sql: "DESCRIBE t", want: "EXPLAIN `t`"},
		{name: "select keyword aliases", sql: "SELECT DISTINCT a, CURRENT_TIMESTAMP, LOCALTIME, LOCALTIMESTAMP FROM t WHERE a = ANY (SELECT b FROM u)", want: "SELECT DISTINCTROW `a` , NOW , NOW , NOW FROM `t` WHERE `a` = SOME ( SELECT `b` FROM `u` )"},
		{name: "show columns alias", sql: "SHOW COLUMNS FROM t", want: "SHOW FIELDS FROM `t`"},
		{name: "show databases alias", sql: "SHOW DATABASES", want: "SHOW SCHEMAS"},
		{name: "interval units", sql: "SELECT NOW()+INTERVAL 1 QUARTER, NOW()+INTERVAL 1 MONTH, NOW()+INTERVAL 1 DAY, NOW()+INTERVAL 1 HOUR, NOW()+INTERVAL 1 MINUTE, NOW()+INTERVAL 1 SECOND", want: "SELECT NOW ( ) + INTERVAL ? SQL_TSI_QUARTER , NOW ( ) + INTERVAL ? SQL_TSI_MONTH , NOW ( ) + INTERVAL ? SQL_TSI_DAY , NOW ( ) + INTERVAL ? SQL_TSI_HOUR , NOW ( ) + INTERVAL ? SQL_TSI_MINUTE , NOW ( ) + INTERVAL ? SQL_TSI_SECOND"},
		{name: "quoted identifier contents", sql: "SELECT `a``b` FROM `t``x`", want: "SELECT `a`b` FROM `t`x`"},
		{name: "optimizer hint numeric", sql: "SELECT /*+ MAX_EXECUTION_TIME(1000) */ 1", want: "SELECT /*+ MAX_EXECUTION_TIME (?) */ ?"},
		{name: "optimizer hint identifiers", sql: "SELECT /*+ JOIN_ORDER(t1,t2) */ * FROM t1 JOIN t2", want: "SELECT /*+ JOIN_ORDER ( `t1` , `t2` ) */ * FROM `t1` JOIN `t2`"},
		{name: "optimizer hint assignment", sql: "SELECT /*+ SET_VAR(sort_buffer_size=1000) */ 1", want: "SELECT /*+ SET_VAR ( `sort_buffer_size` = ? ) */ ?"},
		{name: "only first optimizer hint comment", sql: "SELECT /*+ BKA(t1) */ /*+ NO_BKA(t2) */ * FROM t1 JOIN t2", want: "SELECT /*+ BKA ( `t1` ) */ * FROM `t1` JOIN `t2`"},
		{name: "optimizer hint must immediately follow keyword", sql: "SELECT /* ordinary */ /*+ BKA(t1) */ * FROM t1", want: "SELECT * FROM `t1`"},
		{name: "optimizer hint query block", sql: "SELECT /*+ INDEX(@qb t idx) */ * FROM t", want: "SELECT /*+ INDEX ( @`qb` `t` `idx` ) */ * FROM `t`"},
		{name: "optimizer hint table query block", sql: "SELECT /*+ INDEX(t@qb idx) */ * FROM t", want: "SELECT /*+ INDEX ( `t`@`qb` `idx` ) */ * FROM `t`"},
		{name: "optimizer hint qualified identifier", sql: "SELECT /*+ INDEX(db.t idx) */ * FROM db.t", want: "SELECT /*+ INDEX ( `db` `t` `idx` ) */ * FROM `db` . `t`"},
		{name: "optimizer hint hexadecimal argument", sql: "SELECT /*+ MAX_EXECUTION_TIME(0x10) */ 1", want: "SELECT /*+ MAX_EXECUTION_TIME ( `0x10` ) */ ?"},
		{name: "optimizer hint arithmetic", sql: "SELECT /*+ MAX_EXECUTION_TIME(1+2) */ 1", want: "SELECT /*+ MAX_EXECUTION_TIME ( ? + ? ) */ ?"},
		{name: "optimizer hint identifier value", sql: "INSERT /*+ SET_VAR(foreign_key_checks=OFF) */ INTO t VALUES(1)", want: "INSERT /*+ SET_VAR ( `foreign_key_checks` = `OFF` ) */ INTO `t` VALUES (?)"},
		{name: "optimizer hint numeric lexer", sql: "SELECT /*+ SET_VAR(a=1e3) SET_VAR(b=16K) SET_VAR(c=6.0) SET_VAR(d=.5) SET_VAR(e=6.) */ 1", want: "SELECT /*+ SET_VAR ( `a` = `1e3` ) SET_VAR ( `b` = ? ) SET_VAR ( `c` = ? ) SET_VAR ( `d` = ? ) SET_VAR ( `e` = ) */ ?"},
		{name: "optimizer hint signed contexts", sql: "SELECT /*+ MAX_EXECUTION_TIME(-1) SET_VAR(x=-1) */ 1", want: "SELECT /*+ MAX_EXECUTION_TIME (?) SET_VAR ( `x` = - ? ) */ ?"},
		{name: "unknown optimizer hint", sql: "SELECT /*+ FOO(t) BKA(t) */ * FROM t", want: "SELECT /*+ `FOO` ( `t` ) BKA ( `t` ) */ * FROM `t`"},
		{name: "malformed optimizer hint string", sql: "SELECT /*+ SET_VAR(x='abc) */ 1", want: "SELECT /*+ SET_VAR ( `x` = */ ?"},
		{name: "optimizer hint doubled backtick", sql: "SELECT /*+ INDEX(t `a``b`) */ * FROM t", want: "SELECT /*+ INDEX ( `t` `a`b` ) */ * FROM `t`"},
		{name: "malformed optimizer hint tokens", sql: "SELECT /*+ SET_VAR(x=.5a) SET_VAR(y=6.0a) FOO(@) */ 1", want: "SELECT /*+ SET_VAR ( `x` = `a` ) SET_VAR ( `y` = `a` ) `FOO` ( ) */ ?"},
		{name: "empty optimizer hint", sql: "SELECT /*+ */ 1", want: "SELECT ?"},
		{name: "null column attributes", sql: "CREATE TABLE t(a INT NULL, b VARCHAR(10) NOT NULL, c INT DEFAULT NULL, d INT NULL DEFAULT NULL)", want: "CREATE TABLE `t` ( `a` INTEGER NULL , `b` VARCHARACTER (?) NOT NULL , `c` INTEGER DEFAULT ? , `d` INTEGER NULL DEFAULT ? )"},
		{name: "null reference action", sql: "CREATE TABLE t(a INT, FOREIGN KEY(a) REFERENCES p(a) ON DELETE SET NULL)", want: "CREATE TABLE `t` ( `a` INTEGER , FOREIGN KEY ( `a` ) REFERENCES `p` ( `a` ) ON DELETE SET NULL )"},
		{name: "null MatrixOne fill mode", sql: "SELECT _wstart, sum(v) FROM t GROUP BY id INTERVAL(ts, 1, MINUTE) FILL(NULL)", want: "SELECT `_wstart` , SUM ( `v` ) FROM `t` GROUP BY `id` INTERVAL ( `ts` , ? , SQL_TSI_MINUTE ) FILL ( NULL )"},
		{name: "MySQL NCHAR column type", sql: "CREATE TABLE t(a NCHAR(3), b CHAR(2))", want: "CREATE TABLE `t` ( `a` NCHAR (?) , `b` CHARACTER (?) )"},
		{name: "MySQL row constructor", sql: "SELECT ROW(1,2) FROM t", want: "SELECT ROW (...) FROM `t`"},
		{name: "MySQL sounds like", sql: "SELECT a SOUNDS LIKE b FROM t", want: "SELECT `a` SOUNDS LIKE `b` FROM `t`"},
		{name: "quoted user variables", sql: "SELECT @x, @'odd name', @`odd``name`", want: "SELECT @? , @? , @`odd`name`"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := NormalizeStatementDigest(context.Background(), test.sql, "", 1024)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestNormalizeStatementDigestRejectsInvalidInput(t *testing.T) {
	for _, test := range []struct {
		name string
		sql  string
	}{
		{name: "empty", sql: ""},
		{name: "comment only", sql: "/* only a comment */"},
		{name: "incomplete statement", sql: "SELECT"},
		{name: "multiple statements", sql: "SELECT 1; SELECT 2"},
		{name: "parameter marker", sql: "SELECT ?"},
		{name: "parameter after digest truncation", sql: "SELECT " + strings.Repeat("a,", 1000) + "?"},
		{name: "unterminated string", sql: "SELECT 'unterminated"},
		{name: "unterminated ordinary comment", sql: "SELECT /* unterminated"},
		{name: "unterminated optimizer hint", sql: "SELECT /*+ MAX_EXECUTION_TIME(1)"},
		{name: "embedded nul", sql: "SELECT \x00"},
		{name: "invalid utf8", sql: string([]byte("SELECT \xff"))},
		{name: "charset introducer without literal", sql: "SELECT _utf8"},
		{name: "charset introducer before expression", sql: "SELECT _latin1 + 1"},
		{name: "single element ROW is not a row constructor", sql: "SELECT ROW(1) FROM t"},
		{name: "comment comma is not a ROW element", sql: "SELECT ROW(1 /* , */) FROM t"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NormalizeStatementDigest(context.Background(), test.sql, "", 1024)
			require.Error(t, err)
		})
	}
}

func TestNormalizeStatementDigestVariablesOperatorsAndLimit(t *testing.T) {
	got, err := NormalizeStatementDigest(context.Background(), "SELECT 1 FROM dual WHERE @x != @@sql_mode", "", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT ? FROM DUAL WHERE @? != @@`sql_mode`", got)

	got, err = NormalizeStatementDigest(context.Background(), "SELECT 1 && 0, 1 || 0", "", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT ? && ?, ... || ?", got)

	got, err = NormalizeStatementDigest(context.Background(), "SELECT a<=b, a>=b, a<=>b, a<<1, a>>1, doc->'$.a', doc->>'$.b' FROM t", "", 1024)
	require.NoError(t, err)
	require.Equal(t, "SELECT `a` <= `b` , `a` >= `b` , `a` <=> `b` , `a` << ? , `a` >> ? , `doc` -> ? , `doc` ->> ? FROM `t`", got)

	got, err = NormalizeStatementDigest(context.Background(), "SET @x := 1", "", 1024)
	require.NoError(t, err)
	require.Equal(t, "SET @? := ?", got)

	longSQL := "SELECT " + strings.Repeat("a,", 1000) + "a"
	got, err = NormalizeStatementDigest(context.Background(), longSQL, "", 1024)
	require.NoError(t, err)
	require.Equal(t, 882, len(got))
	require.True(t, strings.HasSuffix(got, ","))

	for _, test := range []struct {
		maxLength int
		want      string
	}{
		{maxLength: 0, want: ""},
		{maxLength: 1, want: ""},
		{maxLength: 2, want: "SELECT"},
		{maxLength: 3, want: "SELECT"},
		{maxLength: 4, want: "SELECT ?"},
	} {
		got, err = NormalizeStatementDigest(context.Background(), "SELECT 1", "", test.maxLength)
		require.NoError(t, err)
		require.Equal(t, test.want, got)
	}

	got, err = NormalizeStatementDigest(context.Background(), "SELECT a", "", 6)
	require.NoError(t, err)
	require.Equal(t, "SELECT", got)
	got, err = NormalizeStatementDigest(context.Background(), "SELECT a", "", 7)
	require.NoError(t, err)
	require.Equal(t, "SELECT `a`", got)
}

func BenchmarkNormalizeStatementDigest(b *testing.B) {
	const sql = "SELECT a, b FROM orders WHERE account_id = 42 AND state IN ('new', 'paid', 'done')"
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := NormalizeStatementDigest(ctx, sql, "", 1024); err != nil {
			b.Fatal(err)
		}
	}
}
