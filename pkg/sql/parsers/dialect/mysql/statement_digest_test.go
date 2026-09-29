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

	"github.com/stretchr/testify/require"
)

func TestNormalizeStatementDigest(t *testing.T) {
	for _, test := range []struct {
		sql  string
		mode string
		max  int
		want string
	}{
		{sql: "SELECT 1", max: 1024, want: "SELECT ?"},
		{sql: "  select 2 /* comment */ where 10=20; -- tail\n", max: 1024, want: "SELECT ? WHERE ? = ?"},
		{sql: "SELECT * FROM t WHERE id IN (1,2,3)", max: 1024, want: "SELECT * FROM `t` WHERE `id` IN (...)"},
		{sql: "SELECT -1, +2", max: 1024, want: "SELECT ?, ..."},
		{sql: `SELECT @'secret', @@session.sql_mode, @name`, max: 1024, want: "SELECT @? , @@SESSION . `sql_mode` , @?"},
		{sql: `SELECT @'secret', @"name"`, mode: "ANSI_QUOTES", max: 1024, want: "SELECT @? , @`name`"},
		{sql: "SELECT _utf8mb4 X'4142'", max: 1024, want: "SELECT (_charset) ?"},
		{sql: "SELECT _binary B'01', _latin1 0x41, _ascii 0b01, N'x'", max: 1024, want: "SELECT (_charset) ? , (_charset) ? , (_charset) ?, ..."},
		{sql: "SELECT * FROM t WHERE (a,b) IN ((1,2),(3,4))", max: 1024, want: "SELECT * FROM `t` WHERE ( `a` , `b` ) IN ( (...) /* , ... */ )"},
		{sql: "SELECT ROW(1,2)", max: 1024, want: "SELECT ROW (...)"},
		{sql: "SELECT ROW((1,2),3)", max: 1024, want: "SELECT ROW ( (...) , ? )"},
		{sql: "SELECT ROW('a,b', /* , */ 3)", max: 1024, want: "SELECT ROW (...)"},
		{sql: "CREATE TABLE t (a INT NULL, b INT NOT NULL DEFAULT NULL, c INT DEFAULT 1 + NULL)", max: 1024, want: "CREATE TABLE `t` ( `a` INTEGER NULL , `b` INTEGER NOT NULL DEFAULT ? , `c` INTEGER DEFAULT ? + ? )"},
		{sql: "SELECT /*!80000 SQL_NO_CACHE */ 1 /*!90000 + 2 */", max: 1024, want: "SELECT SQL_NO_CACHE ?"},
		{sql: "SELECT /*!80000 */ /*+ MAX_EXECUTION_TIME(1000) */ 1", max: 1024, want: "SELECT ?"},
		{sql: "SELECT CAST('x' AS NCHAR), 'a' SOUNDS LIKE 'b'", max: 1024, want: "SELECT CAST ( ? AS NCHAR ) , ? SOUNDS LIKE ?"},
		{sql: "SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t WHERE id=1", max: 1024, want: "SELECT /*+ MAX_EXECUTION_TIME (?) */ * FROM `t` WHERE `id` = ?"},
		{sql: "SELECT /*+ QB_NAME(qb) INDEX(t@qb idx) SET_VAR(sort_buffer_size=16M) */ 1", max: 1024, want: "SELECT /*+ QB_NAME ( `qb` ) INDEX ( `t`@`qb` `idx` ) SET_VAR ( `sort_buffer_size` = ? ) */ ?"},
		{sql: "SELECT /*+ QB_NAME(@qb) SET_VAR(sort_buffer_size=1G) */ 1", max: 1024, want: "SELECT /*+ QB_NAME ( @`qb` ) SET_VAR ( `sort_buffer_size` = ? ) */ ?"},
		{sql: `SELECT /*+ QB_NAME("qb") */ 1`, mode: "ANSI_QUOTES", max: 1024, want: "SELECT /*+ QB_NAME ( `qb` ) */ ?"},
		{sql: "SELECT CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP", max: 1024, want: "SELECT CURDATE , CURTIME , NOW"},
		{sql: "SELECT NULL, id IS NULL", max: 1024, want: "SELECT ? , `id` IS NULL"},
		{sql: "SELECT SESSION_USER, SESSION_USER /* call */ ()", mode: "IGNORE_SPACE", max: 1024, want: "SELECT `SESSION_USER` , SYSTEM_USER ( )"},
		{sql: "SELECT 'a' REGEXP 'b'", max: 1024, want: "SELECT ? RLIKE ?"},
		{sql: "SELECT '$tag$abc$tag$'", max: 1024, want: "SELECT ?"},
		{sql: `SELECT "column" FROM t`, mode: "ANSI_QUOTES", max: 1024, want: "SELECT `column` FROM `t`"},
		{sql: "SELECT 1;", max: 4, want: "SELECT ?"},
		{sql: "SELECT 1", max: 0, want: ""},
	} {
		got, err := NormalizeStatementDigest(context.Background(), test.sql, test.mode, test.max)
		require.NoError(t, err, test.sql)
		require.Equal(t, test.want, got, test.sql)
	}
}

func TestNormalizeStatementDigestRejectsInvalidStatements(t *testing.T) {
	for _, sql := range []string{"", "/* only a comment */", "SELECT ?", "SELECT 1; SELECT 2", "SELECT 'unterminated", "SELECT $tag$abc$tag$", "SELECT /* comment */ $tag$abc$tag$"} {
		_, err := NormalizeStatementDigest(context.Background(), sql, "", 1024)
		require.Error(t, err, sql)
	}
}

func TestNormalizeStatementDigestRejectsMalformedInputAfterTruncation(t *testing.T) {
	_, err := NormalizeStatementDigest(context.Background(), "SELECT 1, _utf8", "", 4)
	require.Error(t, err)

	// MySQL 8.4 permits dollar-quoted strings only in stored-program routine
	// bodies, not as ordinary expression literals. Reject them even when
	// max_digest_length would otherwise stop collection before the token.
	_, err = NormalizeStatementDigest(context.Background(), "SELECT 1, $$/*!80000 1 */$$", "", 4)
	require.Error(t, err)

	_, err = NormalizeStatementDigest(context.Background(), string([]byte{'S', 'E', 'L', 'E', 'C', 'T', ' ', 0xff}), "", 1024)
	require.Error(t, err)

	_, err = NormalizeStatementDigest(context.Background(), "SELECT /*!80000 1", "", 1024)
	require.Error(t, err)
}
