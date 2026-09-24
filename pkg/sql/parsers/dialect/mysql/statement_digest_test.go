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
	for _, sql := range []string{"", "/* only a comment */", "SELECT ?", "SELECT 1; SELECT 2", "SELECT 'unterminated"} {
		_, err := NormalizeStatementDigest(context.Background(), sql, "", 1024)
		require.Error(t, err, sql)
	}
}

func TestNormalizeStatementDigestMySQLCompatibilityEdges(t *testing.T) {
	for _, test := range []struct {
		sql  string
		want string
	}{
		{sql: "SELECT -1, +2", want: "SELECT ?, ?"},
		{sql: "SELECT X'4142', B'0101', 0x4142, 0b0101", want: "SELECT ?, ?, ?, ?"},
		{sql: "SELECT * FROM t WHERE (a, b) IN ((1, 2), (3, 4))", want: "SELECT * FROM `t` WHERE (`a`, `b`) IN (...)"},
		{sql: "SELECT _utf8mb4'hello', N'world'", want: "SELECT (_charset) ?, ?"},
		{sql: "SELECT @'secret', @@session.sql_mode, @name", want: "SELECT ?, @@session.sql_mode, ?"},
		{sql: "SELECT /*!80000 SQL_NO_CACHE */ 1 /*!90000 + 2 */", want: "SELECT SQL_NO_CACHE ?"},
		{sql: "SELECT /*+ MAX_EXECUTION_TIME(1000) */ * FROM t WHERE id = 1", want: "SELECT /*+ MAX_EXECUTION_TIME(?) */ * FROM `t` WHERE `id` = ?"},
	} {
		got, err := NormalizeStatementDigest(context.Background(), test.sql, "", 1024)
		require.NoError(t, err, test.sql)
		require.Equal(t, test.want, got, test.sql)
	}
}

func TestNormalizeStatementDigestRejectsMalformedInputAfterTruncation(t *testing.T) {
	_, err := NormalizeStatementDigest(context.Background(), "SELECT 1, _utf8", "", 4)
	require.Error(t, err)

	_, err = NormalizeStatementDigest(context.Background(), string([]byte{'S', 'E', 'L', 'E', 'C', 'T', ' ', 0xff}), "", 1024)
	require.Error(t, err)

	_, err = NormalizeStatementDigest(context.Background(), "SELECT /*!80000 1", "", 1024)
	require.Error(t, err)
}
