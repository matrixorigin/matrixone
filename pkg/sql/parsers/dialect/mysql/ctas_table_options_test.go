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

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// MySQL accepts table options before the AS SELECT of CREATE TABLE ... SELECT
// but rejects them after it, so options must be parsed before the query,
// preserved in tree.CreateTable.Options, and the formatted replay must stay in
// the legal (options-first) order (#32542 gap 2).
func TestCTASTableOptionsBeforeAsSelectParsePreserveAndRoundtrip(t *testing.T) {
	cases := []struct {
		sql          string
		wantOptions  int
		wantCharset  string
		wantCollate  string
		wantConflict string
		wantTemp     bool
		wantIfNot    bool
		wantDefs     int
	}{
		{sql: "CREATE TABLE ct1 charset utf8mb4 AS SELECT 1 AS a", wantOptions: 1, wantCharset: "utf8mb4"},
		{sql: "CREATE TABLE ct2 DEFAULT CHARSET utf8mb4 COLLATE utf8mb4_general_ci AS SELECT 1 AS a", wantOptions: 2, wantCharset: "utf8mb4", wantCollate: "utf8mb4_general_ci"},
		{sql: "CREATE TABLE ct3 (b INT PRIMARY KEY, c VARCHAR(8)) charset utf8mb4 AS SELECT b FROM src", wantOptions: 1, wantCharset: "utf8mb4", wantDefs: 2},
		{sql: "CREATE TEMPORARY TABLE IF NOT EXISTS ct4 charset utf8mb4 AS SELECT 1", wantOptions: 1, wantCharset: "utf8mb4", wantTemp: true, wantIfNot: true},
		{sql: "CREATE TABLE ct5 charset utf8mb4 IGNORE AS SELECT 1", wantOptions: 1, wantCharset: "utf8mb4", wantConflict: "ignore"},
		{sql: "CREATE TABLE ct6 (a INT) charset utf8mb4 REPLACE AS SELECT 1", wantOptions: 1, wantCharset: "utf8mb4", wantConflict: "replace", wantDefs: 1},
		{sql: "CREATE TABLE ct7 (a INT) charset utf8mb4 IGNORE SELECT 1", wantOptions: 1, wantCharset: "utf8mb4", wantConflict: "ignore", wantDefs: 1},
		{sql: "CREATE TABLE ct8 charset utf8mb4 AS SELECT 1 UNION ALL SELECT 2", wantOptions: 1, wantCharset: "utf8mb4"},
		{sql: "CREATE TABLE ct9 DEFAULT CHARSET utf8mb4 AS SELECT 1 AS a", wantOptions: 1, wantCharset: "utf8mb4"},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err, tc.sql)
			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, tc.sql)
			require.True(t, create.IsAsSelect, tc.sql)
			require.NotNil(t, create.AsSource, tc.sql)
			require.Len(t, create.Options, tc.wantOptions, tc.sql)
			require.Equal(t, tc.wantTemp, create.Temporary, tc.sql)
			require.Equal(t, tc.wantIfNot, create.IfNotExists, tc.sql)
			require.Equal(t, tc.wantConflict, create.CTASConflict, tc.sql)
			require.Len(t, create.Defs, tc.wantDefs, tc.sql)
			var gotCharset, gotCollate string
			for _, opt := range create.Options {
				switch o := opt.(type) {
				case *tree.TableOptionCharset:
					gotCharset = o.Charset
				case *tree.TableOptionCollate:
					gotCollate = o.Collate
				}
			}
			require.Equal(t, tc.wantCharset, gotCharset, tc.sql)
			require.Equal(t, tc.wantCollate, gotCollate, tc.sql)

			// The replay must keep options before AS SELECT so it parses again,
			// and the reparsed statement must preserve the key CTAS fields.
			formatted := tree.String(create, dialect.MYSQL)
			reStmt, err := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, err, formatted)
			reCreate, ok := reStmt.(*tree.CreateTable)
			require.True(t, ok, formatted)
			require.True(t, reCreate.IsAsSelect, formatted)
			require.NotNil(t, reCreate.AsSource, formatted)
			require.Len(t, reCreate.Options, tc.wantOptions, formatted)
			require.Equal(t, tc.wantConflict, reCreate.CTASConflict, formatted)
			require.Equal(t, tc.wantTemp, reCreate.Temporary, formatted)
			require.Equal(t, tc.wantIfNot, reCreate.IfNotExists, formatted)
			require.Len(t, reCreate.Defs, tc.wantDefs, formatted)
			var reCharset, reCollate string
			for _, opt := range reCreate.Options {
				switch o := opt.(type) {
				case *tree.TableOptionCharset:
					reCharset = o.Charset
				case *tree.TableOptionCollate:
					reCollate = o.Collate
				}
			}
			require.Equal(t, tc.wantCharset, reCharset, formatted)
			require.Equal(t, tc.wantCollate, reCollate, formatted)
		})
	}
}

// Options after the query are not accepted MySQL syntax; the issue report's
// "either side" suggestion is rejected on purpose to keep the grammar honest.
func TestCTASTableOptionsAfterSelectStillRejected(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE ct1 AS SELECT 1 AS a charset utf8mb4",
		"CREATE TABLE ct1 AS SELECT 1 AS a DEFAULT CHARSET utf8mb4",
		"CREATE TABLE ct1 AS SELECT 1 AS a COLLATE utf8mb4_general_ci",
		"CREATE TABLE (b INT) ct1 AS SELECT 1 charset utf8mb4",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		})
	}
}

// The existing CTAS SELECT INTO rejection must keep firing when options are
// present, instead of the options rule swallowing the check.
func TestCTASTableOptionsKeepSelectIntoRejected(t *testing.T) {
	for _, sql := range []string{
		"CREATE TABLE t charset utf8mb4 AS SELECT 1 INTO OUTFILE 'x'",
		"CREATE TABLE t charset utf8mb4 AS SELECT 1 INTO dumpfile 'x'",
	} {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.Error(t, err, sql)
		})
	}
}
