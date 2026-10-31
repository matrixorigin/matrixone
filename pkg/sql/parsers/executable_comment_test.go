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

package parsers

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestExecutableCommentVersionGate(t *testing.T) {
	for _, tc := range []struct {
		sql, want string
		wantErr   bool
	}{
		{"/*!99999 select 42 */;", "", false},
		{"select /*!99999 6 */", "", true},
		{"select /*!80031 6 */", "", true},
		{"select /*!80047 6 */", "", true},
		{"select 2 /*!999999999 */", "select 2", false},
		{"select /*!80030 6 */", "select 6", false},
		{"select /*!50000 6 */", "select 6", false},
		{"select /*! 6 */", "select 6", false},
		{"select 2 /* plain */", "select 2", false},
		{"select /*!80029 6 */", "select 6", false},
		{"select /*!00000 6 */", "select 6", false},
		{"select 2 /*!800300 + 1 */", "select 2", false},
		{"select /*!080030 6 */", "select 6", false},
		{"select /*!00080030 6 */", "select 6", false},
		{"select 2 /*!00080031 + 1 */", "select 2", false},
		{"select 2 /*!" + strings.Repeat("9", 128) + " + 1 */", "select 2", false},
		{"select /*!1 6 */", "", true},
		{"select /*!1234 6 */", "", true},
		{"select /*! 1234 */", "select 1234", false},
		{"select 2 /*!99999 unterminated", "", true},
		{"select /*!80030 6 */ /*!99999 + 99 */", "select 6", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmts, err := Parse(t.Context(), dialect.MYSQL, tc.sql, 1)
			defer func() {
				for _, stmt := range stmts {
					stmt.Free()
				}
			}()
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Len(t, stmts, 1)
			if tc.want == "" {
				require.IsType(t, &tree.EmptyStmt{}, stmts[0])
				return
			}
			require.IsType(t, &tree.Select{}, stmts[0])
			require.Equal(t, tc.want, tree.String(stmts[0], dialect.MYSQL))
		})
	}
}

func TestExecutableCommentVersionGateParseOneModes(t *testing.T) {
	for _, tc := range []struct {
		sql, mode, want string
		wantErr         bool
	}{
		{"select /*!80030 6 */", "", "select 6", false},
		{"select /*!99999 6 */", "", "", true},
		{`select 2 /*!99999 '"; ? */`, "ANSI_QUOTES,NO_BACKSLASH_ESCAPES", "select 2", false},
		{`select /*!80030 "c" */`, "ANSI_QUOTES", "select c", false},
	} {
		t.Run(tc.sql+tc.mode, func(t *testing.T) {
			var stmt tree.Statement
			var err error
			if tc.mode == "" {
				stmt, err = ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			} else {
				stmt, err = ParseOneWithSQLMode(t.Context(), dialect.MYSQL, tc.sql, 1, tc.mode)
			}
			if stmt != nil {
				defer stmt.Free()
			}
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, tree.String(stmt, dialect.MYSQL))
		})
	}
	stmts, err := ParseWithSQLMode(t.Context(), dialect.MYSQL, `select 2 /*!99999 '" */`, 1, "ANSI_QUOTES")
	defer func() {
		for _, stmt := range stmts {
			stmt.Free()
		}
	}()
	require.NoError(t, err)
	require.Len(t, stmts, 1)
	require.Equal(t, "select 2", tree.String(stmts[0], dialect.MYSQL))
}
