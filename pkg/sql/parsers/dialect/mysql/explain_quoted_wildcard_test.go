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

package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// The DESCRIBE/DESC/EXPLAIN quoted string argument is the SHOW COLUMNS
// LIKE wildcard pattern: every keyword form must parse into the same
// typed/value LIKE comparison the SHOW ... LIKE form produces, carrying
// the scanner-decoded literal for %, _, escaped characters and quotes,
// never an identifier.
func TestExplainQuotedWildcardParsesAsShowColumnsLike(t *testing.T) {
	forms := []struct {
		sql   string
		table string
	}{
		{"DESC t 'v%'", "t"},
		{"DESCRIBE t 'v%'", "t"},
		{"EXPLAIN t 'v%'", "t"},
		{"DESC db.t 'v%'", "db.t"},
		{"DESCRIBE db.t 'v%'", "db.t"},
		{"EXPLAIN db.t 'v%'", "db.t"},
		{"DESC t 'v_1'", "t"},
		{"DESC t 'va\\%lue'", "t"},
		{"DESC t 'it''s'", "t"},
		{"DESC t 'nomatch%'", "t"},
		{"DESC t ''", "t"},
	}
	for _, tc := range forms {
		stmt, err := ParseOne(context.Background(), tc.sql, 1)
		require.NoError(t, err, tc.sql)
		sc, ok := stmt.(*tree.ShowColumns)
		require.True(t, ok, tc.sql)
		require.NotNil(t, sc.Like, tc.sql)
		require.Nil(t, sc.ColName, tc.sql)

		show, err := ParseOne(context.Background(), "SHOW COLUMNS FROM "+tc.table+" LIKE "+suffixPattern(tc.sql), 1)
		require.NoError(t, err, tc.sql)
		require.Equal(t, tree.String(show, dialect.MYSQL), tree.String(stmt, dialect.MYSQL),
			"the quoted form must be the identical SHOW COLUMNS LIKE statement, got %s", tc.sql)
		stmt.Free()
		show.Free()
	}
}

// suffixPattern extracts the quoted argument starting at its opening
// apostrophe so escaped inner quotes are preserved verbatim for the
// SHOW ... LIKE comparison.
func suffixPattern(sql string) string {
	for i := 0; i < len(sql); i++ {
		if sql[i] == '\'' {
			return sql[i:]
		}
	}
	return ""
}

// Controls: the ordinary identifier argument keeps the named-column path,
// the missing-argument form keeps the unfiltered statement, and the SHOW
// COLUMNS LIKE surface is untouched by the new arm.
func TestExplainQuotedWildcardControls(t *testing.T) {
	named, err := ParseOne(context.Background(), "DESC t v_1", 1)
	require.NoError(t, err)
	sc, ok := named.(*tree.ShowColumns)
	require.True(t, ok)
	require.NotNil(t, sc.ColName)
	require.Nil(t, sc.Like)
	named.Free()

	plain, err := ParseOne(context.Background(), "DESCRIBE db.t", 1)
	require.NoError(t, err)
	sc, ok = plain.(*tree.ShowColumns)
	require.True(t, ok)
	require.Nil(t, sc.ColName)
	require.Nil(t, sc.Like)
	plain.Free()

	show, err := ParseOne(context.Background(), "SHOW COLUMNS FROM db.t LIKE 'v%'", 1)
	require.NoError(t, err)
	sc, ok = show.(*tree.ShowColumns)
	require.True(t, ok)
	require.Nil(t, sc.ColName)
	require.NotNil(t, sc.Like)
	show.Free()
}
