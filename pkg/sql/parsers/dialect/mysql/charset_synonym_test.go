// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mysql

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// charsetSynonymPairs covers every position reported in #31922 where MySQL
// accepts CHARSET as a synonym of CHARACTER SET: column definitions
// (VARCHAR/CHAR/TEXT/ENUM, with optional COLLATE and '='), ALTER TABLE
// ... MODIFY, ALTER TABLE ... CONVERT TO, ALTER DATABASE ... CHARSET,
// CAST(... AS CHAR ... CHARSET ...) and typed CONVERT(..., CHAR ...
// CHARSET ...). Each alias must parse and yield exactly the same AST as
// the spelled-out canonical form.
var charsetSynonymPairs = []struct {
	alias     string
	canonical string
}{
	// column definitions
	{
		"create table x1(a varchar(5) charset utf8mb4)",
		"create table x1(a varchar(5) character set utf8mb4)",
	},
	{
		"create table x1(a varchar(5) charset = utf8mb4)",
		"create table x1(a varchar(5) character set = utf8mb4)",
	},
	{
		"create table x2(a varchar(5) charset utf8mb4 collate utf8mb4_bin)",
		"create table x2(a varchar(5) character set utf8mb4 collate utf8mb4_bin)",
	},
	{
		"create table x6(a text charset utf8mb4, b enum('a') charset utf8mb4, c char charset utf8mb4)",
		"create table x6(a text character set utf8mb4, b enum('a') character set utf8mb4, c char character set utf8mb4)",
	},
	{
		"create table x2b(a varchar(5) char set utf8mb4)",
		"create table x2b(a varchar(5) character set utf8mb4)",
	},
	// alter table ... modify
	{
		"alter table x5 modify a varchar(5) charset utf8mb4",
		"alter table x5 modify a varchar(5) character set utf8mb4",
	},
	{
		"alter table x5 modify a varchar(5) charset utf8mb4 collate utf8mb4_bin",
		"alter table x5 modify a varchar(5) character set utf8mb4 collate utf8mb4_bin",
	},
	// alter table ... convert to [collate]
	{
		"alter table x5 convert to charset utf8mb4",
		"alter table x5 convert to character set utf8mb4",
	},
	{
		"alter table x5 convert to charset utf8mb4 collate utf8mb4_bin",
		"alter table x5 convert to character set utf8mb4 collate utf8mb4_bin",
	},
	// alter database [default] charset [=]
	{
		"alter database xd charset utf8mb4",
		"alter database xd character set utf8mb4",
	},
	{
		"alter database xd charset = utf8mb4",
		"alter database xd character set = utf8mb4",
	},
	{
		"alter database xd default charset utf8mb4",
		"alter database xd default character set utf8mb4",
	},
	// cast
	{
		"select cast('a' as char(5) charset utf8mb4)",
		"select cast('a' as char(5) character set utf8mb4)",
	},
	{
		"select cast('a' as char charset utf8mb4)",
		"select cast('a' as char character set utf8mb4)",
	},
	// typed convert
	{
		"select convert('a', char charset utf8mb4)",
		"select convert('a', char character set utf8mb4)",
	},
	{
		"select convert('a', char(5) charset utf8mb4)",
		"select convert('a', char(5) character set utf8mb4)",
	},
}

func TestCharsetSynonymParsesAndMatchesCanonical(t *testing.T) {
	for _, tc := range charsetSynonymPairs {
		t.Run(tc.alias, func(t *testing.T) {
			aliasStmt, err := ParseOne(t.Context(), tc.alias, 1)
			require.NoError(t, err, "alias form must parse per MySQL synonym rule")
			defer aliasStmt.Free()
			canonicalStmt, err := ParseOne(t.Context(), tc.canonical, 1)
			require.NoError(t, err, "canonical form must keep parsing")
			defer canonicalStmt.Free()

			aliasFmt := tree.String(aliasStmt, dialect.MYSQL)
			canonicalFmt := tree.String(canonicalStmt, dialect.MYSQL)
			require.Equal(t, canonicalFmt, aliasFmt, "alias must produce the same AST as the canonical form")

			// The canonical format must round-trip.
			replayed, err := ParseOne(t.Context(), aliasFmt, 1)
			require.NoError(t, err, aliasFmt)
			defer replayed.Free()
			require.Equal(t, aliasFmt, tree.String(replayed, dialect.MYSQL))
		})
	}
}

func TestAlterCharsetSynonymSemanticDetails(t *testing.T) {
	// ALTER TABLE ... CONVERT TO via CHARSET must mark Convert like canonical.
	stmt, err := ParseOne(t.Context(), "alter table x5 convert to charset utf8mb4", 1)
	require.NoError(t, err)
	defer stmt.Free()
	opt := stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
	require.True(t, opt.Convert)
	require.Equal(t, "utf8mb4", opt.Charset)
	require.False(t, opt.NonCharsetSyntax)
	require.Empty(t, opt.Collate)

	stmt, err = ParseOne(t.Context(), "alter table x5 convert to character set utf8mb4", 1)
	require.NoError(t, err)
	defer stmt.Free()
	require.Equal(t, opt, stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset))

	// CONVERT TO ... COLLATE via CHARSET keeps the collation.
	stmt, err = ParseOne(t.Context(), "alter table x5 convert to charset utf8mb4 collate utf8mb4_bin", 1)
	require.NoError(t, err)
	defer stmt.Free()
	opt = stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset)
	require.True(t, opt.Convert)
	require.Equal(t, "utf8mb4", opt.Charset)
	require.Equal(t, "utf8mb4_bin", opt.Collate)

	stmt, err = ParseOne(t.Context(), "alter table x5 convert to character set utf8mb4 collate utf8mb4_bin", 1)
	require.NoError(t, err)
	defer stmt.Free()
	require.Equal(t, opt, stmt.(*tree.AlterTable).Options[0].(*tree.TableOptionCharset))

	// ALTER DATABASE ... [DEFAULT] CHARSET mirrors CreateOptionCharset semantics.
	for _, tc := range []struct {
		sql       string
		isDefault bool
		charset   string
	}{
		{"alter database xd charset utf8mb4", false, "utf8mb4"},
		{"alter database xd character set utf8mb4", false, "utf8mb4"},
		{"alter database xd default charset utf8mb4", true, "utf8mb4"},
		{"alter database xd default character set utf8mb4", true, "utf8mb4"},
	} {
		stmt, err := ParseOne(t.Context(), tc.sql, 1)
		require.NoError(t, err)
		require.Equal(t, tc.charset, stmt.(*tree.AlterDatabase).Options[0].(*tree.CreateOptionCharset).Charset, tc.sql)
		require.Equal(t, tc.isDefault, stmt.(*tree.AlterDatabase).Options[0].(*tree.CreateOptionCharset).IsDefault, tc.sql)
		stmt.Free()
	}
}

// TestCharsetSynonymPreservesExisting controls: statements that already worked
// before the fix must keep parsing to the same statements, and grammar-level
// parsing must not start rejecting charset names (latin1/binary) — whether a
// charset is supported is a runtime decision, not a grammar one.
func TestCharsetSynonymPreservesExisting(t *testing.T) {
	for _, sql := range []string{
		"create database xd charset utf8mb4",
		"create database xd character set utf8mb4",
		"create database xd charset = utf8mb4",
		"create table x3(a int) charset utf8mb4",
		"create table x4(a int) charset = utf8mb4",
		"alter table t default character set = utf8mb4 collate = utf8mb4_bin",
		"alter table t default charset = utf8mb4 collate = utf8mb4_bin",
		"alter table y1 convert to character set utf8mb4 collate utf8mb4_bin",
		"set names utf8mb4",
		"set character set utf8mb4",
		"set charset utf8mb4",
		"show charset",
		"show character set",
		// No-name ALTER DATABASE: the original two-token spelling, the
		// DEFAULT-led and COLLATE-led routes all stay open for it.
		"alter database character set utf8mb4",
		"alter database default character set utf8mb4",
		"alter database default charset utf8mb4",
		"alter database default charset = utf8mb4",
		"alter database default collate utf8mb4_bin",
		// MO's DEFAULT-prefixed column variant keeps parsing (MySQL 8.4 has
		// no DEFAULT keyword in the column charset clause; MO accepts the
		// full spelling only).
		"create table x1(a varchar(5) default character set utf8mb4)",
		// A database actually named "charset" keeps parsing like before.
		"alter database charset character set utf8mb4",
	} {
		stmt, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err, sql)
		defer stmt.Free()
	}

	// Unsupported or special charset names must stay grammar-acceptable in
	// both spellings; MO rejects latin1 at execution time, not parsing time.
	for _, sql := range []string{
		"create table z(a varchar(5) charset latin1)",
		"create table z(a varchar(5) character set latin1)",
		"alter table x5 convert to charset latin1",
		"alter database xd charset latin1",
		"select cast('a' as char(5) charset latin1)",
		"select convert('a', char charset latin1)",
		"create table z(a varchar(5) charset binary)",
		"select convert('a', char charset binary)",
	} {
		stmt, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err, sql)
		defer stmt.Free()
	}

	// Disambiguation bounds of the zero-conflict grammar: a DEFAULT-prefixed
	// column attribute only accepts the full CHARACTER SET spelling, and in a
	// no-name ALTER DATABASE a leading bare CHARSET token is read as a
	// database name (as CREATE DATABASE already reads it), not as an option
	// keyword.
	for _, sql := range []string{
		"create table x1(a varchar(5) default charset utf8mb4)",
		"create table x1(a varchar(5) default char set utf8mb4)",
		"alter database charset utf8mb4",
	} {
		_, err := ParseOne(t.Context(), sql, 1)
		require.Error(t, err, sql)
	}
}

// TestCharsetNeighbouringControls pins neighbouring syntax that must not move
// while CHARSET doubles as an option keyword: the CHARSET() scalar function,
// "charset" as an ordinary object/column identifier, SHOW CHARACTER SET under
// every accepted spelling, and the pre-existing BINARY / CHAR SET forms.
// These pin existing baseline behaviour only; no runtime charset support is
// added or implied.
func TestCharsetNeighbouringControls(t *testing.T) {
	roundtrip := func(t *testing.T, sql string) string {
		stmt, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err, sql)
		defer stmt.Free()
		formatted := tree.String(stmt, dialect.MYSQL)
		replayed, err := ParseOne(t.Context(), formatted, 1)
		require.NoError(t, err, formatted)
		defer replayed.Free()
		require.Equal(t, formatted, tree.String(replayed, dialect.MYSQL), formatted)
		return formatted
	}

	// Every accepted SHOW spelling builds the same typed ShowCharset target.
	for _, sql := range []string{"show character set", "show charset", "show char set"} {
		stmt, err := ParseOne(t.Context(), sql, 1)
		require.NoError(t, err, sql)
		st, ok := stmt.(*tree.ShowTarget)
		require.True(t, ok, sql)
		require.Equal(t, tree.ShowType(tree.ShowCharset), st.Type, sql)
		stmt.Free()
		roundtrip(t, sql)
	}

	// The CHARSET() scalar function keeps parsing and round-tripping.
	for _, sql := range []string{
		"select charset('a')",
		"select charset(col1) from t1",
	} {
		require.Contains(t, roundtrip(t, sql), "charset(", sql)
	}

	// "charset" stays an ordinary identifier for databases, tables, columns
	// and expressions.
	for _, sql := range []string{
		"create database charset",
		"create table charset(a int)",
		"create table t_charset(charset varchar(10))",
		"select charset from t_charset",
		"select a from t_charset order by charset",
		"alter database charset character set utf8mb4",
	} {
		roundtrip(t, sql)
	}

	// BINARY as a cast target and as a string-literal introducer keeps
	// parsing and round-tripping (the _binary introducer is absorbed into
	// the literal at formatting time, which is the pre-existing baseline).
	for _, sql := range []string{
		"select cast('a' as binary)",
		"select cast('a' as binary(5))",
		"select _binary 'abc'",
	} {
		roundtrip(t, sql)
	}
	// The cast target keeps its binary marker through formatting.
	require.Contains(t, roundtrip(t, "select cast('a' as binary)"), "binary")
	// MO has no column-level BINARY attribute; that baseline boundary is
	// unrelated to the CHARSET synonym change and must not silently open.
	for _, sql := range []string{
		"create table b1(a varchar(5) binary)",
		"create table b2(a char(5) binary)",
	} {
		_, err := ParseOne(t.Context(), sql, 1)
		require.Error(t, err, sql)
	}
}
