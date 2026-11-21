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

// Review 5967618815 (PR 32906): a column declared END must be referenceable in
// every unambiguous shared table-level key form, matching the declaration and
// inline primary key that already accept the bare name.
func TestCreateTableBareENDInTableLevelKeys(t *testing.T) {
	cases := []string{
		"CREATE TABLE ek1 (end INT, PRIMARY KEY (end))",
		"CREATE TABLE ek2 (end INT, UNIQUE KEY (end))",
		"CREATE TABLE ek3 (end INT, UNIQUE KEY uk (end, end DESC))",
		"CREATE TABLE ek4 (end INT, KEY k (end))",
		"CREATE TABLE ek5 (e INT, end INT, PRIMARY KEY (e, end ASC))",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
			formatted := tree.String(stmt, dialect.MYSQL)
			reStmt, reErr := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, reErr, formatted)
			require.Equal(t, formatted, tree.String(reStmt, dialect.MYSQL), formatted)
		})
	}
}

// Positions that worked before the fix keep working: inline keys, quoted
// references, and END's block-terminator role in compound statements.
func TestCreateTableBareENDControlsUnchanged(t *testing.T) {
	cases := []string{
		"CREATE TABLE c1 (end INT PRIMARY KEY)",
		"CREATE TABLE c2 (END INT primary key)",
		"CREATE TABLE c3 (end INT, PRIMARY KEY (`end`))",
		"CREATE TABLE c4 (end INT, end2 VARCHAR(8))",
		"BEGIN DESCRIBE t END",
	}
	for _, sql := range cases {
		t.Run(sql, func(t *testing.T) {
			_, err := ParseOne(context.Background(), sql, 1)
			require.NoError(t, err, sql)
		})
	}
}

// Review 5967618815 (PR 32906): the shared PROPERTIES option must reserialize
// its key and value as SQL string literals so the formatted replay reparses
// (property_elem only accepts STRING '=' STRING) and round-trips identically.
func TestCTASPropertiesQuotedRoundtrip(t *testing.T) {
	cases := []struct {
		sql   string
		kv    [][2]string
		exact string
	}{
		{
			sql:   "CREATE TABLE p1 PROPERTIES ('k'='v') AS SELECT 1 AS a",
			kv:    [][2]string{{"k", "v"}},
			exact: "properties('k' = 'v')",
		},
		{
			sql:   "CREATE TABLE p2 PROPERTIES ('k k'='va l''ue', 'k2'='v2') AS SELECT 1 AS a",
			kv:    [][2]string{{"k k", "va l'ue"}, {"k2", "v2"}},
			exact: "properties('k k' = 'va l''ue', 'k2' = 'v2')",
		},
		{
			sql:   "CREATE TABLE p3 (a INT) PROPERTIES ('k'='v') AS SELECT 1",
			kv:    [][2]string{{"k", "v"}},
			exact: "properties('k' = 'v')",
		},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err, tc.sql)
			create, ok := stmt.(*tree.CreateTable)
			require.True(t, ok, tc.sql)
			require.NotEmpty(t, create.Options, tc.sql)

			formatted := tree.String(create, dialect.MYSQL)
			require.Contains(t, formatted, tc.exact, formatted)

			reStmt, reErr := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, reErr, formatted)
			require.Equal(t, formatted, tree.String(reStmt, dialect.MYSQL), formatted)

			reCreate, ok := reStmt.(*tree.CreateTable)
			require.True(t, ok, formatted)
			var got [][2]string
			for _, opt := range reCreate.Options {
				if props, ok := opt.(*tree.TableOptionProperties); ok {
					for _, p := range props.Preperties {
						got = append(got, [2]string{p.Key, p.Value})
					}
				}
			}
			require.Equal(t, tc.kv, got, formatted)
		})
	}
}

// ctasPropertiesKV extracts the decoded key/value bytes of the PROPERTIES
// option from a parsed CREATE TABLE statement, asserting the option exists.
func ctasPropertiesKV(t *testing.T, stmt tree.Statement) [][2]string {
	t.Helper()
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, "not a create table statement")
	var got [][2]string
	for _, opt := range create.Options {
		if props, ok := opt.(*tree.TableOptionProperties); ok {
			for _, p := range props.Preperties {
				got = append(got, [2]string{p.Key, p.Value})
			}
		}
	}
	require.NotEmpty(t, got, "no PROPERTIES option on statement")
	return got
}

// TestCTASPropertiesDecodedKVBackslashQuoteBoundaries drives the shared
// Property formatter's new escape owners end to end: a value with a backslash
// adjacent to a quote exercises the FormatString stage first and the
// single-quote doubling stage second, and a control character is re-emitted
// as its escaped literal. Both must reparse in the default backslash-aware
// mode to the identical decoded bytes (#32906 review 5967618815).
func TestCTASPropertiesDecodedKVBackslashQuoteBoundaries(t *testing.T) {
	cases := []struct {
		sql   string
		kv    [][2]string
		exact string
	}{
		{
			sql:   `CREATE TABLE bq PROPERTIES ('k'='a\\\'b') AS SELECT 1 AS a`,
			kv:    [][2]string{{"k", `a\'b`}},
			exact: `properties('k' = 'a\\''b')`,
		},
		{
			sql:   "CREATE TABLE cr PROPERTIES ('k'='a\\rb') AS SELECT 1 AS a",
			kv:    [][2]string{{"k", "a\rb"}},
			exact: `properties('k' = 'a\rb')`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := ParseOne(context.Background(), tc.sql, 1)
			require.NoError(t, err, tc.sql)
			require.Equal(t, tc.kv, ctasPropertiesKV(t, stmt), tc.sql)

			formatted := tree.String(stmt, dialect.MYSQL)
			require.Contains(t, formatted, tc.exact, formatted)

			reStmt, reErr := ParseOne(context.Background(), formatted, 1)
			require.NoError(t, reErr, formatted)
			require.Equal(t, tc.kv, ctasPropertiesKV(t, reStmt), formatted)
			require.Equal(t, formatted, tree.String(reStmt, dialect.MYSQL), formatted)
		})
	}
}

// TestCTASPropertiesNoBackslashEscapeModeRoundtrip pins the shared Property
// formatter's NoBackslashEscape branch. The persisted rel_createsql catalog
// path always serializes through the backslash-aware canonical context, so
// this branch is reached only by contexts that opt in; a NO_BACKSLASH_ESCAPES
// parse, a matching WithNoBackslashEscape format and a NO_BACKSLASH_ESCAPES
// reparse must round-trip identical decoded bytes. This adds no new catalog
// semantic requirement.
func TestCTASPropertiesNoBackslashEscapeModeRoundtrip(t *testing.T) {
	sql := `CREATE TABLE nb PROPERTIES ('k'='a\b', 'k2'='c''d') AS SELECT 1 AS a`
	stmt, err := ParseOneWithSQLMode(context.Background(), sql, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, err, sql)
	require.Equal(t, [][2]string{{"k", `a\b`}, {"k2", `c'd`}}, ctasPropertiesKV(t, stmt), sql)

	fmtCtx := tree.NewFmtCtx(dialect.MYSQL, tree.WithNoBackslashEscape())
	stmt.Format(fmtCtx)
	formatted := fmtCtx.String()
	require.Contains(t, formatted, `properties('k' = 'a\b', 'k2' = 'c''d')`, formatted)

	reStmt, reErr := ParseOneWithSQLMode(context.Background(), formatted, 1, "NO_BACKSLASH_ESCAPES")
	require.NoError(t, reErr, formatted)
	require.Equal(t, [][2]string{{"k", `a\b`}, {"k2", `c'd`}}, ctasPropertiesKV(t, reStmt), formatted)

	reFmt := tree.NewFmtCtx(dialect.MYSQL, tree.WithNoBackslashEscape())
	reStmt.Format(reFmt)
	require.Equal(t, formatted, reFmt.String(), formatted)
}
