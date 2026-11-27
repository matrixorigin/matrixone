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
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// Review 6090897398 (inline 4721617673): quoted CHARSET and COLLATE names are
// accepted at the pre-AS table-option region of multi-statement CTAS (and on
// ordinary CREATE) through the name_string STRING alternative, including
// spellings that carry characters a bare identifier cannot hold. The
// canonical rel_createsql render must reparse to the exact authored values
// with the statement shape intact: bare spellings keep their ordinary bare
// render, and everything else is re-emitted as a properly escaped literal.
func TestReview6090QuotedCharsetCollateCreatesqlRoundtrip(t *testing.T) {
	ctx := context.Background()
	roundtrip := func(t *testing.T, sql, wantCharset, wantCollate string) {
		t.Helper()
		stmt, err := ParseOne(ctx, sql, 1)
		require.NoErrorf(t, err, "the pre-AS charset/collate shape parses: %s", sql)
		ct, ok := stmt.(*tree.CreateTable)
		require.Truef(t, ok, "the statement parses as a CreateTable: %s", sql)
		rendered := tree.String(ct, dialect.MYSQL)
		reparsed, rerr := ParseOne(ctx, rendered, 1)
		require.NoErrorf(t, rerr, "the canonical rel_createsql reparse must succeed for %s (rendered %s)", sql, rendered)
		rct, ok2 := reparsed.(*tree.CreateTable)
		require.Truef(t, ok2, "the canonical reparse yields a CreateTable for %s (rendered %s)", sql, rendered)
		var gotCharset, gotCollate string
		for _, opt := range rct.Options {
			switch c := opt.(type) {
			case *tree.TableOptionCharset:
				gotCharset = c.Charset
				if c.Collate != "" {
					gotCollate = c.Collate
				}
			case *tree.TableOptionCollate:
				gotCollate = c.Collate
			}
		}
		require.Equalf(t, wantCharset, gotCharset, "the reparse restores the authored charset value for %s", sql)
		require.Equalf(t, wantCollate, gotCollate, "the reparse restores the authored collate value for %s", sql)
		require.Equalf(t, ct.IsAsSelect, rct.IsAsSelect, "the canonical render keeps the CTAS/ordinary shape for %s", sql)
	}
	// The multi-statement CTAS path: a quoted pre-AS option set whose values
	// a bare token cannot carry still round-trips the catalog text.
	roundtrip(t,
		"create table t_quote charset 'my charset' collate 'utf8mb4 gen ci' as (select 7 as a limit 1) limit 1",
		"my charset", "utf8mb4 gen ci")
	// Digit-only, reserved-word and mixed-case quoted spellings round-trip
	// their exact decoded values: a bare render would lex as a number or a
	// keyword token that name_string cannot consume, and mixed case would
	// be folded by the lowering ident branch.
	roundtrip(t, "create table t_digits (a int) charset '123' collate 'utf8mb4_bin'", "123", "utf8mb4_bin")
	roundtrip(t, "create table t_reserved (a int) charset 'select' collate 'utf8mb4_bin'", "select", "utf8mb4_bin")
	roundtrip(t, "create table t_mixed (a int) charset 'utf8MB4' collate 'UTF8MB4_Bin'", "utf8MB4", "UTF8MB4_Bin")
	// Quote and backslash spellings escape canonically.
	roundtrip(t, "create table t_esc (a int) charset 'a\\\\b' collate 'it''s'", `a\b`, "it's")
	// Quoted ordinary spellings keep their exact values, and the established
	// bare form still writes the ordinary ident spelling out unchanged.
	roundtrip(t,
		"create table t_quoted_plain (a int) charset 'utf8mb4' collate 'utf8mb4_bin'",
		"utf8mb4", "utf8mb4_bin")
	bareCtl, berr := ParseOne(ctx, "create table t_bare (a int) charset utf8mb4 collate utf8mb4_bin", 1)
	require.NoError(t, berr)
	require.Contains(t, tree.String(bareCtl.(*tree.CreateTable), dialect.MYSQL),
		"charset = utf8mb4 Collate = utf8mb4_bin",
		"an ident-origin option keeps its established render (the separable collate table option keeps its historical Collate form)")
	roundtrip(t,
		"create table t_bare_round (a int) charset utf8mb4 collate utf8mb4_bin",
		"utf8mb4", "utf8mb4_bin")
	// An empty quoted value is the author's own value: the reparse restores
	// it as a present option with the empty value, not a dropped default.
	roundtrip(t,
		"create table t_empty (a int) charset '' collate 'utf8mb4_bin'",
		"", "utf8mb4_bin")

	roundtripSelf := func(t *testing.T, sql string) {
		t.Helper()
		stmt, err := ParseOne(ctx, sql, 1)
		require.NoErrorf(t, err, "the quoted-origin shape parses: %s", sql)
		ct, ok := stmt.(*tree.CreateTable)
		require.Truef(t, ok, "the statement parses as a CreateTable: %s", sql)
		rendered := tree.String(ct, dialect.MYSQL)
		reparsed, rerr := ParseOne(ctx, rendered, 1)
		require.NoErrorf(t, rerr, "the canonical reparse must succeed for %s (rendered %s)", sql, rendered)
		rct, ok2 := reparsed.(*tree.CreateTable)
		require.Truef(t, ok2, "the canonical reparse yields a CreateTable for %s (rendered %s)", sql, rendered)
		charsetOf := func(c *tree.CreateTable) (string, string) {
			var charset, collate string
			for _, opt := range c.Options {
				switch o := opt.(type) {
				case *tree.TableOptionCharset:
					charset = o.Charset
					if o.Collate != "" {
						collate = o.Collate
					}
				case *tree.TableOptionCollate:
					collate = o.Collate
				}
			}
			return charset, collate
		}
		wantCharset, wantCollate := charsetOf(ct)
		gotCharset, gotCollate := charsetOf(rct)
		require.Equalf(t, wantCharset, gotCharset, "the reparse restores the authored original's charset value exactly for %s", sql)
		require.Equalf(t, wantCollate, gotCollate, "the reparse restores the authored original's collate value exactly for %s", sql)
	}
	// Backtick-quoted identifier origins: a backtick-quoted reserved word or
	// a spaced spelling decodes into the value, and the canonical render must
	// re-emit a parseable form carrying the same value - never a bare token
	// the name_string alternatives cannot consume.
	roundtripSelf(t, "create table t_tick_reserved (a int) charset `select` collate `utf8mb4_bin`")
	roundtripSelf(t, "create table t_tick_spaced (a int) charset `utf8MB4 gen ci` collate `UTF8MB4_Bin`")
}
