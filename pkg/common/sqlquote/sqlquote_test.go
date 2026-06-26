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

package sqlquote

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIdent(t *testing.T) {
	require.Equal(t, "`col`", Ident("col"))
	require.Equal(t, "``", Ident(""))
	require.Equal(t, "`a``b`", Ident("a`b")) // embedded backtick is doubled
	require.Equal(t, "`db.tbl`", Ident("db.tbl"))
	require.Equal(t, "`weird name`", Ident("weird name"))
}

// TestIdentRoundTrip is the invariant that makes quoting safe: any identifier
// survives quoting — strip the outer backticks, halve the doubled ones, and you
// recover the exact original. The lone-backtick / double-backtick cases are the
// ones a hand-written "`" + name + "`" gets wrong.
func TestIdentRoundTrip(t *testing.T) {
	for _, in := range []string{"col", "", "a`b", "a``b", "`", "``", "weird `name`", "维度"} {
		q := Ident(in)
		require.GreaterOrEqual(t, len(q), 2)
		require.Equal(t, byte('`'), q[0])
		require.Equal(t, byte('`'), q[len(q)-1])
		require.Equal(t, in, strings.ReplaceAll(q[1:len(q)-1], "``", "`"), "round-trip of %q", in)
	}
}

func TestIdentPreventsBreakout(t *testing.T) {
	// A name crafted to close the quote early and inject SQL stays contained:
	// every embedded backtick is doubled, so no lone backtick ends the quote.
	require.Equal(t, "`a`` ; DROP TABLE t; --`", Ident("a` ; DROP TABLE t; --"))
}

func TestQualifiedIdent(t *testing.T) {
	require.Equal(t, "`db`.`tbl`", QualifiedIdent("db", "tbl"))
	require.Equal(t, "`tbl`", QualifiedIdent("tbl"))
	require.Equal(t, "`src`.`col`", QualifiedIdent("src", "col"))
	// embedded backticks are doubled in each part
	require.Equal(t, "`a``b`.`c``d`", QualifiedIdent("a`b", "c`d"))
}

func TestQualifiedIdentSkipsEmptyParts(t *testing.T) {
	require.Equal(t, "`db`.`tbl`", QualifiedIdent("db", "", "tbl"))
	require.Equal(t, "`tbl`", QualifiedIdent("", "tbl"))
	require.Equal(t, "`db`", QualifiedIdent("db", ""))
	require.Equal(t, "", QualifiedIdent())
	require.Equal(t, "", QualifiedIdent("", ""))
}

func TestEscapeString(t *testing.T) {
	require.Equal(t, "abc", EscapeString("abc"))
	require.Equal(t, "", EscapeString(""))
	require.Equal(t, "a''b", EscapeString("a'b")) // single quote doubled
	require.Equal(t, "''''", EscapeString("''"))  // two quotes -> four
	// a JSON params blob with a quote inside a value
	require.Equal(t, `{"included_columns":"col''s"}`, EscapeString(`{"included_columns":"col's"}`))
	// backslash must be doubled: the MySQL scanner treats it as an escape
	require.Equal(t, `a\\b`, EscapeString(`a\b`))
	require.Equal(t, `\\`, EscapeString(`\`))                               // lone backslash -> two
	require.Equal(t, `C:\\Program Files`, EscapeString(`C:\Program Files`)) // Windows path
	require.Equal(t, `x\\`, EscapeString(`x\`))                             // trailing backslash
	require.Equal(t, `a\\''b`, EscapeString(`a\'b`))                        // backslash AND quote both doubled
	require.Equal(t, `\\n`, EscapeString(`\n`))                             // literal backslash-n, not a newline escape
}

func TestString(t *testing.T) {
	require.Equal(t, "'abc'", String("abc"))
	require.Equal(t, "''", String(""))
	require.Equal(t, "'a''b'", String("a'b"))
	require.Equal(t, `'{"k":"v''s"}'`, String(`{"k":"v's"}`))
	require.Equal(t, `'a\\b'`, String(`a\b`))
	require.Equal(t, `'C:\\x'`, String(`C:\x`))
}

// TestIdentNoOpForOrdinaryNames is the safety proof behind the bulk migration of
// ~30 SQL sites to Ident(): for any identifier WITHOUT a backtick, Ident produces
// EXACTLY the bytes the old hand-written "`" + name + "`" produced. Real database /
// table / column / alias names never contain a backtick, so every one of those
// sites generates byte-identical SQL to before — the migration can only change
// output for names that were already producing broken SQL. If this test passes,
// the quoting change cannot have altered any existing query.
func TestIdentNoOpForOrdinaryNames(t *testing.T) {
	for _, s := range []string{
		"id", "vec", "price", "my_table", "MixedCase", "t123", "维度",
		"__mo_index_secondary_018f3a", "with space", "db-name",
		// reserved words: the old code already backtick-wrapped these the same way
		"select", "order", "from", "index",
	} {
		require.Equal(t, "`"+s+"`", Ident(s),
			"Ident(%q) must equal the old raw `name` wrapping (no backtick present)", s)
	}
}

// TestStringNoOpForOrdinaryLiterals is the same proof for String(): any literal
// WITHOUT a single quote or backslash wraps to exactly '<literal>', identical to
// the old '%s'. JSON params / config blobs and generated index_ids contain
// neither, so those sites are byte-identical to before.
func TestStringNoOpForOrdinaryLiterals(t *testing.T) {
	for _, s := range []string{
		"", "cdc_tail", "cdc:1:0:1717000000", "vector_l2_ops",
		`{"op_type":"vector_l2_ops","lists":"1"}`,
		`{"metadata":"__mo_index_secondary_x","index":"y","db":"d","src":"t"}`,
	} {
		require.Equal(t, "'"+s+"'", String(s),
			"String(%q) must equal the old raw '%%s' wrapping (no single quote present)", s)
	}
}

func TestStringPreventsBreakout(t *testing.T) {
	// A literal crafted to close the quote early and inject SQL stays contained:
	// the leading single quote is doubled, so it does not end the literal.
	require.Equal(t, "'''; DROP TABLE t; --'", String("'; DROP TABLE t; --"))
}
