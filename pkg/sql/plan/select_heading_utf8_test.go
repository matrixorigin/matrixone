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

package plan

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// #31731: a generated (unaliased) select heading derived from source text can
// carry raw literal bytes (e.g. a _binary literal) that are not legal UTF-8 and
// break driver metadata decoding. The boundary sanitizer must leave legal names
// untouched and replace only the invalid sequences.
func TestLegalUTF8GeneratedHeading(t *testing.T) {
	for _, unchanged := range []string{"", "abc", "中", "_binary'abc'", "col_1"} {
		require.Equal(t, unchanged, legalUTF8GeneratedHeading(unchanged))
	}
	fixed := legalUTF8GeneratedHeading("_binary'\x00\xff\x80'")
	require.True(t, utf8.ValidString(fixed), "generated heading must be legal UTF-8 on the wire")
	require.Contains(t, fixed, "�", "invalid bytes are replaced, not silently dropped")
	require.True(t, strings.HasPrefix(fixed, "_binary'"), "legal prefix content is preserved")
}

// End-to-end through the real planner: BuildPlan metadata headings are legal
// UTF-8 for a raw _binary literal projection, while explicit aliases, CJK text
// and catalog column identity/provenance stay byte-identical.
func TestBuildPlanGeneratedHeadingsAreLegalUTF8(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	headingsOf := func(sql string) []string {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		hs := p.GetQuery().GetHeadings()
		require.NotEmpty(t, hs)
		return hs
	}

	generated := headingsOf("select _binary '\x00\xff\x80'")[0]
	require.True(t, utf8.ValidString(generated), "generated heading %q is not legal UTF-8", generated)
	require.Contains(t, generated, "�", "invalid literal bytes are replaced in the generated heading")

	require.Equal(t, "bin_alias", headingsOf("select _binary '\x00\xff\x80' as bin_alias")[0],
		"explicit alias identity is untouched")
	cjk := headingsOf("select '中'")[0]
	require.True(t, utf8.ValidString(cjk))
	require.Contains(t, cjk, "中", "legal UTF-8 heading content is preserved")
	require.Equal(t, "a", headingsOf("select a from select_test.bind_select")[0],
		"catalog column heading and provenance are untouched")
}

// review5809254373 / #31731: WITH ROLLUP plus a window function forces the
// rollup-window rewrite (buildRollupWindowSelectExprs), whose generated output
// aliases come from rollupWindowOutputAliasWithProvenance. The rewritten outer
// SELECT treats that alias as a genuine user alias and copies As.Origin
// verbatim, bypassing the general generated-heading sanitizer — so an
// unaliased malformed binary literal still reached client metadata. The
// producer of the rewrite-generated output name must sanitize only the
// generated forms; explicit user aliases, catalog identifiers and provenance
// stay byte-identical.
func TestBuildPlanRollupWindowGeneratedHeadingsAreLegalUTF8(t *testing.T) {
	ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
	headingsOf := func(sql string) []string {
		t.Helper()
		stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt, false, nil)
		require.NoError(t, err)
		hs := p.GetQuery().GetHeadings()
		require.NotEmpty(t, hs)
		return hs
	}

	// Rewrite-forced shape: unaliased malformed _binary literal + window fn.
	hs := headingsOf("select _binary '\x00\xff\x80', row_number() over () from select_test.bind_select group by a with rollup")
	require.Len(t, hs, 2)
	require.True(t, utf8.ValidString(hs[0]), "rollup-window rewrite generated heading %q is not legal UTF-8", hs[0])
	require.Contains(t, hs[0], "�", "invalid literal bytes are replaced, not silently dropped")
	require.True(t, utf8.ValidString(hs[1]), "window output heading %q is not legal UTF-8", hs[1])

	// Explicit user alias on the binary literal stays byte-identical.
	hs = headingsOf("select _binary '\x00\xff\x80' as keepme, row_number() over () from select_test.bind_select group by a with rollup")
	require.Equal(t, "keepme", hs[0], "explicit user alias is untouched by the rewrite producer")

	// Valid UTF-8 generated heading content is preserved (no mangling): the
	// generated form renders the binary-introducer literal's raw content.
	hs = headingsOf("select _binary 'ab', row_number() over () from select_test.bind_select group by a with rollup")
	require.Equal(t, "ab", hs[0], "legal generated heading content is preserved")

	// No-ROLLUP window control: the general projection guard already covers it.
	hs = headingsOf("select _binary '\x00\xff\x80', row_number() over () from select_test.bind_select group by a")
	require.True(t, utf8.ValidString(hs[0]))

	// No-window ROLLUP control: the grouping-set branch owner stays intact.
	hs = headingsOf("select _binary '\x00\xff\x80', count(*) from select_test.bind_select group by a with rollup")
	require.True(t, utf8.ValidString(hs[0]), "no-window rollup heading %q is not legal UTF-8", hs[0])

	// NAME_CONST heading control: the assigned result name is preserved.
	hs = headingsOf("select name_const('n1', _binary '\x00\xff\x80'), row_number() over () from select_test.bind_select group by a with rollup")
	require.Equal(t, "n1", hs[0], "NAME_CONST assigned heading is preserved")
}
