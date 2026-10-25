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

package function

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/stretchr/testify/require"
)

func TestRegexpMultilineSyntaxBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, pattern, subject string
		want                   bool
	}{
		{"active_anchor", `^b`, "a\rb", true},
		{"posix_literal_anchor", `[[:alpha:]^$]`, "$", true},
		{"negated_literal_bracket", `[^]$]`, "a", true},
		{"negated_bracket_excluded", `[^]$]`, "]", false},
		{"scoped_multiline_off", `(?-m:^b)`, "a\rb", false},
		{"global_multiline_off", `(?-m)^b`, "a\rb", false},
		{"dollar_multiline_off", `(?-m:a$)`, "a\rb", false},
		{"scoped_multiline_restored", `(?-m:^a)\r^b`, "a\rb", true},
		{"unscoped_change_stays_in_group", `((?-m)^a)\r^b`, "a\rb", true},
		{"comment_group", `(?=a)(?# (^$ )(?-m:^b)`, "a\rb", false},
		{"free_spacing_comment", "(?=a)(?x:# ) ignored ^$\n(?-m:^b))", "a\rb", false},
		{"comment_cannot_push_scope", `(?-m:(?# ghost(^$)^b)\r^c`, "b\rc", true},
		{"line_comment_cannot_close_scope", "(?x-m:^a# )(?m)^$\n)\\r^b", "a\rb", true},
		{"nested_free_spacing_disabled", `(?x:(?-x:^#))\r^b`, "#\rb", true},
		{"escaped_anchors", `\^\$`, "^$", true},
		{"escaped_class_close", `[a\]\^\$]+`, "a]^$", true},
		{"negated_anchor_class", `[^\^$]+`, "abc", true},
		{"quoted_unicode_and_anchor", `\Q^雪$\E$`, "^雪$", true},
		{"trailing_quote", `\Q^雪$`, "^雪$", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := newOpBuiltInRegexp()
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, "m", false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestRegexpMultilineTerminators(t *testing.T) {
	for _, tc := range []struct{ name, separator string }{
		{"LF", "\n"}, {"CR", "\r"}, {"CRLF", "\r\n"},
		{"NEL", "\u0085"}, {"LS", "\u2028"}, {"PS", "\u2029"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := newOpBuiltInRegexp()
			subject := "a" + tc.separator + "b"
			got, err := op.regMap.regularLikeWithMode(`^b$`, subject, "m", false)
			require.NoError(t, err)
			require.True(t, got)
			got, err = op.regMap.regularLikeWithMode(`a$`, subject, "m", false)
			require.NoError(t, err)
			require.True(t, got, "dollar recognizes a terminator before end-of-input")
			got, err = op.regMap.regularLikeWithMode(`a$`, subject, "mu", false)
			require.NoError(t, err)
			require.Equal(t, tc.name == "LF", got)
			got, err = op.regMap.regularLikeWithMode(`^b$`, subject, "mu", false)
			require.NoError(t, err)
			require.Equal(t, tc.name == "LF" || tc.name == "CRLF", got)
			pos, err := op.regMap.regularInstrWithMatchType(`^b$`, subject, 1, 1, 0, false, "m")
			require.NoError(t, err)
			require.EqualValues(t, 2+utf8.RuneCountInString(tc.separator), pos)
		})
	}
	op := newOpBuiltInRegexp()
	for _, occurrence := range []int64{2, 3} {
		pos, err := op.regMap.regularInstrWithMatchType(`^`, "a\r\nb", 1, occurrence, 0, false, "m")
		require.NoError(t, err)
		want := int64(0)
		if occurrence == 2 {
			want = 4
		}
		require.Equal(t, want, pos, "CRLF has one line boundary")
	}
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^b`, "a\rb", 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "b", value)
	value, err = op.regMap.regularReplaceWithMatchType(`^b`, "a\rb", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\rX", value)
}

func TestRegexpMultilineBinaryAndPositionControls(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, separator := range []string{"\r", "\x85"} {
		got, err := op.regMap.regularLikeWithMode(`^b`, "a"+separator+"b", "m", true)
		require.NoError(t, err)
		require.True(t, got)
		got, err = op.regMap.regularLikeWithMode(`^b`, "a"+separator+"b", "mu", true)
		require.NoError(t, err)
		require.False(t, got)
	}
	for _, mode := range []string{"m", "mu"} {
		// No anchor: this stays on RE2 and reaches its position wrapper.
		pos, err := op.regMap.regularInstrWithMatchType(`b`, "abcb", 3, 1, 0, false, mode)
		require.NoError(t, err)
		require.EqualValues(t, 4, pos)
	}
	for _, pattern := range []string{`\^\$`, `[^^$]`, `\Q^雪$\E`} {
		require.False(t, requiresRegexp2Pattern(pattern))
	}
	require.False(t, requiresRegexp2Pattern(`^b`))
	rule, err := regexpPatternWithPureMatchType(`^b`, "mu", false)
	require.NoError(t, err)
	require.Equal(t, `(?m)^b`, rule)
	rule, err = regexpPatternWithPureMatchType(`b`, "u", false)
	require.NoError(t, err)
	require.Equal(t, `b`, rule)
	// Original-context offset search keeps quoted literals and capture numbering.
	reg, err := op.regMap.getRegularMatcherWithMatchType(`\Q^雪$`, "mu", false)
	require.NoError(t, err)
	require.Equal(t, []int{1, 6}, reg.FindStringIndexAt("x^雪$", 1))
	for _, pattern := range []string{`[`, `\`, `(?-m`, `(?#`, `)`} {
		_, err := op.regMap.regularLikeWithMode(pattern, "x", "m", false)
		require.Error(t, err)
	}
}

func TestRegexpMultilineExpansionAdmission(t *testing.T) {
	for _, binary := range []bool{false, true} {
		rs := newOpBuiltInRegexp().regMap
		for _, pattern := range []string{`(?=a)` + strings.Repeat("^", 1048560), strings.Repeat("x", regexp2MaxPatternBytes+1)} {
			_, err := rs.getRegexp2MatcherWithMatchType(pattern, "m", binary)
			require.ErrorContains(t, err, "maximum supported size")
			require.Empty(t, rs.icu)
			require.Zero(t, rs.icuBytes)
		}
		caret := regexpWideMultilineCaret
		if binary {
			caret = regexpWideMultilineCaretBinary
		}
		count := (regexp2MaxPatternBytes - len("(?m)") - len("(?=a)")) / len(caret)
		pattern := `(?=a)` + strings.Repeat("^", count)
		rule, err := regexpPatternWithPureMatchType(pattern, "m", binary)
		require.NoError(t, err)
		require.LessOrEqual(t, len(rule), regexp2MaxPatternBytes)
		_, err = regexpPatternWithPureMatchType(pattern+"^", "m", binary)
		require.ErrorContains(t, err, "maximum supported size")
	}
}
