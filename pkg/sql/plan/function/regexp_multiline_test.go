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
		{"LF", "\n"}, {"CR", "\r"}, {"CRLF", "\r\n"}, {"VT", "\v"}, {"FF", "\f"},
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
	// The binary matching alphabet terminates lines only on the ASCII
	// control bytes CR, VT and FF (plus the native LF).
	for _, separator := range []string{"\r", "\v", "\f"} {
		got, err := op.regMap.regularLikeWithMode(`^b`, "a"+separator+"b", "m", true)
		require.NoError(t, err)
		require.True(t, got)
		got, err = op.regMap.regularLikeWithMode(`^b`, "a"+separator+"b", "mu", true)
		require.NoError(t, err)
		require.False(t, got)
	}
	// Byte 0x85 never begins a line in the binary alphabet.
	for _, mode := range []string{"m", "mi", "mu"} {
		got, err := op.regMap.regularLikeWithMode(`^b`, "a\x85b", mode, true)
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

// TestRegexpMultilineAdmissionIdentity pins the regexp2 admission contract
// of issue #30210: the native line policy must leave the compiled program
// identical in every match_type, and the existing admission verdicts must
// not move.
func TestRegexpMultilineAdmissionIdentity(t *testing.T) {
	op := newOpBuiltInRegexp()
	require.True(t, requiresRegexp2Pattern(`^b(?=b)`))

	anchorDense := "(?m)"
	for i := 0; i < 32; i++ {
		anchorDense += "^$"
	}
	anchorDense += "(?=\n)"

	// Mode identity: the compiled program is the user pattern, identical
	// TrackCount in every mode.
	for _, pin := range []struct {
		name, expression string
		want             int
	}{
		{"caret_lookahead", `(?m)^b(?=b)`, 7},
		{"anchor_dense_32pairs", anchorDense, 7},
		{"bare_caret", `(?m)^`, 3},
		{"alternation_caret_or_z", "(?m)(?:^\n|z)", 5},
	} {
		_, code, release, err := regexp2PrepareExpression(pin.expression)
		require.NoError(t, err)
		got := code.TrackCount
		release()
		require.Equal(t, pin.want, got, pin.name)
	}
	// Production composition identity: composer plus translator keep the
	// plain compiled program in wide and Unix multiline modes.
	for _, pin := range []struct {
		name, pat, matchType string
	}{
		{"production_wideM_caret_lookahead", `^b(?=b)`, "m"},
		{"production_unixMU_caret_lookahead", `^b(?=b)`, "mu"},
		{"production_wideM_anchor_dense", anchorDense[len("(?m)"):], "m"},
	} {
		rule, err := regexpPatternWithPureMatchType(pin.pat, pin.matchType, false)
		require.NoError(t, err)
		expression, err := translateRegexp2Pattern(rule, false)
		require.NoError(t, err)
		_, code, release, err := regexp2PrepareExpression(expression)
		require.NoError(t, err)
		got := code.TrackCount
		release()
		require.Equal(t, 7, got, pin.name)
	}

	// Admission parity at the subject boundary: 50k and 74k admit with no
	// error, 150k keeps the existing states error.
	admittedMid := strings.Repeat("b", 50000)
	got, err := op.regMap.regularLikeWithMode("^b(?=b)", admittedMid, "m", false)
	require.NoError(t, err)
	require.True(t, got, "50k-rune admitted subject")
	admittedEdge := strings.Repeat("b", 74000)
	got, err = op.regMap.regularLikeWithMode("^b(?=b)", admittedEdge, "m", false)
	require.NoError(t, err)
	require.True(t, got, "74k-rune admitted-edge subject")
	_, err = op.regMap.regularLikeWithMode("^b(?=b)", strings.Repeat("b", 150000), "m", false)
	require.Error(t, err, "150k-rune beyond-boundary subject keeps the states error")

	// A wide-terminator-bearing 74k subject keeps the same verdict and the
	// wide contract at the CR boundary.
	wide74 := "b\rb" + strings.Repeat("b", 73997)
	got, err = op.regMap.regularLikeWithMode("^b(?=b)", wide74, "m", false)
	require.NoError(t, err)
	require.True(t, got, "74k wide-bearing subject: ICU expects true after the CR terminator")

	// An anchor-dense 30k boundary subject keeps the plain-pattern verdict.
	denseSubject := "\n" + strings.Repeat("a", 29998) + "\r"
	_, err = op.regMap.regularLikeWithMode(anchorDense, denseSubject, "m", false)
	require.NoError(t, err, "anchor-dense 30k subject keeps the plain admission verdict")

	// Global reservation arithmetic is deterministic: overlapping
	// reservations accumulate, failed ones register nothing, releases
	// restore the baseline exactly.
	_, code74, release74, err := regexp2PrepareExpression(`(?m)^b(?=b)`)
	require.NoError(t, err)
	defer release74()
	eval74, err := regexp2EvaluationMemoryBudget(code74, 74000, 0)
	require.NoError(t, err)
	baseline := regexp2ActiveSubjectBytes.Load()
	var held []int64
	releaseAll := func() {
		for _, r := range held {
			regexp2ActiveSubjectBytes.Add(-r)
		}
		held = nil
	}
	t.Cleanup(releaseAll)
	acquire := func() (int64, error) {
		r, err := acquireRegexp2SubjectBudget(0, eval74)
		if err == nil {
			held = append(held, r)
		}
		return r, err
	}
	_, err = acquire()
	require.NoError(t, err)
	_, err = acquire()
	require.NoError(t, err)
	require.Equal(t, baseline+2*(64<<10+eval74), regexp2ActiveSubjectBytes.Load(), "overlap arithmetic")
	count := 2
	for i := 0; i < 64; i++ {
		if _, err = acquire(); err != nil {
			break
		}
		count++
	}
	require.Error(t, err, "global cap must keep rejecting beyond the limit")
	require.Equal(t, baseline+int64(count)*(64<<10+eval74), regexp2ActiveSubjectBytes.Load(),
		"failed reservation must not register")
	regexp2ActiveSubjectBytes.Add(-held[0])
	regexp2ActiveSubjectBytes.Add(-held[1])
	held = held[2:]
	_, err = acquire()
	require.NoError(t, err, "reservation after releases must be admitted")
	releaseAll()
	require.Equal(t, baseline, regexp2ActiveSubjectBytes.Load(), "all reservations drain")
}

func TestRegexpMultilineExpansionAdmission(t *testing.T) {
	for _, binary := range []bool{false, true} {
		rs := newOpBuiltInRegexp().regMap
		// The composer no longer expands anchors; plain-size admission owns
		// the pattern budget, including the flag prefix.
		for _, pattern := range []string{strings.Repeat("^", regexp2MaxPatternBytes), strings.Repeat("x", regexp2MaxPatternBytes+1)} {
			_, err := rs.getRegexp2MatcherWithMatchType(pattern, "m", binary)
			require.ErrorContains(t, err, "maximum supported size")
			require.Empty(t, rs.icu)
			require.Zero(t, rs.icuBytes)
		}
		pattern := strings.Repeat("^", regexp2MaxPatternBytes-len("(?m)"))
		rule, err := regexpPatternWithPureMatchType(pattern, "m", binary)
		require.NoError(t, err)
		require.LessOrEqual(t, len(rule), regexp2MaxPatternBytes)
		_, err = regexpPatternWithPureMatchType(pattern+"^", "m", binary)
		require.ErrorContains(t, err, "maximum supported size")
	}
}
