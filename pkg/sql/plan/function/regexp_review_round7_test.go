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

	"github.com/stretchr/testify/require"
)

// TestRegexpReviewMixedClassCoverage covers review round 7 finding 1:
// membership of the shorthand, POSIX and property classes is exact on
// both production routes (regexp/syntax.Perl on the RE2 route; the
// vendored fork compiled with regexp2.RE2 at regexp_icu.go:1463/2086),
// so a negated union that covers every rune - complement pairs across
// families, subset relationships, or De Morgan property complements -
// resolves to an empty class instead of conservatively masking the
// parallel \nx arm. Expected values match the ICU 77.1 probes rev16
// rows R7A1..R7A17 and rev16b rows R7A13..R7A17. Plain rows exercise
// the RE2 route; the (?=) rows force the regexp2 route.
func TestRegexpReviewMixedClassCoverage(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		aCRLFxb = "a\r\nxb"
		aLFLFxb = "a\n\nxb"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: pre-fix the mixed families never resolve, so
		// the empty first arm conservatively counts as live consuming
		// content, masking the match (ICU rev16 R7A1..R7A8: true [2,4)).
		{"R7A1_alternation", `^(?:[^[:digit:]\D]|\nx)`, aCRLFxb, "m", true},
		{"R7A3_word", `^(?:[^[:word:]\W]|\nx)`, aCRLFxb, "m", true},
		{"R7A4_space", `^(?:[^[:space:]\S]|\nx)`, aCRLFxb, "m", true},
		{"R7A5_alnum", `^(?:[^[:alnum:][:^alpha:]]|\nx)`, aCRLFxb, "m", true},
		{"R7A6_lower", `^(?:[^[:alpha:][:^lower:]]|\nx)`, aCRLFxb, "m", true},
		{"R7A7_r2", `^(?:[^[:digit:]\D]|\nx)(?=)`, aCRLFxb, "m", true},
		{"R7A8_star", `^(?:[^[:digit:]\D])*\nx`, aCRLFxb, "m", true},
		{"R7A15_prop", `^(?:[^\p{L}\P{Lu}]|\nx)`, aCRLFxb, "m", true},
		{"R7A16_demorgan", `^(?:[^\P{Lu}\P{Ll}]|\nx)(?=)`, aCRLFxb, "m", true},
		// Adjacent-LF controls (ICU true [2,4) under both modes).
		{"R7A2_adjacent", `^(?:[^[:digit:]\D]|\nx)`, aLFLFxb, "m", true},
		{"R7A2_adjacent_mu", `^(?:[^[:digit:]\D]|\nx)`, aLFLFxb, "mu", true},
		{"R7A15_adj", `^(?:[^\p{L}\P{Lu}]|\nx)`, aLFLFxb, "m", true},
		// mu controls: no CRLF-interior boundary in Unix-only mode (ICU
		// false).
		{"R7A1_mu", `^(?:[^[:digit:]\D]|\nx)`, aCRLFxb, "mu", false},
		{"R7A15_mu", `^(?:[^\p{L}\P{Lu}]|\nx)`, aCRLFxb, "mu", false},
		// Guards: identical pre and post. A mandatory repetition of the
		// empty class and a dead head never match (ICU R7A9/R7A10
		// false); the genuinely live ~word arm and the ~L union stay
		// live and seal the interior (ICU rev16b R7A17/R7A13 false).
		{"R7A9_mandatory", `^(?:[^[:digit:]\D])+\nx`, aCRLFxb, "m", false},
		{"R7A10_dead_head", `^[^[:digit:]\D]x`, aCRLFxb, "m", false},
		{"R7A17_live_word", `^(?:[^[:digit:]\w]|\nx)`, aCRLFxb, "m", false},
		{"R7A17_live_word_mu", `^(?:[^[:digit:]\w]|\nx)`, aCRLFxb, "mu", false},
		{"R7A13_prop_partition", `^(?:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}]|\nx)`, aCRLFxb, "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the revived match [2,4), on
	// both routes (plain = RE2, (?=) = regexp2).
	for _, pattern := range []string{
		`^(?:[^[:digit:]\D]|\nx)`,
		`^(?:[^[:digit:]\D]|\nx)(?=)`,
		`^(?:[^\p{L}\P{Lu}]|\nx)`,
	} {
		pos, err := op.regMap.regularInstrWithMatchType(pattern, aCRLFxb, 1, 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, int64(3), pos, "pattern %q", pattern)
		matched, value, err := op.regMap.regularSubstrWithMatchType(pattern, aCRLFxb, 1, 1, false, "m")
		require.NoError(t, err)
		require.True(t, matched, "pattern %q", pattern)
		require.Equal(t, "\nx", value, "pattern %q", pattern)
		replaced, err := op.regMap.regularReplaceWithMatchType(pattern, aCRLFxb, "X", 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, "a\rXb", replaced, "pattern %q", pattern)
	}
	// mu seals the interior through the positional entries.
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[^[:digit:]\D]|\nx)`, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	// Bare-class pins: the empty mixed classes match nothing live on the
	// regexp2 route, and the fork's POSIX digit table is Unicode Nd
	// while the RE2 route keeps ASCII [0-9] (divergence documented by
	// the engine-conformance unit test).
	for _, tc := range []struct {
		pattern string
		subject string
		want    bool
	}{
		{`^[^[:digit:]\D](?=)`, "a", false},
		{`^[^[:digit:]\D](?=)`, "5", false},
		{`^[^[:digit:]\D](?=)`, "٩", false}, // U+0666
		{`^[^[:digit:]\D](?=)`, "+", false},
		{`^[^[:word:]\W](?=)`, "_", false},
		{`^[^[:space:]\S](?=)`, " ", false},
		{`^[^[:space:]\S](?=)`, " ", false}, // U+00A0
		{`^[^[:space:]\S](?=)`, "a", false},
		{`^[^\p{L}\P{Lu}](?=)`, "5", false},
		{`^[^\p{L}\P{Lu}](?=)`, "\n", false},
		{`^[[:digit:]](?=)`, "٩", true}, // fork: Nd
		{`^[[:digit:]]`, "٩", false},    // RE2 route: [0-9]
		{`^[[:space:]](?=)`, " ", true}, // fork: ECMA space includes NBSP
		{`^[[:space:]]`, " ", false},    // RE2 route: ASCII space
	} {
		got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, "m", false)
		require.NoError(t, err, "pattern %q subject %q", tc.pattern, tc.subject)
		require.Equal(t, tc.want, got, "pattern %q subject %q", tc.pattern, tc.subject)
	}
}

// TestRegexpReviewFoldedSubtraction covers review round 7 finding 2:
// under case folding the engines fold every operand BEFORE complement
// and subtraction (the fork lowercases the completed operand charset at
// internal/regexp2/syntax/parser.go:1623-1627 -> charclass.go
// addLowercase, ahead of the CharIn membership negation at
// charclass.go:281-288), so (?i)[a-[^A]] is the live set {a,A}; the
// classifier folded the raw intervals after subtracting instead,
// resolving the class to empty and granting a bogus string exemption
// that admitted the CRLF-interior match. Expected values match the ICU
// 77.1 probes rev16 rows R7B1..R7B4 and the literal-equivalent rows
// R7C1..R7C3 (ICU reads -[ literally inside classes; the alternation
// verdicts agree on every subject, and the fork's membership is the
// production truth for the bare-class pins). Every executable row
// carries the trailing (?=) anchor that puts the pattern on the
// regexp2 route; the unsuffixed form keeps its clean parse error.
func TestRegexpReviewFoldedSubtraction(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
		aCRLFxy = "a\r\nxy"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: pre-fix (?i:[a-[^A]]) resolves empty, so the
		// optional dead unit grants the exemption and admits the
		// CRLF-interior match; ICU rejects it (rev16 R7B1/R7B2 false;
		// the literal equivalent R7C1 agrees).
		{"R7B1_opt", `^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R7B1_opt_mu", `^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, "mu", false},
		{"R7B2_opt_rev", `^(?i:[A-[^a]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R7B1_kelvin", `^(?i:[k-[^k]])?\nx(?=)`, zCRLFxy, "m", false},
		// Adjacent-LF controls (ICU true [2,4) under both modes, pre and
		// post).
		{"R7B1_adjacent", `^(?i:[a-[^A]])?\nx(?=)`, zLFLFxy, "m", true},
		{"R7B1_adjacent_mu", `^(?i:[a-[^A]])?\nx(?=)`, zLFLFxy, "mu", true},
		{"R7B2_adjacent", `^(?i:[A-[^a]])?\nx(?=)`, zLFLFxy, "m", true},
		// Live-minuend control: the {a,A} arm matches 'a' at [0,1)
		// (ICU R7B4 true under both modes).
		{"R7B4_live", `^(?i:[a-[^A]]|\nx)(?=)`, aCRLFxy, "m", true},
		{"R7B4_live_mu", `^(?i:[a-[^A]]|\nx)(?=)`, aCRLFxy, "mu", true},
		// Unfolded guard, identical pre and post: [a-[^A]] is empty
		// without fold, so the dead optional still grants the exemption
		// (round-6 semantics; ICU-literal reading R7B* diverges on
		// membership, not on this verdict).
		{"R7B5_unfolded", `^(?:[a-[^A]])?\nx(?=)`, zCRLFxy, "m", true},
		{"R7B5_unfolded_mu", `^(?:[a-[^A]])?\nx(?=)`, zCRLFxy, "mu", false},
		{"R7B5_unfolded_adj", `^(?:[a-[^A]])?\nx(?=)`, zLFLFxy, "m", true},
		// Bare-class fork-truth pins: (?i)[a-[^A]] == {a,A},
		// (?i)[k-[^k]] == {k,K,U+212A}; the unfolded [a-[^A]] is empty.
		{"R7B3a_class_a", `^(?i:[a-[^A]])(?=)`, "a", "m", true},
		{"R7B3b_class_A", `^(?i:[a-[^A]])(?=)`, "A", "m", true},
		{"R7B3c_class_b", `^(?i:[a-[^A]])(?=)`, "b", "m", false},
		{"R7B3d_class_dash", `^(?i:[a-[^A]])(?=)`, "-", "m", false},
		{"R7B3e_class_k", `^(?i:[k-[^k]])(?=)`, "k", "m", true},
		{"R7B3f_class_K", `^(?i:[k-[^k]])(?=)`, "K", "m", true},
		{"R7B3g_class_kelvin", `^(?i:[k-[^k]])(?=)`, "K", "m", true}, // U+212A
		{"R7B3h_class_x", `^(?i:[k-[^k]])(?=)`, "x", "m", false},
		{"R7B3i_unfolded_a", `^[a-[^A]](?=)`, "a", "m", false},
		{"R7B3j_unfolded_A", `^[a-[^A]](?=)`, "A", "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Routing pin: without the (?=) anchor the pattern stays on the RE2
	// route and rejects the reversed range, identical pre and post.
	_, err := op.regMap.regularLikeWithMode(`^(?i:[a-[^A]])?\nx`, zCRLFxy, "m", false)
	require.Error(t, err)

	// Positional/replacement fidelity: the sealed interior reports no
	// match; the adjacent control matches [2,4); the live minuend lands
	// at [0,1).
	pos, err := op.regMap.regularInstrWithMatchType(`^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err := op.regMap.regularSubstrWithMatchType(`^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, zCRLFxy, replaced)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[a-[^A]])?\nx(?=)`, zLFLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[a-[^A]])?\nx(?=)`, zLFLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nXy", replaced)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[a-[^A]]|\nx)(?=)`, aCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[a-[^A]]|\nx)(?=)`, aCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "X\r\nxy", replaced)
}

// TestRegexpReviewRound7ScalingShapes pins bounded compilation cost for
// the round-7 constructs at the SQL entry points: long runs of empty
// mixed-coverage classes and of folded live subtractions must compile
// and evaluate without error and with the same verdicts as the short
// forms. The classifier scaling evidence lives in
// BenchmarkClassifyMixedClassScalingR7 and
// BenchmarkClassifyFoldSubtractionScalingR7.
func TestRegexpReviewRound7ScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// 60 empty mixed-coverage classes then a literal: matches nothing on
	// either route.
	empties := strings.Repeat(`[^[:digit:]\D]`, 60) + `x`
	got, err := op.regMap.regularLikeWithMode(`^`+empties, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+empties+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// 40 optional empty mixed classes: the interior match survives.
	arms := strings.Repeat(`(?:[^[:digit:]\D])?`, 40)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// 60 folded live subtractions then a literal: no match; the engine
	// keeps {a,A} and 'x' is not a member.
	foldLive := strings.Repeat(`(?i:[a-[^A]])`, 60) + `x`
	got, err = op.regMap.regularLikeWithMode(`^`+foldLive+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}
