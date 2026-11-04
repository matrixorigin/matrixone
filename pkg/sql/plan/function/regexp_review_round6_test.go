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

// TestRegexpReviewPosixComplementPairs covers review round 6 finding 1:
// a POSIX class member keeps its polarity, so [[:alpha:]] paired with
// its own complement [[:^alpha:]] in the same body proves full coverage
// and [^[:alpha:][:^alpha:]] matches nothing, exactly like the production
// engines (the vendored fork reads the caret in RE2 mode at
// internal/regexp2/syntax/parser.go:1545-1563 and Go's RE2 supports the
// [:^name:] form natively). Expected values match the ICU 77.1 probes
// rev15 rows R6A1..R6A12. Plain rows exercise the RE2 route (Go accepts
// POSIX classes); the (?=) rows force the regexp2 route.
func TestRegexpReviewPosixComplementPairs(t *testing.T) {
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
		// Discriminating: pre-fix the pair never resolves (the caret hid
		// inside the atom name), so the empty first arm conservatively
		// counts as consuming content, masking the match (ICU: true).
		{"R6A1_alternation", `^(?:[^[:alpha:][:^alpha:]]|\nx)`, aCRLFxb, "m", true},
		{"R6A3_word", `^(?:[^[:word:][:^word:]]|\nx)`, aCRLFxb, "m", true},
		{"R6A12_digit", `^(?:[^[:digit:][:^digit:]]|\nx)`, aCRLFxb, "m", true},
		{"R6A4_r2", `^(?:[^[:alpha:][:^alpha:]]|\nx)(?=)`, aCRLFxb, "m", true},
		{"R6A5_star", `^(?:[^[:alpha:][:^alpha:]])*\nx`, aCRLFxb, "m", true},
		{"R6A6_lazy", `^(?:[^[:alpha:][:^alpha:]])*?\nx`, aCRLFxb, "m", true},
		{"R6A10_fold_scoped", `^(?:(?i:[^[:alpha:][:^alpha:]])|\nx)`, aCRLFxb, "m", true},
		// Adjacent-LF controls (ICU true [2,4) under both modes).
		{"R6A2_adjacent", `^(?:[^[:alpha:][:^alpha:]]|\nx)`, aLFLFxb, "m", true},
		{"R6A2_adjacent_mu", `^(?:[^[:alpha:][:^alpha:]]|\nx)`, aLFLFxb, "mu", true},
		// mu controls: no CRLF-interior boundary in Unix-only mode (ICU
		// false).
		{"R6A1_mu", `^(?:[^[:alpha:][:^alpha:]]|\nx)`, aCRLFxb, "mu", false},
		{"R6A10_fold_mu", `^(?:(?i:[^[:alpha:][:^alpha:]])|\nx)`, aCRLFxb, "mu", false},
		// Guards: identical pre and post. A mandatory repetition of the
		// empty class and a dead head never match (ICU false); the single
		// negated member is a live class that seals the interior (ICU
		// R6A8 false); the non-negated pair is a live all-rune arm that
		// matches 'a' at [0,1) (ICU R6A9 true).
		{"R6A7_mandatory", `^(?:[^[:alpha:][:^alpha:]])+\nx`, aCRLFxb, "m", false},
		{"R6A11_dead_head", `^[^[:alpha:][:^alpha:]]x`, aCRLFxb, "m", false},
		{"R6A8_single_neg", `^(?:[^[:alpha:]]|\nx)`, aCRLFxb, "m", false},
		{"R6A9_nonneg_pair", `^(?:[[:alpha:][:^alpha:]]|\nx)`, aCRLFxb, "m", true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the revived match [2,4), on the
	// RE2 route (plain) and the regexp2 route ((?=)).
	for _, pattern := range []string{
		`^(?:[^[:alpha:][:^alpha:]]|\nx)`,
		`^(?:[^[:alpha:][:^alpha:]]|\nx)(?=)`,
		`^(?:(?i:[^[:alpha:][:^alpha:]])|\nx)`,
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
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[^[:alpha:][:^alpha:]]|\nx)`, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	// The live non-negated pair lands on 'a' at [0,1).
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[[:alpha:][:^alpha:]]|\nx)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
}

// TestRegexpReviewSubtractionNegation covers review round 6 finding 2:
// the class complement applies BEFORE the subtraction (the vendored
// fork's CharIn negates the whole set before recursing into the
// subtracted sub-charset, internal/regexp2/syntax/charclass.go:281-288),
// so [^a-[\s\S]] = (~a) minus everything = empty; and the nested
// subtraction class recognizes a leading '^' recursively, so
// [a-[^a]] = {a} minus ~{a} = {a} (CharSet from
// internal/regexp2/syntax/parser.go scanCharSet). Expected values match
// the ICU 77.1 probes rev15 rows R6B1..R6B9 and the literal-equivalent
// rows R6C1..R6C3; ICU reads -[ literally (rev15 R6B5a/R6B5b true on
// both 'a' and 'b'), which yields the same alternation verdicts as the
// fork's {a} reading on every probe subject, and the same empty reading
// as the fork for [^a-[\s\S]] (R6B4a/R6B4b false). Go's RE2 rejects the
// -[ subtraction as a reversed character-class range, so every
// executable row carries the trailing (?=) anchor that puts the pattern
// on the regexp2 route; the unsuffixed form keeps its clean parse error
// (routing pin, identical pre and post).
func TestRegexpReviewSubtractionNegation(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
		aCRLFxy = "a\r\nxy"
		bCRLFxy = "b\r\nxy"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: pre-fix [^a-[\s\S]] resolves live (~∅ after
		// the wrong-order subtraction), masking the match (ICU: true);
		// pre-fix [a-[^a]] resolves empty (the nested caret was a
		// literal), admitting the interior match (ICU: false).
		{"R6B1_alternation", `^(?:[^a-[\s\S]]|\nx)(?=)`, zCRLFxy, "m", true},
		{"R6B1b_proppair", `^(?:[^ab-[\d\D]]|\nx)(?=)`, zCRLFxy, "m", true},
		{"R6B8_star", `^(?:[^a-[\s\S]])*\nx(?=)`, zCRLFxy, "m", true},
		{"R6B6_opt", `^(?:[a-[^a]])?\nx(?=)`, zCRLFxy, "m", false},
		// mu controls (ICU false under mu for both families, pre and
		// post).
		{"R6B1_mu", `^(?:[^a-[\s\S]]|\nx)(?=)`, zCRLFxy, "mu", false},
		{"R6B6_opt_mu", `^(?:[a-[^a]])?\nx(?=)`, zCRLFxy, "mu", false},
		// Adjacent-LF controls (ICU true [2,4) under both modes, pre and
		// post).
		{"R6B2_adjacent", `^(?:[^a-[\s\S]]|\nx)(?=)`, zLFLFxy, "m", true},
		{"R6B2_adjacent_mu", `^(?:[^a-[\s\S]]|\nx)(?=)`, zLFLFxy, "mu", true},
		{"R6B6_opt_adjacent", `^(?:[a-[^a]])?\nx(?=)`, zLFLFxy, "m", true},
		// Live-minuend controls: the singleton lands at [0,1); under mu
		// the mu row is discriminating (pre-fix the dead arm's \nx
		// candidate is sealed at the CRLF even under m, and mu has no LF
		// start before it, so the pre-fix mu answer is false).
		{"R6B3_live_a", `^(?:[a-[^a]]|\nx)(?=)`, aCRLFxy, "m", true},
		{"R6B3_live_a_mu", `^(?:[a-[^a]]|\nx)(?=)`, aCRLFxy, "mu", true},
		{"R6B7_live_ab", `^(?:[ab-[^ab]]|\nx)(?=)`, bCRLFxy, "m", true},
		// Guards: identical pre and post. A mandatory repetition of the
		// empty class never matches (ICU R6B9 false); the negated plain
		// subtraction stays live ~{a,b} either way (ICU R6B7 true on
		// 'c', false on 'a').
		{"R6B9_mandatory", `^(?:[^a-[\s\S]])+\nx(?=)`, zCRLFxy, "m", false},
		{"R6B7_negb_c", `^[^a-[b]](?=)`, "c", "m", true},
		{"R6B7_negb_a", `^[^a-[b]](?=)`, "a", "m", false},
		// Bare-class fork-truth pins: [a-[^a]] == {a}, [ab-[^ab]] ==
		// {a,b}, [^a-[\s\S]] == empty. ICU's literal reading diverges on
		// 'b' here (rev15 R6B5b true); the fork's membership is the
		// production truth on this route.
		{"R6B5a_class_a", `^[a-[^a]](?=)`, "a", "m", true},
		{"R6B5b_class_b", `^[a-[^a]](?=)`, "b", "m", false},
		{"R6B5c_class_dash", `^[a-[^a]](?=)`, "-", "m", false},
		{"R6B5d_class_ab_a", `^[ab-[^ab]](?=)`, "a", "m", true},
		{"R6B5e_class_ab_c", `^[ab-[^ab]](?=)`, "c", "m", false},
		{"R6B4a_class_z", `^[^a-[\s\S]](?=)`, "z", "m", false},
		{"R6B4b_class_lf", `^[^a-[\s\S]](?=)`, "\n", "m", false},
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
	_, err := op.regMap.regularLikeWithMode(`^(?:[^a-[\s\S]]|\nx)`, zCRLFxy, "m", false)
	require.Error(t, err)
	_, err = op.regMap.regularLikeWithMode(`^(?:[a-[^a]])?\nx`, zCRLFxy, "m", false)
	require.Error(t, err)

	// Positional/replacement fidelity: the empty arm's revived match is
	// [2,4); the sealed interior of the live-singleton optional reports
	// no match.
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[^a-[\s\S]]|\nx)(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^(?:[^a-[\s\S]]|\nx)(?=)`, zCRLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^(?:[^a-[\s\S]]|\nx)(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\rXy", replaced)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[^a]])?\nx(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err = op.regMap.regularSubstrWithMatchType(`^(?:[a-[^a]])?\nx(?=)`, zCRLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?:[a-[^a]])?\nx(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, zCRLFxy, replaced)
	// Adjacent match [2,4) through every entry point.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[^a]])?\nx(?=)`, zLFLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?:[a-[^a]])?\nx(?=)`, zLFLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nXy", replaced)
	// Live singletons land at [0,1) with replacement fidelity.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[^a]]|\nx)(?=)`, aCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?:[a-[^a]]|\nx)(?=)`, aCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "X\r\nxy", replaced)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[ab-[^ab]]|\nx)(?=)`, bCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
}

// TestRegexpReviewRound6ScalingShapes pins bounded compilation cost for
// the round-6 constructs at the SQL entry points: long runs of empty
// POSIX complement pairs and of negated subtractions must compile and
// evaluate without error and with the same verdicts as short forms. The
// classifier scaling evidence lives in BenchmarkClassifyPosixPairScalingR6
// and BenchmarkClassifyNegatedSubtractionScalingR6.
func TestRegexpReviewRound6ScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// 60 empty POSIX complement pairs then a literal: matches nothing on
	// either route.
	empties := strings.Repeat(`[^[:alpha:][:^alpha:]]`, 60) + `x`
	got, err := op.regMap.regularLikeWithMode(`^`+empties, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+empties+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// 40 optional empty POSIX pairs: the interior match survives.
	arms := strings.Repeat(`(?:[^[:alpha:][:^alpha:]])?`, 40)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^`+arms+`\nx`, "a\r\nxb", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// 60 {a} singletons from nested negation evaluate as {a} chains on
	// the regexp2 route (the trailing (?=) selects that route over the
	// RE2 parse error).
	sixty := strings.Repeat("a", 60)
	got, err = op.regMap.regularLikeWithMode(`^`+strings.Repeat(`[a-[^a]]`, 60)+`(?=)`, sixty, "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// 60 empty outer-negation subtractions: no match anywhere.
	got, err = op.regMap.regularLikeWithMode(`^`+strings.Repeat(`[^a-[\s\S]]`, 60)+`x(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}
