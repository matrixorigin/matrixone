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

// TestRegexpReviewMixedCoverageClasses covers review round 5 finding 1:
// a class whose intervals (or atom pair) already prove full coverage must
// resolve even when another member is unresolved (shorthand, property or
// POSIX), so ^(?:[^\x00-\x{10ffff}\d]|\nx) and ^(?:[^\s\S[:alpha:]]|\nx)
// count the empty first arm as matching nothing and stop masking the
// CRLF-interior match. Expected values match the ICU 77.1 probes rev14
// rows R5A1..R5A13 and rev14b rows R5C3/R5C3-mu/R5C4. The (?=) rows force
// the regexp2 route.
func TestRegexpReviewMixedCoverageClasses(t *testing.T) {
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
		// Discriminating: pre-fix the empty first arm conservatively
		// counts as consuming content, masking the match (ICU: true).
		{"R5A1_interval_digit", `^(?:[^\x00-\x{10ffff}\d]|\nx)`, aCRLFxb, "m", true},
		{"R5A2_posix_pair", `^(?:[^\s\S[:alpha:]]|\nx)`, aCRLFxb, "m", true},
		{"R5A13_interval_posix", `^(?:[^\x00-\x{10ffff}[:alpha:]]|\nx)`, aCRLFxb, "m", true},
		{"R5A3_star", `^(?:[^\x00-\x{10ffff}\d])*\nx`, aCRLFxb, "m", true},
		{"R5A4_star", `^(?:[^\s\S[:alpha:]])*\nx`, aCRLFxb, "m", true},
		{"R5A4_lazy", `^(?:[^\s\S[:alpha:]])*?\nx`, aCRLFxb, "m", true},
		{"R5A6_r2", `^(?:[^\x00-\x{10ffff}\d]|\nx)(?=)`, aCRLFxb, "m", true},
		{"R5A7_r2", `^(?:[^\s\S[:alpha:]]|\nx)(?=)`, aCRLFxb, "m", true},
		{"R5C3_fold_scoped", `^(?:(?i:[^\s\S[:alpha:]])|\nx)`, aCRLFxb, "m", true},
		// Adjacent-LF controls (ICU true [2,4) under both modes).
		{"R5A5_adjacent", `^(?:[^\x00-\x{10ffff}\d]|\nx)`, aLFLFxb, "m", true},
		{"R5A5_adjacent_mu", `^(?:[^\x00-\x{10ffff}\d]|\nx)`, aLFLFxb, "mu", true},
		{"R5A12_adjacent_star", `^(?:[^\x00-\x{10ffff}\d])*\nx`, aLFLFxb, "m", true},
		{"R5C4_fold_adjacent", `^(?:(?i:[^\s\S[:alpha:]])|\nx)`, aLFLFxb, "m", true},
		// mu controls: no CRLF-interior boundary in Unix-only mode (ICU
		// false).
		{"R5A1_mu", `^(?:[^\x00-\x{10ffff}\d]|\nx)`, aCRLFxb, "mu", false},
		{"R5A2_mu", `^(?:[^\s\S[:alpha:]]|\nx)`, aCRLFxb, "mu", false},
		{"R5A3_star_mu", `^(?:[^\x00-\x{10ffff}\d])*\nx`, aCRLFxb, "mu", false},
		{"R5C3_fold_mu", `^(?:(?i:[^\s\S[:alpha:]])|\nx)`, aCRLFxb, "mu", false},
		// Guards: identical pre and post. Mandatory repetitions of the
		// empty class and a dead head never match (ICU false); the
		// non-negated full interval plus atom is a live consuming arm
		// matching 'a' at [0,1) (ICU R5A8 true).
		{"R5A9_mandatory", `^(?:[^\x00-\x{10ffff}\d])+\nx`, aCRLFxb, "m", false},
		{"R5A10_mandatory", `^(?:[^\s\S[:alpha:]])+\nx`, aCRLFxb, "m", false},
		{"R5A11_dead_head", `^[^\s\S[:alpha:]]x`, aCRLFxb, "m", false},
		{"R5A8_live_full", `^(?:[\x00-\x{10ffff}\d]|\nx)`, aCRLFxb, "m", true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the revived match [2,4) for one
	// row per unresolved-member family.
	for _, pattern := range []string{
		`^(?:[^\x00-\x{10ffff}\d]|\nx)`,
		`^(?:[^\s\S[:alpha:]]|\nx)`,
		`^(?:[^\x00-\x{10ffff}[:alpha:]]|\nx)`,
		`^(?:(?i:[^\s\S[:alpha:]])|\nx)`,
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
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[^\x00-\x{10ffff}\d]|\nx)`, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	// The live-full guard lands on 'a' at [0,1).
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[\x00-\x{10ffff}\d]|\nx)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	// regexp2 route positional fidelity on the interior match.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[^\s\S[:alpha:]]|\nx)(?=)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^(?:[^\x00-\x{10ffff}\d]|\nx)(?=)`, aCRLFxb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
}

// TestRegexpReviewSubtractionMinuend covers review round 5 finding 2: a
// regexp2 '-[...]' set subtraction keeps the just-scanned member, so
// [a-[b]] resolves to the live singleton {a} exactly like the vendored
// fork (internal/regexp2/syntax/parser.go retains chPrev before scanning
// the nested class). The alternation then classifies like ^(?:a|\nx):
// masked, so the CRLF-interior candidate drops (ICU 77.1 probes rev14
// rows R5B1..R5B6 read [a-[b]] as literals, which yields the same
// alternation verdicts; the {a} shape itself is fork-source evidence).
// Go's RE2 rejects [a-[b]] as a reversed character-class range, and a
// parse error does not fall back to the regexp2 route, so every
// executable subtraction row carries the trailing (?=) anchor that puts
// the pattern on the regexp2 route; the unsuffixed form keeps its clean
// parse error (routing pin, identical pre and post).
func TestRegexpReviewSubtractionMinuend(t *testing.T) {
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
		// Discriminating: pre-fix [a-[b]] resolves empty, the empty arm
		// grants StringExempt and the engine accepts the CRLF-interior
		// match at [2,4) (ICU: false).
		{"R5B1_alternation", `^(?:[a-[b]]|\nx)(?=)`, zCRLFxy, "m", false},
		{"R5B7_nested", `^(?:[a-[b-[c]]]|\nx)(?=)`, zCRLFxy, "m", false},
		{"R5B6_star", `^(?:[a-[b]])*\nx(?=)`, zCRLFxy, "m", false},
		{"R5B6_lazy", `^(?:[a-[b]])*?\nx(?=)`, zCRLFxy, "m", false},
		// mu controls (ICU false pre and post: mu has no CRLF-interior
		// boundary at all).
		{"R5B4_sub_mu", `^(?:[a-[b]]|\nx)(?=)`, zCRLFxy, "mu", false},
		{"R5B6_star_mu", `^(?:[a-[b]])*\nx(?=)`, zCRLFxy, "mu", false},
		// Adjacent-LF controls (ICU true [2,4) under both modes).
		{"R5B4_adjacent", `^(?:[a-[b]]|\nx)(?=)`, zLFLFxy, "m", true},
		{"R5B4_adjacent_mu", `^(?:[a-[b]]|\nx)(?=)`, zLFLFxy, "mu", true},
		{"R5B5_star_adjacent", `^(?:[a-[b]])*\nx(?=)`, zLFLFxy, "m", true},
		{"R5C6_star_adjacent_mu", `^(?:[a-[b]])*\nx(?=)`, zLFLFxy, "mu", true},
		{"R5B7_nested_adjacent", `^(?:[a-[b-[c]]]|\nx)(?=)`, zLFLFxy, "m", true},
		// Live-minuend controls: the retained member matches at [0,1).
		{"R5B3_live_a", `^(?:[a-[b]]|\nx)(?=)`, aCRLFxy, "m", true},
		{"R5B3_live_a_mu", `^(?:[a-[b]]|\nx)(?=)`, aCRLFxy, "mu", true},
		{"R5B8_live_pair_ab", `^(?:[ab-[cd]]|\nx)(?=)`, bCRLFxy, "m", true},
		// Bare-class fork-truth pins: [a-[b]] == {a}.
		{"R5B9a_class_a", `^[a-[b]](?=)`, "a", "m", true},
		{"R5B9b_class_b", `^[a-[b]](?=)`, "b", "m", false},
		{"R5B9c_class_dash", `^[a-[b]](?=)`, "-", "m", false},
		{"R5B9d_class_empty", `^[a-[a]](?=)`, "a", "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Routing pin: without the (?=) anchor the pattern stays on the RE2
	// route and rejects the reversed range, identical pre and post (the
	// fork's subtraction reading remains reachable through the regexp2
	// route only).
	_, err := op.regMap.regularLikeWithMode(`^(?:[a-[b]]|\nx)`, zCRLFxy, "m", false)
	require.Error(t, err)

	// Positional/replacement fidelity: the sealed interior reports no
	// match; the adjacent and live-head rows keep exact positions.
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err := op.regMap.regularSubstrWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zCRLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, zCRLFxy, replaced)
	// Adjacent match [2,4) through every entry point.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zLFLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zLFLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, zLFLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nXy", replaced)
	// The star form lands on the same adjacent match.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[b]])*\nx(?=)`, zLFLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// Live minuend matches at [0,1) with replacement fidelity.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, aCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?:[a-[b]]|\nx)(?=)`, aCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "X\r\nxy", replaced)
}

// TestRegexpReviewRound5ScalingShapes pins bounded compilation cost for
// the round-5 constructs at the SQL entry points: long runs of mixed
// full-coverage-plus-atom classes and of set subtractions must compile
// and evaluate without error and with the same verdicts as short forms.
// The classifier scaling evidence lives in
// BenchmarkClassifyMixedClassScalingR5 and
// BenchmarkClassifySubtractionScalingR5.
func TestRegexpReviewRound5ScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// 60 empty full-interval-plus-shorthand classes then a literal:
	// matches nothing on either route.
	empties := strings.Repeat(`[^\x00-\x{10ffff}\d]`, 60) + `x`
	got, err := op.regMap.regularLikeWithMode(`^`+empties, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+empties+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// 40 optional empty mixed classes: the interior match survives.
	arms := strings.Repeat(`(?:[^\s\S[:alpha:]])?`, 40)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^`+arms+`\nx`, "a\r\nxb", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// 60 retained minuends evaluate as {a} chains on the regexp2 route
	// (the trailing (?=) selects that route over the RE2 parse error).
	sixty := strings.Repeat("a", 60)
	got, err = op.regMap.regularLikeWithMode(`^`+strings.Repeat(`[a-[b]]`, 60)+`(?=)`, sixty, "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+strings.Repeat(`[a-[b-[c]]]`, 60)+`(?=)`, sixty, "m", false)
	require.NoError(t, err)
	require.True(t, got)
}
