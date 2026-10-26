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

// TestRegexpReviewMinusFlagGroups covers review round 3 finding 1: inline
// flag groups whose header begins with a minus ((?-i) and (?-i:...)) must be
// parsed as flag groups, not opaque spans, so a following caret keeps its
// line-anchor classification (scope restoration included). Expected values
// match the ICU 77.1 probe rev12 rows R1a..R1j on both engines; the (?=)
// rows force the regexp2 route.
func TestRegexpReviewMinusFlagGroups(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		aCRLFb  = "a\r\nb"
		aLFLFb  = "a\n\nb"
		aCRLFxb = "a\r\nxb"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: pre-fix the minus header turns the whole group
		// opaque, the mask is dropped, and the CRLF-interior LF matches
		// (ICU: false).
		{"R1a_crlf", `(?-i)^\n`, aCRLFb, "m", false},
		{"R1c_folded_header", `(?i-i)^\n`, aCRLFb, "m", false},
		{"R1d_scoped", `(?-i:^\n)x`, aCRLFxb, "m", false},
		{"R1a_r2_crlf", `(?-i)^\n(?=)`, aCRLFb, "m", false},
		// Guard rows: identical pre and post (ICU values).
		{"R1a_mu", `(?-i)^\n`, aCRLFb, "mu", false},
		{"R1b_adjacent", `(?-i)^\n`, aLFLFb, "m", true},
		{"R1b_mu", `(?-i)^\n`, aLFLFb, "mu", true},
		{"R1d_mu", `(?-i:^\n)x`, aCRLFxb, "mu", false},
		{"R1e_plain_scope", `(?:^\n)x`, aCRLFxb, "m", false},
		{"R1e_mu", `(?:^\n)x`, aCRLFxb, "mu", false},
		{"R1f_flat_run", `^\nx`, aCRLFxb, "m", true},
		{"R1f_mu", `^\nx`, aCRLFxb, "mu", false},
		{"R1g_minus_m", `(?-im)^\n`, aCRLFb, "m", false},
		{"R1g_mu", `(?-im)^\n`, aCRLFb, "mu", false},
		{"R1h_trailing", `(?i)^\n(?-i)`, aCRLFb, "m", false},
		{"R1i_mid_scope", `x(?-i:\n)`, "xx", "m", false},
		// Engine-level CRLF-interior assertion fires after the \r (guard;
		// classification never masks a zero-width-only pattern).
		{"R1j_lookaround", `(?-i)(?=^\n)`, aCRLFb, "m", true},
		// Scope restoration: the minus scope cannot leak out of its group.
		{"restore", `(?-i:(?i)^\n)x`, aCRLFxb, "m", false},
		{"restore_adjacent", `(?-i:(?i)^\n)x`, "a\n\nxb", "m", true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the adjacent-LF match [2,3).
	pos, err := op.regMap.regularInstrWithMatchType(`(?-i)^\n`, aLFLFb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`(?-i)^\n`, aLFLFb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\n", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(`(?-i)^\n`, aLFLFb, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\nXb", replaced)
	// CRLF interior stays sealed through the positional entry points.
	pos, err = op.regMap.regularInstrWithMatchType(`(?-i)^\n`, aCRLFb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err = op.regMap.regularSubstrWithMatchType(`(?-i)^\n`, aCRLFb, 1, 1, false, "m")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err = op.regMap.regularReplaceWithMatchType(`(?-i)^\n`, aCRLFb, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, aCRLFb, replaced)
	// The flat 2-rune run keeps its exempt start inside CRLF (ICU R1f).
	pos, err = op.regMap.regularInstrWithMatchType(`^\nx`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err = op.regMap.regularSubstrWithMatchType(`^\nx`, aCRLFxb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	// regexp2 route: suppressed interior, adjacent match.
	r2pos, err := op.regMap.regularInstrWithMatchType(`(?-i)^\n(?=)`, aLFLFb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), r2pos)
}

// TestRegexpReviewZeroMinRepetitions covers review round 3 finding 2: a
// zero-minimum repetition of a never-matching class/group bypasses the unit,
// so the live path must look as if the unit were absent. Expected values
// match ICU 77.1 rev12 rows R2a..R2l and rev12b rows R2m..R2u (live-unit
// guards that must stay masked).
func TestRegexpReviewZeroMinRepetitions(t *testing.T) {
	op := newOpBuiltInRegexp()
	dead := `[^\x00-\x{10ffff}]`
	const (
		aCRLFb  = "a\r\nb"
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
		// Discriminating: pre-fix the dead state leaks past the bypass and
		// the interior candidate is masked away (ICU: true [2,4)).
		{"R2a_star_group", `^(?:` + dead + `)*\nx`, aCRLFxb, "m", true},
		{"R2b_question", `^(?:` + dead + `)?\nx`, aCRLFxb, "m", true},
		{"R2c_bounded", `^(?:` + dead + `){0,2}\nx`, aCRLFxb, "m", true},
		{"R2d_bare_star", `^` + dead + `*\nx`, aCRLFxb, "m", true},
		{"R2dq_bare_question", `^` + dead + `?\nx`, aCRLFxb, "m", true},
		{"R2a_r2_route", `^(?:` + dead + `)*\nx(?=)`, aCRLFxb, "m", true},
		// mu controls: no interior LF candidate (ICU false).
		{"R2a_mu", `^(?:` + dead + `)*\nx`, aCRLFxb, "mu", false},
		{"R2d_mu", `^` + dead + `*\nx`, aCRLFxb, "mu", false},
		// Mandatory repetitions of the dead class never match; guards
		// against over-reviving (ICU false), pre and post identical.
		{"R2e_plus_group", `^(?:` + dead + `)+\nx`, aCRLFxb, "m", false},
		{"R2f_plus_bare", `^` + dead + `+\nx`, aCRLFxb, "m", false},
		{"R2e12_exact", `^(?:` + dead + `){1,2}\nx`, aCRLFxb, "m", false},
		// Zero-exact and flavor forms (ICU true), guards pre and post.
		{"R2i_exact_zero", `^(?:` + dead + `){0}\nx`, aCRLFxb, "m", true},
		{"R2g_lazy", `^(?:` + dead + `)*?x`, aCRLFxb, "m", true},
		{"R2h_possessive", `^(?:` + dead + `)*+x`, aCRLFxb, "m", true},
		{"R2j_adjacent", `^(?:` + dead + `)*\nx`, aLFLFxb, "m", true},
		// Live alternation arm keeps the mask (ICU false), guard.
		{"R2k_live_arm", `^(?:x|` + dead + `)*\nb`, aCRLFb, "m", false},
		// Mid-pattern dead star, unanchored (ICU true [0,1)), guard.
		{"R2l_mid_pattern", dead + `*x`, "xx", "m", true},
		// rev12b live-unit guards: masked before and after (ICU false).
		{"R2m_live_star", `^x*\ny`, "a\r\ny", "m", false},
		{"R2n_live_pair", `^(?:xy)*\nz`, "a\r\nz", "m", false},
		{"R2o_bounded_live", `^a{0,2}\ny`, "a\r\ny", "m", false},
		{"R2p_live_question", `^x?\ny`, "a\r\ny", "m", false},
		{"R2q_star_of_option", `^(?:x?)*\ny`, "a\r\ny", "m", false},
		{"R2t_all_live", `^(?:x|xx?)*\ny`, "a\r\ny", "m", false},
		// Plain control with no LF (ICU true [0,2)).
		{"R2u_plain", `^x*y`, "xy", "m", true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the bypass match [2,4).
	star := `^(?:` + dead + `)*\nx`
	pos, err := op.regMap.regularInstrWithMatchType(star, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(star, aCRLFxb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(star, aCRLFxb, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\rXb", replaced)
	// The lazy star matches only the x tail at [3,4).
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:`+dead+`)*?x`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(4), pos)
	// mu mode keeps Unix LF semantics through the same entries.
	pos, err = op.regMap.regularInstrWithMatchType(star, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err = op.regMap.regularSubstrWithMatchType(star, aCRLFxb, 1, 1, false, "mu")
	require.NoError(t, err)
	require.False(t, matched)
}

// TestRegexpReviewFoldComplementClass covers review round 3 finding 3: a
// negated multi-member class under case folding must apply the fold closure
// before the complement cardinality, so (?i:[^\x00-\x40\x42-\x{10ffff}]) is
// empty, not singleton 'A'. Expected values match ICU 77.1 rev12 rows
// R3a..R3h. The discriminating evidence is positional: pre-fix the folded-A
// arm steals the match at [0,1).
func TestRegexpReviewFoldComplementClass(t *testing.T) {
	op := newOpBuiltInRegexp()
	fcls := `(?i:[^\x00-\x40\x42-\x{10ffff}])`
	const (
		aCRLFxb = "a\r\nxb"
		aLFLFxb = "a\n\nxb"
		axb     = "Axb"
	)
	like := func(pattern, subject, mode string, want bool) {
		t.Helper()
		got, err := op.regMap.regularLikeWithMode(pattern, subject, mode, false)
		require.NoError(t, err)
		require.Equal(t, want, got, "pattern %q subject %q mode %q", pattern, subject, mode)
	}
	instr := func(pattern, subject, mode string, ret int8, want int64) {
		t.Helper()
		pos, err := op.regMap.regularInstrWithMatchType(pattern, subject, 1, 1, ret, false, mode)
		require.NoError(t, err)
		require.Equal(t, want, pos, "pattern %q subject %q mode %q", pattern, subject, mode)
	}
	substr := func(pattern, subject, mode string, wantMatch bool, want string) {
		t.Helper()
		matched, value, err := op.regMap.regularSubstrWithMatchType(pattern, subject, 1, 1, false, mode)
		require.NoError(t, err)
		require.Equal(t, wantMatch, matched, "pattern %q subject %q mode %q", pattern, subject, mode)
		if wantMatch {
			require.Equal(t, want, value)
		}
	}
	replace := func(pattern, subject, mode, want string) {
		t.Helper()
		got, err := op.regMap.regularReplaceWithMatchType(pattern, subject, "X", 1, 0, false, mode)
		require.NoError(t, err)
		require.Equal(t, want, got, "pattern %q subject %q mode %q", pattern, subject, mode)
	}

	// R3a: with the dead folded arm, only the \nx arm survives; like is
	// true through [2,4) on both sides of the fix, so the positional
	// entries carry the discrimination ([0,1) pre-fix, [2,4) post).
	ra := `^(?:` + fcls + `|\nx)`
	like(ra, aCRLFxb, "m", true)
	instr(ra, aCRLFxb, "m", 0, 3)
	substr(ra, aCRLFxb, "m", true, "\nx")
	replace(ra, aCRLFxb, "m", "a\rXb")
	// mu mode (ICU R3a-mu false): pre-fix the folded-A arm matches [0,1).
	like(ra, aCRLFxb, "mu", false)
	// R3b adjacent-LF control: only the \nx arm can match post-fix.
	rb := `^(?:` + fcls + `|\nx)`
	like(rb, aLFLFxb, "m", true)
	instr(rb, aLFLFxb, "m", 0, 3)
	substr(rb, aLFLFxb, "m", true, "\nx")
	// R3c: case-sensitive live arm consumes the full Ax (ICU [0,2)).
	rc := `^(?:` + fcls + `|Ax)`
	like(rc, axb, "m", true)
	instr(rc, axb, "m", 1, 3)
	substr(rc, axb, "m", true, "Ax")
	replace(rc, axb, "m", "Xb")
	// R3d: folded live arm consumes the full axb (ICU [0,3)).
	rd := `(?i)^(?:` + fcls + `|axb)`
	like(rd, axb, "m", true)
	substr(rd, axb, "m", true, "Axb")
	// R3e/R3f negative guards: folded empties never match (R3f is the
	// discriminating row: pre-fix the arm is the folded singleton A).
	like(`(?i)[^\x00-\x{10ffff}]`, "xx", "m", false)
	like(fcls, axb, "m", false)
	like(`x`+fcls+`y`, "xy", "m", false)
	// R3g/R3h: U+10FFFF singleton classes never match these subjects.
	like(`^[\x{10ffff}]x`, aCRLFxb, "m", false)
	like(`^[^\x00-\x{10fffe}]x`, aCRLFxb, "m", false)
	// regexp2 route with the same constructs.
	like(ra+`(?=)`, aCRLFxb, "m", true)
	instr(ra+`(?=)`, aCRLFxb, "m", 0, 3)
}

// TestRegexpReviewScalingShapes covers review round 3 findings 4 and 5 at
// the SQL entry points: long runs of escaped angle literals and repeated
// negated singleton classes must compile and evaluate without error and
// with the same verdicts as short forms. The scaling evidence lives in the
// package benchmarks; these rows pin result equivalence on both routes.
func TestRegexpReviewScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// Finding 4: 200 literal \< copies (fork-mirrored recognition must stop
	// at the first invalid body character).
	angles := strings.Repeat(`\<`, 200)
	got, err := op.regMap.regularLikeWithMode(angles, "<", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`(?=)`+angles, "<", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(angles, strings.Repeat("<", 200), "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// Finding 5: 40 negated singleton classes (complement walks interval
	// gaps instead of enumerating Unicode).
	negs := strings.Repeat(`[^\x00-\x{10fffe}]`, 40) + `x`
	got, err = op.regMap.regularLikeWithMode(`^`+negs, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+negs+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(negs, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}
