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

// TestRegexpReviewFoldedPropertyCoverage covers review round 8 finding
// 1: folded Unicode properties resolve against each production engine's
// membership (Go RE2 merges the property with its FoldCategory /
// FoldScript companion and resolves canonical names plus aliases; the
// fork evaluates categories in lowercase space with its Ll/Lu/Lt triple
// and the runner's ToLower preimage), so an empty folded class grants
// the alternation the string exemption instead of masking the sole
// \nx-arm match. The fold scope closes around the class itself
// ((?i:...)): a leaked (?i) reaching the \nx arm folds that run too,
// and the engines keep the mask for a folded run. Expected values
// match the independent ICU 77.1 rev17 probes and the fork/Go engine
// membership probes (fork-probe-round8.log). Plain rows exercise the
// RE2 route; the trailing (?=) rows force the regexp2 route.
func TestRegexpReviewFoldedPropertyCoverage(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
		sCRLFxy = "s\r\nxy"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: the fold scope closes around the class itself
		// ((?i:...)), leaving the \nx arm unfolded. Pre-fix the folded
		// empty class stays a conservative atom, counts as live content
		// and masks the sole [2,4) match; post-fix it classifies empty
		// and the exemption revives the match (review evidence; ICU
		// rev17). mu revives nothing inside CRLF on these anchored
		// rows.
		{"R8A1_foldprop", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zCRLFxy, "m", true},
		{"R8A1_foldprop_mu", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zCRLFxy, "mu", false},
		{"R8A3_foldprop_r2", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)(?=)`, zCRLFxy, "m", true},
		{"R8A3_foldprop_r2_mu", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)(?=)`, zCRLFxy, "mu", false},
		{"R8A4_alias", `^(?:(?i:[^\p{Letter}\P{Lu}])|\nx)`, zCRLFxy, "m", true},
		{"R8A4_alias_mu", `^(?:(?i:[^\p{Letter}\P{Lu}])|\nx)`, zCRLFxy, "mu", false},
		// Round 9 (pre-PR review finding 1) corrects this row from a
		// mis-recorded true: the ICU compatibility contract keeps the
		// folded De Morgan class live (Ll minus Lu retains U+FB00
		// among others), so the scope masks the interior and the ICU
		// replay (rev18 E4-demorgan-plain-crlf NOMATCH) is restored.
		// The pre-round-8 behavior was already false; the round-8 row
		// had promoted a compatibility regression.
		{"R8A5_demorgan_fold_r2", `^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)(?=)`, zCRLFxy, "m", false},
		// Guards, identical pre and post. The review's top-level shape
		// has no active mask under m (no line-anchored start), and the
		// bare \nx arm matches anywhere on both engines, so mu matches
		// too. The leaked-(?i) group form folds the \nx arm itself, so
		// the folded-run rule keeps the mask up pre and post. The
		// folded letter partition is genuinely live on both engines;
		// the RE2 dialect keeps folded [^\p{Lu}\P{Ll}] live through
		// the ligature residual; a mandatory repetition and a dead
		// head never match; LF-LF adjacency matches under both modes.
		{"R8A1_toplevel", `^(?:(?i)[^\p{L}\P{Lu}])|\nx`, zCRLFxy, "m", true},
		{"R8A1_toplevel_mu", `^(?:(?i)[^\p{L}\P{Lu}])|\nx`, zCRLFxy, "mu", true},
		{"R8A1_toplevel_r2", `^(?:(?i)[^\p{L}\P{Lu}])|\nx(?=)`, zCRLFxy, "m", true},
		{"R8A3_leakedfold_r2", `^(?:(?i)[^\p{L}\P{Lu}]|\nx)(?=)`, zCRLFxy, "m", false},
		{"R8A3_leakedfold_r2_mu", `^(?:(?i)[^\p{L}\P{Lu}]|\nx)(?=)`, zCRLFxy, "mu", false},
		{"R8A2_foldprop_adj", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zLFLFxy, "m", true},
		{"R8A2_foldprop_adj_mu", `^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zLFLFxy, "mu", true},
		{"R8A6_partition", `^(?:(?i:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}])|\nx)(?=)`, zCRLFxy, "m", false},
		{"R8A6_partition_re2", `^(?:(?i:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}])|\nx)`, zCRLFxy, "m", false},
		{"R8A7_re2_ligature_residual", `^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)`, zCRLFxy, "m", false},
		{"R8A8_mandatory", `^(?:(?i)[^\p{L}\P{Lu}])+\nx`, zCRLFxy, "m", false},
		{"R8A9_dead_head", `^(?i:[^\p{L}\P{Lu}])x`, zCRLFxy, "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional and replacement fidelity on the revived match [2,4):
	// INSTR=3, SUBSTR "\nx" (0A78), REPLACE "z\rXy" (7A0D5879), and mu
	// reports no match.
	for _, pattern := range []string{
		`^(?:(?i:[^\p{L}\P{Lu}])|\nx)`,
		`^(?:(?i:[^\p{L}\P{Lu}])|\nx)(?=)`,
		`^(?:(?i:[^\p{Letter}\P{Lu}])|\nx)`,
	} {
		pos, err := op.regMap.regularInstrWithMatchType(pattern, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, int64(3), pos, "pattern %q", pattern)
		matched, value, err := op.regMap.regularSubstrWithMatchType(pattern, zCRLFxy, 1, 1, false, "m")
		require.NoError(t, err)
		require.True(t, matched, "pattern %q", pattern)
		require.Equal(t, "\nx", value, "pattern %q", pattern)
		replaced, err := op.regMap.regularReplaceWithMatchType(pattern, zCRLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, "z\rXy", replaced, "pattern %q", pattern)
	}
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zCRLFxy, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, zCRLFxy, "X", 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, zCRLFxy, replaced)

	// The live-minuend control consumes the first literal via the
	// alternation arm: (?i)\p{Lu} matches 's' under fold on both routes
	// (the leftmost match lands at [0,1)).
	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[\p{Lu}]|\nx)`, sCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[\p{Lu}]|\nx)(?=)`, sCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)

	// Bare-class membership pins: the folded empty class matches
	// nothing on either route, and the dialect split pins the RE2
	// ligature residual against the fork-empty truth.
	for _, tc := range []struct {
		pattern string
		subject string
		want    bool
	}{
		{`^(?i)[^\p{L}\P{Lu}]`, "a", false},
		{`^(?i)[^\p{L}\P{Lu}]`, "A", false},
		{`^(?i)[^\p{L}\P{Lu}]`, "5", false},
		{`^(?i)[^\p{L}\P{Lu}]`, "中", false},
		{`^(?i)[^\p{L}\P{Lu}]`, "ſ", false},
		{`^(?i)[^\p{L}\P{Lu}]`, "ﬀ", false}, // U+FB00
		{`^(?i)[^\p{L}\P{Lu}](?=)`, "a", false},
		{`^(?i)[^\p{L}\P{Lu}](?=)`, "5", false},
		{`^(?i)[^\p{L}\P{Lu}](?=)`, "中", false},
		{`^(?i)[^\p{L}\P{Lu}](?=)`, "ﬀ", false},
		{`^(?i)[^\p{Lu}\P{Ll}]`, "ﬀ", true}, // RE2: FB00 orbit survives
		// These rows pin the fork ENGINE's own consuming verdict
		// (its ToLower fiber), not the classifier contract: round 9
		// keeps the engine as-is while the mask derivation follows
		// the ICU closure; TestRegexpReviewRound9DemorganMask pins
		// the masked start-position behavior the contract drives.
		{`^(?i)[^\p{Lu}\P{Ll}](?=)`, "ﬀ", false}, // fork engine: fiber-miss
		{`^(?i)[^\p{Lu}\P{Ll}](?=)`, "ŉ", false}, // fork engine: fiber-miss
		{`^(?i)[^\p{Lu}\P{Ll}]`, "a", false},
		{`^(?i)[^\p{Lu}\P{Ll}](?=)`, "a", false},
	} {
		got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, "m", false)
		require.NoError(t, err, "pattern %q subject %q", tc.pattern, tc.subject)
		require.Equal(t, tc.want, got, "pattern %q subject %q", tc.pattern, tc.subject)
	}

	// Routing pin: the fork rejects Go's Letter alias outright, so the
	// (?=)-routed alias row is a clean engine error, identical pre and
	// post.
	_, err = op.regMap.regularLikeWithMode(`^(?:(?i)[^\p{Letter}\P{Lu}]|\nx)(?=)`, zCRLFxy, "m", false)
	require.Error(t, err)
}

// TestRegexpReviewForkFoldSubtraction covers review round 8 finding 2:
// the regexp2 route folds classes under the fork's lowercase semantics
// (each class lowercases its completed operand set at close, and the
// runner compares unicode.ToLower of the input), never under Go's
// SimpleFold orbits. (?i)[s-[ſ]] keeps the live set {s,S}; the
// SimpleFold reading erased it to empty and granted a bogus string
// exemption that admitted the CRLF-interior match. Expected values
// match the independent ICU 77.1 rev17 probes (ICU rejects the
// interior and accepts the adjacent-LF control) and the fork engine
// membership probes (fork-probe-round8.log). Every row carries the
// trailing (?=) anchor that puts the pattern on the regexp2 route.
func TestRegexpReviewForkFoldSubtraction(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
		zLFsLFx = "z\ns\nx"
		zLFslx  = "z\nſ\nx"
		sCRLFxy = "s\r\nxy"
	)
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		// Discriminating: pre-fix the class resolves empty (SimpleFold
		// orbit collides), granting the exemption that admits the
		// interior match; the fork keeps {s,S} live and seals it
		// (ICU rev17 agrees).
		{"R8B1_opt", `^(?i:[s-[ſ]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R8B1_opt_mu", `^(?i:[s-[ſ]])?\nx(?=)`, zCRLFxy, "mu", false},
		{"R8B2_opt_rev", `^(?i:[S-[ſ]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R8B3_longs", `^(?i:[ſ-[s]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R8B4_micro", `^(?i:[µ-[μ]])?\nx(?=)`, zCRLFxy, "m", false},
		// Adjacent-LF controls (true [2,4) under both modes).
		{"R8B1_adjacent", `^(?i:[s-[ſ]])?\nx(?=)`, zLFLFxy, "m", true},
		{"R8B1_adjacent_mu", `^(?i:[s-[ſ]])?\nx(?=)`, zLFLFxy, "mu", true},
		{"R8B3_adjacent", `^(?i:[ſ-[s]])?\nx(?=)`, zLFLFxy, "m", true},
		// Consuming controls: the optional live arm takes its member
		// before \nx (matches [2,5) with INSTR=3 through the arm).
		{"R8B1_consuming", `^(?i:[s-[ſ]])?\nx(?=)`, zLFsLFx, "m", true},
		{"R8B3_consuming", `^(?i:[ſ-[s]])?\nx(?=)`, zLFslx, "m", true},
		// Live-minuend control: the {s,S} arm matches 's' at [0,2).
		{"R8B1_live", `^(?i:[s-[ſ]]|\nx)(?=)`, sCRLFxy, "m", true},
		{"R8B1_live_mu", `^(?i:[s-[ſ]]|\nx)(?=)`, sCRLFxy, "mu", true},
		// Guards, identical pre and post: the fork-empty [s-[S]]
		// optional grants the exemption, the live Kelvin class seals,
		// and mu revives nothing inside CRLF.
		{"R8B5_dead", `^(?i:[s-[S]])?\nx(?=)`, zCRLFxy, "m", true},
		{"R8B5_dead_mu", `^(?i:[s-[S]])?\nx(?=)`, zCRLFxy, "mu", false},
		{"R8B6_kelvin_live", `^(?i:[k-[ſ]])?\nx(?=)`, zCRLFxy, "m", false},
		{"R8B7_round7_opt", `^(?i:[a-[^A]])?\nx(?=)`, zCRLFxy, "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional and replacement fidelity: the sealed interior reports
	// no match through all three positional entries; the consuming
	// controls land at [2,5); the live minuend lands at [0,2).
	pos, err := op.regMap.regularInstrWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err := op.regMap.regularSubstrWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zCRLFxy, 1, 1, false, "m")
	require.NoError(t, err)
	require.False(t, matched)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, zCRLFxy, replaced)

	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zLFLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zLFLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nXy", replaced)

	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zLFsLFx, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zLFsLFx, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "s\nx", value)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[s-[ſ]])?\nx(?=)`, zLFsLFx, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nX", replaced)

	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[ſ-[s]])?\nx(?=)`, zLFslx, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[ſ-[s]])?\nx(?=)`, zLFslx, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\nX", replaced)

	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[s-[ſ]]|\nx)(?=)`, sCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[s-[ſ]]|\nx)(?=)`, sCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "X\r\nxy", replaced)

	// The dead-optional guard admits the interior even under fold:
	// [s-[S]] is empty on the fork, so the optional grants the
	// exemption (identical pre and post).
	pos, err = op.regMap.regularInstrWithMatchType(`^(?i:[s-[S]])?\nx(?=)`, zCRLFxy, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	replaced, err = op.regMap.regularReplaceWithMatchType(`^(?i:[s-[S]])?\nx(?=)`, zCRLFxy, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "z\rXy", replaced)

	// Bare-class fork-truth pins: differing fold classes keep the
	// fork's exact singleton/partner memberships.
	for _, tc := range []struct {
		pattern string
		subject string
		want    bool
	}{
		{`^(?i:[s-[ſ]])(?=)`, "s", true},
		{`^(?i:[s-[ſ]])(?=)`, "S", true},
		{`^(?i:[s-[ſ]])(?=)`, "ſ", false},
		{`^(?i:[S-[ſ]])(?=)`, "S", true},
		{`^(?i:[ſ-[s]])(?=)`, "ſ", true},
		{`^(?i:[ſ-[s]])(?=)`, "s", false},
		{`^(?i:[ſ-[s]])(?=)`, "S", false},
		{`^(?i:[µ-[μ]])(?=)`, "µ", true},
		{`^(?i:[µ-[μ]])(?=)`, "μ", false},
		{`^(?i:[µ-[μ]])(?=)`, "Μ", false},
		{`^(?i:[μ-[µ]])(?=)`, "μ", true},
		{`^(?i:[μ-[µ]])(?=)`, "Μ", true},
		{`^(?i:[μ-[µ]])(?=)`, "µ", false},
		{`^(?i:[s])(?=)`, "ſ", false}, // ſ is not a fork fold partner of s
		{`^(?i:[k-[ſ]])(?=)`, "K", true},
	} {
		got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, "m", false)
		require.NoError(t, err, "pattern %q subject %q", tc.pattern, tc.subject)
		require.Equal(t, tc.want, got, "pattern %q subject %q", tc.pattern, tc.subject)
	}
}

// TestRegexpReviewRound8ScalingShapes pins bounded compilation cost
// for the round-8 constructs at the SQL entry points: long runs of
// empty folded-property classes and of fork folded subtractions must
// compile and evaluate without error with the same verdicts as the
// short forms. The classifier scaling evidence lives in the round-8
// scaling benchmarks.
func TestRegexpReviewRound8ScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// 60 empty folded-property classes then a literal: no match on
	// either route.
	empties := strings.Repeat(`(?i)[^\p{L}\P{Lu}]`, 60) + `x`
	got, err := op.regMap.regularLikeWithMode(`^`+empties, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+empties+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// 40 optional empty folded-property arms: the interior match
	// survives on both routes.
	arms := strings.Repeat(`(?:(?i)[^\p{L}\P{Lu}])?`, 40)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx(?=)`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// 60 fork folded live subtractions then a literal: no match (the
	// engine keeps {s,S} and 'x' is not a member).
	foldLive := strings.Repeat(`(?i:[s-[ſ]])`, 60) + `x`
	got, err = op.regMap.regularLikeWithMode(`^`+foldLive+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// A 512-member duplicate literal class under fold: no match, and
	// the classify path stays bounded (finding 3).
	dup := "(?i)[" + strings.Repeat("a", 512) + "]"
	got, err = op.regMap.regularLikeWithMode(`^`+dup, "b", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}
