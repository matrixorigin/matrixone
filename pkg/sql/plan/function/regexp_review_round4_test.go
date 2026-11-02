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

// TestRegexpReviewNestedDeadAlternatives covers review round 4 finding 1:
// alternatives nested inside an already-dead arm must inherit the dead
// entry state, so ^(?:[^\x00-\x{10ffff}](?:a|b)|\nx) classifies like ^\nx
// and the CRLF-interior match survives. Expected values match the ICU 77.1
// probes rev13 rows R4Aa..R4Aj and rev13c rows R4D1, R4Aa-mu-adj,
// R4Aj-star and R4Ac-mu. The (?=) rows force the regexp2 route.
func TestRegexpReviewNestedDeadAlternatives(t *testing.T) {
	op := newOpBuiltInRegexp()
	dead := `[^\x00-\x{10ffff}]`
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
		// Discriminating: pre-fix the inner group revives its b arm inside
		// the dead outer arm, the extra initial content masks the sole
		// match [2,4) away (ICU: true).
		{"R4Aa_nested", `^(?:` + dead + `(?:a|b)|\nx)`, aCRLFxb, "m", true},
		{"R4Aa_r2", `^(?:` + dead + `(?:a|b)|\nx)(?=)`, aCRLFxb, "m", true},
		{"R4Ac_star", `^(?:` + dead + `(?:a|b))*\nx`, aCRLFxb, "m", true},
		{"R4Ad_lazy", `^(?:` + dead + `(?:a|b))*?\nx`, aCRLFxb, "m", true},
		{"R4Ac_r2_star", `^(?:` + dead + `(?:a|b))*\nx(?=)`, aCRLFxb, "m", true},
		{"R4Af_adjacent", `^(?:` + dead + `(?:a|b)|\nx)`, aLFLFxb, "m", true},
		{"R4Ag_adjacent_star", `^(?:` + dead + `(?:a|b))*\nx`, aLFLFxb, "m", true},
		// mu controls: no CRLF-interior LF boundary (ICU false); the
		// adjacent LF boundary is a Unix line start (ICU true).
		{"R4Aa_mu", `^(?:` + dead + `(?:a|b)|\nx)`, aCRLFxb, "mu", false},
		{"R4Ac_mu", `^(?:` + dead + `(?:a|b))*\nx`, aCRLFxb, "mu", false},
		{"R4Aa_mu_adjacent", `^(?:` + dead + `(?:a|b)|\nx)`, aLFLFxb, "mu", true},
		// Guards: identical pre and post. A mandatory repetition of the
		// dead group never matches (ICU R4Ae false); the inner alternation
		// entered live still masks (ICU R4Ah false); a live second arm
		// keeps its own verdict from engine enumeration (ICU R4Ai false).
		{"R4Ae_mandatory", `^(?:` + dead + `(?:a|b))+\nx`, aCRLFxb, "m", false},
		{"R4Ah_live_entry", `^(?:(?:a|b)` + dead + `|\nx)`, aCRLFxb, "m", false},
		{"R4Ai_live_arm", `^(?:` + dead + `(?:a|b)|x\nb)`, aCRLFxb, "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the revived match [2,4).
	nested := `^(?:` + dead + `(?:a|b)|\nx)`
	pos, err := op.regMap.regularInstrWithMatchType(nested, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(nested, aCRLFxb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(nested, aCRLFxb, "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\rXb", replaced)
	// The star form lands on the same match.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:`+dead+`(?:a|b))*\nx`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// mu mode keeps the interior sealed through the positional entries.
	pos, err = op.regMap.regularInstrWithMatchType(nested, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	matched, _, err = op.regMap.regularSubstrWithMatchType(nested, aCRLFxb, 1, 1, false, "mu")
	require.NoError(t, err)
	require.False(t, matched)
	// Adjacent LF matches identically through INSTR on both engines.
	pos, err = op.regMap.regularInstrWithMatchType(nested, aLFLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	pos, err = op.regMap.regularInstrWithMatchType(nested+`(?=)`, aLFLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// regexp2 route positional fidelity on the interior match.
	pos, err = op.regMap.regularInstrWithMatchType(nested+`(?=)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	matched, value, err = op.regMap.regularSubstrWithMatchType(nested+`(?=)`, aCRLFxb, 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
}

// TestRegexpReviewEmptyShorthandClasses covers review round 4 finding 2:
// classes built from supported shorthand/property atoms resolve their
// actual cardinality, so ^(?:[^\s\S]|\nx) and its complement-pair family
// count the empty first arm as matching nothing and stop masking the
// CRLF-interior match. Expected values match the ICU 77.1 probes rev13
// rows R4Ba..R4Bo, rev13b rows R4Ca..R4Cj and rev13c/rev13d adjuncts.
func TestRegexpReviewEmptyShorthandClasses(t *testing.T) {
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
		// Discriminating: pre-fix every empty first arm conservatively
		// counts as consuming content, masking the match (ICU: true).
		{"R4Ba_space_pair", `^(?:[^\s\S]|\nx)`, aCRLFxb, "m", true},
		{"R4Bb_digit_pair", `^(?:[^\d\D]|\nx)`, aCRLFxb, "m", true},
		{"R4Bc_word_pair", `^(?:[^\w\W]|\nx)`, aCRLFxb, "m", true},
		{"R4Bo_three_atom", `^(?:[^\d\s\D]|\nx)`, aCRLFxb, "m", true},
		{"R4Bf_star", `^(?:[^\s\S])*\nx`, aCRLFxb, "m", true},
		{"R4D2_lazy", `^(?:[^\s\S])*?\nx`, aCRLFxb, "m", true},
		{"R4Bl_r2", `^(?:[^\s\S]|\nx)(?=)`, aCRLFxb, "m", true},
		// Unicode property complement pairs (engine-accepted names; Go's
		// RE2 dialect and the fork share the Scripts/Categories catalogs).
		{"R4Ca_upper_pair", `^(?:[^\p{Lu}\P{Lu}]|\nx)`, aCRLFxb, "m", true},
		{"R4Cb_script_pair", `^(?:[^\p{Greek}\P{Greek}]|\nx)`, aCRLFxb, "m", true},
		{"R4Cf_category_pair", `^(?:[^\p{L}\P{L}]|\nx)`, aCRLFxb, "m", true},
		{"R4Ch_pair_plus_prop", `^(?:[^\s\S\p{Lu}]|\nx)`, aCRLFxb, "m", true},
		{"R4Ci_pair_plus_literal", `^(?:[^\s\Sx]|\nx)`, aCRLFxb, "m", true},
		{"R4Ce_prop_star", `^(?:[^\p{Lu}\P{Lu}])*\nx`, aCRLFxb, "m", true},
		{"R4Ca_r2", `^(?:[^\p{Lu}\P{Lu}]|\nx)(?=)`, aCRLFxb, "m", true},
		// The Any forms: \P{Any} matches nothing and [^\p{Any}] is empty on
		// both ICU and the Go RE2 dialect (the fork lacks the Any property
		// and rejects it at parse time, so these rows stay on the RE2
		// route; classifier-level pins cover the shared resolver there).
		{"R4Bd_negated_any", `^(?:[^\p{Any}]|\nx)`, aCRLFxb, "m", true},
		{"R4Be_pany_arm", `^(?:\P{Any}|\nx)`, aCRLFxb, "m", true},
		{"R4D5_any_star", `^(?:[^\p{Any}])*\nx`, aCRLFxb, "m", true},
		{"R4D6_pany_star", `^(?:\P{Any})*\nx`, aCRLFxb, "m", true},
		// mu controls (ICU false on a CRLF subject).
		{"R4Ba_mu", `^(?:[^\s\S]|\nx)`, aCRLFxb, "mu", false},
		{"R4Bf_mu", `^(?:[^\s\S])*\nx`, aCRLFxb, "mu", false},
		{"R4Ca_mu", `^(?:[^\p{Lu}\P{Lu}]|\nx)`, aCRLFxb, "mu", false},
		{"R4Cb_mu", `^(?:[^\p{Greek}\P{Greek}]|\nx)`, aCRLFxb, "mu", false},
		{"R4Bd_mu", `^(?:[^\p{Any}]|\nx)`, aCRLFxb, "mu", false},
		{"R4Be_mu", `^(?:\P{Any}|\nx)`, aCRLFxb, "mu", false},
		// Adjacent-LF controls (ICU true [2,4)).
		{"R4Bh_adjacent", `^(?:[^\s\S]|\nx)`, aLFLFxb, "m", true},
		{"R4D8_adjacent_star", `^(?:[^\s\S])*\nx`, aLFLFxb, "m", true},
		{"R4Ca_adjacent", `^(?:[^\p{Lu}\P{Lu}]|\nx)`, aLFLFxb, "m", true},
		{"R4Ba_mu_adjacent", `^(?:[^\s\S]|\nx)`, aLFLFxb, "mu", true},
		// Guards: identical pre and post. Mandatory repetitions of the
		// empty class and dead head classes never match (ICU false); the
		// non-negated pair is the full live class (ICU R4Cg false).
		{"R4Bg_mandatory", `^(?:[^\s\S])+\nx`, aCRLFxb, "m", false},
		{"R4Cd_mandatory", `^(?:[^\p{Lu}\P{Lu}])+\nx`, aCRLFxb, "m", false},
		{"R4D4_pany_mandatory", `^(?:\P{Any})+\nx`, aCRLFxb, "m", false},
		{"R4Bi_dead_head", `^[^\s\S]x`, aCRLFxb, "m", false},
		{"R4Bn_dead_head", `^[^\d\D]x`, aCRLFxb, "m", false},
		{"R4Cc_dead_head", `^[^\p{Lu}\P{Lu}]x`, aCRLFxb, "m", false},
		{"R4D7_dead_head_any", `^[^\p{Any}]x`, aCRLFxb, "m", false},
		{"R4Cg_live_full_pair", `^(?:\P{L}\p{L}|\nx)`, aCRLFxb, "m", false},
		{"R4Cj_live_prop_class", `^[^\p{Lu}x]b`, aCRLFxb, "m", false},
		{"R4Bj_live_full_head", `^[\s\S]x`, aCRLFxb, "m", false},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Positional/replacement fidelity on the revived match [2,4) for one
	// row per atom family.
	for _, pattern := range []string{
		`^(?:[^\s\S]|\nx)`,
		`^(?:[^\d\D]|\nx)`,
		`^(?:[^\p{Lu}\P{Lu}]|\nx)`,
		`^(?:\P{Any}|\nx)`,
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
	pos, err := op.regMap.regularInstrWithMatchType(`^(?:[^\s\S]|\nx)`, aCRLFxb, 1, 1, 0, false, "mu")
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)
	// regexp2 route positional fidelity.
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[^\s\S]|\nx)(?=)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	pos, err = op.regMap.regularInstrWithMatchType(`^(?:[^\p{Lu}\P{Lu}]|\nx)(?=)`, aCRLFxb, 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
}

// TestRegexpReviewRound4ScalingShapes pins bounded compilation cost for
// the round-4 constructs at the SQL entry points: long runs of empty
// complement-pair classes and nested dead alternations must compile and
// evaluate without error and with the same verdicts as short forms. The
// classifier scaling evidence lives in BenchmarkClassifyEmptyPairScalingR4.
func TestRegexpReviewRound4ScalingShapes(t *testing.T) {
	op := newOpBuiltInRegexp()
	// 60 empty complement-pair classes then a literal: matches nothing.
	empties := strings.Repeat(`[^\s\S\p{Lu}]`, 60) + `x`
	got, err := op.regMap.regularLikeWithMode(`^`+empties, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^`+empties+`(?=)`, "x", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// 40 copies of the reviewer's empty pair as optional units then \nx
	// under m: every unit bypasses, so the interior match survives at
	// [2,4) (per-copy semantics probed as R4Bf and the round-3 R2b row).
	arms := strings.Repeat(`(?:[^\s\S])?`, 40)
	got, err = op.regMap.regularLikeWithMode(`^`+arms+`\nx`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^`+arms+`\nx`, "a\r\nxb", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), pos)
	// Deeply nested dead alternations scale linearly through the fold.
	deep := `^(?:[^\x00-\x{10ffff}](?:` + strings.Repeat(`a|`, 30) + `b)|\nx)`
	got, err = op.regMap.regularLikeWithMode(deep, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
}
