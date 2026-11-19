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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRegexpReviewRound9DemorganMask covers review round 9 finding 1:
// folded property cardinality derives from the ICU compatibility
// contract, independent of the fork's consuming behavior. The scoped
// De Morgan class [^\p{Lu}\P{Ll}] stays live (folded Ll minus Lu
// retains U+FB00 among others), so the alternation keeps the mask and
// the CRLF-interior match stays sealed, exactly as ICU 77.1 replays it
// (rev18 E4-demorgan-plain-crlf NOMATCH, lflf MATCH [2,4)); round 8
// mis-recorded this row true and the review caught the regression.
// Positional rows prove the whole contract: sealed on CRLF with m and
// mu, revived at [2,4) on the LF-LF adjacent-line control.
func TestRegexpReviewRound9DemorganMask(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
	)
	forkPat := `^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)(?=)`
	re2Pat := `^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)`

	for _, tc := range []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		{"demorgan_fork_crlf_m", forkPat, zCRLFxy, "m", false},
		{"demorgan_fork_crlf_mu", forkPat, zCRLFxy, "mu", false},
		{"demorgan_fork_lflf_m", forkPat, zLFLFxy, "m", true},
		{"demorgan_fork_lflf_mu", forkPat, zLFLFxy, "mu", true},
		{"demorgan_re2_crlf_m", re2Pat, zCRLFxy, "m", false},
		{"demorgan_re2_crlf_mu", re2Pat, zCRLFxy, "mu", false},
		{"demorgan_re2_lflf_m", re2Pat, zLFLFxy, "m", true},
		{"demorgan_re2_lflf_mu", re2Pat, zLFLFxy, "mu", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	// Sealed positional rows on CRLF: no match anywhere (INSTR=0,
	// SUBSTR no row, REPLACE byte-identical), both routes.
	for _, pattern := range []string{forkPat, re2Pat} {
		pos, err := op.regMap.regularInstrWithMatchType(pattern, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, int64(0), pos, "pattern %q", pattern)
		matched, _, err := op.regMap.regularSubstrWithMatchType(pattern, zCRLFxy, 1, 1, false, "m")
		require.NoError(t, err)
		require.False(t, matched, "pattern %q", pattern)
		replaced, err := op.regMap.regularReplaceWithMatchType(pattern, zCRLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, zCRLFxy, replaced, "pattern %q", pattern)
		pos, err = op.regMap.regularInstrWithMatchType(pattern, zCRLFxy, 1, 1, 0, false, "mu")
		require.NoError(t, err)
		require.Equal(t, int64(0), pos, "pattern %q", pattern)
	}
	// Adjacent-line control revives [2,4): INSTR=3, SUBSTR "\nx"
	// (0A78), REPLACE "z\nXy" (7A0A5879).
	for _, pattern := range []string{forkPat, re2Pat} {
		pos, err := op.regMap.regularInstrWithMatchType(pattern, zLFLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, int64(3), pos, "pattern %q", pattern)
		matched, value, err := op.regMap.regularSubstrWithMatchType(pattern, zLFLFxy, 1, 1, false, "m")
		require.NoError(t, err)
		require.True(t, matched, "pattern %q", pattern)
		require.Equal(t, "\nx", value, "pattern %q", pattern)
		replaced, err := op.regMap.regularReplaceWithMatchType(pattern, zLFLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err)
		require.Equal(t, "z\nXy", replaced, "pattern %q", pattern)
	}
}

// TestRegexpReviewRound9SubtractionPredecessors covers review round 9
// finding 2: subtraction is recognized after every completed member
// form at both nesting levels (parser.go:1599), so a statically erased
// class ({0}) leaves the caret provably line-anchored and the ICU
// contract masks the CRLF interior (rev18 F2 rows: NOMATCH on crlf,
// MATCH [2,3) on the LF-LF control, mu NOMATCH). Lazy quantifiers stay
// engine-decided on CRLF (the classifier keeps declining that start
// proof, as for every optional shape), and the must-be-last sibling
// forms are engine parse errors.
func TestRegexpReviewRound9SubtractionPredecessors(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
	)
	// {0}: erased class, masked, sealed on crlf, revived on lflf.
	pats := []string{
		`[a-z-[b]]{0}^\n(?=)`,
		`[\d-[5]]{0}^\n(?=)`,
		`[\w-[x]]{0}^\n(?=)`,
		`[\p{L}-[a]]{0}^\n(?=)`,
		`[[:alpha:]-[b]]{0}^\n(?=)`,
		`[A-Z-[B]]{0}^\n(?=)`,
		`[a-[b-[c]]]{0}^\n(?=)`,
		`[a-z-[b-[c]]]{0}^\n(?=)`,
	}
	for _, p := range pats {
		got, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.NoError(t, err, "pattern %q", p)
		require.False(t, got, "pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zCRLFxy, "mu", false)
		require.NoError(t, err, "pattern %q", p)
		require.False(t, got, "pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zLFLFxy, "m", false)
		require.NoError(t, err, "pattern %q", p)
		require.True(t, got, "pattern %q", p)
		pos, err := op.regMap.regularInstrWithMatchType(p, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, int64(0), pos, "pattern %q", p)
		matched, _, err := op.regMap.regularSubstrWithMatchType(p, zCRLFxy, 1, 1, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.False(t, matched, "pattern %q", p)
		replaced, err := op.regMap.regularReplaceWithMatchType(p, zCRLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, zCRLFxy, replaced, "pattern %q", p)
		// LF-LF control: [2,3) on the second LF -> INSTR=3, SUBSTR
		// "\n" (0A), REPLACE "z\nXxy" (7A0A587879).
		pos, err = op.regMap.regularInstrWithMatchType(p, zLFLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, int64(3), pos, "pattern %q", p)
		matched, value, err := op.regMap.regularSubstrWithMatchType(p, zLFLFxy, 1, 1, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.True(t, matched, "pattern %q", p)
		require.Equal(t, "\n", value, "pattern %q", p)
		replaced, err = op.regMap.regularReplaceWithMatchType(p, zLFLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, "z\nXxy", replaced, "pattern %q", p)
	}

	// Lazy quantifier controls: only the statically dead {0} erases
	// the class; ? * {0,1} keep a maybe-consuming unit, so the
	// classifier declines the start proof (unchanged from earlier
	// rounds) and the fork engine decides at run time, matching at the
	// CRLF-interior line start [2,3). The adjacent-line control
	// matches under both modes.
	for _, p := range []string{
		`[a-z-[b]]?^\n(?=)`,
		`[a-z-[b]]*^\n(?=)`,
		`[a-z-[b]]{0,1}^\n(?=)`,
	} {
		got, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.NoError(t, err, "lazy pattern %q", p)
		require.True(t, got, "lazy pattern %q: engine-decided interior start", p)
		pos, err := op.regMap.regularInstrWithMatchType(p, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "lazy pattern %q", p)
		require.Equal(t, int64(3), pos, "lazy pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zLFLFxy, "m", false)
		require.NoError(t, err, "lazy pattern %q", p)
		require.True(t, got, "lazy pattern %q", p)
	}

	// Must-be-last: a sibling after the nested class is a fork parse
	// error. Both arrive as clean engine errors on the SQL surface.
	for _, p := range []string{
		`[a-z-[b]-[c]]{0}^\n(?=)`,
		`[a-z-[b]c]{0}^\n(?=)`,
	} {
		_, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.Error(t, err, "must-be-last pattern %q", p)
	}
}

// TestRegexpReviewRound9ForkFoldCanonicalize covers review round 9
// finding 3: the fork canonicalizes a completed class (merging
// overlapping AND abutting ranges, charclass.go:605-650) before
// lowercasing (:655), so the adjacent fold-special pair [KÅ] forms the
// interval U+212A-U+212B, whose lcTable supplies no lowercase image.
// In the subtrahend it therefore drops nothing from k, keeping
// {k,K,K} live and sealing the CRLF interior; in the bare class it
// stays empty on the fork (the documented divergence from ICU/Go,
// which fold [KÅ] live). The equivalent interval spelling must behave
// identically.
func TestRegexpReviewRound9ForkFoldCanonicalize(t *testing.T) {
	op := newOpBuiltInRegexp()
	const (
		zCRLFxy = "z\r\nxy"
		zLFLFxy = "z\n\nxy"
	)
	for _, p := range []string{`^(?i:[k-[KÅ]])?\nx(?=)`, `^(?i:[k-[K-Å]])?\nx(?=)`} {
		got, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.NoError(t, err, "pattern %q", p)
		require.False(t, got, "pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zCRLFxy, "mu", false)
		require.NoError(t, err, "pattern %q", p)
		require.False(t, got, "pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zLFLFxy, "m", false)
		require.NoError(t, err, "pattern %q", p)
		require.True(t, got, "pattern %q", p)
		// Sealed CRLF positional rows and the revived LF-LF [2,4).
		pos, err := op.regMap.regularInstrWithMatchType(p, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, int64(0), pos, "pattern %q", p)
		matched, _, err := op.regMap.regularSubstrWithMatchType(p, zCRLFxy, 1, 1, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.False(t, matched, "pattern %q", p)
		replaced, err := op.regMap.regularReplaceWithMatchType(p, zCRLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, zCRLFxy, replaced, "pattern %q", p)
		pos, err = op.regMap.regularInstrWithMatchType(p, zLFLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, int64(3), pos, "pattern %q", p)
		matched, value, err := op.regMap.regularSubstrWithMatchType(p, zLFLFxy, 1, 1, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.True(t, matched, "pattern %q", p)
		require.Equal(t, "\nx", value, "pattern %q", p)
		replaced, err = op.regMap.regularReplaceWithMatchType(p, zLFLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, "z\nXy", replaced, "pattern %q", p)
	}

	// Bare-class fork truths (the engine's own verdict, unchanged).
	// The (?=) suffix routes to the fork; without it [k-[KÅ]] is a Go
	// parse error and [KÅ] rides SimpleFold live on the RE2 route.
	for _, tc := range []struct {
		subject string
		want    bool
	}{
		{"k", true},
		{"K", true},
		{"K", true},  // U+212A survives: lcTable has no image for the coalesced interval
		{"Å", false}, // U+212B subtrahend dropped nothing it holds
		{"Å", false},
		{"å", false},
	} {
		got, err := op.regMap.regularLikeWithMode(`^(?i)[k-[KÅ]](?=)`, tc.subject, "m", false)
		require.NoError(t, err, "subject %q", tc.subject)
		require.Equal(t, tc.want, got, "subject %q", tc.subject)
		got, err = op.regMap.regularLikeWithMode(`^(?i)[k-[K-Å]](?=)`, tc.subject, "m", false)
		require.NoError(t, err, "interval spelling, subject %q", tc.subject)
		require.Equal(t, tc.want, got, "interval spelling, subject %q", tc.subject)
	}
	// Dialect split on the bare adjacent fold-special pair: the RE2
	// route folds [KÅ] live over the SimpleFold orbits; the fork route
	// canonicalizes [K-Å] before lowercasing, finds no lcTable image,
	// and erases it (the documented divergence).
	for _, s := range []string{"k", "K", "K", "Å", "Å", "å"} {
		got, err := op.regMap.regularLikeWithMode(`^(?i)[KÅ]`, s, "m", false)
		require.NoError(t, err, "RE2 subject %q", s)
		require.True(t, got, "RE2 route subject %q", s)
		for _, p := range []string{`^(?i)[KÅ](?=)`, `^(?i)[K-Å](?=)`} {
			got, err = op.regMap.regularLikeWithMode(p, s, "m", false)
			require.NoError(t, err, "pattern %q subject %q", p, s)
			require.False(t, got, "pattern %q subject %q", p, s)
		}
	}
	// The ToLower(K)=k subtrahend empties the class: the dead optional
	// grants the string exemption like any erased head.
	got, err := op.regMap.regularLikeWithMode(`^(?i:[k-[kK]])?\nx(?=)`, zCRLFxy, "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^(?i:[k-[kK]])?\nx(?=)`, zCRLFxy, "mu", false)
	require.NoError(t, err)
	require.False(t, got)
}

// TestRegexpReviewRound9AnySpellings covers review round 9 finding 4:
// canonical Any spellings (any, a_n_y, ANY - regexp/syntax
// canonicalName drops '_', '-' and ' ' and case-normalizes) resolve
// through the one property path on the RE2 route, bracketed or
// standalone, so the negated arms go empty and the alternation revives
// the interior match exactly as ICU replays it (rev18 F4-any rows:
// MATCH [2,4) on crlf; mu NOMATCH). The (?=) suffix routes to the
// fork, which rejects every Any spelling: a clean engine error.
func TestRegexpReviewRound9AnySpellings(t *testing.T) {
	op := newOpBuiltInRegexp()
	const zCRLFxy = "z\r\nxy"

	pats := []string{
		`^(?:[^\p{any}]|\nx)`,
		`^(?:\P{a_n_y}|\nx)`,
		`^(?:[^\p{ANY}]|\nx)`,
		`^(?:\p{^Any}|\nx)`,
		`^(?:(?i:[^\p{any}])|\nx)`, // group-scoped fold over the same universe
	}
	for _, p := range pats {
		got, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.NoError(t, err, "pattern %q", p)
		require.True(t, got, "pattern %q", p)
		got, err = op.regMap.regularLikeWithMode(p, zCRLFxy, "mu", false)
		require.NoError(t, err, "pattern %q", p)
		require.False(t, got, "pattern %q", p)
		pos, err := op.regMap.regularInstrWithMatchType(p, zCRLFxy, 1, 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, int64(3), pos, "pattern %q", p)
		matched, value, err := op.regMap.regularSubstrWithMatchType(p, zCRLFxy, 1, 1, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.True(t, matched, "pattern %q", p)
		require.Equal(t, "\nx", value, "pattern %q", p)
		replaced, err := op.regMap.regularReplaceWithMatchType(p, zCRLFxy, "X", 1, 0, false, "m")
		require.NoError(t, err, "pattern %q", p)
		require.Equal(t, "z\rXy", replaced, "pattern %q", p)
	}
	// Leaked-fold guard (round-8 contract): an unscoped (?i) folding
	// the class bleeds the fold flag onto the \nx arm and the folded
	// run keeps the mask up; the adjacent-line control still matches.
	got, err := op.regMap.regularLikeWithMode(`^(?:(?i)[^\p{any}]|\nx)`, zCRLFxy, "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^(?:(?i)[^\p{any}]|\nx)`, "z\n\nxy", "m", false)
	require.NoError(t, err)
	require.True(t, got)

	// Bare resolution checks on the RE2 route: universe and empty.
	for _, tc := range []struct {
		pattern string
		subject string
		want    bool
	}{
		{`^[\p{ANY}]`, "x", true},
		{`^[\p{a_n_y}]`, "\n", true},
		{`^[\P{ANY}]`, "x", false},
		{`^[^\p{any}]`, "x", false},
		{`^(?i)[\p{ANY}]`, "x", true},
	} {
		got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, "m", false)
		require.NoError(t, err, "pattern %q subject %q", tc.pattern, tc.subject)
		require.Equal(t, tc.want, got, "pattern %q subject %q", tc.pattern, tc.subject)
	}

	// Routing pins: the fork rejects every Any spelling outright, so
	// the (?=)-routed rows are clean engine errors.
	for _, p := range []string{
		`^(?:[^\p{any}]|\nx)(?=)`,
		`^(?:\P{a_n_y}|\nx)(?=)`,
	} {
		_, err := op.regMap.regularLikeWithMode(p, zCRLFxy, "m", false)
		require.Error(t, err, "fork-routed pattern %q", p)
	}
}
