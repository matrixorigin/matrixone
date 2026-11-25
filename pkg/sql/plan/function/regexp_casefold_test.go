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
	"math/rand"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/text/cases"
)

// The rows in this file pin MySQL/ICU case-insensitive REGEXP semantics for
// literal matching (issue #31175 review): ICU compares literals by full
// Unicode case folding, so ß matches ss (and Straße matches STRASSE) while
// quantified atoms and character classes still compare per character. Every
// expected value below was probed against ICU 78.3 find() semantics, which
// is what MySQL 8.0 REGEXP uses; the probe rows and logs are preserved in
// the round-5 evidence (/tmp/31175/round5/probe_icu*.log).

func regexpMatchWithEngineRE2(t *testing.T, pattern, subject, pureMatchType string) bool {
	t.Helper()
	rs := newOpBuiltInRegexp().regMap
	reg, err := rs.getRegularMatcherWithMatchType(pattern, pureMatchType, false)
	require.NoError(t, err)
	return reg.MatchString(subject)
}

func regexpMatchWithEngineRegexp2(t *testing.T, pattern, subject, pureMatchType string) bool {
	t.Helper()
	rs := newOpBuiltInRegexp().regMap
	matcher, err := rs.getRegexp2MatcherWithMatchType(pattern, pureMatchType, false)
	require.NoError(t, err)
	got, err := rs.regexp2MatchWithMatchType(matcher, subject, false, pureMatchType)
	require.NoError(t, err)
	return got
}

type regexpFoldRow struct {
	subject string
	pattern string
	want    bool
}

func TestRegexpCaseFoldLiteralRows(t *testing.T) {
	rows := []regexpFoldRow{
		// Full-fold expansion equivalence, both directions.
		{"ß", "ss", true},
		{"ss", "ß", true},
		{"SS", "ß", true},
		{"ß", "SS", true},
		{"ſs", "ß", true},
		{"sſ", "ß", true},
		{"Straße", "STRASSE", true},
		{"STRASSE", "Straße", true},
		{"strasse", "Straße", true},
		{"busineß", "BUSINESS", true},
		{"BUSINESS", "busineß", true},
		{"ﬀ", "ff", true},
		{"ff", "ﬀ", true},
		{"FF", "ﬀ", true},
		{"ﬃ", "ffi", true},
		{"İ", "i\u0307", true},
		{"i\u0307", "İ", true},
		{"ŉ", "\u02bcn", true},
		{"ΐ", "\u03b9\u0308\u0301", true},
		{"և", "\u0565\u0582", true},
		{"\u0565\u0582", "և", true},
		{"ﬅ", "st", true},
		{"ﬆ", "st", true},
		// U+1E9E full-folds to ss and cross-folds with ß and ss in ICU
		// 78.3 caseless matching (probed for issue #31175).
		{"ẞ", "ss", true},
		{"ss", "ẞ", true},
		{"ẞ", "ß", true},
		{"ß", "ẞ", true},
		{"xẞ", "xss", true},
		{"Straẞe", "STRASSE", true},
		{"BUSINEẞ", "business", true},
		{"ẞ", "ẞ", true},
		// Multi-character subjects and search semantics.
		{"ßß", "ss", true},
		{"ßß", "ssss", true},
		{"ssss", "ßß", true},
		{"sSs", "ßs", true},
		{"ßs", "sß", true},
		{"xßy", "ss", true},
		{"xssy", "xßy", true},
		{"xsy", "xßy", false},
		// Groups and alternations expand like plain literal runs.
		{"ß", "(ss)", true},
		{"ß", "(?:ss)", true},
		{"ß", "ss|x", true},
		{"x", "ss|x", true},
		// Quantified atoms stay per-character: no multi-character fold span.
		{"ß", "s", false},
		{"ß", "ss?", false},
		{"ß", "s{2}", false},
		{"ß", "ss+", false},
		{"ß", "s+", false},
		{"ss", "s{2}", true},
		{"ss", "s+", true},
		{"sß", "^ss*$", false},
		{"anything", "ß*", true},
		{"ßß", "ß+", true},
		// Character classes keep the engine's per-character fold orbit.
		{"ß", "[A-Z]", false},
		{"ẞ", "[A-Z]", false},
		{"SS", "[A-Z]+", true},
		{"abc", "[A-Z]+", true},
		{"ſ", "[s]", true},
		{"K", "[k]", true},
		{"ß", "[ß]", true},
		// Accented characters without a fold mapping stay distinct.
		{"á", "a", false},
		{"œ", "oe", false},
		{"œ", "Œ", true},
		{"Ǆ", "ǆ", true},
		{"ς", "Σ", true},
		{"K", "k", true},
		{"ſ", "s", true},
		// Anchors bound the match as before.
		{"ß", "^.$", true},
		{"ß", "^..$", false},
		{"ß", "^ß$", true},
		{"ß", "^ss$", true},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRE2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "RE2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
}

func TestRegexpCaseFoldMatchTypeControls(t *testing.T) {
	// Case-sensitive match types must keep byte-oriented behavior. The pure
	// match type carries no case marker when matching is case-sensitive.
	require.False(t, regexpMatchWithEngineRE2(t, "ss", "ß", ""))
	require.False(t, regexpMatchWithEngineRE2(t, "STRASSE", "Straße", ""))
	require.False(t, regexpMatchWithEngineRE2(t, "A", "a", ""))
	require.True(t, regexpMatchWithEngineRE2(t, "A", "a", "i"))
	// User-level match_type values canonicalize through getPureMatchType:
	// 'c' selects case-sensitive matching and the rightmost case flag wins.
	rs := newOpBuiltInRegexp().regMap
	for _, tc := range []struct {
		pattern, subject, matchType string
		want                        bool
	}{
		{"ss", "ß", "c", false},
		{"STRASSE", "Straße", "c", false},
		{"ss", "ß", "i", true},
		{"ss", "ß", "ci", true},
		{"ss", "ß", "ic", false},
		{"(ss)\\1", "ßß", "c", false},
		{"(ss)\\1", "ßß", "i", true},
	} {
		got, err := rs.regularLike(tc.pattern, tc.subject, tc.matchType)
		require.NoError(t, err)
		require.Equal(t, tc.want, got, "REGEXP_LIKE(%q, %q, %q)", tc.subject, tc.pattern, tc.matchType)
	}
}

func TestRegexpCaseFoldEmbeddedFlags(t *testing.T) {
	// An embedded (?i) enables full folding inside its scope even when the
	// match_type itself carries no case flag.
	require.True(t, regexpMatchWithEngineRE2(t, "(?i)ss", "ß", ""))
	require.True(t, regexpMatchWithEngineRE2(t, "(?i:ss)", "ß", ""))
	require.True(t, regexpMatchWithEngineRE2(t, "x(?i)ss", "xß", ""))
	require.False(t, regexpMatchWithEngineRE2(t, "x(?i)ss", "Xß", ""))
	// A scoped (?-i:...) disables folding inside the scope.
	require.False(t, regexpMatchWithEngineRE2(t, "(?-i:ss)", "ß", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, "a(?-i:ss)", "ASS", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, "a(?-i:ss)", "Ass", "i"))
}

func TestRegexpCaseFoldRegexp2Rows(t *testing.T) {
	// Backreference and lookbehind patterns route to the regexp2 engine; the
	// same full-fold expansion applies there.
	rows := []regexpFoldRow{
		{"ßß", "(ss)\\1", true},
		{"ssss", "(ss)\\1", true},
		{"xß", "(?<=x)ss", true},
		{"xSS", "(?<=x)ss", true},
		// Known engine divergence from ICU: a captured group compares
		// backreferences with simple case mapping, so "ssß" does not match
		// (ss)\1 here while ICU folds the backreference. Pinned to keep the
		// divergence explicit.
		{"ssß", "(ss)\\1", false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive control on the regexp2 path (no case marker in the
	// pure match type).
	require.False(t, regexpMatchWithEngineRegexp2(t, "(ss)\\1", "ßß", ""))
}

// TestRegexpCaseFoldNamedReferences pins that the fold expansion preserves
// named backreference tokens verbatim: folding the name's case would make
// the reference undefined (ICU compile-errors on a wrong-case name), and
// regexp2 resolves names case-sensitively (issue #31175 round-6 review).
func TestRegexpCaseFoldNamedReferences(t *testing.T) {
	rows := []regexpFoldRow{
		{"aa", `(?<X>a)\k<X>`, true},
		{"aA", `(?<X>a)\k<X>`, true},
		{"ab", `(?<X>a)\k<X>`, false},
		{"aa", `(?<Ss>a)\k<Ss>`, true},
		{"aa", `(?P<X>a)\k<X>`, true},
		{"aa", `(?'X'a)\k'X'`, true},
		{"aa", `(?<X>a)\<X>`, true},
		{"aa", `(?<X>a)\'X'`, true},
		{"aß", `(?<X>a)\k<X>`, false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive control: the name is preserved and nothing folds.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?<Ss>a)\k<Ss>`, "aa", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?<Ss>a)\k<Ss>`, "aA", ""))
	// End-to-end routing: RE2 rejects backreferences, so the query path must
	// reach regexp2 with the name intact.
	rs := newOpBuiltInRegexp().regMap
	got, err := rs.regularLike(`(?<Ss>a)\k<Ss>`, "aa", "i")
	require.NoError(t, err)
	require.True(t, got)
	// A wrong-case reference must keep erroring as an undefined group instead
	// of being folded into a valid one.
	_, err = rs.regularLike(`(?<X>a)\k<x>`, "aa", "i")
	require.Error(t, err)
}

// TestRegexpCaseFoldQuantifiedGreekPairs pins the round-6 F2 repair: a
// quantified atom whose full fold is multi-rune keeps its whole
// single-character orbit (ICU matches ᾀ against ᾈ+), it is not locked to the
// pattern's own character. All values probed against ICU 78.3.
func TestRegexpCaseFoldQuantifiedGreekPairs(t *testing.T) {
	rows := []regexpFoldRow{
		{"ᾀ", "ᾈ+", true},
		{"ᾈ", "ᾀ+", true},
		{"ᾀᾈ", "ᾈ+", true},
		{"ᾀᾀ", "ᾈ+", true},
		{"ᾀ", "ᾈ{1}", true},
		{"ᾀ", "ᾈ+?", true},
		// Known divergence from ICU: ICU additionally spans the quantified
		// atom over the subject's multi-character fold decomposition
		// (ἀ+ι against ᾈ+ is 1 there); the expansion here keeps the
		// single-character orbit, per the issue contract.
		{"ἀι", "ᾈ+", false},
		// Controls: the fold-source isolation of ß is unchanged.
		{"ß", "ss+", false},
		{"sss", "ss+", true},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRE2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "RE2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive control: no orbit without the caseless flag.
	require.False(t, regexpMatchWithEngineRE2(t, "ᾈ+", "ᾀ", ""))
	require.True(t, regexpMatchWithEngineRE2(t, "ᾈ+", "ᾈ", ""))
}

// TestRegexpCaseFoldLongRuns pins the round-6 F3 repair: literal runs whose
// segmentation count exceeds the flat enumeration cap still fold exactly,
// with the match boundary required to land on a subject character boundary
// (ICU's caseless literal semantics, probed against ICU 78.3).
func TestRegexpCaseFoldLongRuns(t *testing.T) {
	rep := strings.Repeat
	rows := []regexpFoldRow{
		{rep("ß", 5), rep("s", 10), true},
		{rep("ß", 5), rep("s", 9), false},
		{rep("ß", 4) + "s", rep("s", 9), true},
		{rep("ß", 5), rep("s", 11), false},
		{rep("ß", 10), rep("s", 20), true},
		{rep("s", 10), rep("ß", 5), true},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRE2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "RE2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
}

// TestRegexpCaseFoldEscapedLiterals pins the round-6 F4 repair: literal text
// carried by \Q...\E, \x{..} and \xNN escapes joins the literal stream, so it
// full-folds like any other literal (ICU probed: ß matches \Qss\E). Escapes
// that are not plain literal text keep their verbatim treatment.
func TestRegexpCaseFoldEscapedLiterals(t *testing.T) {
	rows := []regexpFoldRow{
		{"ß", `\Qss\E`, true},
		{"ſs", `\Qss\E`, true},
		{"STRASSE", `\QStraße\E`, true},
		{"ß", `s\x{73}`, true},
		{"ss", `\x{DF}`, true},
		{"ss", `\xDF`, true},
		{"ssss", `\x{DF}\x{DF}`, true},
		{"(A)", `\x{28}a\x{29}`, true},
		{"(", `\Q(\E`, true},
		{"(a)", `\Q(a)\E`, true},
		{".", `\Q.\E`, true},
		{"*", `\Q*\E`, true},
		// Decoded metacharacters re-escape, so they stay literals.
		{"a*", `a\x{2A}`, true},
		{"aa", `a\x{2A}`, false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRE2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "RE2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive controls: decoding keeps the identical literal.
	require.True(t, regexpMatchWithEngineRE2(t, `\x{DF}`, "ß", ""))
	require.False(t, regexpMatchWithEngineRE2(t, `\Qss\E`, "ß", ""))
	require.True(t, regexpMatchWithEngineRE2(t, `\Qss\E`, "ss", ""))
	// Quoted whitespace decodes into the literal stream and still matches.
	require.True(t, regexpMatchWithEngineRE2(t, `\Qa b\E`, "a b", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `\Qa b\E`, "A B", "i"))
}

// TestRegexpCaseFoldFreeSpacing pins the round-9 F1 repair: free-spacing
// mode ((?x)) and comments are pattern trivia for the engines, so the fold
// expansion must drop unescaped whitespace, # line comments and (?#...)
// comments and fold the literal pieces around them as one run (ICU 78.3
// probed: (?xi)"s s" matches ß). Folding trivia into the emitted
// alternation instead would let a '#' swallow the generated parentheses
// and break compilation. These patterns route to the regexp2 engine.
func TestRegexpCaseFoldFreeSpacing(t *testing.T) {
	rows := []regexpFoldRow{
		// Trivia between literals joins the fold run.
		{"ß", "(?xi)s s", true},
		{"ss", "(?xi)s s", true},
		{"ab", "(?xi)a b", true},
		{"a b", "(?xi)a b", false},
		{"ab", "(?xi)a\tb", true},
		// A # line comment after a fold run must not swallow the emitted
		// alternation (the round-9 review's compile-failure example).
		{"ß", "(?xi)ss#comment", true},
		{"ss", "(?xi)ss#comment", true},
		{"ßß", "(?xi)ss # hi\nss", true},
		{"ssss", "(?xi)ss # hi\nss", true},
		{"ßx", "(?xi)ss # c\nx", true},
		{"ßß", "(?xi)ß ß", true},
		// A quantifier binds across the dropped trivia.
		{"ßß", "(?xi)ß {2}", true},
		{"ẞẞ", "(?xi)ß {2}", true},
		{"ss", "(?xi)ß {2}", false},
		// A scoped (?-x:...) keeps whitespace literal inside the scope.
		{"ab cd", "(?xi)a(?-x:b c)d", true},
		{"abcd", "(?xi)a(?-x:b c)d", false},
		// (?#...) comments are trivia in every mode, so the literal pieces
		// around them fold as one run.
		{"AB", "a(?#c)b", true},
		{"ab", "a(?#c)b", true},
		{"ß", "s(?#ß)s", true},
		{"ßx", "ss(?#c ß)x", true},
		{"a b", "a(?#c) b", true},
		{"ab", "a(?#c) b", false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Outside free-spacing mode '#' and whitespace stay literal.
	require.True(t, regexpMatchWithEngineRegexp2(t, "ss # c", "ss # c", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, "ss # c", "ß # C", "i"))
	// Case-sensitive controls: trivia is still dropped, nothing folds.
	require.True(t, regexpMatchWithEngineRegexp2(t, "a(?#c)b", "ab", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, "a(?#c)b", "AB", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, "(?x)a b", "ab", ""))
	// An unterminated (?# comment keeps its verbatim bytes through the
	// expansion, so the engine decides its fate exactly as for the
	// unrewritten pattern: this regexp2 engine accepts it as a comment
	// running to the end of the pattern (a pre-existing engine behavior;
	// ICU instead reports a compile error).
	rs := newOpBuiltInRegexp().regMap
	got, err := rs.regularLike("a(?#cb", "ab", "i")
	require.NoError(t, err)
	require.True(t, got)
	// End-to-end routing: the comment pattern must reach the engine with
	// the fold expansion applied and compile cleanly.
	got, err = rs.regularLike("(?xi)ss#comment", "ß", "i")
	require.NoError(t, err)
	require.True(t, got)
}

// TestRegexpCaseFoldGReferences pins the round-9 F2 repair: the \g<name>,
// \g'name' and \g{name} backreference forms keep their exact bytes through
// the fold expansion, exactly like \k, so a mixed-case reference name still
// resolves (the engines resolve reference names case-sensitively).
func TestRegexpCaseFoldGReferences(t *testing.T) {
	rows := []regexpFoldRow{
		{"aa", `(?<X>a)\g<X>`, true},
		{"aA", `(?<X>a)\g<X>`, true},
		{"ab", `(?<X>a)\g<X>`, false},
		{"aa", `(?<X>a)\g'X'`, true},
		{"aa", `(?<X>a)\g{X}`, true},
		{"aa", `(?<Ss>a)\g<Ss>`, true},
		{"aß", `(?<X>a)\g<X>`, false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive control: the name is preserved and nothing folds.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?<Ss>a)\g<Ss>`, "aa", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?<Ss>a)\g<Ss>`, "aA", ""))
	// End-to-end routing with a mixed-case name.
	rs := newOpBuiltInRegexp().regMap
	got, err := rs.regularLike(`(?<Ss>a)\g<Ss>`, "aa", "i")
	require.NoError(t, err)
	require.True(t, got)
	// A wrong-case reference must keep erroring as an undefined group
	// instead of being folded into a valid one.
	_, err = rs.regularLike(`(?<X>a)\g<x>`, "aa", "i")
	require.Error(t, err)
}

// TestRegexpCaseFoldQuotedWhitespace pins the round-9 F3 repair: whitespace
// and '#' carried by \Q...\E, \x{..} or \xNN decode into the literal stream
// and full-fold with the surrounding run (ICU 78.3 probed: \Qss ss\E
// matches "ß ß" in caseless mode). Under free-spacing mode they are
// re-emitted as \x{..} escapes so they stay literal. \Q patterns route to
// the regexp2 engine; the plain hex forms also exercise the RE2 path.
func TestRegexpCaseFoldQuotedWhitespace(t *testing.T) {
	rows := []regexpFoldRow{
		{"ß ß", `\Qss ss\E`, true},
		{"SS SS", `\Qss ss\E`, true},
		{"ss ss", `\Qss ss\E`, true},
		{"ßß", `\Qss ss\E`, false},
		{"ß#", `\Qss#\E`, true},
		{"ss#", `\Qss#\E`, true},
		{"ß\nß", "\\Qss\nss\\E", true},
		{"a b", `\Qa b\E`, true},
		{"A B", `\Qa b\E`, true},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Under free-spacing mode the quoted whitespace and '#' stay literal:
	// the run around them folds, but the quoted characters still match
	// themselves only.
	xrows := []regexpFoldRow{
		{"ß ß", `(?xi)\Qss ss\E`, true},
		{"ßß", `(?xi)\Qss ss\E`, false},
		{"ß#", `(?xi)\Qss#\E`, true},
		{"ß #", `(?xi)\Qss#\E`, false},
		{"a b", `(?xi)a\x{20}b`, true},
		{"ab", `(?xi)a\x{20}b`, false},
		{"a\tb", `(?xi)a\x09b`, true},
		{"a#b", `(?xi)a\x{23}b`, true},
	}
	for _, row := range xrows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive controls: decoding keeps the identical literal.
	require.True(t, regexpMatchWithEngineRegexp2(t, `\Qss ss\E`, "ss ss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\Qss ss\E`, "ß ß", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\Qss#\E`, "ß#", ""))
	// RE2 path: plain hex escapes decode into the literal stream there too.
	require.True(t, regexpMatchWithEngineRE2(t, `a\x{20}b`, "A B", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `a\x{20}b`, "AB", "i"))
	// Escaped whitespace (\ ) stays verbatim and literal under (?x).
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?x)a\ b`, "a b", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?x)a\ b`, "ab", "i"))
}

// TestRegexpCaseFoldDifferentialLongRuns extends the differential oracle
// across the flat enumeration cap: patterns and subjects over the maximally
// branching fold alphabet with run lengths that produce more segmentations
// than the old 64-alternative bound (issue #31175 round-6 review).
func TestRegexpCaseFoldDifferentialLongRuns(t *testing.T) {
	folder := cases.Fold()
	alphabet := []rune("sSſßẞ")
	rng := rand.New(rand.NewSource(31179))
	randomString := func(minLen, maxLen int) string {
		n := minLen + rng.Intn(maxLen-minLen+1)
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteRune(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	rs := newOpBuiltInRegexp().regMap
	for i := 0; i < 300; i++ {
		pattern := randomString(8, 16)
		subject := randomString(8, 30)
		anchored := "^" + pattern + "$"
		want := regexpFoldOracleString(folder, subject) == regexpFoldOracleString(folder, pattern)
		reg, err := rs.getRegularMatcherWithMatchType(anchored, "i", false)
		require.NoError(t, err)
		got := reg.MatchString(subject)
		require.Equal(t, want, got, "ci whole-match: subject %q pattern %q", subject, pattern)
	}
}

// regexpFoldOracleString is the per-character full case fold, matching the
// ICU regex behavior probed for issue #31175 (U+1E9E folds to ss like any
// other multi-rune folding).
func regexpFoldOracleString(folder cases.Caser, s string) string {
	var b strings.Builder
	for _, r := range s {
		b.WriteString(folder.String(string(r)))
	}
	return b.String()
}

func TestRegexpCaseFoldDifferential(t *testing.T) {
	folder := cases.Fold()
	alphabet := []rune("aAsSſßẞkKKiIİﬀﬃﬅfFtToOeœեւևσςΣxy-")
	rng := rand.New(rand.NewSource(31175))
	randomString := func(maxLen int) string {
		n := rng.Intn(maxLen + 1)
		var b strings.Builder
		for i := 0; i < n; i++ {
			b.WriteRune(alphabet[rng.Intn(len(alphabet))])
		}
		return b.String()
	}
	rs := newOpBuiltInRegexp().regMap
	for i := 0; i < 3000; i++ {
		pattern := randomString(5)
		if pattern == "" {
			continue
		}
		subject := randomString(6)
		anchored := "^" + pattern + "$"
		want := regexpFoldOracleString(folder, subject) == regexpFoldOracleString(folder, pattern)
		reg, err := rs.getRegularMatcherWithMatchType(anchored, "i", false)
		require.NoError(t, err)
		got := reg.MatchString(subject)
		require.Equal(t, want, got, "ci whole-match: subject %q pattern %q", subject, pattern)
	}
}

// TestRegexpCaseFoldEscapeTokens pins issue #31175 round-10 finding F1: an
// escape whose payload looks like foldable literal text — a \UXXXXXXXX or
// \uXXXX hex payload containing FF, a \c control designator, an octal
// escape, a multi-digit backreference — must reach the engine as one
// complete verbatim token; only the literal text around it folds. The
// expected values are the ICU 78.3 rows probed in round 10 (probe_icu7)
// plus the vendored engine's own escape grammar (scanCharEscape/scanOctal).
// \U and \c route to the regexp2 engine; the octal form also runs on RE2.
func TestRegexpCaseFoldEscapeTokens(t *testing.T) {
	rows := []regexpFoldRow{
		{"ａ", `\U0000FF41`, true},     // U+FF41 the escape denotes
		{"Ａ", `\U0000FF41`, true},     // U+FF21 case-folds with it
		{"a", `\U0000FF41`, false},    // ASCII a is a different letter
		{"ａß", `\U0000FF41ss`, true},  // the run after the token folds
		{"ＡSS", `\U0000FF41ss`, true}, // fold on both sides
		{"ÿß", `\u00FFss`, true},      // U+00FF token, ss run folds
		{"ŸSS", `\u00FFss`, true},     // U+0178 pairs with U+00FF
		{"\x13ß", `\cSss`, true},      // \cS is one control character
		{"\nß", `\012ss`, true},       // octal \012 is one newline
		{"abcdefghijkkß", `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\11ss`, true},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive controls: the tokens keep their exact bytes and the
	// foldable text after them stays unfolded.
	require.True(t, regexpMatchWithEngineRegexp2(t, `\U0000FF41ss`, "ａss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\U0000FF41ss`, "ａß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `\u00FFss`, "ÿss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\u00FFss`, "ÿß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `\cSss`, "\x13ss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\cSss`, "\x13ß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\11ss`, "abcdefghijkkss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\11ss`, "abcdefghijkkß", ""))
	// RE2 path: the octal escape keeps its payload there too.
	require.True(t, regexpMatchWithEngineRE2(t, `\012ss`, "\nß", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `\012ss`, "\nSS", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `\012ss`, "\nss", ""))
	require.False(t, regexpMatchWithEngineRE2(t, `\012ss`, "\nß", ""))
}

// TestRegexpCaseFoldInlineOptionCase pins issue #31175 round-10 finding F2:
// the vendored regexp2 parser's optionFromCode accepts the uppercase mates
// 'I' and 'X' and its scanOptions accepts '+' transitions, so the case-fold
// rewrite must track them exactly like the lowercase forms — otherwise it
// folds text the engine compares case-sensitively ((?-I)A), or leaves a
// caseless/free-spacing scope unfolded and lets a '#' comment swallow the
// generated parentheses ((?X)ss#comment). These option forms are an engine
// extension: ICU rejects them (probe_icu7 E1-E3), and they always route to
// the regexp2 engine because RE2 does not accept them either.
func TestRegexpCaseFoldInlineOptionCase(t *testing.T) {
	// Uppercase I disables and enables case-insensitive matching exactly
	// like lowercase i (engine facts G1-G4, G11-G14).
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?-I)A`, "A", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?-I)A`, "a", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?I)ss`, "SS", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?I)ss`, "ss", ""))
	// The pattern's own (?I) also enables full folding under a
	// case-sensitive ambient: ß now matches, like (?i)ss would.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?I)ss`, "ß", ""))
	// '+' re-enables cumulatively, so the final letter wins.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?+i)ss`, "SS", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?+i)ss`, "ß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?-i+I)ss`, "ß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?-i+I)ss`, "ß", "i"))
	// Scoped (?-I:...) is case-sensitive inside and restores after.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?i)a(?-I:b)c`, "AbC", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?i)a(?-I:b)c`, "ABC", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?i)(?-I:ss)SS`, "ssSS", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?i)(?-I:ss)SS`, "SSSS", ""))
	// Uppercase X enables free-spacing: trivia is dropped, the run around
	// it folds, and the emitted alternation contains no '#' to swallow the
	// generated parentheses.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?X)ss#comment`, "ss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?X)ss#comment`, "ß", "i"))
	// X alone carries no case flag: under a case-sensitive ambient the
	// comment is still trivia, but ß does not match.
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?X)ss#comment`, "ß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?X)s s`, "ss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?X)s s`, "s s", ""))
	// (?-X:...) keeps its whitespace literal inside the scope.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?X)a(?-X:b c)d`, "ab cd", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?X)a(?-X:b c)d`, "abcd", ""))
}

// TestRegexpCaseFoldEmptyQuotedSegments pins issue #31175 round-10 finding
// F3: an empty \Q\E segment carries no text, so it is transparent to full
// folding — the literal pieces around it fold as one run and a following
// quantifier binds across it, exactly as ICU treats the empty segment
// (probe_icu7 D1-D12). \Q patterns route to the regexp2 engine; a trailing
// (?#x) comment forces the regexp2 route for the stereo row, and the plain
// form also runs on RE2.
func TestRegexpCaseFoldEmptyQuotedSegments(t *testing.T) {
	rows := []regexpFoldRow{
		{"ß", `^s\Q\Es$`, true},   // the pieces join one fold run
		{"ss", `^s\Q\Es$`, true},  // the joined run still matches itself
		{"s", `^s\Q\Es$`, false},  // anchored: one s is not enough
		{"aaa", `^a\Q\E+$`, true}, // the quantifier binds across the segment
		{"a", `^a\Q\E+$`, true},
		{"", `^a\Q\E+$`, false},
		{"sss", `^ss\Q\E{2}$`, true}, // interval binds the last atom
		{"ss", `^ss\Q\E{2}$`, false},
	}
	for _, row := range rows {
		got := regexpMatchWithEngineRegexp2(t, row.pattern, row.subject, "i")
		require.Equal(t, row.want, got, "regexp2 ci: %q REGEXP %q", row.subject, row.pattern)
	}
	// Case-sensitive controls: the joined run compares literally.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^s\Q\Es$`, "ss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^s\Q\Es$`, "ß", ""))
	// The quantifier-split pieces keep the per-character boundary: ICU
	// does not span the subject's fold image across them either (D11).
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ss\Q\E{2}$`, "ßs", "i"))
	// An empty quoted segment alone matches the empty string anywhere.
	require.True(t, regexpMatchWithEngineRegexp2(t, `\Q\E`, "x", ""))
	// RE2 path and the regexp2 stereo route agree.
	require.True(t, regexpMatchWithEngineRE2(t, `^s\Q\Es$`, "ß", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `^s\Q\Es$`, "s", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `^s\Q\Es$`, "ss", ""))
	require.False(t, regexpMatchWithEngineRE2(t, `^s\Q\Es$`, "ß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^s\Q\Es$(?#x)`, "ß", "i"))
}

// TestRegexpCaseFoldDecimalReferences pins issue #31175 round-11 finding
// F1: the regexp2 parser's scanDecimal consumes the complete digit run of
// a decimal escape before choosing between a backreference and the
// octal/literal fallback (engine facts H1-H9), so the fold rewrite must
// keep the whole run attached to the backslash. Splitting \18 into \1 plus
// a literal "8ss" turned the group-18 reference into a group-1 reference
// followed by folded text. \1-\9 always route to the regexp2 engine.
func TestRegexpCaseFoldDecimalReferences(t *testing.T) {
	g18 := `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)\18ss`
	g19 := `(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)(l)(m)(n)(o)(p)(q)(r)(s)\19ss`
	// \18 names group 18: the run after the token folds under a caseless
	// ambient, the reference does not (H1-H4).
	require.True(t, regexpMatchWithEngineRegexp2(t, g18, "abcdefghijklmnopqrrss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, g18, "abcdefghijklmnopqrrß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, g18, "abcdefghijklmnopqrrß", ""))
	// The false positive the split token produced: group 1, then "8ss".
	require.False(t, regexpMatchWithEngineRegexp2(t, g18, "abcdefghijklmnopqra8ss", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, g19, "abcdefghijklmnopqrssss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, g19, "abcdefghijklmnopqrsa9ss", "i"))
	// Octal fallback stability: with fewer groups the decimal run falls
	// back to scanCharEscape, so \12 is the LF octal escape and \18 the
	// \x01 byte followed by a literal '8' (H5-H7). These rows matched the
	// same before the fix — the token text was already complete.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\12ss`, "ab\nss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\12ss`, "ab\nß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\12ss`, "ab\nß", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\12ss`, "ab12ss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\18ss`, "ab\x018ss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\18ss`, "ab\x018ß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(a)(b)\18ss`, "ab\x018ß", ""))
}

// TestRegexpCaseFoldShortProperties pins issue #31175 round-11 finding F2:
// the short property form \pX / \PX is one escape token on both engines
// (Go regexp's \pN and the vendored parser's parseProperty, H10-H14), so
// its letter must not join the fold run — folding \pLss into \p(?:lss|...)
// made the pattern a compile error where the unrewritten pattern matched.
// \p is not a regexp2 routing trigger, so the plain form runs on RE2; ICU
// rejects the short form outright (probe B1-B4), an engine-extension
// divergence this issue's contract documents.
func TestRegexpCaseFoldShortProperties(t *testing.T) {
	require.True(t, regexpMatchWithEngineRE2(t, `\pLss`, "ass", ""))
	require.True(t, regexpMatchWithEngineRE2(t, `\pLss`, "aß", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `\pLss`, "aß", ""))
	require.False(t, regexpMatchWithEngineRE2(t, `\pLss`, "1ss", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `\PLss`, "1ss", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `\PLss`, "aß", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `\p{L}ss`, "aß", "i"))
	// The regexp2 engine accepts the same short form (H10-H12).
	require.True(t, regexpMatchWithEngineRegexp2(t, `\pLss(?#x)`, "ass", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `\pLss(?#x)`, "aß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\pLss(?#x)`, "aß", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `\pLss(?#x)`, "1ss", "i"))
}

// TestRegexpCaseFoldNestedClasses pins issue #31175 round-11 finding F3:
// the vendored regexp2 grammar nests classes for subtraction — [a-z-[bc]]
// is a-z minus {b,c} (scanCharSet recurses, then requires the outer ']',
// H15-H23) — so the fold rewrite must consume the nested set instead of
// closing the class at the first ']'. Closing early left "]ss" to fold,
// and the emitted alternation between a nested class and an outer ']'
// failed to compile where the unrewritten pattern matched. Go regexp has
// no nesting: the same text is the class {a-z,-,[,b,c} followed by a
// literal ']', and the RE2 rows keep that reading byte-identical. ICU
// reads [a-z-[bc]] as a union (probe C1-C8), a cross-engine divergence
// this issue's contract documents; (?#x) forces the regexp2 route.
func TestRegexpCaseFoldNestedClasses(t *testing.T) {
	sub := `[a-z-[bc]]ss(?#x)`
	require.True(t, regexpMatchWithEngineRegexp2(t, sub, "ass", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, sub, "zss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, sub, "bss", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, sub, "css", ""))
	// Caseless: the run folds (aß), the engine folds the class (ASS), and
	// the subtraction still excludes b and c.
	require.True(t, regexpMatchWithEngineRegexp2(t, sub, "aß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, sub, "aß", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, sub, "ASS", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, sub, "BSS", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, sub, "bß", "i"))
	// Recursive subtraction: [a-z-[b-c-[d]]] is a-z minus (b-c minus d),
	// so c is excluded and d stays a member (H22-H23).
	rec := `[a-z-[b-c-[d]]]ss(?#x)`
	require.False(t, regexpMatchWithEngineRegexp2(t, rec, "css", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, rec, "dss", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, rec, "dß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, rec, "cß", "i"))
	// A ']' in first position is a literal member and the subtraction
	// after it still nests.
	require.True(t, regexpMatchWithEngineRegexp2(t, `[]-[a]]ss(?#x)`, "]ss", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `[]-[a]]ss(?#x)`, "]ß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `[]-[a]]ss(?#x)`, "aß", "i"))
	// RE2 route: no nesting — the class ends at the first ']' and the
	// second ']' is a literal; byte-identical before and after the fix.
	require.True(t, regexpMatchWithEngineRE2(t, `[a-z-[bc]]ss`, "a]ss", ""))
	require.False(t, regexpMatchWithEngineRE2(t, `[a-z-[bc]]ss`, "ass", ""))
	require.True(t, regexpMatchWithEngineRE2(t, `[a-z-[bc]]ss`, "a]ß", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `[a-z-[bc]]ss`, "aß", "i"))
}

// TestRegexpCaseFoldAtomicGroups pins issue #31175 round-12 finding F1:
// (?> opens an atomic group, which requiresRegexp2Pattern routes to the
// vendored engine (RE2 has no atomic groups), so the fold rewrite must
// copy the complete opener. Copying only "(?" left the '>' to start a fold
// run, and the vendored parser read the result as a conditional whose
// empty branches zero-matched every subject — REGEXP_LIKE('x','(?>ss)','i')
// returned 1 where ICU 78.3 returns 0 (probes A1-A15, G1-G3). The body
// keeps folding and the group keeps its atomicity.
func TestRegexpCaseFoldAtomicGroups(t *testing.T) {
	// The review's false positive: an unrelated subject must not match.
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "x", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "s", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "xs", "i"))
	// The body still folds (ICU A3-A5).
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "ss", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "ß", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "SS", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^(?>ss)$`, "ss", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^(?>ss)$`, "x", "i"))
	// Atomicity survives the rewrite: the engine must not backtrack into
	// the group (ICU A8-A9); the non-atomic control does backtrack.
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>a+)ab`, "aaab", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(a+)ab`, "aaab", ""))
	// Nested atomic groups fold inside and reject the unrelated subject.
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>(?>ss))`, "x", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?>(?>ss))`, "ß", "i"))
	// Case-sensitive controls: no fold, exact bytes match.
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "SS", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?>ss)`, "ss", ""))
}

// TestRegexpCaseFoldRegexp2Orbit pins issue #31175 round-12 finding F2: the
// vendored regexp2 engine's caseless comparison is Go's unicode.ToLower
// closure on both the pattern (parser addToConcatenate/addLowercase) and
// the subject (runner forwardcharnext), which does not reach every
// full-fold source: ς full-folds to σ but ToLower(ς)=ς, so the rewritten
// ^ς$ → ^σ$ stopped matching even the identical text (probe G4). The
// rewrite now lists the missed sources as extra class members
// (regexpSingleCharFoldExtras); every expected value was engine-probed
// (G5-G13) and cross-checked against ICU 78.3 (B1-B28). (?#x) forces the
// regexp2 route; the RE2 route's simple-fold orbit is complete (G8).
func TestRegexpCaseFoldRegexp2Orbit(t *testing.T) {
	// Final sigma: self-match, orbit mates, and the negative (ICU B1-B6).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "ς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "Σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ςς$(?#x)`, "ςς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ςς$(?#x)`, "Σσ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ςς$(?#x)`, "ςx", "i"))
	// Long s: ſ folds to s, so every mix with s matches the run (B7-B12).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ſ$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ſ$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "ſs", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "sſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "ſſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "SS", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "ß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ss$(?#x)`, "sx", "i"))
	// Micro sign µ folds to μ (B13-B16).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^µ$(?#x)`, "µ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^µ$(?#x)`, "μ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^µ$(?#x)`, "Μ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^µ$(?#x)`, "m", "i"))
	// Cyrillic yat/iotated vowels: U+1C80-1C88 fold to base letters.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᲀ$(?#x)`, "ᲀ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᲀ$(?#x)`, "в", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᲀ$(?#x)`, "В", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ᲀ$(?#x)`, "б", "i"))
	// The image block carries its runes' extras: ἀι accepts ἀ plus U+0345
	// or U+1FBE, both full-fold sources of ι (B17-B28).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ἀι", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ᾀ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ἀι", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ᾀ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ᾈ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ᾀ$(?#x)`, "ἀx", "i"))
	// RE2 route: the complete simple-fold orbit needs no extras and the
	// expansion stays byte-identical.
	require.True(t, regexpMatchWithEngineRE2(t, `^ς$`, "ς", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `^ς$`, "Σ", "i"))
	require.False(t, regexpMatchWithEngineRE2(t, `^ς$`, "s", "i"))
	// Case-sensitive controls: no fold anywhere.
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "Σ", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς$(?#x)`, "ς", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ᲀ$(?#x)`, "в", ""))
}

// TestRegexpCaseFoldRegexp2ClassOrbit pins issue #31175 round-16 finding
// F2: the regexp2 engine's caseless class closure and quantified-atom
// comparison are Go's unicode.ToLower space, which misses full-fold
// sources — ToLower(ς)=ς, not σ — so 'σ' REGEXP '^[ς]$' and
// 'σ' REGEXP '^ς+$' were false where ICU (and therefore MySQL REGEXP)
// returns true. The rewrite now completes the per-character orbit on
// these paths: an explicit class gains the orbit members the ToLower
// closure cannot reach, and a quantified atom with a single-rune fold
// image emits the orbit class. Every expected value was probed against
// ICU 78.3 (rows A1-A8, B1-B5, C1-C3, D1-D7, E1-E5, F1-F4 in the
// round-16 evidence, /tmp/31175/round16/probe_icu16.log).
func TestRegexpCaseFoldRegexp2ClassOrbit(t *testing.T) {
	// Singleton classes: the class gains the orbit mate (A1-A8).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "ς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "Σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[σ]$(?#x)`, "ς", "i"))
	// Quantified atoms fold per character through the orbit (B1-B5).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς+$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς+$(?#x)`, "σς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^ς{2}$(?#x)`, "σσ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ς+$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^s+$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[s]$(?#x)`, "ſ", "i"))
	// Orbit mates the ToLower closure already covers need no added
	// member: Kelvin sign, sharp s in both directions.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[k]$(?#x)`, "K", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ß]$(?#x)`, "ẞ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ẞ]$(?#x)`, "ß", "i"))
	// Negated classes gain the orbit too, so the mate stays excluded
	// (C1-C3).
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[^ς]$(?#x)`, "σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[^ς]$(?#x)`, "Σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[^ς]$(?#x)`, "x", "i"))
	// Ranges close over their members' orbits (D1-D7): [a-z] and [A-Z]
	// gain ſ through s/S; [Α-Ω] gains ς and σ through Σ; [Α-Ρ] has no
	// sigma mate (it ends at Ρ, below Σ) and stays shut.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[a-z]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[A-Z]$(?#x)`, "ſ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[^A-Z]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[Α-Ω]$(?#x)`, "ς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[Α-Ω]$(?#x)`, "σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[Α-Ρ]$(?#x)`, "ς", "i"))
	// A range with the fold source inside needs no addition (D7).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[À-ÿ]$(?#x)`, "ẞ", "i"))
	// Subtraction classes stay entirely verbatim: the engine's caseless
	// subtraction algebra is its own per-character ToLower fiber (the
	// round-8 contract pinned by TestRegexpReviewForkFoldSubtraction),
	// and completing the orbit on either side changes it — completing
	// the exclusion set would erase raw minuend members ([s-[ſ]] would
	// collapse to empty, reviving the bogus empty-class exemption the
	// multiline contract seals), and completing the minuend can revive
	// a class the fiber keeps dead ([s-[S]] would gain ſ). ICU's
	// per-set case closure gives different verdicts for these corners,
	// both probed on ICU 78.3 and documented as divergences in the
	// issue contract: ICU excludes ς from [α-ω--[σ]] (E1) and matches
	// ſ against [a-z--[bc]] (E4). On the engine's raw fiber ς is a raw
	// member of [α-ω] and not excluded by [σ]; Σ is excluded through
	// its ToLower image σ; β matches as in ICU (E2-E3); ſ lowers to
	// itself, outside [a-z]; b stays subtracted (E5).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[α-ω-[σ]]$(?#x)`, "ς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[α-ω-[σ]]$(?#x)`, "Σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[α-ω-[σ]]$(?#x)`, "β", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[a-z-[bc]]$(?#x)`, "ſ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[a-z-[bc]]$(?#x)`, "b", "i"))
	// Escaped members: \x{..} decodes into the class and gains the orbit
	// (F1-F4); \uXXXX decodes the same way (round-17 F2, ICU rows
	// U1-U18), so the case equivalence no longer depends on the escape
	// spelling.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\x{3C2}]$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\x{3C2}]$(?#x)`, "ς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\x73]$(?#x)`, "ſ", "i"))
	// Round-17 F2 supersedes the round-16 false pin here: [\u03C2] gains
	// the orbit exactly like the literal [ς] and the hex-escaped
	// [\x{3C2}] (ICU 78.3 probe rows U1-U3).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "ς", "i"))
	// A multi-character fold source in a class stays a single character:
	// ICU's class string closure (matching "ff" against [ﬀ]) is a
	// documented divergence.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ﬀ]$(?#x)`, "ﬀ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[ﬀ]$(?#x)`, "ff", "i"))
	// RE2 route oracle: the complete simple-fold orbit needs no
	// augmentation.
	require.True(t, regexpMatchWithEngineRE2(t, `^[ς]$`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `^ς+$`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRE2(t, `^[a-z]$`, "ſ", "i"))
	// Case-sensitive controls: the augmentation never applies.
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "σ", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[ς]$(?#x)`, "ς", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^ς+$(?#x)`, "σ", ""))
}

// Round-17 F2: \uXXXX and \UXXXXXXXX escapes inside character classes
// decode into the set and join the per-character fold orbit exactly like
// literal and \x-decoded members — the case equivalence of a class no
// longer depends on the escape spelling. Every expected value was probed
// against ICU 78.3 (rows U1-U18, V1-V5, X1-X2 and Y1-Y2 in the round-17
// evidence, /tmp/31175/round17/probe_icu17.log). Malformed escapes keep
// the engine's verdict: a \u with fewer than four hex digits is the
// engine's "insufficient hexadecimal digits" error, and a \U without
// eight hex digits is not translated, so the class reads 'U' as a
// literal member (ICU spells the malformed \u error
// U_REGEX_BAD_ESCAPE_SEQUENCE; the vendored engine's own error text is
// preserved, the same divergence boundary the base revision has).
func TestRegexpCaseFoldRegexp2UnicodeClassEscape(t *testing.T) {
	// Singleton classes through the \u spelling (U1-U3, U13).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "Σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "ς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "x", "i"))
	// Negated classes close before negation (U4-U5).
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[^\u03C2]$(?#x)`, "σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[^\u03C2]$(?#x)`, "Σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[^\u03C2]$(?#x)`, "x", "i"))
	// Escaped members of the s orbit (U6, U8, U9).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u0073]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u017F]$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u017F]$(?#x)`, "S", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u0053]$(?#x)`, "ſ", "i"))
	// Ranges spelled with escapes close over their members' orbits
	// (U7, U10, U15, U16).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u0041-\u005A]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u0061-\u007A]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u0041-Z]$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[A-\u005A]$(?#x)`, "ſ", "i"))
	// Orbit mates the ToLower closure already covers need no addition,
	// in both directions (U11-U12).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u00DF]$(?#x)`, "ẞ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u1E9E]$(?#x)`, "ß", "i"))
	// The \U spelling decodes through the \x{..} translation and joins
	// the orbit the same way (Y1; ICU decodes \U natively).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\U000003C2]$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\U000003C2]$(?#x)`, "ς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[\U000003C2]$(?#x)`, "x", "i"))
	// Case-sensitive controls: the decode never applies (U17-U18).
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "σ", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\u03C2]$(?#x)`, "ς", ""))
	// Malformed escapes keep the engine's verdict (V1-V2): an error,
	// never a panic and never a silently literal match.
	rs := newOpBuiltInRegexp().regMap
	for _, pat := range []string{`^[\u12]$(?#x)`, `^[\uZZZZ]$(?#x)`, `^[\u]$(?#x)`} {
		for _, mt := range []string{"i", ""} {
			_, err := rs.getRegexp2MatcherWithMatchType(pat, mt, false)
			require.Error(t, err, "pattern %s match_type %q", pat, mt)
			require.Contains(t, err.Error(), "insufficient hexadecimal digits", "pattern %s match_type %q", pat, mt)
		}
	}
	// A \U without eight hex digits is not translated: the class reads
	// 'U' as a literal member and the digits as members, so '1' matches
	// and 'σ' does not — the base engine's literal fallback, preserved.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^[\U0041]$(?#x)`, "1", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^[\U0041]$(?#x)`, "σ", "i"))
}

// Round-17 F1: a class whose closing delimiter is escaped — or that ends
// before any delimiter — is unterminated, and the fold rewrite must pass
// it through verbatim instead of augmenting a body whose final escape is
// truncated (the augmentation read past the body end). Both engines then
// report the same unterminated-class error as for the unrewritten
// pattern, under the case-insensitive route exactly as under the
// case-sensitive one (ICU 78.3 reports U_REGEX_MISSING_CLOSE_BRACKET for
// every shape below, rows W1-W9 in the round-17 evidence).
func TestRegexpCaseFoldUnterminatedClass(t *testing.T) {
	rs := newOpBuiltInRegexp().regMap
	pats := []string{
		`(?=)[s\]`,       // escaped final bracket
		`(?=)[^s\]`,      // negated, escaped final bracket
		`(?=)[a-z\]`,     // range, escaped final bracket
		`(?=)[\p{L}\]`,   // category, escaped final bracket
		`(?=)[a-z-[bc\]`, // unterminated nested subtraction set
		`(?=)[s\`,        // trailing backslash
		`(?=)[s`,         // no delimiter at all
	}
	for _, pat := range pats {
		for _, mt := range []string{"i", ""} {
			_, err := rs.getRegexp2MatcherWithMatchType(pat, mt, false)
			require.Error(t, err, "pattern %s match_type %q", pat, mt)
			require.Contains(t, err.Error(), "unterminated [] set", "pattern %s match_type %q", pat, mt)
		}
	}
	// The RE2 route reports its own unterminated-class error for the
	// same shapes (Go regexp has no nesting, so the subtraction shape
	// closes differently — its genuine ']' ends the class and the
	// trailing ']' starts an empty one).
	_, err := rs.getRegularMatcherWithMatchType(`[s\]`, "", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing closing ]")
	// A genuine closer after an escaped bracket still augments and
	// matches through the orbit (probe rows W3-W4).
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?=)[s\\]`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?=)[s\\]`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?=)[s\\]`, `\`, "i"))
	// Case-sensitive control: the escaped final bracket still errors.
	_, err = rs.getRegularMatcherWithMatchType(`[s\]`, "", false)
	require.Error(t, err)
}

// Round-18 F1: a \uXXXX or \UXXXXXXXX escape the regexp2 engine decodes
// into one literal atom receives the same per-character fold orbit as
// the literal spelling — the engine's caseless ToLower fiber misses
// orbit members (ſ for \u0073, σ for \u03C2), because the parser decodes
// and lowercases the atom while the runner only lowercases the subject.
// The completed atom stays a single token: it never joins a literal run,
// so the documented multi-character fold boundary stays (an escaped ß
// does not full-fold into ss, where ICU — decoding the escape into the
// pattern text and full-folding it — matches; the divergence is pinned
// below). Malformed escapes pass through verbatim so the engine's own
// error verdict stands. Every expected value was probed against ICU 78.3
// find() semantics (round-18 probe rows), except the pinned divergences.
func TestRegexpCaseFoldRegexp2UnicodeEscapeAtom(t *testing.T) {
	// Sigma orbit, both directions and every spelling.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "Σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "ς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "x", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C3$(?#x)`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03A3$(?#x)`, "ς", "i"))
	// Long-s orbit, both directions.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u0073$(?#x)`, "ſ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u0073$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u0073$(?#x)`, "S", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u017F$(?#x)`, "s", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u017F$(?#x)`, "S", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u0053$(?#x)`, "ſ", "i"))
	// The \U spelling decodes the same way (the spelling itself routes
	// to the regexp2 engine).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\U000003C2$`, "σ", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\U000003C2$`, "ς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\U000003C2$`, "x", "i"))
	// A following quantifier binds the completed atom exactly as it
	// binds the literal spelling.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2+$(?#x)`, "σς", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2+$(?#x)`, "Σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u03C2+$(?#x)`, "x", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2{2}$(?#x)`, "Σς", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u03C2{2}$(?#x)`, "σ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "σς", "i"))
	// Multi-character fold boundary: the escaped atom never joins a
	// literal run, so an escaped ß keeps its per-character orbit
	// {ß, ẞ} and does not full-fold into ss. ICU decodes the escape
	// into the pattern text and full-folds it, so ICU matches "ss"
	// here (probed) — the documented divergence the repair preserves.
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u00DF$(?#x)`, "ß", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u00DF$(?#x)`, "ẞ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u00DF$(?#x)`, "ss", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u1E9E$(?#x)`, "ß", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u1E9E$(?#x)`, "ss", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u00DF+$(?#x)`, "ss", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u00DF+$(?#x)`, "ßẞ", "i"))
	// Case-sensitive controls: the decode never applies.
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "σ", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u03C2$(?#x)`, "ς", ""))
	require.False(t, regexpMatchWithEngineRegexp2(t, `^\u0073$(?#x)`, "ſ", ""))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\u0073$(?#x)`, "s", ""))
	// Free-spacing mode: a decoded whitespace stays literal — the
	// rewrite re-emits it as \x{..}, whose literal meaning the engine
	// applies exactly as to the escape it decoded.
	require.True(t, regexpMatchWithEngineRegexp2(t, `(?x)^\u0020$`, " ", "i"))
	require.False(t, regexpMatchWithEngineRegexp2(t, `(?x)^\u0020$`, "", "i"))
	// The literal run around the decoded token still full-folds
	// (pre-existing behavior, pinned as a control).
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\U0000FF41ss$(?#x)`, "ａß", "i"))
	require.True(t, regexpMatchWithEngineRegexp2(t, `^\U0000FF41ss$(?#x)`, "Ａß", "i"))
	// Malformed escapes keep the engine's verdict — an error, never a
	// panic and never a silently literal match — under both modes.
	rs := newOpBuiltInRegexp().regMap
	for _, pat := range []string{`^\u12$(?#x)`, `^\uZZZZ$(?#x)`, `^\u$(?#x)`} {
		for _, mt := range []string{"i", ""} {
			_, err := rs.getRegexp2MatcherWithMatchType(pat, mt, false)
			require.Error(t, err, "pattern %s match_type %q", pat, mt)
			require.Contains(t, err.Error(), "insufficient hexadecimal digits", "pattern %s match_type %q", pat, mt)
		}
	}
	// The RE2 route has no \u escape at all: the pattern keeps the base
	// engine's syntax error, unchanged by the rewrite.
	_, err := rs.getRegularMatcherWithMatchType(`^\u03C2$`, "i", false)
	require.Error(t, err)
}
