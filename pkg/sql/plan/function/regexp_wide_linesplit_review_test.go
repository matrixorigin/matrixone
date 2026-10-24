// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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
	"time"

	"github.com/stretchr/testify/require"
)

// Wide-multiline anchor regressions (#30210, #30646): trailing-LF termination,
// prefix short-circuiting, regular subject admission, whole-subject anchors,
// replace occurrence accounting across lines, and zero-width matches at the
// requested start position.

// A trailing LF leaves one final empty line; it must be yielded exactly once
// so a non-matching predicate terminates.
func TestReviewWideLinesTrailingLFTerminates(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		name    string
		subject string
		binary  bool
	}{
		{"text_lf", "a\rb\n", false},
		{"binary_lf", "a\rb\n", true},
		{"binary_crlf", "a\x85b\r\n", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			done := make(chan bool, 1)
			go func() {
				got, err := op.regMap.regularLikeWithMode(`^z$`, tc.subject, "m", tc.binary)
				require.NoError(t, err)
				done <- got
			}()
			select {
			case got := <-done:
				require.False(t, got)
			case <-time.After(3 * time.Second):
				t.Fatal("REGEXP_LIKE on a trailing-LF subject did not terminate")
			}
		})
	}
	// The final empty line must still be yielded once: it is matchable.
	got, err := op.regMap.regularLikeWithMode(`^$`, "a\n", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^b$`, "a\rb\n", "m", false)
	require.NoError(t, err)
	require.True(t, got)
}

// Route selection must depend on pattern and match_type alone; no whole-subject
// pre-scan may sit in front of a prefix short-circuit match.
func TestReviewWideLineRouteSelection(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, pattern := range []string{`^alpha`, `a$`, `^a\rb`, `^a\s`, `^a\z`} {
		require.False(t, op.regMap.requiresRegexp2(pattern, "m", false), pattern)
	}
	require.True(t, op.regMap.requiresRegexp2(`(?<=a)b`, "m", false))
}

// A regular dot pattern must retain the regular engine's subject domain.
func TestReviewWideLineDotAdmission(t *testing.T) {
	op := newOpBuiltInRegexp()
	got, err := op.regMap.regularLikeWithMode(`^a.`, strings.Repeat("a", 16384)+"\r", "m", false)
	require.NoError(t, err)
	require.True(t, got)
}

// Whole-subject anchors (\A, \z) and (?-m) scopes must keep whole-subject
// routing; per-line evaluation would hand them fake line boundaries.
func TestReviewWideLineSubjectAnchors(t *testing.T) {

	op := newOpBuiltInRegexp()
	for _, pat := range []string{`^a\z`, `\Ab$`, `(?-m:^b)|^z`} {
		got, err := op.regMap.regularLikeWithMode(pat, "a\rb", "m", false)
		require.NoError(t, err)
		require.False(t, got, pat)
	}
	// Control: a subject-start anchor still matches at the subject start.
	got, err := op.regMap.regularLikeWithMode(`(?-m:^a)`, "a\rb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
}

// Replace occurrence counts matches across lines: lines holding the earlier
// matches stay unchanged, and only the requested ordinal is replaced.
func TestReviewWideLineReplaceOccurrence(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		name     string
		pos, occ int64
		want     string
	}{
		{"first", 1, 1, "X\ra"},
		{"second", 1, 2, "a\rX"},
		{"beyond", 1, 3, "a\ra"},
		{"all", 1, 0, "X\rX"},
		{"pos_into_second_line", 3, 1, "a\rX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularReplaceWithMatchType(`^a`, "a\ra", "X", tc.pos, tc.occ, false, "m")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// A zero-width match on an initial empty line is a valid hit at the requested
// start position.
func TestReviewWideLineInstrInitialEmptyLine(t *testing.T) {
	op := newOpBuiltInRegexp()
	got, err := op.regMap.regularInstrWithMatchType(`^`, "\ra", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(1), got)
	got, err = op.regMap.regularInstrWithMatchType(`^`, "\ra", 1, 2, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(2), got)
}

// Interior and trailing empty lines keep their own anchor matches: an empty
// line is not an empty subject, so the base empty-subject shortcut must not
// swallow them (mo-bot review on #30646: '\na' with '^' becomes 'X\nXa').
func TestReviewWideLineEmptyLineReplace(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		name     string
		subject  string
		pat      string
		repl     string
		pos, occ int64
		want     string
	}{
		{"leading", "\na", "^", "X", 1, 0, "X\nXa"},
		{"interior", "a\n\nb", "^", "X", 1, 0, "Xa\nX\nXb"},
		{"trailing_lf", "a\n", "^", "X", 1, 0, "Xa\nX"},
		{"empty_subject_shortcut", "", "^", "X", 1, 0, ""},
		{"occurrence_second_empty_line", "\n\n", "^", "X", 1, 2, "\nX\n"},
		{"dollar_interior_empty_line", "a\n\nb", "$", "X", 1, 0, "aX\nX\nbX"},
		{"template_on_empty_line", "\na", "^", "[$0]", 1, 0, "[]\n[]a"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularReplaceWithMatchType(tc.pat, tc.subject, tc.repl, tc.pos, tc.occ, false, "m")
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// The terminal segment after a trailing lone CR/NEL/LS/PS still binds '$'
// at the true end of input, while '^' stays suppressed there (mo-bot review
// on #30646: 'a\r' with '$' becomes 'aX\rX', matching ICU 76.1).
func TestReviewWideLineTerminalDollar(t *testing.T) {
	op := newOpBuiltInRegexp()
	for _, tc := range []struct {
		name    string
		subject string
		binary  bool
		want    string
	}{
		{"cr", "a\r", false, "aX\rX"},
		{"nel_text", "a\xc2\x85", false, "aX\xc2\x85X"},
		{"ls_text", "a\xe2\x80\xa8", false, "aX\xe2\x80\xa8X"},
		{"ps_text", "a\xe2\x80\xa9", false, "aX\xe2\x80\xa9X"},
		{"nel_binary", "a\x85", true, "aX\x85X"},
		{"crlf_keeps_lf_contract", "a\r\n", false, "aX\r\nX"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, pattern := range []string{`$`, `($)+`, `($){1,2}`} {
				got, err := op.regMap.regularReplaceWithMatchType(pattern, tc.subject, "X", 1, 0, tc.binary, "m")
				require.NoError(t, err, pattern)
				require.Equal(t, tc.want, got, pattern)
				at, err := op.regMap.regularInstrWithMatchType(pattern, tc.subject, 1, 2, 0, tc.binary, "m")
				require.NoError(t, err, pattern)
				require.Equal(t, regexpSubjectLength(tc.subject, tc.binary)+1, at, pattern)
				matched, value, err := op.regMap.regularSubstrWithMatchType(pattern, tc.subject, 1, 2, tc.binary, "m")
				require.NoError(t, err, pattern)
				require.True(t, matched, pattern)
				require.Empty(t, value, pattern)
			}
		})
	}
	// The caret stays suppressed on the same terminal segment.
	got, err := op.regMap.regularReplaceWithMatchType(`^`, "a\r", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "Xa\r", got)
	liked, err := op.regMap.regularLikeWithMode(`^$`, "a\r", "m", false)
	require.NoError(t, err)
	require.False(t, liked)
	// A mandatory repetition of a forbidden caret still cannot match there.
	at, err := op.regMap.regularInstrWithMatchType(`(^)+`, "a\r", 1, 2, 0, false, "m")
	require.NoError(t, err)
	require.Zero(t, at)
	// INSTR sees the terminal '$' match as the second occurrence.
	at, err = op.regMap.regularInstrWithMatchType(`$`, "a\r", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(2), at)
	at, err = op.regMap.regularInstrWithMatchType(`$`, "a\r", 1, 2, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, int64(3), at)
}

// The aggregate replacement-size budget is enforced across lines: nine 8
// MiB individual replacements each fit the limit, but the accumulated
// result must stop at the shared maximum (mo-bot review on #30646).
func TestReviewWideLineReplaceAggregateLimit(t *testing.T) {
	op := newOpBuiltInRegexp()
	repl := strings.Repeat("R", 8<<20)
	_, err := op.regMap.regularReplaceWithMatchType(`^a`, strings.Repeat("a\n", 9), repl, 1, 0, false, "m")
	require.Error(t, err)
	require.Contains(t, err.Error(), "maximum supported size")
	// Control: one large replacement under the budget stays legal.
	got, err := op.regMap.regularReplaceWithMatchType(`^a`, "a", repl, 1, 0, false, "m")
	require.NoError(t, err)
	require.Len(t, got, 8<<20)
}

// Anchored-prefix matches on a long terminator-free line keep the prefix
// short-circuit cost of the regular engine without scanning the subject.
func TestReviewWideLinePrefixShortCircuit(t *testing.T) {
	op := newOpBuiltInRegexp()
	long := "alpha" + strings.Repeat("x", 32768)
	got, err := op.regMap.regularLikeWithMode(`^alpha`, long, "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^alphz`, long, "m", false)
	require.NoError(t, err)
	require.False(t, got)
	// Unanchored hits also retain their ordinary result.
	got, err = op.regMap.regularLikeWithMode(`ay`, "ay"+strings.Repeat("x", 32768), "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// Search can also reach a later line.
	got, err = op.regMap.regularLikeWithMode(`^z`, "ab\r\n"+strings.Repeat("x", 32768)+"\r\nz", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// No match anywhere on a multi-line long subject still finishes false.
	got, err = op.regMap.regularLikeWithMode(`^z`, "aa\r"+strings.Repeat("x", 32768)+"\rbb", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}

// Whitespace-consuming patterns retain the regular subject domain with both
// LF and wide terminators.
func TestReviewWideLineTerminatorClassRoute(t *testing.T) {
	subject := "a\n" + strings.Repeat("y", 16384)
	// A wide terminator must not change engine admission.
	wide := "a\r" + strings.Repeat("y", 1024)

	op := newOpBuiltInRegexp()
	got, err := op.regMap.regularLikeWithMode(`^a\s`, subject, "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^a\s`, "a\n"+strings.Repeat("y", 1048579), "m", false)
	require.NoError(t, err)
	require.True(t, got)
	// The wide-terminator control agrees.
	got, err = op.regMap.regularLikeWithMode(`^a\s`, wide, "m", false)
	require.NoError(t, err)
	require.True(t, got)
}
