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

// Regression coverage for the mo-bot review round on the per-line evaluation
// of wide-multiline anchors (#30210, #30646): trailing-LF termination,
// short-circuit routing, regexp2 admission for '.', whole-subject anchors,
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
	subject := "alpha\n" + strings.Repeat("a", 32768)
	require.Equal(t, regexpRouteWideLines, regexpWideMultilineRoute(`^alpha`, "m", subject, false))
	require.Equal(t, regexpRouteWideLines, regexpWideMultilineRoute(`a$`, "m", "a\rb", false))
	require.Equal(t, regexpRouteRE2, regexpWideMultilineRoute(`^a`, "", "a\rb", false))
	// An anchorless pattern skips routing entirely and runs native RE2.
	require.Equal(t, regexpRouteRE2, regexpWideMultilineRoute(`a\rb`, "m", "a\rb", false))
	// A terminator-capable pattern with anchors keeps whole-subject routing.
	require.Equal(t, regexpRouteRegexp2, regexpWideMultilineRoute(`^a\rb`, "m", "a\rb", false))
}

// '.' under the default match_type never matches a line terminator under the
// MySQL/ICU contract, so it must not force the regexp2 admission path.
func TestReviewWideLineDotAdmission(t *testing.T) {
	require.Equal(t, regexpRouteWideLines, regexpWideMultilineRoute(`^a.`, "m", "a\rb", false))
	// match_type 'n' makes '.' terminator-capable and stays off the line route.
	require.Equal(t, regexpRouteRegexp2, regexpWideMultilineRoute(`^a.`, "mn", "a\rb", false))

	op := newOpBuiltInRegexp()
	got, err := op.regMap.regularLikeWithMode(`^a.`, strings.Repeat("a", 16384)+"\r", "m", false)
	require.NoError(t, err)
	require.True(t, got)
}

// Whole-subject anchors (\A, \z) and (?-m) scopes must keep whole-subject
// routing; per-line evaluation would hand them fake line boundaries.
func TestReviewWideLineSubjectAnchors(t *testing.T) {
	require.Equal(t, regexpRouteRegexp2, regexpWideMultilineRoute(`^a\z`, "m", "a\rb", false))

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
		name      string
		pos, occ  int64
		want      string
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
