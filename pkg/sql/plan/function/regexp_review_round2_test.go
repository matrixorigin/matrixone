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
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRegexpReviewAngleRefTrailing covers review round 2 finding 1: trailing
// escaped punctuation and malformed angle references on the regexp2 route.
// The fork scans trailing \< and \' as literal characters; classification
// must not panic, and execution keeps accepting those literals.
func TestRegexpReviewAngleRefTrailing(t *testing.T) {
	op := newOpBuiltInRegexp()
	rows := []struct {
		name    string
		pattern string
		subject string
		mode    string
		want    bool
	}{
		{"trailing_angle", `(?=<)\<`, "<", "m", true},
		{"trailing_angle_mu", `(?=<)\<`, "<", "mu", true},
		{"trailing_apostrophe", `(?=')\'`, "'", "m", true},
		{"unterminated_body", `(?=<)\<na`, "<na", "m", true},
		{"empty_body", `(?=<)\<>`, "<>", "m", true},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got, err := op.regMap.regularLikeWithMode(tc.pattern, tc.subject, tc.mode, false)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestRegexpReviewLookaroundAlternation covers review round 2 finding 2:
// a skipped lookaround must hand the enclosing alternation scope back, so a
// following arm's candidates are not masked by the lookaround's stale frame.
// Expected values match the ICU 77.1 probe rev11 (F2a..F2h).
func TestRegexpReviewLookaroundAlternation(t *testing.T) {
	op := newOpBuiltInRegexp()

	// The reviewer example: ICU finds the LF at [2,3).
	got, err := op.regMap.regularLikeWithMode(`^a(?=b)|\n`, "a\r\nc", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^a(?=b)|\n`, "a\r\nc", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^a(?=b)|\n`, "a\r\nc", 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\n", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^a(?=b)|\n`, "a\r\nc", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\rXc", replaced)

	// Unix-lines control: no mask contributor, same find.
	got, err = op.regMap.regularLikeWithMode(`^a(?=b)|\n`, "a\r\nc", "mu", false)
	require.NoError(t, err)
	require.True(t, got)

	// AdjacentLF variant keeps the leftmost LF.
	pos, err = op.regMap.regularInstrWithMatchType(`^a(?=b)|\n`, "a\r\n\nc", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)

	// Nested scopes and quantifiers (ICU F2b/F2f/F2g).
	got, err = op.regMap.regularLikeWithMode(`(?:^a(?=b)|\nx)+`, "a\r\nxc", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`((?=b)^a|\nx)`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`(?:(?=b)^a|\nx){1,2}`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)

	// Capture scope with positional and replacement assertions (ICU F2c).
	pos, err = op.regMap.regularInstrWithMatchType(`(^a(?=b)|\nc)`, "a\r\nc", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	matched, value, err = op.regMap.regularSubstrWithMatchType(`(^a(?=b)|\nc)`, "a\r\nc", 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nc", value)
	replaced, err = op.regMap.regularReplaceWithMatchType(`(^a(?=b)|\nc)`, "a\r\nc", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\rX", replaced)

	// Negative control: the caret in the later arm is bare (ICU F2e).
	got, err = op.regMap.regularLikeWithMode(`x|(?=a)^\n`, "a\r\nc", "m", false)
	require.NoError(t, err)
	require.False(t, got)
}

// TestRegexpReviewClassIntervalMerge covers review round 2 finding 3:
// multi-member class intervals must stay a set. Expected values match the
// ICU 77.1 probe rev11 (F3a..F3e) on both engine routes.
func TestRegexpReviewClassIntervalMerge(t *testing.T) {
	op := newOpBuiltInRegexp()

	// RE2 route: the multi-member class is not string-exempt, so the
	// CRLF-interior candidate is suppressed like ICU.
	got, err := op.regMap.regularLikeWithMode(`^[\n-\r\n]x`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.False(t, got)

	// Positive control: a line may begin with a terminator member.
	got, err = op.regMap.regularLikeWithMode(`^[\n-\r\n]x`, "a\n\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^[\n-\r\n]x`, "a\n\nxb", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^[\n-\r\n]x`, "a\n\nxb", 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\nx", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^[\n-\r\n]x`, "a\n\nxb", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\nXb", replaced)

	// Same contract through the regexp2 route (lookahead forces routing).
	got, err = op.regMap.regularLikeWithMode(`^[\n-\r\n]x(?=b)`, "a\r\nxb", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^[\n-\r\n]x(?=b)`, "a\n\nxb", "m", false)
	require.NoError(t, err)
	require.True(t, got)

	// Adjacent class members must not collapse into a singleton.
	got, err = op.regMap.regularLikeWithMode(`^[\n\v]`, "a\n\nb", "m", false)
	require.NoError(t, err)
	require.True(t, got)

	// Unix-lines controls.
	got, err = op.regMap.regularLikeWithMode(`^[\n-\r\n]x`, "a\n\nxb", "mu", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(`^[\n-\r\n]x`, "a\r\nxb", "mu", false)
	require.NoError(t, err)
	require.False(t, got)
}

// TestRegexpReviewUnicodeSourceEscapes covers review round 2 finding 4:
// \Uxxxxxxxx converts to one rune and \Q..\E quotes on the regexp2 text
// route; expected values match the ICU 77.1 probe rev11 (F4a..F4f).
func TestRegexpReviewUnicodeSourceEscapes(t *testing.T) {
	op := newOpBuiltInRegexp()

	// Single LF head via \U: CRLF-interior stays suppressed.
	got, err := op.regMap.regularLikeWithMode(`^\U0000000A(?=)`, "a\r\nb", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(`^\U0000000A(?=)`, "a\r\nb", "mu", false)
	require.NoError(t, err)
	require.False(t, got)

	// Positive controls with position, matched text and replacement.
	got, err = op.regMap.regularLikeWithMode(`^\U0000000A(?=)`, "a\n\nb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	pos, err := op.regMap.regularInstrWithMatchType(`^\U0000000A(?=)`, "a\n\nb", 1, 1, 0, false, "m")
	require.NoError(t, err)
	require.EqualValues(t, 3, pos)
	matched, value, err := op.regMap.regularSubstrWithMatchType(`^\U0000000A(?=)`, "a\n\nb", 1, 1, false, "m")
	require.NoError(t, err)
	require.True(t, matched)
	require.Equal(t, "\n", value)
	replaced, err := op.regMap.regularReplaceWithMatchType(`^\U0000000A(?=)`, "a\n\nb", "X", 1, 0, false, "m")
	require.NoError(t, err)
	require.Equal(t, "a\nXb", replaced)
	got, err = op.regMap.regularLikeWithMode(`^\U0000000A(?=)`, "a\n\nb", "mu", false)
	require.NoError(t, err)
	require.True(t, got)

	// Non-terminator conversion control.
	got, err = op.regMap.regularLikeWithMode(`^\U00000041(?=)`, "b\nA", "m", false)
	require.NoError(t, err)
	require.True(t, got)

	// Quoted single LF has the same contract as the converted escape.
	quoteLF := "^\\Q" + "\n" + "\\E(?=)"
	got, err = op.regMap.regularLikeWithMode(quoteLF, "a\r\nb", "m", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(quoteLF, "a\r\nb", "mu", false)
	require.NoError(t, err)
	require.False(t, got)
	got, err = op.regMap.regularLikeWithMode(quoteLF, "a\n\nb", "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(quoteLF, "a\n\nb", "mu", false)
	require.NoError(t, err)
	require.True(t, got)

	// Binary route: \U escapes are outside the public binary grammar and
	// rejected at admission (pinned boundary).
	_, err = op.regMap.regularLikeWithMode(`\U0000000A`, "U0000000A", "m", true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Unicode code-point escapes")
}

// TestRegexpReviewSparseClassCompile covers review round 2 finding 5 at the
// SQL layer: a large sparse class compiles and matches member-exactly.
func TestRegexpReviewSparseClassCompile(t *testing.T) {
	op := newOpBuiltInRegexp()
	var body strings.Builder
	body.WriteString("[")
	for i := 0; i < 1024; i++ {
		body.WriteRune(rune(0x10000 + 2*i))
	}
	body.WriteString("]")
	pattern := body.String()
	got, err := op.regMap.regularLikeWithMode(pattern, string(rune(0x10000+2*1023)), "m", false)
	require.NoError(t, err)
	require.True(t, got)
	got, err = op.regMap.regularLikeWithMode(pattern, string(rune(0x10000+2*1023-1)), "m", false)
	require.NoError(t, err)
	require.False(t, got)
}

// TestRegexp2AdmissionBeforeClassification covers review round 2 finding 6:
// the size admission verdicts must be reached before the classifier runs, so
// a doomed pattern cannot force per-byte classifier allocations.
func TestRegexp2AdmissionBeforeClassification(t *testing.T) {
	var rs regexpSet
	// One million literal chars fits the source byte cap but the translated
	// size blows the admission budget; classification must never see it.
	huge := "(?=)" + strings.Repeat("a", 1_000_000)
	_, err := rs.getRegexp2MatcherWithMatchType(huge, "m", false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "exceeds the maximum supported")

	var m1, m2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m1)
	for i := 0; i < 3; i++ {
		var rs2 regexpSet
		_, err := rs2.getRegexp2MatcherWithMatchType(huge, "m", false)
		require.Error(t, err)
	}
	runtime.ReadMemStats(&m2)
	require.Less(t, m2.TotalAlloc-m1.TotalAlloc, uint64(16<<20),
		"three doomed regexp2 compiles must not materialize per-byte classifier events")
}

// TestRegexp2ConcurrentAdmissionRejection covers review round 2 finding 6
// for concurrent compiles: rejected patterns stay out of the aggregate
// subject budget in both directions.
func TestRegexp2ConcurrentAdmissionRejection(t *testing.T) {
	huge := "(?=)" + strings.Repeat("a", 1_000_000)
	before := regexp2ActiveSubjectBytes.Load()
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var rs regexpSet
			_, err := rs.getRegexp2MatcherWithMatchType(huge, "m", false)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.Error(t, err)
		require.Contains(t, err.Error(), "exceeds the maximum supported")
	}
	require.Equal(t, before, regexp2ActiveSubjectBytes.Load(),
		"rejected concurrent compiles must release the aggregate budget")

	// Verdict preservation: a medium pattern below every gate still compiles.
	var rs regexpSet
	_, err := rs.getRegexp2MatcherWithMatchType("(?=)"+strings.Repeat("a", 50_000), "m", false)
	require.NoError(t, err)
}
