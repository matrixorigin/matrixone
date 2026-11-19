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

package collation

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// likeWeightCase pins one LIKE comparison under a character-weight function
// (issue #31175).
type likeWeightCase struct {
	value   string
	pattern string
	want    bool
}

func runLikeWeightCases(t *testing.T, name string, charWeight CharWeightFunc, cases []likeWeightCase) {
	t.Helper()
	for _, c := range cases {
		tokens, err := TokenizeLikePattern(context.Background(), []byte(c.pattern), '\\', true, charWeight)
		if err != nil {
			t.Fatalf("%s: tokenize %q: %v", name, c.pattern, err)
		}
		got, err := MatchLikePattern(context.Background(), tokens, []byte(c.value), charWeight)
		if err != nil {
			t.Fatalf("%s: match %q LIKE %q: %v", name, c.value, c.pattern, err)
		}
		if got != c.want {
			t.Errorf("%s: %q LIKE %q = %v, want %v", name, c.value, c.pattern, got, c.want)
		}
	}
}

func TestGeneralCILikeWeights(t *testing.T) {
	runLikeWeightCases(t, "general_ci", GeneralCICharWeight, []likeWeightCase{
		// MySQL utf8mb4_general_ci per-character equivalences.
		{"Alice", "alice%", true},
		{"á", "a", true},
		{"Áb", "a_", true},
		{"ı", "I", true},
		{"ß", "s", true},
		{"ß", "ss", false}, // general_ci has no multi-character expansion
		{"ς", "Σ", true},
		{"σ", "ς", true},
		{"Σx", "σ%", true},
		{"œ", "o", false},
		{"æ", "a", false},
		// Wildcards consume whole characters.
		{"á", "_", true},
		{"ab", "_", false},
		{"", "%", true},
		{"", "", true},
		{"a", "", false},
		{"xYz", "%y%", true},
		{"abc", "a%c", true},
		{"abc", "a_c", true},
		// Escaped wildcards fold but stay literal.
		{"a_b", `a\_b`, true},
		{"aXb", `a\_b`, false},
		{"Σ_ς", `σ\_σ`, true},
		// MySQL's utf8mb4_general_ci wildcard matcher folds every
		// supplementary character to the 0xFFFD replacement weight
		// (my_tosort_unicode with maxchar 0xFFFF), so distinct
		// supplementary characters compare equal (issue #31175 review).
		{"\U0001F600", "\U0001F601", true},
		{"\U0001F600", "_", true},
		{"\U0001F600", "__", false},
		{"x\U0001F600y", "%\U0001F601%", true},
	})
}

func TestUCA400LikeWeights(t *testing.T) {
	runLikeWeightCases(t, "uca400", UCA400CharWeight, []likeWeightCase{
		// MySQL utf8mb4_unicode_ci per-character weight equivalences
		// (my_wildcmp_uca): one pattern character against one value
		// character, compared by UCA 4.0.0 primary weight streams.
		{"Alice", "alice%", true},
		{"á", "a", true},
		{"Áb", "a_", true},
		{"ı", "I", false},  // UCA400 gives U+0131 and I distinct weights
		{"ß", "s", false},  // U+00DF expands to "ss", so it is not plain s
		{"ß", "ss", false}, // LIKE never spans an expansion across characters
		{"ss", "ß", false},
		{"œ", "oe", false},
		{"oe", "œ", false},
		{"æ", "ae", false},
		{"ς", "Σ", true},
		{"σ", "ς", true},
		// '_' consumes exactly one character, however many weights it has,
		// and '%' consumes only whole characters.
		{"ß", "_", true},
		{"ß", "__", false},
		{"xßy", "%ss%", false},
		{"xßy", "%s%", false},
		{"xssy", "%ss%", true},
		{"œ", "_", true},
		{"", "%", true},
		{"", "", true},
		{"a", "", false},
		// Escapes quote the next character, including a trailing escape.
		{"a_b", `a\_b`, true},
		{"a%b", `a\%b`, true},
		{`a\b`, `a\\b`, true},
		{`a\`, `a\`, true},
		{"ab", `a\`, false},
		// MySQL's my_uca_charcmp falls back to code-point identity for a
		// character with no table weight: distinct supplementary
		// characters never match, identical ones still do, and '_'
		// consumes exactly one character (issue #31175 review).
		{"\U0001F600", "\U0001F601", false},
		{"\U0001F600", "\U0001F600", true},
		{"\U0001F600", "_", true},
		{"\U0001F600", "__", false},
		{"x\U0001F600y", "%\U0001F601%", false},
		{"x\U0001F600y", "%\U0001F600%", true},
		{"\uFFFD", "\U0001F600", false}, // genuine U+FFFD keeps its real weight
	})
}

// MySQL's my_uca_charcmp returns code-point identity when a character has
// no UCA weight (wc > maxchar or a weightless page). The pinned UCA 4.0.0
// table marks exactly those characters with the 0xFFFD sentinel weight, so
// the LIKE matcher must map the sentinel to a per-code-point synthetic
// weight instead of equating every unweighted character (issue #31175
// review).
func TestUCA400CharWeightSupplementaryIdentity(t *testing.T) {
	weightsOf := func(s string) []uint32 {
		w, size := UCA400CharWeight(nil, []byte(s))
		if size != len(s) {
			t.Fatalf("%q must decode as one character, got %d bytes", s, size)
		}
		return w
	}
	grinning, beaming := weightsOf("\U0001F600"), weightsOf("\U0001F601")
	if slices.Equal(grinning, beaming) {
		t.Errorf("distinct supplementary characters must not share a weight stream: %v", grinning)
	}
	if !slices.Equal(grinning, weightsOf("\U0001F600")) {
		t.Errorf("identical supplementary characters must share one weight stream")
	}
	// The genuine U+FFFD replacement character has a real UCA 4.0.0 table
	// weight (0x0DC6) and must not take the synthetic identity path.
	if got := weightsOf("\uFFFD"); !slices.Equal(got, []uint32{0x0DC6}) {
		t.Errorf("U+FFFD weight = %v, want the real table weight [0x0DC6]", got)
	}
	// Unassigned BMP code points keep their per-code-point implicit
	// weights, so two different unassigned code points stay distinct.
	if slices.Equal(weightsOf("\u0378"), weightsOf("\u0379")) {
		t.Errorf("distinct unassigned BMP code points must not share implicit weights")
	}
}

// BenchmarkMatchLikePatternAdversarial measures a 60,000-character subject
// against 60,000 '_' wildcards: only one position is reachable at every
// step, so the matcher must not scan the whole position array per token
// (issue #31175 review).
func BenchmarkMatchLikePatternAdversarial(b *testing.B) {
	value := bytes.Repeat([]byte("x"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(), bytes.Repeat([]byte("_"), 60000), '\\', true, UCA400CharWeight)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight)
		if err != nil {
			b.Fatal(err)
		}
		if !match {
			b.Fatal("60k '_' wildcards must match a 60k subject")
		}
	}
}

// countDownCtx cancels after a fixed number of Err checks, giving a
// deterministic mid-loop cancellation point (issue #31175 review).
type countDownCtx struct {
	context.Context
	remaining int
}

func (c *countDownCtx) Err() error {
	if c.remaining <= 0 {
		return context.Canceled
	}
	c.remaining--
	return nil
}

// Tokenization and matching must observe context cancellation inside their
// loops, not only before they start: an adversarial 60,000-character input
// otherwise runs to completion after the query is cancelled.
func TestLikePatternCancellation(t *testing.T) {
	value := bytes.Repeat([]byte("x"), 60000)
	pattern := bytes.Repeat([]byte("_"), 60000)

	// An already-cancelled context fails both entry points.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := TokenizeLikePattern(cancelled, pattern, '\\', true, UCA400CharWeight); !errors.Is(err, context.Canceled) {
		t.Errorf("tokenize on a cancelled context = %v, want context.Canceled", err)
	}
	tokens, err := TokenizeLikePattern(context.Background(), pattern, '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := MatchLikePattern(cancelled, tokens, value, UCA400CharWeight); !errors.Is(err, context.Canceled) {
		t.Errorf("match on a cancelled context = %v, want context.Canceled", err)
	}

	// A context that cancels after a few checks stops a long tokenization
	// and a long match deterministically.
	ctx := &countDownCtx{Context: context.Background(), remaining: 5}
	if _, err := TokenizeLikePattern(ctx, pattern, '\\', true, UCA400CharWeight); !errors.Is(err, context.Canceled) {
		t.Errorf("mid-loop tokenize cancellation = %v, want context.Canceled", err)
	}
	ctx = &countDownCtx{Context: context.Background(), remaining: 5}
	if _, err := MatchLikePattern(ctx, tokens, value, UCA400CharWeight); !errors.Is(err, context.Canceled) {
		t.Errorf("mid-loop match cancellation = %v, want context.Canceled", err)
	}

	// The same work completes on a live context.
	if match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight); err != nil || !match {
		t.Errorf("60k '_' must match a 60k subject on a live context: %v %v", match, err)
	}
}

func TestTokenizeLikePatternCustomEscape(t *testing.T) {
	tokens, err := TokenizeLikePattern(context.Background(), []byte("Σ!_Σ!%x"), '!', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 1 || tokens[0].Kind != LikeTokenLiteral {
		t.Fatalf("escaped wildcards must stay one literal token: %+v", tokens)
	}
	// Σ!_Σ!%x is the literal stream Σ _ Σ % x under '!' escapes.
	if match, err := MatchLikePattern(context.Background(), tokens, []byte("σ_ς%x"), UCA400CharWeight); err != nil || !match {
		t.Errorf("σ_ς%%x must match the escaped literal pattern: %v %v", match, err)
	}
	if match, err := MatchLikePattern(context.Background(), tokens, []byte("σXςYx"), UCA400CharWeight); err != nil || match {
		t.Errorf("escaped '_' and '%%' must not act as wildcards: %v %v", match, err)
	}

	disabled, err := TokenizeLikePattern(context.Background(), []byte(`a\_b`), '!', false, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	if match, err := MatchLikePattern(context.Background(), disabled, []byte(`a\Xb`), UCA400CharWeight); err != nil || !match {
		t.Errorf("with escapes disabled the backslash is ordinary and '_' stays a wildcard: %v %v", match, err)
	}
}

// Consecutive '%' wildcards match the same values as one '%', so the
// tokenizer merges them and the matcher never pays one token each for a
// degenerate run (issue #31175 review).
func TestTokenizeLikePatternMergesConsecutivePercent(t *testing.T) {
	tokens, err := TokenizeLikePattern(context.Background(), []byte("a%%%b%%"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	kinds := []LikeTokenKind{LikeTokenLiteral, LikeTokenAnySeq, LikeTokenLiteral, LikeTokenAnySeq}
	if len(tokens) != len(kinds) {
		t.Fatalf("consecutive %% must merge into one token: %+v", tokens)
	}
	for i, k := range kinds {
		if tokens[i].Kind != k {
			t.Fatalf("token %d kind = %v, want %v (%+v)", i, tokens[i].Kind, k, tokens)
		}
	}
}

func TestLikeWeightMalformedBytes(t *testing.T) {
	// Malformed bytes share one synthetic weight above every valid
	// character weight: the legacy regex matcher decodes each malformed
	// byte as U+FFFD, so a malformed pattern byte matches any malformed
	// subject byte but never a valid character.
	for _, charWeight := range []CharWeightFunc{GeneralCICharWeight, UCA400CharWeight} {
		tokens, err := TokenizeLikePattern(context.Background(), []byte{0xFF}, '\\', true, charWeight)
		if err != nil {
			t.Fatal(err)
		}
		if match, err := MatchLikePattern(context.Background(), tokens, []byte{0xFF}, charWeight); err != nil || !match {
			t.Errorf("malformed byte must match itself: %v %v", match, err)
		}
		if match, err := MatchLikePattern(context.Background(), tokens, []byte{0xFE}, charWeight); err != nil || !match {
			t.Errorf("malformed bytes decode as U+FFFD and must match: %v %v", match, err)
		}
		if match, err := MatchLikePattern(context.Background(), tokens, []byte("a"), charWeight); err != nil || match {
			t.Errorf("malformed byte must not match a valid character: %v %v", match, err)
		}
		one, err := TokenizeLikePattern(context.Background(), []byte("_"), '\\', true, charWeight)
		if err != nil {
			t.Fatal(err)
		}
		if match, err := MatchLikePattern(context.Background(), one, []byte{0xFF}, charWeight); err != nil || !match {
			t.Errorf("'_' consumes one character, malformed or not: %v %v", match, err)
		}
	}
}

// Literal-only LIKE is exactly per-character weight equality: the operands
// must have the same character count and equal weight streams character by
// character (my_wildcmp_uca). Pin the matcher against independently computed
// per-character weights as a counterexample guard.
func TestMatchLikePatternLiteralStreamConsistency(t *testing.T) {
	perChar := func(s string) [][]uint32 {
		var chars [][]uint32
		for value := []byte(s); len(value) > 0; {
			var size int
			var weights []uint32
			weights, size = UCA400CharWeight(weights, value)
			chars = append(chars, weights)
			value = value[size:]
		}
		return chars
	}
	pairs := [][2]string{
		{"á", "a"}, {"ı", "I"}, {"ß", "ss"}, {"ß", "s"}, {"œ", "oe"},
		{"æ", "ae"}, {"ς", "σ"}, {"Straße", "strasse"}, {"résumé", "resume"},
	}
	for _, pair := range pairs {
		left, right := perChar(pair[0]), perChar(pair[1])
		eq, eqErr := likeCharsEqual(&likeSearchBudget{ctx: context.Background()}, right, left)
		if eqErr != nil {
			t.Fatal(eqErr)
		}
		want := len(left) == len(right) && eq
		tokens, err := TokenizeLikePattern(context.Background(), []byte(pair[1]), '\\', true, UCA400CharWeight)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := MatchLikePattern(context.Background(), tokens, []byte(pair[0]), UCA400CharWeight); err != nil || got != want {
			t.Errorf("uca400: %q LIKE %q = %v disagrees with per-character weights: %v", pair[0], pair[1], got, err)
		}
	}
}

func TestAppendGeneralCIFold(t *testing.T) {
	// The fold is a one-to-one rune mapping of the input, so byte-oriented
	// LIKE consumers compare exactly the per-character weight stream.
	for _, value := range []string{"", "Alice", "áéíóú", "ςσΣ", "ß", "ıI", "œæ"} {
		folded := AppendGeneralCIFold(nil, []byte(value))
		if utf8.RuneCount(folded) != utf8.RuneCountInString(value) {
			t.Errorf("AppendGeneralCIFold(%q) changed the character count", value)
		}
	}
	if got := AppendGeneralCIFold(nil, []byte("á")); !bytes.Equal(got, []byte("A")) {
		t.Errorf("AppendGeneralCIFold(á) = %q, want A", got)
	}
	if got := AppendGeneralCIFold(nil, []byte("ß")); !bytes.Equal(got, []byte("S")) {
		t.Errorf("AppendGeneralCIFold(ß) = %q, want S", got)
	}
	if got := AppendGeneralCIFold(nil, []byte("ς")); !bytes.Equal(got, []byte("Σ")) {
		t.Errorf("AppendGeneralCIFold(ς) = %q, want Σ", got)
	}
	// Pure-ASCII input folds exactly like an ASCII uppercase, keeping the
	// executor's allocation-free fast path honest.
	ascii := "Hello, World! 123 %_\\"
	if got := AppendGeneralCIFold(nil, []byte(ascii)); !bytes.Equal(got, []byte("HELLO, WORLD! 123 %_\\")) {
		t.Errorf("AppendGeneralCIFold ASCII = %q", got)
	}
	// Malformed bytes fold to one shared synthetic rune, mirroring the
	// U+FFFD decoding of the legacy regex matcher, and never collide with
	// a valid character's weight.
	bad := AppendGeneralCIFold(nil, []byte{'a', 0xFF})
	valid := AppendGeneralCIFold(nil, []byte("ab"))
	if bytes.Equal(bad, valid) {
		t.Errorf("malformed byte must not fold onto a valid weight: %q", bad)
	}
	if !bytes.Equal(AppendGeneralCIFold(nil, []byte{0xFF}), AppendGeneralCIFold(nil, []byte{0xFE})) {
		t.Errorf("malformed bytes must share one fold weight")
	}
}

// Anchored exact/prefix/suffix/affix and fixed-width shapes must return
// exactly the general matcher's answer; the anchored fast paths exist to
// avoid rescanning the subject per wildcard position (issue #31175 review).
func TestMatchLikePatternAffixShapes(t *testing.T) {
	match := func(pattern, value string) bool {
		tokens, err := TokenizeLikePattern(context.Background(), []byte(pattern), '\\', true, UCA400CharWeight)
		if err != nil {
			t.Fatal(err)
		}
		got, err := MatchLikePattern(context.Background(), tokens, []byte(value), UCA400CharWeight)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	longA30 := strings.Repeat("a", 30)
	cases := []struct {
		pattern, value string
		want           bool
	}{
		// exact (no wildcards)
		{"abc", "abc", true}, {"abc", "abcd", false}, {"abc", "ab", false},
		{"", "", true}, {"", "abc", false},
		// prefix-anchored (trailing %)
		{"ab%", "abcd", true}, {"ab%", "ab", true}, {"ab%", "aXcd", false}, {"ab%", "a", false},
		// suffix-anchored (leading %)
		{"%ab", "xxab", true}, {"%ab", "ab", true}, {"%ab", "xabY", false}, {"%ab", "b", false},
		// affix-anchored (one interior %)
		{"a%z", "aXYZz", true}, {"a%z", "az", true}, {"a%z", "aXzY", false}, {"a%z", "ZaXz", false},
		{"ab%ba", "abba", true}, {"ab%ba", "aba", false}, {"ab%ba", "abXba", true},
		// fixed width with '_' (no %), and '_' inside affix shapes
		{"a_c", "abc", true}, {"a_c", "abxc", false}, {"a_c%", "axcd", true}, {"%a_c", "xaXc", true},
		{"_", "é", true}, {"__", "é", false},
		// collation weights still decide the anchored comparisons
		{"%é", "xÉ", true}, {"É%", "éx", true},
		// long anchored suffix: the whole literal must be present at the end
		{"%" + longA30 + "b", longA30 + "b", true},
		{"%" + longA30 + "b", strings.Repeat("a", 60), false},
		{"%" + longA30 + "b", strings.Repeat("a", 29) + "b", false},
		{strings.Repeat("a", 600), strings.Repeat("a", 600), true},
	}
	for _, tc := range cases {
		if got := match(tc.pattern, tc.value); got != tc.want {
			t.Errorf("%q LIKE %q = %v, want %v", tc.value, tc.pattern, got, tc.want)
		}
	}
}

// Cancellation must reach inside one long literal comparison: a context
// whose budget ends after the subject is weighed aborts the anchored
// comparison itself instead of running it to completion (issue #31175
// review).
func TestMatchLikePatternCancellationInLiteralCompare(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(), bytes.Repeat([]byte("a"), 60000), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	// Entry, subject weighing and the token dispatch cost well under 100
	// checks; the remaining budget expires inside the 60,000-character
	// literal comparison.
	ctx := &countDownCtx{Context: context.Background(), remaining: 100}
	match, err := MatchLikePattern(ctx, tokens, value, UCA400CharWeight)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("mid-comparison cancellation = match %v, err %v, want context.Canceled", match, err)
	}
	// The same work completes on a live context.
	if match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight); err != nil || !match {
		t.Errorf("60k literal must match a 60k subject on a live context: %v %v", match, err)
	}
}

// Cancellation must also reach the multi-'%' segment search itself: a
// context whose budget expires mid-scan aborts the contains search instead
// of running it to completion (issue #31175 review).
func TestMatchLikePatternCancellationInWildcardShift(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("a", 30000)+"b%"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	// Entry, subject weighing and the KMP failure-table build cost under
	// 100 checks; the remaining budget expires inside the subject scan.
	ctx := &countDownCtx{Context: context.Background(), remaining: 100}
	if match, err := MatchLikePattern(ctx, tokens, value, UCA400CharWeight); !errors.Is(err, context.Canceled) {
		t.Errorf("mid-search cancellation = match %v, err %v, want context.Canceled", match, err)
	}
	if match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight); err != nil || match {
		t.Errorf("the contains pattern must not match an all-'a' subject on a live context: %v %v", match, err)
	}
}

// A '%literal%' search must scan the subject once, not retry the whole
// literal run at every subject position: the pre-repair matcher expanded
// the '%' into 60,000 candidate positions and re-compared the 30,001
// literal characters from each (quadratic work, issue #31175 review), so
// the budget below expired long before the scan finished there, while a
// single leftmost KMP pass finishes well inside it.
func TestMatchLikePatternContainsBoundedWork(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("a", 30000)+"b%"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	// Subject weighing plus one KMP pass cost ~150 ctx checks; a matcher
	// retrying the literal from every position needs hundreds of thousands.
	ctx := &countDownCtx{Context: context.Background(), remaining: 1000}
	match, err := MatchLikePattern(ctx, tokens, value, UCA400CharWeight)
	if err != nil {
		t.Fatalf("a single leftmost scan must finish inside the budget: %v", err)
	}
	if match {
		t.Error("an all-'a' subject must not match '%a...ab%'")
	}
}

// A segment containing '_' between two '%' wildcards must also match in
// one pass (bit-parallel Shift-And), not by shifting a live-position set
// through the whole subject once per pattern token (issue #31175 review).
func TestMatchLikePatternMultiSegmentBoundedWork(t *testing.T) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("_a", 15000)+"%"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	// Subject weighing plus one Shift-And pass cost ~120 ctx checks; the
	// live-set matcher needs tens of checks per one of its 30,000 tokens.
	ctx := &countDownCtx{Context: context.Background(), remaining: 2000}
	match, err := MatchLikePattern(ctx, tokens, value, UCA400CharWeight)
	if err != nil {
		t.Fatalf("one Shift-And pass must finish inside the budget: %v", err)
	}
	if !match {
		t.Error("the all-'a' subject must match '%(_a)x15000%'")
	}
}

// Shapes with two or more '%' wildcards: the leftmost greedy segment
// search must return exactly the general LIKE answer, including anchored
// leading/trailing segments, repeated overlapping segments, '_'-only
// segments and collation-weight comparisons inside segments (issue #31175
// review).
func TestMatchLikePatternMultiPercentShapes(t *testing.T) {
	runLikeWeightCases(t, "uca400", UCA400CharWeight, []likeWeightCase{
		// three anchored segments with two interior gaps
		{"abXXcdYYef", "ab%cd%ef", true},
		{"abXXef", "ab%cd%ef", false},       // missing middle segment
		{"XXabXXcdXXef", "ab%cd%ef", false}, // leading segment anchored at 0
		{"abXXcdXXefYY", "ab%cd%ef", false}, // trailing segment anchored at the end
		{"abef", "ab%cd%ef", false},
		// repeated segments with overlapping occurrences
		{"aaa", "%a%a%", true},
		{"aa", "%a%a%", true},
		{"ba", "%a%a%", false},
		{"aaa", "%aa%a%", true},
		{"aaaa", "%aa%aa%", true},
		{"aaa", "%aa%aa%", false},
		// '_' inside segments
		{"xxabYczzdww", "%ab_c%d%", true},
		{"xxabYczz", "%ab_c%d%", false},
		{"abYczzd", "ab_c%d", true},
		{"xabYczzd", "ab_c%d", false},
		// the total fixed width must fit the subject
		{"abcde", "abc%def%", false},
		{"abcdef", "abc%def%", true},
		// segments of only '_' wildcards consume their width anywhere
		{"xyz", "%__%", true},
		{"x", "%__%", false},
		// collation weights decide inside every segment
		{"xÁy", "%a%y%", true}, // Á and a share the UCA 4.0.0 primary weight
		{"xıy", "%i%y%", false},
		{"xΣy", "%σ%y%", true},
	})
}

// weighLikeSubject decodes value into per-character weight streams the way
// MatchLikePattern does, for the naive reference matcher.
func weighLikeSubject(charWeight CharWeightFunc, value string) [][]uint32 {
	var chars [][]uint32
	for rest := []byte(value); len(rest) > 0; {
		var weights []uint32
		var size int
		weights, size = charWeight(weights, rest)
		chars = append(chars, weights)
		rest = rest[size:]
	}
	return chars
}

// naiveLikeMatch is the exhaustive reference matcher: try every split at
// every '%', memoized on (token, subject) position pairs.
func naiveLikeMatch(tokens []LikeToken, chars [][]uint32) bool {
	memo := make(map[[2]int]bool)
	var match func(ti, ci int) bool
	match = func(ti, ci int) bool {
		if ti == len(tokens) {
			return ci == len(chars)
		}
		key := [2]int{ti, ci}
		if v, ok := memo[key]; ok {
			return v
		}
		token := tokens[ti]
		var result bool
		switch token.Kind {
		case LikeTokenAnySeq:
			for k := ci; k <= len(chars); k++ {
				if match(ti+1, k) {
					result = true
					break
				}
			}
		case LikeTokenAnyOne:
			result = ci < len(chars) && match(ti+1, ci+1)
		default:
			width := len(token.Chars)
			if ci+width <= len(chars) {
				eq, err := likeCharsEqual(&likeSearchBudget{ctx: context.Background()}, token.Chars, chars[ci:ci+width])
				if err != nil {
					panic(err)
				}
				result = eq && match(ti+1, ci+width)
			}
		}
		memo[key] = result
		return result
	}
	return match(0, 0)
}

// The greedy segment matcher must agree with the exhaustive reference on
// randomized patterns and subjects over an alphabet full of case pairs and
// fold-equivalent characters, under both weight functions (issue #31175
// review).
func TestMatchLikePatternSegmentGreedyDifferential(t *testing.T) {
	rng := rand.New(rand.NewSource(31176))
	alphabet := []string{"a", "A", "b", "B", "á", "ß", "x"}
	weightFuncs := []struct {
		name string
		fn   CharWeightFunc
	}{
		{"uca400", UCA400CharWeight},
		{"general_ci", GeneralCICharWeight},
	}
	for _, cw := range weightFuncs {
		for iter := 0; iter < 10000; iter++ {
			var pattern, subject strings.Builder
			for tokens := 1 + rng.Intn(6); tokens > 0; tokens-- {
				switch r := rng.Intn(100); {
				case r < 30:
					pattern.WriteByte('%')
				case r < 45:
					pattern.WriteByte('_')
				default:
					for k := 1 + rng.Intn(3); k > 0; k-- {
						pattern.WriteString(alphabet[rng.Intn(len(alphabet))])
					}
				}
			}
			for k := rng.Intn(11); k > 0; k-- {
				subject.WriteString(alphabet[rng.Intn(len(alphabet))])
			}
			tokens, err := TokenizeLikePattern(context.Background(), []byte(pattern.String()), '\\', false, cw.fn)
			if err != nil {
				t.Fatalf("%s iter %d: tokenize %q: %v", cw.name, iter, pattern.String(), err)
			}
			got, err := MatchLikePattern(context.Background(), tokens, []byte(subject.String()), cw.fn)
			if err != nil {
				t.Fatalf("%s iter %d: match %q LIKE %q: %v", cw.name, iter, subject.String(), pattern.String(), err)
			}
			want := naiveLikeMatch(tokens, weighLikeSubject(cw.fn, subject.String()))
			if got != want {
				t.Fatalf("%s iter %d: %q LIKE %q = %v, naive reference = %v",
					cw.name, iter, subject.String(), pattern.String(), got, want)
			}
		}
	}
}

// BenchmarkMatchLikePatternContains measures '%' + 30,000 literal
// characters + 'b%' over a 60,000-character subject: one leftmost KMP pass
// instead of one literal retry per subject position (900,030,000 character
// comparisons for one false result on the pre-repair matcher, issue #31175
// review).
func BenchmarkMatchLikePatternContains(b *testing.B) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("a", 30000)+"b%"), '\\', true, UCA400CharWeight)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight)
		if err != nil {
			b.Fatal(err)
		}
		if match {
			b.Fatal("an all-'a' subject must not match the contains pattern")
		}
	}
}

// BenchmarkMatchLikePatternMultiSegment measures a 30,000-wide '_a'
// segment between two '%' wildcards over a 60,000-character subject: one
// bit-parallel Shift-And pass instead of a live-position set shifted
// through the whole subject per pattern token (issue #31175 review).
func BenchmarkMatchLikePatternMultiSegment(b *testing.B) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("_a", 15000)+"%"), '\\', true, UCA400CharWeight)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight)
		if err != nil {
			b.Fatal(err)
		}
		if !match {
			b.Fatal("the all-'a' subject must match the multi-segment pattern")
		}
	}
}

// BenchmarkMatchLikePatternAnchoredSuffix measures a 60,000-character
// subject against '%' + 30,000 literal characters + 'b': the literal can
// only match at the subject end, so the matcher must not retry it at every
// wildcard position (900,030,000 character comparisons for one false
// result) (issue #31175 review).
func BenchmarkMatchLikePatternAnchoredSuffix(b *testing.B) {
	value := bytes.Repeat([]byte("a"), 60000)
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%"+strings.Repeat("a", 30000)+"b"), '\\', true, UCA400CharWeight)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		match, err := MatchLikePattern(context.Background(), tokens, value, UCA400CharWeight)
		if err != nil {
			b.Fatal(err)
		}
		if match {
			b.Fatal("the anchored suffix must not match an all-'a' subject")
		}
	}
}
