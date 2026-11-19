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
	"context"
	"errors"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestMatchLikePatternAllocationMeasurement records the representative
// per-row allocation of the weighted LIKE matcher on a constant pattern
// (the independent r4 design review's P3 action): pattern tokenization is
// cached per batch, and each match weighs the subject into per-character
// weight slices — O(subject length) storage per row. The measurement is
// informational: it gives the implementation report a concrete figure for
// the hot-path-cost contract and carries no fixed byte bound.
func TestMatchLikePatternAllocationMeasurement(t *testing.T) {
	tokens, err := TokenizeLikePattern(context.Background(),
		[]byte("%straßen%fest_"), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	subject := []byte("Die Straßenfeste")
	allocs := testing.AllocsPerRun(200, func() {
		match, err := MatchLikePattern(context.Background(), tokens, subject, UCA400CharWeight)
		if err != nil || !match {
			t.Fatalf("constant-pattern match must succeed: %v %v", match, err)
		}
	})
	t.Logf("unicode_ci LIKE constant-pattern match: %.1f allocations/row for a %d-byte subject", allocs, len(subject))
}

// likeBudgetTestAlphabet holds 92 runes with pairwise distinct UCA 4.0.0
// primary weights (the same fixture the segment tests use).
const likeBudgetTestAlphabet = "abcdefghijklmnopqrstuvwxyz0123456789" +
	"αβγδεζηθικλμνξοπρστυφχψω" +
	"абвгдежзийклмнопрстуфхцчшщъыьэюя"

// likeBudgetFragmentedTokens builds the design's fragmented counterexample
// pattern (issue #31175 design, invariant 7): '%' + 15,000 'a_' pairs +
// the 92-weight alphabet interleaved with '_' + '%'. The middle is one
// 30,184-wide segment of one-character literals separated by '_' whose 93
// distinct weight vectors exceed the Shift-And mask budget, forcing the
// longest-literal anchor fallback with a one-character anchor.
func likeBudgetFragmentedTokens(t *testing.T) []LikeToken {
	t.Helper()
	if utf8.RuneCountInString(likeBudgetTestAlphabet) != 92 {
		t.Fatalf("alphabet must hold 92 runes, got %d", utf8.RuneCountInString(likeBudgetTestAlphabet))
	}
	var sb strings.Builder
	sb.WriteByte('%')
	sb.WriteString(strings.Repeat("a_", 15000))
	for _, r := range likeBudgetTestAlphabet {
		sb.WriteRune(r)
		sb.WriteByte('_')
	}
	sb.WriteByte('%')
	tokens, err := TokenizeLikePattern(context.Background(), []byte(sb.String()), '\\', true, UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	if len(tokens) != 2+15000*2+92*2 {
		t.Fatalf("unexpected tokenization: %d tokens", len(tokens))
	}
	seg := likeSegment{tokens: tokens[1 : len(tokens)-1], width: 30184}
	if shiftAndLikeSegmentFits(seg) {
		t.Fatal("the fragmented segment must exceed the Shift-And mask budget and take the anchor fallback")
	}
	return tokens
}

// TestMatchLikePatternAnchorSearchResourceLimit pins the total-work bound
// of the anchor-search fallback (issue #31175 design, invariant 7): against
// an all-'a' subject every position is a candidate for the one-character
// anchor, and each candidate verifies 30,001 positions before the alphabet
// tail rejects it, so the cumulative charged comparisons cross
// 4 × words × subjectChars and the match reports the typed bounded
// resource-limit error instead of running hundreds of millions of
// comparisons.
func TestMatchLikePatternAnchorSearchResourceLimit(t *testing.T) {
	tokens := likeBudgetFragmentedTokens(t)
	subject := strings.Repeat("a", 60000)
	_, err := MatchLikePattern(context.Background(), tokens, []byte(subject), UCA400CharWeight)
	var limitErr *LikeResourceLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("fragmented anchor search must report LikeResourceLimitError, got %v", err)
	}
	if !strings.HasPrefix(err.Error(), "LIKE pattern resource limit exceeded: total anchor-search work bound") {
		t.Fatalf("the error must keep the stable SQL-visible prefix, got %q", err.Error())
	}
	// words = ceil((30184+1)/64) = 472, so the bound is 4*472*60000.
	wantBound := int64(4) * 472 * 60000
	if limitErr.Bound != wantBound || limitErr.Width != 30184 || limitErr.SubjectChars != 60000 {
		t.Fatalf("unexpected bound fields: %+v, want bound %d width 30184 subject 60000", limitErr, wantBound)
	}
}

// TestMatchLikePatternAnchorSearchChargedCount pins the charged comparison
// count at the resource-limit error: the charge that would cross the bound
// is refused, so the observed count never exceeds the bound — the match
// aborts at or under it, never after overshooting (issue #31175 design,
// invariant 7).
func TestMatchLikePatternAnchorSearchChargedCount(t *testing.T) {
	tokens := likeBudgetFragmentedTokens(t)
	subject := strings.Repeat("a", 60000)
	chars, err := prepareLikeSubjectChars(context.Background(), []byte(subject), UCA400CharWeight)
	if err != nil {
		t.Fatal(err)
	}
	b := &likeSearchBudget{ctx: context.Background()}
	_, err = matchLikeChars(b, tokens, chars)
	var limitErr *LikeResourceLimitError
	if !errors.As(err, &limitErr) {
		t.Fatalf("expected LikeResourceLimitError, got %v", err)
	}
	wantBound := int64(4) * 472 * 60000
	if b.total > wantBound {
		t.Fatalf("charged comparisons crossed the bound: %d > %d", b.total, wantBound)
	}
	if b.total != wantBound {
		t.Fatalf("the search must abort exactly at the bound: %d, want %d", b.total, wantBound)
	}
}

// TestMatchLikePatternAnchorSearchBelowBound pins the below-bound controls:
// a subject the fragmented pattern matches at its first candidate and a
// subject presenting no anchor occurrence at all both complete inside the
// total-work bound with the exact verdict and no error.
func TestMatchLikePatternAnchorSearchBelowBound(t *testing.T) {
	tokens := likeBudgetFragmentedTokens(t)
	var middle strings.Builder
	middle.WriteString(strings.Repeat("aX", 15000))
	for _, r := range likeBudgetTestAlphabet {
		middle.WriteRune(r)
		middle.WriteByte('X')
	}
	subject := "q" + middle.String() + "z"
	match, err := MatchLikePattern(context.Background(), tokens, []byte(subject), UCA400CharWeight)
	if err != nil || !match {
		t.Errorf("the exact occurrence must match inside the bound: %v %v", match, err)
	}
	// No 'a' anywhere: the anchor never occurs, so the search scans the
	// subject once and reports no match well under the bound.
	match, err = MatchLikePattern(context.Background(), tokens, []byte(strings.Repeat("b", 60000)), UCA400CharWeight)
	if err != nil || match {
		t.Errorf("an anchor-free subject must not match: %v %v", match, err)
	}
}

// TestMatchLikeSegmentMaskTransition pins the Shift-And mask-budget
// boundary: the largest segment whose masks fit and the smallest segment
// that takes the anchor fallback give identical verdicts over the same
// subject matrix — the fallback changes the search strategy, never the
// segment semantics (issue #31175 design, invariant 7).
func TestMatchLikeSegmentMaskTransition(t *testing.T) {
	build := func(k int) ([]LikeToken, bool) {
		pat := "%" + strings.Repeat(likeBudgetTestAlphabet, k) + "_%"
		tokens, err := TokenizeLikePattern(context.Background(), []byte(pat), '\\', true, UCA400CharWeight)
		if err != nil {
			t.Fatal(err)
		}
		seg := likeSegment{tokens: tokens[1 : len(tokens)-1], width: 92*k + 1}
		return tokens, shiftAndLikeSegmentFits(seg)
	}
	// Locate the boundary: the last repetition count whose masks fit.
	fitsK, k := -1, 1
	for ; ; k++ {
		_, fits := build(k)
		if !fits {
			break
		}
		fitsK = k
	}
	if fitsK < 1 {
		t.Fatalf("no fitting segment found before %d repetitions", k)
	}
	t.Logf("Shift-And mask boundary: %d repetitions fit, %d take the anchor fallback", fitsK, k)
	under, underFits := build(fitsK)
	over, overFits := build(k)
	if !underFits || overFits {
		t.Fatalf("boundary mismatch: fitsK=%d fits=%v, k=%d fits=%v", fitsK, underFits, k, overFits)
	}
	// Each pattern width needs its own subject matrix: the exact occurrence,
	// one corrupted literal position (which every alignment covers, so no
	// candidate can verify), and a subject too short for the segment.
	for _, pc := range []struct {
		name   string
		tokens []LikeToken
		k      int
	}{
		{"Shift-And", under, fitsK},
		{"anchor fallback", over, k},
	} {
		good := "q" + strings.Repeat(likeBudgetTestAlphabet, pc.k) + "rz"
		corrupt := []rune(good)
		corrupt[1+46*pc.k] = '!'
		short := strings.Repeat(likeBudgetTestAlphabet, pc.k)
		for _, tc := range []struct {
			name    string
			subject string
			want    bool
		}{
			{"exact occurrence", good, true},
			{"corrupted literal position", string(corrupt), false},
			{"missing trailing room", short, false},
		} {
			got, err := MatchLikePattern(context.Background(), pc.tokens, []byte(tc.subject), UCA400CharWeight)
			if err != nil {
				t.Fatalf("%s %s: %v", pc.name, tc.name, err)
			}
			if got != tc.want {
				t.Errorf("%s %s: got %v, want %v", pc.name, tc.name, got, tc.want)
			}
		}
	}
}
