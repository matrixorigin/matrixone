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

package regexpclass

import (
	"fmt"
	"strings"
	"testing"
)

// TestScanPosixPairClassCardsR6 pins review round 6 finding 1 at the
// event layer: a POSIX class member must keep its polarity so a named
// class paired with its own complement in the same body ([[:alpha:]]
// with [[:^alpha:]]) proves full coverage, exactly like \s with \S. The
// vendored fork honors the caret in production RE2 mode
// (internal/regexp2/syntax/parser.go:1545-1563 cc.addNamedASCII(nm,
// negate)), and Go's RE2 supports the [:^name:] form natively, so the
// negated union [^[:alpha:][:^alpha:]] matches nothing on both engines.
func TestScanPosixPairClassCardsR6(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    []string
		note    string
	}{
		// Pre-fix every one of these is card=2 (the caret hid inside the
		// atom name, so the pair never resolved).
		{`[^[:alpha:][:^alpha:]]`, []string{"class-open", "class-close card=0", "end"}, "review example"},
		{`[^[:word:][:^word:]]`, []string{"class-open", "class-close card=0", "end"}, "second POSIX name"},
		{`[^[:digit:][:^digit:]]`, []string{"class-open", "class-close card=0", "end"}, "third POSIX name"},
		{`(?i)[^[:alpha:][:^alpha:]]`, []string{"flags", "class-open", "class-close card=0", "end"}, "fold keeps the empty complement empty"},
	}
	for _, r := range discriminating {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			events := DebugEvents(r.pattern, dialect)
			if len(events) != len(r.want) {
				t.Errorf("pattern %q (%v): got %d events, want %d: %v (%s)", r.pattern, dialect, len(events), len(r.want), events, r.note)
				continue
			}
			for i, w := range r.want {
				if got := shortEvent(events[i]); got != w {
					t.Errorf("pattern %q (%v) event %d: got %q want %q (all: %v; %s)", r.pattern, dialect, i, got, w, events, r.note)
					break
				}
			}
		}
	}
	// Guards: identical pre and post.
	guards := []struct {
		pattern string
		want    string
		note    string
	}{
		{`[[:alpha:][:^alpha:]]`, "class-close card=2", "non-negated full union stays live either way"},
		{`[[:alpha:]]`, "class-close card=2", "lone POSIX stays conservative-live"},
		{`[[:^alpha:]]`, "class-close card=2", "lone negated POSIX stays conservative-live"},
		{`[^[:alpha:]]`, "class-close card=2", "lone negated member of the class stays conservative-live"},
		{`[^\s\S[:^alpha:]]`, "class-close card=0", "round-5 pin: shorthand pair still resolves with a POSIX member present"},
		{`[^[:alpha:]\S]`, "class-close card=2", "POSIX paired with a shorthand from another family never pairs"},
	}
	for _, r := range guards {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			events := DebugEvents(r.pattern, dialect)
			if len(events) != 3 || shortEvent(events[1]) != r.want {
				t.Errorf("guard pattern %q (%v): got %v, want %q (%s)", r.pattern, dialect, events, r.want, r.note)
			}
		}
	}
}

// TestScanSubtractionNegationR6 pins review round 6 finding 2 at the
// event layer: the fork applies a class's complement BEFORE the
// subtraction (internal/regexp2/syntax/charclass.go:281-288 negates the
// whole set in CharIn before recursing into c.sub), so [^a-[\s\S]] is
// (~a) minus everything, which is empty; and a nested subtraction class
// recognizes a leading '^' recursively (parser.go scanCharSet handles
// the caret at every level), so [a-[^a]] is {a} minus ~{a} = {a}.
// Subtraction is regexp2-dialect only.
func TestScanSubtractionNegationR6(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    string
		note    string
	}{
		// Pre-fix the negation was applied after (or never), leaving the
		// wrong card on every one of these.
		{`[^a-[\s\S]]`, "class-close card=0", "review example: (~a) minus everything is empty"},
		{`[^ab-[\s\S]]`, "class-close card=0", "two-rune minuend under outer negation"},
		{`[^ab-[\d\D]]`, "class-close card=0", "property-pair full subtrahend under outer negation"},
		{`[a-[^a]]`, "class-close card=1 U+0061", "review example: {a} minus ~{a} keeps a"},
		{`[ab-[^ab]]`, "class-close card=2", "{a,b} minus ~{a,b} keeps both"},
		{`[a-[^b]]`, "class-close card=0", "{a} minus ~{b} is empty"},
		{`[a-[^\s\S]]`, "class-close card=1 U+0061", "subtrahend ~full is empty, minuend survives"},
	}
	for _, r := range discriminating {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) != 3 || shortEvent(events[1]) != r.want {
			t.Errorf("pattern %q: got %v, want %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
	// Guards: identical pre and post.
	guards := []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		{`[^a-[b]]`, DialectRegexp2, "class-close card=2", "negated then subtracted stays live ~{a,b} either way"},
		{`[a-[b]]`, DialectRegexp2, "class-close card=1 U+0061", "round-5 pin: minuend retention unchanged"},
		{`[a-[\s\S]]`, DialectRegexp2, "class-close card=0", "round-4 pin: full-coverage operand empties the class"},
		{`[a-[\d]]`, DialectRegexp2, "class-close card=1 U+0061", "round-4 pin updated by round 7: exact [0-9] operand leaves the singleton {a}"},
		{`[a-[^[:alpha:]]]`, DialectRegexp2, "class-close card=1 U+0061", "round 7 resolves [[:alpha:]] exactly; ~alpha excludes a, so the singleton {a} survives"},
		{`[b-[ab]]`, DialectRegexp2, "class-close card=0", "round-5 guard: operand covers the whole class"},
	}
	for _, r := range guards {
		events := DebugEvents(r.pattern, r.dialect)
		if len(events) != 3 || shortEvent(events[1]) != r.want {
			t.Errorf("guard pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
	// RE2 dialect: identical pre and post. The engine rejects the
	// reversed range; the classifier answers conservatively and the
	// class closes at the inner ']', leaving the outer one a literal.
	events := DebugEvents(`[a-[^a]]`, DialectRE2)
	want := []string{"class-open", "class-close card=2", "literal U+005D", "end"}
	if len(events) != len(want) {
		t.Errorf("RE2-dialect guard: got %v, want %d events", events, len(want))
	} else {
		for i, w := range want {
			if got := shortEvent(events[i]); got != w {
				t.Errorf("RE2-dialect guard event %d: got %q want %q (all: %v)", i, got, w, events)
				break
			}
		}
	}
}

// TestClassifyPosixPairDescriptorR6 pins the descriptor consequences of
// review round 6 finding 1: [^[:alpha:][:^alpha:]] matches nothing
// (ICU 77.1 probes rev15 R6A1/R6A3/R6A12: true [2,4) on aCRLFxb under
// m), so the \nx arm keeps its exempt shape on both routes.
func TestClassifyPosixPairDescriptorR6(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRE2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: ^\\nx row = %+v, want {true true}", want)
	}
	rows := []string{
		`(?m)^(?:[^[:alpha:][:^alpha:]]|\nx)`,
		`(?m)^(?:[^[:word:][:^word:]]|\nx)`,
		`(?m)^(?:[^[:alpha:][:^alpha:]])*\nx`,
		`(?m)^(?:[^[:alpha:][:^alpha:]])*?\nx`,
		`(?m)^(?:[^[:alpha:][:^alpha:]]){0,2}\nx`,
	}
	for _, pattern := range rows {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			got := Classify(pattern, dialect)
			if got != want {
				t.Errorf("pattern %q (%v): got %+v, want the ^\\nx row %+v", pattern, dialect, got, want)
			}
		}
	}
	// Scoped fold over the class only (probe R6A10: true [2,4)).
	foldRow := `(?m)^(?:(?i:[^[:alpha:][:^alpha:]])|\nx)`
	for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
		if got := Classify(foldRow, dialect); got != want {
			t.Errorf("scoped-fold row (%v): got %+v, want the ^\\nx row %+v", dialect, got, want)
		}
	}
	// Guards that must stay masked before and after (ICU R6A7/R6A8/R6A9
	// and R6A11 false or live rows): a mandatory repetition of the empty
	// class, the single negated member, and the non-negated pair are all
	// live consuming content (or a dead head).
	for _, pattern := range []string{
		`(?m)^(?:[^[:alpha:][:^alpha:]])+\nx`,
		`(?m)^(?:[^[:alpha:]]|\nx)`,
		`(?m)^(?:[[:alpha:][:^alpha:]]|\nx)`,
		`(?m)^[^[:alpha:][:^alpha:]]x`,
	} {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			got := Classify(pattern, dialect)
			if !got.LineAnchoredStart || got.StringExempt {
				t.Errorf("guard pattern %q (%v): got %+v, want the masked row {line=true exempt=false}", pattern, dialect, got)
			}
		}
	}
}

// TestClassifySubtractionNegationDescriptorR6 pins the descriptor
// consequences of review round 6 finding 2: with the complement applied
// before the subtraction, [^a-[\s\S]] is empty and its alternation
// classifies exactly like ^\nx (ICU 77.1 probes rev15 R6B1/R6B8: true
// [2,4) on zCRLFxy under m); with the nested negation recognized,
// [a-[^a]] is the live singleton {a} and the optional form classifies
// exactly like ^(?:a)?\nx (ICU R6B6/R6C1: false on zCRLFxy under m).
// Subtraction is regexp2-dialect only.
func TestClassifySubtractionNegationDescriptorR6(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRegexp2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: ^\\nx row = %+v, want {true true}", want)
	}
	for _, pattern := range []string{
		`(?m)^(?:[^a-[\s\S]]|\nx)(?=)`,
		`(?m)^(?:[^a-[\s\S]])*\nx(?=)`,
		`(?m)^(?:[^ab-[\d\D]]|\nx)(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != want {
			t.Errorf("pattern %q: got %+v, want the ^\\nx row %+v", pattern, got, want)
		}
	}
	// The optional live-singleton form must equal the literal-a row.
	liveWant := Classify(`(?m)^(?:a)?\nx(?=)`, DialectRegexp2)
	if !liveWant.LineAnchoredStart || liveWant.StringExempt {
		t.Fatalf("sanity: ^(?:a)?\\nx(?=) row = %+v, want the masked row", liveWant)
	}
	got := Classify(`(?m)^(?:[a-[^a]])?\nx(?=)`, DialectRegexp2)
	if got != liveWant {
		t.Errorf("optional singleton row: got %+v, want the ^(?:a)?\\nx(?=) row %+v", got, liveWant)
	}
	// Guards: mandatory repetition and plain lived subtractions stay
	// masked before and after (ICU R6B9 false, R6B7 live).
	for _, pattern := range []string{
		`(?m)^(?:[^a-[\s\S]])+\nx(?=)`,
		`(?m)^(?:[^a-[b]]|\nx)(?=)`,
		`(?m)^(?:[a-[b]]|\nx)(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q: got %+v, want the masked row {line=true exempt=false}", pattern, got)
		}
	}
}

// TestStreamingEquivalenceRound6 folds the round-6 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound6(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:[^[:alpha:][:^alpha:]]|\nx)`, DialectRE2},
		{`(?m)^(?:[^[:alpha:][:^alpha:]]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^[:alpha:][:^alpha:]])*\nx`, DialectRE2},
		{`(?m)^(?:[^a-[\s\S]]|\nx)(?=)`, DialectRegexp2},
		{`(?m)^(?:[a-[^a]])?\nx(?=)`, DialectRegexp2},
		{`[^a-[\s\S]]`, DialectRegexp2},
		{`[ab-[^ab]]`, DialectRegexp2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// BenchmarkClassifyPosixPairScalingR6 measures repeated POSIX complement
// pairs (review round 6, finding 1). Polarity pairing is a map lookup
// per atom, so an uncached Classify stays linear in the pattern size.
func BenchmarkClassifyPosixPairScalingR6(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^[:alpha:][:^alpha:]]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// BenchmarkClassifyNegatedSubtractionScalingR6 measures repeated
// negations through regexp2 set subtraction (review round 6, finding 2):
// complement-before-subtract at the top level and recursive negation in
// the nested operand. Both use the same linear gap arithmetic, so an
// uncached Classify stays linear in the pattern size.
func BenchmarkClassifyNegatedSubtractionScalingR6(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^a-[\s\S]][a-[^b]]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}
