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

// TestScanMixedCoverageCardsR7 pins review round 7 finding 1 at the
// event layer: membership of the ASCII shorthand classes (\d, \s, \w),
// the POSIX named classes and the Unicode properties is exact on both
// production routes (the RE2 route parses with regexp/syntax.Perl; the
// fork compiles with regexp2.RE2 at regexp_icu.go:1463/2086 and its
// tables live at internal/regexp2/syntax/charclass.go:393-590), so a
// negated union that provably covers every rune - via complement pairs
// across families ([[^[:digit:]] with \D), subset relationships
// ([[:space:]] with \S, [[:alpha:]] with [[:^lower:]]) or De Morgan
// complements of disjoint/subsuming properties - resolves to CardEmpty
// instead of conservatively masking the parallel \nx arm.
func TestScanMixedCoverageCardsR7(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    string
		note    string
	}{
		// Pre-fix every one of these is card=2 (the POSIX/shorthand
		// family split never resolved mixed combinations).
		{`[^[:digit:]\D]`, "class-close card=0", "review example: digit union D covers all"},
		{`[^[:word:]\W]`, "class-close card=0", "word union W covers all"},
		{`[^[:space:]\S]`, "class-close card=0", "space superset of s: subset relationship"},
		{`[^[:alnum:][:^alpha:]]`, "class-close card=0", "alnum superset of alpha: subset relationship"},
		{`[^[:alpha:][:^lower:]]`, "class-close card=0", "alpha superset of lower"},
		{`[^\p{L}\P{Lu}]`, "class-close card=0", "L superset of Lu across complements"},
		{`[^\P{Lu}\P{Ll}]`, "class-close card=0", "De Morgan complements of disjoint categories"},
		{`[^\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}\P{L}]`, "class-close card=0", "letter partition plus complement"},
	}
	for _, r := range discriminating {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			events := DebugEvents(r.pattern, dialect)
			if len(events) != 3 || shortEvent(events[1]) != r.want {
				t.Errorf("pattern %q (%v): got %v, want %q (%s)", r.pattern, dialect, events, r.want, r.note)
			}
		}
	}
	// Guards: identical pre and post.
	guards := []struct {
		pattern string
		want    string
		note    string
	}{
		{`[^[:digit:]]`, "class-close card=2", "lone negated POSIX stays live"},
		{`[[:digit:]\D]`, "class-close card=2", "non-negated full union stays live"},
		{`[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}]`, "class-close card=2", "L unions its own partition: live ~L (ICU rev16b R7A13)"},
		{`[^\p{Greek}\P{Lu}]`, "class-close card=2", "Greek intersect Lu nonempty: live Lu minus Greek"},
		{`[^\p{L}\S]`, "class-close card=2", `complement is \s minus L = \s, live (ICU rev16 R7-prop-adjusted)`},
	}
	for _, r := range guards {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			events := DebugEvents(r.pattern, dialect)
			if len(events) != 3 || shortEvent(events[1]) != r.want {
				t.Errorf("guard pattern %q (%v): got %v, want %q (%s)", r.pattern, dialect, events, r.want, r.note)
			}
		}
	}
	// Dialect-split guard: [^\s[:^space:]] is live on both engines but
	// the exact residual is the space members outside \s. RE2's
	// [[:space:]] adds only VT beside \s (Go perl_groups code14 versus
	// code2), so the complement is the singleton U+000B; the fork's
	// ECMAScript space list adds NBSP and friends, staying many.
	for _, r := range []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		{`[^\s[:^space:]]`, DialectRE2, "class-close card=1 U+000B", "RE2 space owns VT alone beside \\s"},
		{`[^\s[:^space:]]`, DialectRegexp2, "class-close card=2", "ECMA space adds NBSP et al beside \\s"},
	} {
		events := DebugEvents(r.pattern, r.dialect)
		if len(events) != 3 || shortEvent(events[1]) != r.want {
			t.Errorf("guard pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
}

// TestScanFoldSubtractionCardsR7 pins review round 7 finding 2 at the
// event layer: under case folding the engines fold each operand BEFORE
// any complement or subtraction applies (the fork lowercases the
// completed operand charset ahead of membership negation -
// internal/regexp2/syntax/parser.go:1623-1627 cc.addLowercase() at
// charclass.go:650 - and Go's RE2 expands classes by fold orbit during
// parse), so (?i)[a-[^A]] is {a,A} minus ~{a,A} = the live {a,A},
// never the raw-order empty set. Subtraction is regexp2-dialect only.
func TestScanFoldSubtractionCardsR7(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    string
		note    string
	}{
		// Pre-fix every folded row here gets the raw-interval answer.
		{`(?i)[a-[^A]]`, "class-close card=2", "review example: folded operands leave {a,A} live"},
		{`(?i)[A-[^a]]`, "class-close card=2", "other orientation"},
		{`(?i)[k-[^k]]`, "class-close card=2", "fold orbit includes U+212A"},
		{`(?i)[z-[^Z]]`, "class-close card=2", "letter at the range edge"},
		{`(?i)[ab-[^AB]]`, "class-close card=2", "two-rune minuend under fold"},
		{`(?i)[a-[\d]]`, "class-close card=2", "folded minuend minus digits stays {a,A}"},
		{`(?i)[0-[^0]]`, "class-close card=1 U+0030", "fold-stable singleton survives"},
		{`(?i)[a-[^b]]`, "class-close card=0", "folded {a,A} minus ~{b,B} is empty"},
		{`(?i)[^a-[b]]`, "class-close card=2", "complement first: ~{a,A} minus {b,B} stays live"},
	}
	for _, r := range discriminating {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) < 3 || shortEvent(events[len(events)-2]) != r.want {
			t.Errorf("pattern %q: got %v, want close %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
	// Guards: identical pre and post (no fold, or round-6 semantics).
	guards := []struct {
		pattern string
		want    string
		note    string
	}{
		{`[a-[^A]]`, "class-close card=0", "unfolded {a} minus ~{A} stays empty"},
		{`[a-[^a]]`, "class-close card=1 U+0061", "round-6 pin"},
		{`[^a-[\s\S]]`, "class-close card=0", "round-6 pin"},
		{`[a-[^b]]`, "class-close card=0", "round-6 pin"},
		{`[ab-[^ab]]`, "class-close card=2", "round-6 pin"},
		{`[a-[^\s\S]]`, "class-close card=1 U+0061", "round-6 pin"},
	}
	for _, r := range guards {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) != 3 || shortEvent(events[1]) != r.want {
			t.Errorf("guard pattern %q: got %v, want %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
}

// TestClassifyMixedCoverageDescriptorR7 pins finding 1 at the descriptor
// level: the empty mixed-coverage classes behave exactly like the ^\nx
// row (the alternation grants the string exemption), while genuinely
// live mixed classes keep the masked row pre and post.
func TestClassifyMixedCoverageDescriptorR7(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRE2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: (?m)^\\nx row = %+v, want {true true}", want)
	}
	for _, tc := range []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:[^[:digit:]\D]|\nx)`, DialectRE2},
		{`(?m)^(?:[^[:digit:]\D]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^[:word:]\W]|\nx)`, DialectRE2},
		{`(?m)^(?:[^[:space:]\S]|\nx)`, DialectRE2},
		{`(?m)^(?:[^[:alnum:][:^alpha:]]|\nx)`, DialectRE2},
		{`(?m)^(?:[^\p{L}\P{Lu}]|\nx)`, DialectRE2},
		{`(?m)^(?:[^\P{Lu}\P{Ll}]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^[:digit:]\D])*\nx`, DialectRE2},
		{`(?m)^(?:[^[:digit:]\D])??\nx`, DialectRE2},
		{`(?m)^(?:[^[:digit:]\D]){0,2}\nx`, DialectRE2},
	} {
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): got %+v, want the ^\\nx row %+v", tc.pattern, tc.dialect, got, want)
		}
	}
	// Masked guards, identical pre and post: mandatory repetition, dead
	// head, and the genuinely live ~word arm (ICU rev16b R7A17).
	for _, tc := range []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:[^[:digit:]\D])+\nx`, DialectRE2},
		{`(?m)^[^[:digit:]\D]x`, DialectRE2},
		{`(?m)^(?:[^[:digit:]\w]|\nx)`, DialectRE2},
		{`(?m)^(?:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}]|\nx)`, DialectRE2},
	} {
		got := Classify(tc.pattern, tc.dialect)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q (%v): got %+v, want the masked row", tc.pattern, tc.dialect, got)
		}
	}
}

// TestClassifyFoldSubtractionDescriptorR7 pins finding 2 at the
// descriptor level: the folded live class (?i:[a-[^A]]) must classify
// exactly like the literal live pair (?:[ab])? - the masked row that
// seals the CRLF interior - and never like the dead optional unit that
// grants the string exemption.
func TestClassifyFoldSubtractionDescriptorR7(t *testing.T) {
	liveWant := Classify(`(?m)^(?:[ab])?\nx(?=)`, DialectRegexp2)
	if !liveWant.LineAnchoredStart || liveWant.StringExempt {
		t.Fatalf("sanity: (?m)^(?:[ab])?\\nx(?=) row = %+v, want the masked row", liveWant)
	}
	for _, pattern := range []string{
		`(?m)^(?i:[a-[^A]])?\nx(?=)`,
		`(?m)^(?i:[A-[^a]])?\nx(?=)`,
		`(?m)^(?i:[k-[^k]])?\nx(?=)`,
		`(?m)^(?i:[a-[\d]])?\nx(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != liveWant {
			t.Errorf("pattern %q: got %+v, want the live-optional row %+v", pattern, got, liveWant)
		}
	}
	// The unfolded dead optional keeps the exempt row pre and post.
	exempt := Classify(`(?m)^(?:[a-[a]])?\nx(?=)`, DialectRegexp2)
	got := Classify(`(?m)^(?:[a-[^A]])?\nx(?=)`, DialectRegexp2)
	if got != exempt {
		t.Errorf("unfolded guard: got %+v, want the exempt row %+v", got, exempt)
	}
}

// TestStreamingEquivalenceRound7 folds the round-7 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound7(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:[^[:digit:]\D]|\nx)`, DialectRE2},
		{`(?m)^(?:[^[:word:]\W]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^\p{L}\P{Lu}]|\nx)`, DialectRE2},
		{`(?m)^(?i:[a-[^A]])?\nx(?=)`, DialectRegexp2},
		{`(?m)^(?i:[a-[\d]])?\nx(?=)`, DialectRegexp2},
		{`(?i)[^a-[b]]`, DialectRegexp2},
		{`[^[:space:]\S]`, DialectRE2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// BenchmarkClassifyMixedClassScalingR7 measures repeated mixed
// POSIX/shorthand/property classes (review round 7, finding 1). Member
// tables resolve by map lookup plus interval arithmetic, so an uncached
// Classify stays linear in the pattern size.
func BenchmarkClassifyMixedClassScalingR7(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^[:digit:]\D][^[:space:]\S]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// BenchmarkClassifyFoldSubtractionScalingR7 measures repeated folded
// nested subtractions (review round 7, finding 2): per-member fold
// closure ahead of complement and subtraction. The closure of a short
// member adds O(orbit) runes, so an uncached Classify stays linear.
func BenchmarkClassifyFoldSubtractionScalingR7(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)(?i)" + strings.Repeat(`[a-[^A]][b-[^c]]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}
