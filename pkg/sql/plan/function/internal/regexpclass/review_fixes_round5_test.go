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

// TestScanMixedCoverageClassCardsR5 pins review round 5 finding 1 at the
// event layer: a class whose intervals or atoms already prove full rune
// coverage must resolve through the interval arithmetic even when another
// member is unresolved (a shorthand, property or POSIX class), because
// unioning into full coverage cannot shrink it. A POSIX member is a plain
// unresolved atom now instead of short-circuiting the whole class.
func TestScanMixedCoverageClassCardsR5(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
		note    string
	}{
		// Discriminating rows: pre-fix every one of these is card=2.
		{`[^\x00-\x{10ffff}\d]`, []string{"class-open", "class-close card=0", "end"}, "full interval plus shorthand"},
		{`[^\x00-\x{10ffff}\w]`, []string{"class-open", "class-close card=0", "end"}, "full interval plus word shorthand"},
		{`[^\x00-\x{10ffff}\p{Lu}]`, []string{"class-open", "class-close card=0", "end"}, "full interval plus property"},
		{`[^\s\S[:alpha:]]`, []string{"class-open", "class-close card=0", "end"}, "full pair plus POSIX (review example)"},
		{`[^\x00-\x{10ffff}[:alpha:]]`, []string{"class-open", "class-close card=0", "end"}, "full interval plus POSIX"},
		{`[^\s\S[:^alpha:]]`, []string{"class-open", "class-close card=0", "end"}, "POSIX negation never pairs"},
		{`(?i)[^\s\S[:alpha:]]`, []string{"flags", "class-open", "class-close card=0", "end"}, "fold keeps the empty complement empty"},
		// Guards: identical pre and post (the conservative-live answers).
		{`[\x00-\x{10ffff}\d]`, []string{"class-open", "class-close card=2", "end"}, "non-negated full interval plus atom"},
		{`[\s\S[:alpha:]]`, []string{"class-open", "class-close card=2", "end"}, "non-negated full pair plus POSIX"},
		{`[[:alpha:]]`, []string{"class-open", "class-close card=2", "end"}, "lone POSIX stays conservative-live"},
		{`[^[:alpha:]]`, []string{"class-open", "class-close card=2", "end"}, "lone negated POSIX stays conservative-live"},
		{`[\d[:alpha:]]`, []string{"class-open", "class-close card=2", "end"}, "unresolved shorthand plus POSIX"},
		{`[^\x00-\x42\x44-\x{10ffff}\d]`, []string{"class-open", "class-close card=1 U+0043", "end"}, "gapped interval plus shorthand: round 7 resolves \\d exactly and [0-9] is inside the gap-free span, so the complement is the singleton U+0043"},
	}
	for _, r := range rows {
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
}

// TestScanSubtractionMinuendR5 pins review round 5 finding 2 at the event
// layer: a regexp2 '-[...]' set subtraction must keep the just-scanned
// member before subtracting, exactly like the vendored fork
// (internal/regexp2/syntax/parser.go retains chPrev before scanning the
// nested class). Both scanners (top-level and nested) keep the member.
func TestScanSubtractionMinuendR5(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    string
		note    string
	}{
		// Pre-fix every one of these lost the pending member.
		{`[a-[b]]`, "class-close card=1 U+0061", "review example: {a} minus {b} keeps a"},
		{`[a-[b-[c]]]`, "class-close card=1 U+0061", "nested: [b-[c]]={b}, then {a}-{b}"},
		{`[z-[x-[y]]]`, "class-close card=1 U+007A", "nested: keeps z"},
		{`[ab-[cd]]`, "class-close card=2", "pending b survives subtraction of {c,d}"},
		{`[abc-[b]]`, "class-close card=2", "pending c survives: {a,b,c}-{b}={a,c}"},
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
		want    string
		note    string
	}{
		{`[a-[a]]`, "class-close card=0", "empty result either way"},
		{`[b-[ab]]`, "class-close card=0", "operand covers the whole class"},
		{`[a-[\s\S]]`, "class-close card=0", "round-4 pin: full-coverage operand empties the class"},
		{`[a-[\d]]`, "class-close card=1 U+0061", "round-4 pin updated by round 7: exact [0-9] operand leaves the singleton {a}"},
	}
	for _, r := range guards {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) != 3 || shortEvent(events[1]) != r.want {
			t.Errorf("guard pattern %q: got %v, want %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
}

// TestClassifyMixedCoverageDescriptorR5 pins the descriptor consequences
// of review round 5 finding 1: an empty first arm built from a full
// interval (or full atom pair) plus an unresolved member matches nothing,
// so the \nx arm keeps its exempt shape (ICU 77.1 probes R5A1..R5A13,
// R5C3: true [2,4) on aCRLFxb under m).
func TestClassifyMixedCoverageDescriptorR5(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRE2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: ^\\nx row = %+v, want {true true}", want)
	}
	rows := []string{
		`(?m)^(?:[^\x00-\x{10ffff}\d]|\nx)`,
		`(?m)^(?:[^\x00-\x{10ffff}\p{Lu}]|\nx)`,
		`(?m)^(?:[^\s\S[:alpha:]]|\nx)`,
		`(?m)^(?:[^\x00-\x{10ffff}[:alpha:]]|\nx)`,
		`(?m)^(?:[^\x00-\x{10ffff}\d])*\nx`,
		`(?m)^(?:[^\s\S[:alpha:]])*\nx`,
		`(?m)^(?:[^\s\S[:alpha:]])*?\nx`,
	}
	for _, pattern := range rows {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			got := Classify(pattern, dialect)
			if got != want {
				t.Errorf("pattern %q (%v): got %+v, want the ^\\nx row %+v", pattern, dialect, got, want)
			}
		}
	}
	// Scoped fold over the class only (probe R5C3: true [2,4)).
	foldRow := `(?m)^(?:(?i:[^\s\S[:alpha:]])|\nx)`
	for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
		if got := Classify(foldRow, dialect); got != want {
			t.Errorf("scoped-fold row (%v): got %+v, want the ^\\nx row %+v", dialect, got, want)
		}
	}
	// Guards that must stay masked before and after (ICU R5A9/R5A10/R5A11
	// false rows): mandatory repetitions of the empty class, the
	// non-negated full interval plus atom (live consuming arm), and a
	// dead-head class.
	for _, pattern := range []string{
		`(?m)^(?:[^\x00-\x{10ffff}\d])+\nx`,
		`(?m)^(?:[^\s\S[:alpha:]])+\nx`,
		`(?m)^(?:[\x00-\x{10ffff}\d]|\nx)`,
		`(?m)^[^\s\S[:alpha:]]x`,
		`(?m)^(?:[^\x00-\x42\x44-\x{10ffff}\d]|\nx)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q: got %+v, want the masked row {line=true exempt=false}", pattern, got)
		}
	}
}

// TestClassifySubtractionDescriptorR5 pins the descriptor consequences of
// review round 5 finding 2: with the minuend retained, [a-[b]] resolves
// to the live singleton {a}, so ^(?:[a-[b]]|\nx) classifies exactly like
// ^(?:a|\nx) and the CRLF-interior candidate stays masked (ICU 77.1
// probes R5B1..R5B6: false on zCRLFxy under m and mu, true [2,4) on the
// adjacent subject). Subtraction is regexp2-dialect only.
func TestClassifySubtractionDescriptorR5(t *testing.T) {
	want := Classify(`(?m)^(?:a|\nx)`, DialectRegexp2)
	if !want.LineAnchoredStart || want.StringExempt {
		t.Fatalf("sanity: ^(?:a|\\nx) row = %+v, want {true false}", want)
	}
	for _, pattern := range []string{
		`(?m)^(?:[a-[b]]|\nx)`,
		`(?m)^(?:[a-[b-[c]]]|\nx)`,
		`(?m)^(?:[ab-[cd]]|\nx)`,
		`(?m)^(?:[a-[b]])*\nx`,
		`(?m)^(?:[a-[b]])*?\nx`,
		`(?m)^(?:[a-[b]]){0,2}\nx`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != want {
			t.Errorf("pattern %q: got %+v, want the masked ^(?:a|\\nx) row %+v", pattern, got, want)
		}
	}
	// Absolute pin: the reviewer's alternation is the masked row.
	got := Classify(`(?m)^(?:[a-[b]]|\nx)(?=)`, DialectRegexp2)
	if !got.LineAnchoredStart || got.StringExempt {
		t.Errorf("(?=) form: got %+v, want the masked row {line=true exempt=false}", got)
	}
}

// TestStreamingEquivalenceRound5 folds the round-5 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound5(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:[^\x00-\x{10ffff}\d]|\nx)`, DialectRE2},
		{`(?m)^(?:[^\s\S[:alpha:]]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^\s\S[:alpha:]])*\nx`, DialectRE2},
		{`(?m)^(?:[a-[b]]|\nx)`, DialectRegexp2},
		{`(?m)^(?:[a-[b]])*\nx`, DialectRegexp2},
		{`[a-[b-[c]]]`, DialectRegexp2},
		{`[^\x00-\x{10ffff}\d]`, DialectRegexp2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// BenchmarkClassifyMixedClassScalingR5 measures repeated classes whose
// full interval coverage must survive an unresolved member (review round
// 5, finding 1). Coverage is proven from normalized intervals once, so an
// uncached Classify stays linear in the pattern size.
func BenchmarkClassifyMixedClassScalingR5(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^\x00-\x{10ffff}\d]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// BenchmarkClassifySubtractionScalingR5 measures repeated regexp2 set
// subtractions with the minuend retained (review round 5, finding 2). The
// interval difference is a single sweep, so an uncached Classify stays
// linear in the pattern size.
func BenchmarkClassifySubtractionScalingR5(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[a-[b-[c]]]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}
