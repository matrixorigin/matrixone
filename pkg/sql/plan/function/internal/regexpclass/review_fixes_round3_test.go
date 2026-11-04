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

// TestScanMinusFlagGroupsR3 pins dispatch of inline flag groups that begin
// with '-' (review round 3, finding 1): (?-i), (?i-i) and scoped (?-i:...)
// are valid in both dialects and must parse through the flag scanner, with
// the caret kind following the m flag afterwards.
func TestScanMinusFlagGroupsR3(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
	}{
		{`(?m)(?-i)^\n`, []string{"flags", "flags", "anchor 0", `literal U+000A`, "end"}},
		{`(?m)(?i-i)^\n`, []string{"flags", "flags", "anchor 0", `literal U+000A`, "end"}},
		{`(?-m)^\n`, []string{"flags", "anchor 1", `literal U+000A`, "end"}},
		{`(?m)(?-m)^\n`, []string{"flags", "flags", "anchor 1", `literal U+000A`, "end"}},
		{`(?m)(?-i:^\n)x`, []string{"flags", "group-open scoped", "anchor 0", `literal U+000A`, "group-close scoped", `literal U+0078`, "end"}},
		{`x(?-i:\n)`, []string{`literal U+0078`, "group-open scoped", `literal U+000A`, "group-close scoped", "end"}},
		{`(?-im)^\n`, []string{"flags", "anchor 1", `literal U+000A`, "end"}},
		{`(?m)(?-im)^\n`, []string{"flags", "flags", "anchor 1", `literal U+000A`, "end"}},
	}
	for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
		for _, r := range rows {
			events := DebugEvents(r.pattern, dialect)
			if len(events) != len(r.want) {
				t.Errorf("pattern %q (%v): got %d events, want %d: %v", r.pattern, dialect, len(events), len(r.want), events)
				continue
			}
			for i, w := range r.want {
				if got := shortEvent(events[i]); got != w {
					t.Errorf("pattern %q (%v) event %d: got %q want %q (all: %v)", r.pattern, dialect, i, got, w, events)
					break
				}
			}
		}
	}
}

// TestClassifyMinusFlagDescriptorsR3 pins the descriptor consequences of
// round 3 finding 1: minus-led flag headers preserve the line-anchor and
// exemption analysis exactly like their flag-free or positively flagged
// forms, and a negated m header switches line anchoring off.
func TestClassifyMinusFlagDescriptorsR3(t *testing.T) {
	rows := []struct {
		pattern string
		line    bool
		exempt  bool
		note    string
	}{
		{`(?m)(?-i)^\n`, true, false, "review example: (?-i) keeps the ^\n descriptor"},
		{`(?m)(?i-i)^\n`, true, false, "i on then off is a no-op"},
		{`(?m)(?-i:^\n)x`, true, false, "scoped minus keeps the grouped-head mask"},
		{`(?m)(?s:^\n)x`, true, false, "positive scoped control is identical"},
		{`(?m)(?:^\n)x`, true, false, "plain group control is identical"},
		{`(?m)(?-i)^\nx`, true, true, "flat 2-rune run stays exempt"},
		{`(?m)(?-im)^\n`, false, false, "negated m ends line anchoring"},
		{`(?m)(?-m)^\nx`, false, true, "later text caret is not line-anchored; the 2-rune run shape is preserved"},
	}
	for _, r := range rows {
		got := Classify(r.pattern, DialectRegexp2)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q: got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)",
				r.pattern, got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note)
		}
	}
	// Pairwise identity: the minus form classifies exactly like the forms
	// the engines treat it as.
	pairs := [][2]string{
		{`(?m)(?-i)^\n`, `(?m)^\n`},
		{`(?m)(?i-i)^\n`, `(?m)^\n`},
		{`(?m)(?-i:^\n)x`, `(?m)(?s:^\n)x`},
		{`(?m)(?-i:^\n)x`, `(?m)(?:^\n)x`},
		{`(?m)x(?-i:\n)`, `(?m)x(?:\n)`},
		{`(?m)(?-i)^\nx`, `(?m)^\nx`},
	}
	for _, p := range pairs {
		a := Classify(p[0], DialectRE2)
		b := Classify(p[1], DialectRE2)
		if a != b {
			t.Errorf("descriptor divergence: %q = %+v, %q = %+v", p[0], a, p[1], b)
		}
	}
}

// TestClassifyZeroMinDeadClassR3 pins live-state forwarding around
// zero-minimum repetitions of empty (never-matching) classes (review round
// 3, finding 2): the bypass path must look exactly like the unit were
// absent, on both the grouped and the directly quantified forms.
func TestClassifyZeroMinDeadClassR3(t *testing.T) {
	dead := `[^\x00-\x{10ffff}]`
	pairs := [][2]string{
		{`(?m)^(?:` + dead + `)*\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `)?\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `){0,2}\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `){0}\nx`, `(?m)^\nx`},
		{`(?m)^` + dead + `*\nx`, `(?m)^\nx`},
		{`(?m)^` + dead + `?\nx`, `(?m)^\nx`},
		{`(?m)^` + dead + `{0,3}\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `)*?x`, `(?m)^(?:` + dead + `)*x`},
		{`(?m)^(?:` + dead + `)*+x`, `(?m)^(?:` + dead + `)*x`},
	}
	for _, p := range pairs {
		a := Classify(p[0], DialectRegexp2)
		b := Classify(p[1], DialectRegexp2)
		if a != b {
			t.Errorf("zero-min dead-class divergence: %q = %+v, %q = %+v", p[0], a, p[1], b)
		}
		if a.LineAnchoredStart != b.LineAnchoredStart || a.StringExempt != b.StringExempt {
			t.Errorf("zero-min dead-class descriptor flip: %q = %+v, %q = %+v", p[0], a, p[1], b)
		}
	}
	// The reviewer's exact bypass rows land on the exempt 2-rune-run row.
	exemptRows := []string{
		`(?m)^(?:` + dead + `)*\nx`,
		`(?m)^(?:` + dead + `)?\nx`,
		`(?m)^(?:` + dead + `){0,2}\nx`,
		`(?m)^` + dead + `*\nx`,
	}
	for _, pattern := range exemptRows {
		got := Classify(pattern, DialectRE2)
		if !got.LineAnchoredStart || !got.StringExempt {
			t.Errorf("pattern %q: got %+v, want the exempt ^\\nx row {true true}", pattern, got)
		}
	}
	// A live alternation arm inside the loop consumes initial content, so
	// the pattern masks like ICU (probe R2k: false on a CRLF subject).
	masked := Classify(`(?m)^(?:x|`+dead+`)*\nb`, DialectRegexp2)
	if !masked.LineAnchoredStart || masked.StringExempt {
		t.Errorf("live-arm loop: got %+v, want {line=true exempt=false}", masked)
	}
	// Mid-pattern and x-headed forms: the dead loop revives (match results
	// equal the unit-absent forms; probe R2l true [0,1) on "xx"); the
	// StringExempt bit differs from a flat run because the class forces a
	// run split, which is mask-irrelevant without a leading LF-headed caret.
	if got := Classify(`(?m)x`+dead+`*y`, DialectRegexp2); got.LineAnchoredStart || got.SuppressCRLFInterior() {
		t.Errorf(`(?m)x%s*y: got %+v, want no line anchor and no interior mask`, dead, got)
	}
	if got := Classify(`(?m)^x`+dead+`*\ny`, DialectRegexp2); !got.LineAnchoredStart || !got.SuppressCRLFInterior() {
		t.Errorf(`(?m)^x%s*\ny: got %+v, want the x-headed masked row {line=true, mask on}`, dead, got)
	}
	// Mandatory dead units keep masking; a never-matching pattern may mask
	// freely (both ICU forms are false on every subject).
	for _, pattern := range []string{
		`(?m)^(?:` + dead + `)+\nx`,
		`(?m)^` + dead + `+\nx`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got.StringExempt {
			t.Errorf("mandatory dead unit: pattern %q must not be string-exempt, got %+v", pattern, got)
		}
	}
}

// TestClassIntervalFoldComplementR3 pins fold closure before complement
// cardinality (review round 3, finding 3) and gap-derived singleton
// complements (finding 5).
func TestClassIntervalFoldComplementR3(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
		note    string
	}{
		{`(?i:[^\x00-\x40\x42-\x{10ffff}])`, []string{"group-open scoped", "class-open", "class-close card=0", "group-close scoped", "end"},
			"review example: fold closure fills the complement"},
		{`(?i)[^\x00-\x40\x42-\x{10ffff}]`, []string{"flags", "class-open", "class-close card=0", "end"},
			"pattern-level fold reaches the same dead class"},
		{`(?i)[^\x00-\x{10ffff}]`, []string{"flags", "class-open", "class-close card=0", "end"},
			"full-range body stays empty under fold"},
		{`[^\x00-\x{10ffff}]`, []string{"class-open", "class-close card=0", "end"},
			"full-range body without fold"},
		{`(?i:[^\x00-\x{10fffe}])`, []string{"group-open scoped", "class-open", "class-close card=1 U+10FFFF", "group-close scoped", "end"},
			"scope-folded singleton gap derived without a rune sweep"},
		{`[^\x00-\x{10fffe}]`, []string{"class-open", "class-close card=1 U+10FFFF", "end"},
			"finding 5 shape: singleton complement from interval gaps"},
		{`[^\x00-\x{10fffd}]`, []string{"class-open", "class-close card=2", "end"},
			"two-member complement"},
		{`(?i:[^ab])`, []string{"group-open scoped", "class-open", "class-close card=2", "group-close scoped", "end"},
			"folded small negated class stays a set"},
		{`[^\x{10ffff}]`, []string{"class-open", "class-close card=2", "end"},
			"complement of a high singleton is many"},
	}
	for _, r := range rows {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) != len(r.want) {
			t.Errorf("pattern %q: got %d events, want %d: %v (%s)", r.pattern, len(events), len(r.want), events, r.note)
			continue
		}
		for i, w := range r.want {
			if got := shortEvent(events[i]); got != w {
				t.Errorf("pattern %q event %d: got %q want %q (all: %v; %s)", r.pattern, i, got, w, events, r.note)
				break
			}
		}
	}
	// The reviewer descriptor row: the folded dead arm leaves the \nx arm as
	// the sole initial content, so the row classifies like ^\nx (ICU probe
	// R3a: true at [2,4) under m).
	got := Classify(`(?m)^(?:(?i:[^\x00-\x40\x42-\x{10ffff}])|\nx)`, DialectRegexp2)
	want := Classify(`(?m)^\nx`, DialectRegexp2)
	if got != want {
		t.Errorf("fold-complement descriptor: got %+v, want the ^\\nx row %+v", got, want)
	}
	if !got.LineAnchoredStart || !got.StringExempt {
		t.Errorf("fold-complement descriptor: got %+v, want {true true}", got)
	}
	// Case-sensitive live alternatives keep their own semantics (probe R3c/R3d).
	cs := Classify(`(?m)^(?:(?i:[^\x00-\x40\x42-\x{10ffff}])|Ax)`, DialectRegexp2)
	if !cs.LineAnchoredStart || !cs.StringExempt {
		t.Errorf("case-sensitive live arm: got %+v, want the exempt Ax row {true true}", cs)
	}
}

// TestScanAngleRefLinearSuffixR3 pins incremental angle-reference scanning
// (review round 3, finding 4): recognition stops at the first invalid
// character, so long invalid bodies never trigger a suffix search. The
// digit-led and name-led forms below would each scan the whole remaining
// pattern for a closing delimiter before the fix.
func TestScanAngleRefLinearSuffixR3(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
	}{
		// A digit-led body stops at the first non-digit; the fork never
		// takes the name path for it.
		{`\<1a>`, []string{`literal U+003C`, `literal U+0031`, `literal U+0061`, `literal U+003E`, "end"}},
		// A name-led body stops at the first non-word character.
		{`\<ab cd>`, []string{`literal U+003C`, `literal U+0061`, `literal U+0062`, `literal U+0020`, `literal U+0063`, `literal U+0064`, `literal U+003E`, "end"}},
		{`\<ab`, []string{`literal U+003C`, `literal U+0061`, `literal U+0062`, "end"}},
		// Valid references are unchanged.
		{`\<13>`, []string{"backref", "end"}},
		{`\'name'`, []string{"backref", "end"}},
		{`\g{na me}`, []string{`literal U+0067`, `literal U+007B`, `literal U+006E`, `literal U+0061`, `literal U+0020`, `literal U+006D`, `literal U+0065`, `literal U+007D`, "end"}},
	}
	for _, r := range rows {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) != len(r.want) {
			t.Errorf("pattern %q: got %d events, want %d: %v", r.pattern, len(events), len(r.want), events)
			continue
		}
		for i, w := range r.want {
			if got := shortEvent(events[i]); got != w {
				t.Errorf("pattern %q event %d: got %q want %q (all: %v)", r.pattern, i, got, w, events)
				break
			}
		}
	}
}

// BenchmarkClassifyAngleLiteralScalingR3 measures repeated escaped angle
// literals (review round 3, finding 4). An uncached Classify must stay
// linear in the pattern size; before the fix each literal rescanned the
// rest of the pattern for a closing delimiter.
func BenchmarkClassifyAngleLiteralScalingR3(b *testing.B) {
	for _, n := range []int{1000, 4000, 16000} {
		pattern := `(?=)` + strings.Repeat(`\<`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// BenchmarkClassifyNegatedSingletonScalingR3 measures repeated negated
// classes whose complement is one rune (review round 3, finding 5).
// Deriving the complement from interval gaps keeps an uncached Classify
// near-linear; enumerating the rune space per class is quadratic.
func BenchmarkClassifyNegatedSingletonScalingR3(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^\x00-\x{10fffe}]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// TestStreamingEquivalenceRound3 folds the round-3 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound3(t *testing.T) {
	dead := `[^\x00-\x{10ffff}]`
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)(?-i)^\n`, DialectRE2},
		{`(?m)(?-i:^\n)x`, DialectRegexp2},
		{`(?m)^(?:` + dead + `)*\nx`, DialectRegexp2},
		{`(?m)^` + dead + `?\nx`, DialectRE2},
		{`(?m)^(?:(?i:[^\x00-\x40\x42-\x{10ffff}])|\nx)`, DialectRegexp2},
		{`(?i)[^\x00-\x{10fffe}]x`, DialectRE2},
		{`(?=)\<\<\<\<`, DialectRegexp2},
		{`(?m)(?-im)^\n`, DialectRE2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}
