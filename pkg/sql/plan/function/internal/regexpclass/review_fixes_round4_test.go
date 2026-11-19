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

// TestClassifyNestedDeadAlternativeR4 pins review round 4 finding 1: a
// sibling alternative must start with the frame's entry reachability, not
// unconditionally live. For ^(?:[^\x00-\x{10ffff}](?:a|b)|\nx) the inner
// group is entered dead, so its b arm can never contribute initial
// content; the whole first arm is unreachable and the pattern classifies
// like ^\nx (ICU 77.1 probes R4Aa/R4Ac/R4Ad: true [2,4) on aCRLFxb).
func TestClassifyNestedDeadAlternativeR4(t *testing.T) {
	dead := `[^\x00-\x{10ffff}]`
	pairs := [][2]string{
		// Discriminating rows: pre-fix the inner group revives its b arm
		// inside the dead outer arm, adding initial content and masking
		// the exempt \nx arm.
		{`(?m)^(?:` + dead + `(?:a|b)|\nx)`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `(?:a|b))*\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `(?:a|b))?\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `(?:a|b)){0,2}\nx`, `(?m)^\nx`},
		{`(?m)^(?:` + dead + `(?:a|b))*?\nx`, `(?m)^\nx`},
		// The live-arm head keeps its own descriptor: with the dead arm
		// gone the row equals the flat 3-rune run.
		{`(?m)^(?:` + dead + `(?:a|b)|x\nb)`, `(?m)^x\nb`},
		// Guard: the inner alternation entered LIVE (frame snap live) must
		// still revive its sibling arm; equals the live-group form, which
		// masks like ICU (probe R4Ah false on aCRLFxb).
		{`(?m)^(?:(?:a|b)` + dead + `|\nx)`, `(?m)^(?:a|\nx)`},
	}
	for _, p := range pairs {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			a := Classify(p[0], dialect)
			b := Classify(p[1], dialect)
			if a != b {
				t.Errorf("nested-dead descriptor divergence (%v): %q = %+v, %q = %+v", dialect, p[0], a, p[1], b)
			}
		}
	}
	// Possessive zero-minimum forms exist only in the fork dialect (probe
	// R4D1: true [2,4)).
	for _, p := range [][2]string{
		{`(?m)^(?:` + dead + `(?:a|b))*+\nx`, `(?m)^\nx`},
	} {
		a := Classify(p[0], DialectRegexp2)
		b := Classify(p[1], DialectRegexp2)
		if a != b {
			t.Errorf("nested-dead possessive divergence: %q = %+v, %q = %+v", p[0], a, p[1], b)
		}
	}
	// Absolute pins for the discriminating rows (the ^\nx row shape).
	exempt := []string{
		`(?m)^(?:` + dead + `(?:a|b)|\nx)`,
		`(?m)^(?:` + dead + `(?:a|b))*\nx`,
		`(?m)^(?:` + dead + `(?:a|b))*?\nx`,
	}
	for _, pattern := range exempt {
		got := Classify(pattern, DialectRegexp2)
		if !got.LineAnchoredStart || !got.StringExempt {
			t.Errorf("pattern %q: got %+v, want the exempt ^\\nx row {true true}", pattern, got)
		}
	}
	// Guards that must stay masked before and after (ICU probes R4Ae/R4Ah).
	for _, pattern := range []string{
		`(?m)^(?:` + dead + `(?:a|b))+\nx`,
		`(?m)^(?:` + dead + `(?:a|b)){1,2}\nx`,
		`(?m)^(?:(?:a|b)` + dead + `|\nx)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q: got %+v, want the masked row {line=true exempt=false}", pattern, got)
		}
	}
}

// TestScanEmptyPairClassCardsR4 pins review round 4 finding 2 at the event
// layer: supported shorthand/property atoms are resolved for cardinality
// instead of blanket-conservative CardMany. A union of an atom and its own
// complement covers the whole rune space, so negating it leaves an empty
// class; \P{Any} matches nothing on its own; unresolved forms keep the
// previous CardMany answer.
func TestScanEmptyPairClassCardsR4(t *testing.T) {
	rows := []struct {
		pattern string
		want    []string
		note    string
	}{
		// Discriminating rows: pre-fix every one of these is card=2.
		{`[^\s\S]`, []string{"class-open", "class-close card=0", "end"}, "review example"},
		{`[^\d\D]`, []string{"class-open", "class-close card=0", "end"}, "digit pair"},
		{`[^\w\W]`, []string{"class-open", "class-close card=0", "end"}, "word pair"},
		{`[^\d\s\D]`, []string{"class-open", "class-close card=0", "end"}, "one pair among several atoms covers all"},
		{`[^\s\S\p{Lu}]`, []string{"class-open", "class-close card=0", "end"}, "extra property subsumed by full coverage"},
		{`[^\s\Sx]`, []string{"class-open", "class-close card=0", "end"}, "extra literal subsumed by full coverage"},
		{`[^\p{Lu}\P{Lu}]`, []string{"class-open", "class-close card=0", "end"}, "property pair"},
		{`[^\p{Greek}\P{Greek}]`, []string{"class-open", "class-close card=0", "end"}, "script pair"},
		{`[^\p{L}\P{L}]`, []string{"class-open", "class-close card=0", "end"}, "group-category pair"},
		{`[^\pL\PL]`, []string{"class-open", "class-close card=0", "end"}, "single-letter property pair"},
		{`[\P{Any}]`, []string{"class-open", "class-close card=0", "end"}, "\\P{Any} matches nothing"},
		{`[^\p{Any}]`, []string{"class-open", "class-close card=0", "end"}, "complement of everything is nothing"},
		{`[\P{Any}x]`, []string{"class-open", "class-close card=1 U+0078", "end"}, "\\P{Any} contributes nothing beside a literal"},
		{`(?i)[^\s\S]`, []string{"flags", "class-open", "class-close card=0", "end"}, "fold keeps the empty complement empty"},
		{`(?i)[^\p{L}\P{L}]`, []string{"flags", "class-open", "class-close card=0", "end"}, "fold over a full coverage pair"},
		// Pre/post identical rows (the conservative-live card=2 answers).
		{`[\s\S]`, []string{"class-open", "class-close card=2", "end"}, "non-negated pair is the full set (BVT-critical)"},
		{`[\P{L}\p{L}]`, []string{"class-open", "class-close card=2", "end"}, "non-negated property pair is the full set"},
		{`[^\P{Any}]`, []string{"class-open", "class-close card=2", "end"}, "complement of nothing is everything"},
		{`[\s]`, []string{"class-open", "class-close card=2", "end"}, "lone shorthand stays conservative-live"},
		{`[^\s]`, []string{"class-open", "class-close card=2", "end"}, "lone negated shorthand stays conservative-live"},
		{`[\d]`, []string{"class-open", "class-close card=2", "end"}, "lone digit class"},
		{`[\p{Lu}]`, []string{"class-open", "class-close card=2", "end"}, "lone property"},
		{`[^\p{Lu}]`, []string{"class-open", "class-close card=2", "end"}, "lone negated property stays conservative-live"},
		{`[^\p{Lu}x]`, []string{"class-open", "class-close card=2", "end"}, "unpaired property plus literal"},
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
	// Standalone properties: \P{Any} is empty, every other property keeps
	// the conservative CardMany answer.
	for _, tc := range []struct {
		pattern string
		want    string
	}{
		{`\P{Any}`, "class-close card=0"},
		{`\p{Any}`, "class-close card=2"},
		{`\P{Lu}`, "class-close card=2"},
		{`\p{Greek}`, "class-close card=2"},
	} {
		events := DebugEvents(tc.pattern, DialectRegexp2)
		if len(events) != 3 {
			t.Fatalf("pattern %q: got %d events, want 3: %v", tc.pattern, len(events), events)
		}
		if got := shortEvent(events[1]); got != tc.want {
			t.Errorf("pattern %q: got %q want %q (all: %v)", tc.pattern, got, tc.want, events)
		}
	}
	// regexp2 nested subtraction of a full-covering operand empties the
	// outer class (the fork's .NET set subtraction: a minus everything is
	// nothing; ICU reads the dash literally, probe R4Cho, so this pin is
	// regexp2-dialect only).
	sub := DebugEvents(`[a-[\s\S]]`, DialectRegexp2)
	if len(sub) != 3 || shortEvent(sub[1]) != "class-close card=0" {
		t.Errorf(`[a-[\s\S]]: got %v, want an empty class close`, sub)
	}
	// Subtraction against a shorthand operand: round 6 and earlier kept
	// \d unresolved, so the conservative answer was live-many. Round 7
	// records the engines' exact [0-9] table (Go perl_groups code1; the
	// fork's RE2-mode addDigit), so {a} minus [0-9] resolves to the
	// singleton U+0061 exactly like the fork's subtraction.
	subLive := DebugEvents(`[a-[\d]]`, DialectRegexp2)
	if len(subLive) != 3 || shortEvent(subLive[1]) != "class-close card=1 U+0061" {
		t.Errorf(`[a-[\d]]: got %v, want the exact singleton U+0061 close`, subLive)
	}
}

// TestClassifyEmptyPairDescriptorR4 pins the descriptor consequences of
// review round 4 finding 2: an empty first arm built from shorthand or
// property complements contributes no initial content, so the \nx arm
// keeps its exempt shape (ICU 77.1 probes R4Ba..R4Bo, R4Ca..R4Ci: true
// [2,4) on aCRLFxb under m).
func TestClassifyEmptyPairDescriptorR4(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRE2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: ^\\nx row = %+v, want {true true}", want)
	}
	rows := []string{
		`(?m)^(?:[^\s\S]|\nx)`,
		`(?m)^(?:[^\d\D]|\nx)`,
		`(?m)^(?:[^\w\W]|\nx)`,
		`(?m)^(?:[^\d\s\D]|\nx)`,
		`(?m)^(?:[^\s\S\p{Lu}]|\nx)`,
		`(?m)^(?:[^\s\Sx]|\nx)`,
		`(?m)^(?:[^\p{Lu}\P{Lu}]|\nx)`,
		`(?m)^(?:[^\p{Greek}\P{Greek}]|\nx)`,
		`(?m)^(?:[^\p{L}\P{L}]|\nx)`,
		`(?m)^(?:\P{Any}|\nx)`,
		`(?m)^(?:[^\s\S])*\nx`,
		`(?m)^(?:[^\p{Lu}\P{Lu}])*\nx`,
		`(?m)^(?:[^\s\S])*?\nx`,
	}
	for _, pattern := range rows {
		for _, dialect := range []Dialect{DialectRE2, DialectRegexp2} {
			got := Classify(pattern, dialect)
			if got != want {
				t.Errorf("pattern %q (%v): got %+v, want the ^\\nx row %+v", pattern, dialect, got, want)
			}
		}
	}
	// Possessive bypass exists only in the fork dialect (probe R4D3 true).
	if got := Classify(`(?m)^(?:[^\s\S])*+\nx`, DialectRegexp2); got != want {
		t.Errorf(`(?m)^(?:[^\s\S])*+\nx: got %+v, want the ^\nx row %+v`, got, want)
	}
	// Guards that must stay masked before and after (ICU false rows):
	// mandatory repetitions of an empty class, the non-negated full
	// coverage pair (live consuming arm), and dead-head classes.
	for _, pattern := range []string{
		`(?m)^(?:[^\s\S])+\nx`,
		`(?m)^(?:[^\p{Lu}\P{Lu}])+\nx`,
		`(?m)^(?:\P{L}\p{L}|\nx)`,
		`(?m)^[^\s\S]x`,
		`(?m)^[^\p{Lu}\P{Lu}]x`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q: got %+v, want the masked row {line=true exempt=false}", pattern, got)
		}
	}
}

// TestStreamingEquivalenceRound4 folds the round-4 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound4(t *testing.T) {
	dead := `[^\x00-\x{10ffff}]`
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:` + dead + `(?:a|b)|\nx)`, DialectRE2},
		{`(?m)^(?:` + dead + `(?:a|b))*\nx`, DialectRegexp2},
		{`(?m)^(?:(?:a|b)` + dead + `|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^\s\S]|\nx)`, DialectRE2},
		{`(?m)^(?:[^\p{Lu}\P{Lu}]|\nx)`, DialectRegexp2},
		{`(?m)^(?:\P{Any}|\nx)`, DialectRegexp2},
		{`(?m)^(?:[^\s\S])*\nx`, DialectRE2},
		{`[a-[\s\S]]`, DialectRegexp2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// BenchmarkClassifyEmptyPairScalingR4 measures repeated classes whose
// shorthand/property atoms resolve to empty complements (review round 4,
// finding 2). Pair resolution walks the recorded atoms once, so an
// uncached Classify stays linear in the pattern size.
func BenchmarkClassifyEmptyPairScalingR4(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?m)" + strings.Repeat(`[^\s\S\p{Lu}]`, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}
