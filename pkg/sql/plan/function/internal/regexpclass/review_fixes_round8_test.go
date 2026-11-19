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

// TestScanForkFoldCardsR8 pins review round 8 finding 2 at the event
// layer: on the regexp2 dialect the engines fold classes under the
// fork's lowercase semantics - each class is stored in lowercase space
// (addLowercase at close, charclass.go:655-670 via parser.go:1628) and
// matched against unicode.ToLower of the input (runner.go:1181-1182) -
// never under Go's SimpleFold orbits. (?i)[s-[ſ]] keeps the live set
// {s,S}: SimpleFold's orbit {s,S,ſ} would erase it. Discriminating
// rows were engine-probed in fork-probe-round8.log.
func TestScanForkFoldCardsR8(t *testing.T) {
	discriminating := []struct {
		pattern string
		want    string
		note    string
	}{
		// Pre-fix (SimpleFold semantics) every one of these resolves
		// card=0: both operands expand to the shared orbit.
		{`(?i)[s-[ſ]]`, "class-close card=2", "review example: fork keeps {s,S} live"},
		{`(?i)[S-[ſ]]`, "class-close card=2", "uppercase orientation, same fork truth"},
		{`(?i)[ſ-[s]]`, "class-close card=1 U+017F", "fork: ToLower(ſ)=ſ so ſ survives alone"},
		{`(?i)[µ-[μ]]`, "class-close card=1 U+00B5", "ToLower(µ)=µ survives; Μ,μ subtract out"},
		{`(?i)[μ-[µ]]`, "class-close card=2", "ToLower(Μ)=μ: {Μ,μ} live minus {µ}"},
	}
	for _, r := range discriminating {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if len(events) < 3 || shortEvent(events[len(events)-2]) != r.want {
			t.Errorf("pattern %q: got %v, want close %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
	// Guards: identical pre and post (SimpleFold and fork lowercase
	// agree on these).
	guards := []struct {
		pattern string
		want    string
		note    string
	}{
		{`(?i)[a-[^A]]`, "class-close card=2", "round-7 pin: {a,A} on both fold models"},
		{`(?i)[k-[ſ]]`, "class-close card=2", "disjoint orbits: {k,K,\\u212A} minus {ſ}"},
		{`(?i)[k-[^k]]`, "class-close card=2", "round-7 pin kept"},
		{`(?i)[A-[µ]]`, "class-close card=2", "{a,A} minus {µ}"},
		{`(?i)[s-[S]]`, "class-close card=0", "both models empty"},
		{`[s-[ſ]]`, "class-close card=1 U+0073", "unfolded singleton"},
		{`(?i)[0-[^0]]`, "class-close card=1 U+0030", "digits fold trivially (round-7 pin)"},
		{`(?i)[0-[^1]]`, "class-close card=0", "negated subtrahend covers {0}"},
		{`[0-[^5]]`, "class-close card=0", "unfolded negated subtrahend"},
		{`(?i)[^0-[0]]`, "class-close card=2", "complement {0} minus {0} stays live"},
	}
	for _, r := range guards {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if !closeEventIs(events, r.want) {
			t.Errorf("guard pattern %q: got %v, want %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
}

// TestScanFoldedPropertyCardsR8 pins review round 8 finding 1 at the
// event layer: case-folded Unicode properties resolve against each
// production engine's membership instead of degrading to the
// conservative atom. RE2 folds by merging the property with Go's
// FoldCategory/FoldScript companion tables (syntax/parse.go
// unicodeTable + parseUnicodeClass in Go 1.27.1). Round 9 (its own
// pre-PR review finding 1) re-derives the fork dialect's folded
// property membership from the same ICU compatibility contract
// (per-operand tab U fold-companion closure, complemented after the
// merge), independent of the fork's consuming behavior: the runner's
// ToLower fiber diverges from both ICU and Go RE2 at the fold-special
// runes, so the contract - not the fork's fiber - decides structural
// cardinality. TestForkFoldPropertyEngineSplitR9 pins the fork
// divergence witnesses.
func TestScanFoldedPropertyCardsR8(t *testing.T) {
	discriminating := []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		// Pre-fix every folded property records an unresolved atom, so
		// every discriminating row stays CardMany.
		{`(?i)[^\p{L}\P{Lu}]`, DialectRE2, "class-close card=0", "review example: empty on ICU/Go"},
		{`(?i)[^\p{L}\P{Lu}]`, DialectRegexp2, "class-close card=0", "engine probe: dead on the fork"},
		{`(?i)[^\p{Lu}\P{Ll}]`, DialectRegexp2, "class-close card=2", "round-9 finding 1: ICU per-operand closure keeps Ll minus Lu live (U+FB00, U+0149, ...), independent of the fork's consuming behavior"},
		{`(?i)[^\p{Letter}\P{Lu}]`, DialectRE2, "class-close card=0", "Go resolves the Letter alias to L"},
		{`[^\p{Letter}\P{Lu}]`, DialectRE2, "class-close card=0", "alias resolution is fold-independent"},
		{`(?i)[^\p{Assigned}\p{Cn}]`, DialectRE2, "class-close card=0", "Assigned is the sign-flipped Cn complement"},
		{`(?i)[^\p{Greek}\P{Greek}]`, DialectRE2, "class-close card=0", "script pairs via FoldScript"},
		{`(?i)[^\p{Greek}\P{Greek}]`, DialectRegexp2, "class-close card=0", "fork lowercase preimage pairs"},
		{`(?i)[^\p{White_Space}\S]`, DialectRegexp2, "class-close card=0", "fork property merges supercede \\s"},
	}
	for _, r := range discriminating {
		events := DebugEvents(r.pattern, r.dialect)
		if !closeEventIs(events, r.want) {
			t.Errorf("pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
	// Guards: identical pre and post, including the engine-rejected
	// names that keep the conservative atom treatment per route.
	guards := []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		{`[^\p{L}\P{Lu}]`, DialectRE2, "class-close card=0", "round-7 pin preserved"},
		{`[^\p{L}\P{Lu}]`, DialectRegexp2, "class-close card=0", "round-7 pin preserved"},
		{`[^\P{Lu}\P{Ll}]`, DialectRE2, "class-close card=0", "round-7 pin preserved"},
		{`[^\P{Lu}\P{Ll}]`, DialectRegexp2, "class-close card=0", "round-7 pin preserved"},
		{`(?i)[^\p{Lu}\P{Ll}]`, DialectRE2, "class-close card=2", "RE2: U+FB00 orbit survives in Ll minus Lu"},
		{`(?i)[^\p{L}]`, DialectRE2, "class-close card=2", "lone folded complement stays live"},
		{`(?i)[^\p{L}]`, DialectRegexp2, "class-close card=2", "lone folded complement stays live"},
		{`(?i)[\p{Greek}]`, DialectRE2, "class-close card=2", "folded script stays live"},
		{`(?i)[^\p{Assigned}\P{Cn}]`, DialectRE2, "class-close card=2", "Assigned == \\P{Cn}: residual Cn"},
		{`(?i)[^\p{White_Space}\S]`, DialectRE2, "class-close card=2", "RE2 rejects White_Space: conservative"},
		{`(?i)[^\p{Letter}\P{Lu}]`, DialectRegexp2, "class-close card=2", "fork rejects Letter: conservative"},
		{`(?i)[^\p{Any}\p{Any}]`, DialectRegexp2, "class-close card=0", "fork rejects Any: the recorded atom keeps full coverage (round-6 semantics)"},
		{`(?i)[\p{Assigned}]`, DialectRegexp2, "class-close card=2", "fork rejects Assigned: conservative"},
	}
	for _, r := range guards {
		events := DebugEvents(r.pattern, r.dialect)
		if !closeEventIs(events, r.want) {
			t.Errorf("guard pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
}

// closeEventIs reports that a scanned class-close event prints exactly
// as want under shortEvent; a leading (?i) flag event keeps the close
// event at a variable offset, so look from the end.
func closeEventIs(events []Event, want string) bool {
	if len(events) < 3 {
		return false
	}
	return shortEvent(events[len(events)-2]) == want
}

// TestClassifyForkFoldDescriptorR8 pins finding 2 at the descriptor
// level: folded subtraction classes that are live on the fork behave
// exactly like a live literal optional (masking the CRLF interior),
// while the fork-empty class grants the same string exemption as any
// dead optional.
func TestClassifyForkFoldDescriptorR8(t *testing.T) {
	liveWant := Classify(`(?m)^(?:[ab])?\nx(?=)`, DialectRegexp2)
	if !liveWant.LineAnchoredStart || liveWant.StringExempt {
		t.Fatalf("sanity: (?m)^(?:[ab])?\\nx(?=) row = %+v, want the masked row", liveWant)
	}
	for _, pattern := range []string{
		`(?m)^(?i:[s-[ſ]])?\nx(?=)`,
		`(?m)^(?i:[S-[ſ]])?\nx(?=)`,
		`(?m)^(?i:[ſ-[s]])?\nx(?=)`,
		`(?m)^(?i:[µ-[μ]])?\nx(?=)`,
		`(?m)^(?i:[k-[ſ]])?\nx(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != liveWant {
			t.Errorf("pattern %q: got %+v, want the live-optional row %+v", pattern, got, liveWant)
		}
	}
	// The genuinely fork-empty optional keeps the exempt row pre and
	// post.
	exempt := Classify(`(?m)^(?:[a-[a]])?\nx(?=)`, DialectRegexp2)
	got := Classify(`(?m)^(?i:[s-[S]])?\nx(?=)`, DialectRegexp2)
	if got != exempt {
		t.Errorf("dead guard pattern: got %+v, want the exempt row %+v", got, exempt)
	}
}

// TestClassifyFoldedPropertyDescriptorR8 pins finding 1 at the
// descriptor level: folded-property classes that are empty on the
// production engine grant the alternation the string exemption just
// like the ^\nx row, while genuinely live folded classes keep the
// masked row. The fold scope closes around the class itself ((?i:...)):
// an unscoped (?i) reaching the \nx arm would fold that run as well,
// and a folded run counts as two initial units, keeping the mask up
// exactly as the engines do.
func TestClassifyFoldedPropertyDescriptorR8(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{Letter}\P{Lu}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{L}\P{Lu}])|\nx)(?=)`, DialectRegexp2},
	} {
		want := Classify(`(?m)^\nx`, tc.dialect)
		if !want.LineAnchoredStart || !want.StringExempt {
			t.Fatalf("sanity: (?m)^\\nx row (%v) = %+v, want {true true}", tc.dialect, want)
		}
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): got %+v, want the ^\\nx row %+v", tc.pattern, tc.dialect, got, want)
		}
	}
	// The review's top-level shape grants the exemption bit with the
	// repair too: pre-repair the live first arm counts as initial
	// content (StringExempt false); post-repair the dead arm drops
	// out. The leading caret dies with that first arm, so
	// LineAnchoredStart stays false either way.
	if got := Classify(`(?m)^(?:(?i)[^\p{L}\P{Lu}])|\nx`, DialectRE2); got.LineAnchoredStart || !got.StringExempt {
		t.Errorf("top-level pattern: got %+v, want {false true}", got)
	}
	// Masked guards: the folded letter partition is live on both
	// engines (seals the interior), and the RE2 dialect keeps
	// [^\p{Lu}\P{Ll}] live through the ligature residual. Round 9
	// (review finding 1) moves the fork demorgan row here too: the
	// ICU per-operand closure keeps folded Ll minus Lu nonempty
	// (U+FB00, U+0149, ...), so the class masks on the fork route as
	// well; ICU 77.1 rev18/rev18b replay confirms NOMATCH on the
	// CRLF-interior probe and MATCH [2,4) on the LF-LF control.
	for _, tc := range []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:(?i:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}])|\nx)(?=)`, DialectRegexp2},
		{`(?m)^(?:(?i:[^\p{L}\p{Lu}\p{Ll}\p{Lt}\p{Lm}\p{Lo}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)(?=)`, DialectRegexp2},
	} {
		got := Classify(tc.pattern, tc.dialect)
		if !got.LineAnchoredStart || got.StringExempt {
			t.Errorf("guard pattern %q (%v): got %+v, want the masked row", tc.pattern, tc.dialect, got)
		}
	}
}

// TestStreamingEquivalenceRound8 folds the round-8 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound8(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:(?i:[^\p{L}\P{Lu}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{Letter}\P{Lu}])|\nx)`, DialectRE2},
		{`(?m)^(?:(?i:[^\p{L}\P{Lu}])|\nx)(?=)`, DialectRegexp2},
		{`(?m)^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)(?=)`, DialectRegexp2},
		{`(?m)^(?i:[s-[ſ]])?\nx(?=)`, DialectRegexp2},
		{`(?m)^(?i:[ſ-[s]])?\nx(?=)`, DialectRegexp2},
		{`(?i)[µ-[μ]]`, DialectRegexp2},
		{`(?i)[^\p{Assigned}\p{Cn}]`, DialectRE2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// benchmarkPatternRepeat reports the ns/op and allocation shape of an
// uncached Classify over a pattern built by repeating unit n times.
func benchmarkPatternRepeat(b *testing.B, dialect Dialect, prefix, unit string) {
	for _, n := range []int{64, 256, 1024} {
		pattern := prefix + strings.Repeat(unit, n)
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, dialect)
			}
		})
	}
}

// BenchmarkClassifyDuplicateLiteralScalingR8 measures review round 8
// finding 3's worst case: one class containing n repeated literal
// characters under case folding. Amortized growth plus one lazy fold
// closure per class keeps uncached Classify linear.
func BenchmarkClassifyDuplicateLiteralScalingR8(b *testing.B) {
	for _, n := range []int{64, 256, 1024} {
		pattern := "(?i)[" + strings.Repeat("a", n) + "]"
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRegexp2)
			}
		})
	}
}

// BenchmarkClassifyFoldedLiteralScalingR8 measures long folded classes
// of distinct case-bearing literals (fold-relevant content through the
// same insertion path).
func BenchmarkClassifyFoldedLiteralScalingR8(b *testing.B) {
	units := []string{"k", "K", "ſ", "µ", "s", "S"}
	for _, n := range []int{64, 256, 1024} {
		var sb strings.Builder
		sb.WriteString("(?i)[")
		for i := 0; i < n; i++ {
			sb.WriteString(units[i%len(units)])
		}
		sb.WriteString("]")
		pattern := sb.String()
		b.Run(fmt.Sprintf("copies=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(pattern)))
			for i := 0; i < b.N; i++ {
				Classify(pattern, DialectRE2)
			}
		})
	}
}

// BenchmarkClassifyForkFoldSubtractionScalingR8 measures repeated fork
// folded subtraction classes (review round 8, finding 2): the lowercase
// evaluation runs once per class and stays linear.
func BenchmarkClassifyForkFoldSubtractionScalingR8(b *testing.B) {
	benchmarkPatternRepeat(b, DialectRegexp2, "(?m)", `(?i:[s-[ſ]])(?i:[ſ-[s]])`)
}

// BenchmarkClassifyFoldedPropertyScalingR8 measures repeated folded
// property classes on both dialects (review round 8, finding 1): table
// resolution is memoized and the per-class work stays linear.
func BenchmarkClassifyFoldedPropertyScalingR8(b *testing.B) {
	b.Run("dialect=RE2", func(b *testing.B) {
		benchmarkPatternRepeat(b, DialectRE2, "(?m)", `(?i)[^\p{L}\P{Lu}]`)
	})
	b.Run("dialect=Regexp2", func(b *testing.B) {
		benchmarkPatternRepeat(b, DialectRegexp2, "(?m)", `(?i)[^\p{L}\P{Lu}]`)
	})
}
