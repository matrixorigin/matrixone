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

// TestScanSubtractionCardsR9 pins review round 9 finding 2 at the
// event layer: subtraction is recognized after every completed member
// form at both nesting levels, and adjacent fold-special subtrahend
// operands canonicalize (merge before lowercase) exactly like the fork
// (syntax/charclass.go canonicalize ahead of addLowercase).
func TestScanSubtractionCardsR9(t *testing.T) {
	for _, r := range []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		// Finding 2: predecessors of every form consume the nested class.
		{`[a-z-[b]]`, DialectRegexp2, "class-close card=2", "review example: range predecessor {a,c,...,z}"},
		{`(?i)[a-z-[b]]`, DialectRegexp2, "class-close card=2", "folded range predecessor"},
		{`[A-Z-[B]]`, DialectRegexp2, "class-close card=2", "uppercase range"},
		{`[\d-[5]]`, DialectRegexp2, "class-close card=2", "shorthand predecessor"},
		{`[\w-[x]]`, DialectRegexp2, "class-close card=2", "shorthand predecessor"},
		{`[[:alpha:]-[b]]`, DialectRegexp2, "class-close card=2", "POSIX predecessor"},
		{`[\p{L}-[a]]`, DialectRegexp2, "class-close card=2", "property predecessor"},
		{`[s-[b]]`, DialectRegexp2, "class-close card=1 U+0073", "literal predecessor {s}"},
		{`[--[b]]`, DialectRegexp2, "class-close card=1 U+002D", "literal dash then subtraction"},
		{`[a-[b-[c]]]`, DialectRegexp2, "class-close card=1 U+0061", "nested subtraction"},
		{`[a-z-[b-[c]]]`, DialectRegexp2, "class-close card=2", "range then nested subtraction"},
		// Finding 3: fold-special subtrahends canonicalize first.
		{`(?i)[k-[KÅ]]`, DialectRegexp2, "class-close card=2", "{k,K,K} live: [KÅ] canonicalizes and drops"},
		{`(?i)[k-[K-Å]]`, DialectRegexp2, "class-close card=2", "equivalent interval spelling"},
		{`(?i)[k-[kK]]`, DialectRegexp2, "class-close card=0", "ToLower(K)=k subtracts out"},
		{`(?i)[KÅ]`, DialectRegexp2, "class-close card=0", "coalesced [K-Å] has no lcTable image"},
		{`(?i)[K-Å]`, DialectRegexp2, "class-close card=0", "equivalent interval spelling, empty"},
	} {
		events := DebugEvents(r.pattern, r.dialect)
		if !closeEventIs(events, r.want) {
			t.Errorf("pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
	// Guards, identical pre and post.
	for _, r := range []struct {
		pattern string
		want    string
		note    string
	}{
		{`(?i)[s-[ſ]]`, "class-close card=2", "round-8 pin: {s,S} live"},
		{`(?i)[µ-[μ]]`, "class-close card=1 U+00B5", "adjacent table pair: µ survives alone"},
		{`(?i)[k-[ſ]]`, "class-close card=2", "disjoint fold-specials"},
	} {
		events := DebugEvents(r.pattern, DialectRegexp2)
		if !closeEventIs(events, r.want) {
			t.Errorf("guard pattern %q: got %v, want %q (%s)", r.pattern, events, r.want, r.note)
		}
	}
}

// TestClassifySubtractionDescriptorR9 pins findings 2 and 3 at the
// descriptor level. A statically erased class ({0}) leaves the caret
// provably line-anchored with live initial content around it, so the
// descriptor takes the masked row (the ICU replay rev18: NOMATCH on the
// CRLF interior, MATCH [2,3) on the LF-LF control). Lazy quantifiers
// keep the maybe-consuming class opaque to the start proof, so those
// rows stay unmasked exactly as every earlier optional shape did; the
// engine then decides at run time. The fork's subtraction-must-be-last
// rule makes the tail forms parse errors, so they never mask.
func TestClassifySubtractionDescriptorR9(t *testing.T) {
	masked := Classify(`(?m)^(?:[ab])?\nx(?=)`, DialectRegexp2)
	if !masked.LineAnchoredStart || masked.StringExempt {
		t.Fatalf("sanity: masked row = %+v, want {true false}", masked)
	}
	for _, pattern := range []string{
		`(?m)[a-z-[b]]{0}^\n(?=)`,
		`(?m)[\d-[5]]{0}^\n(?=)`,
		`(?m)[\w-[x]]{0}^\n(?=)`,
		`(?m)[\p{L}-[a]]{0}^\n(?=)`,
		`(?m)[[:alpha:]-[b]]{0}^\n(?=)`,
		`(?m)[A-Z-[B]]{0}^\n(?=)`,
		`(?m)[a-[b-[c]]]{0}^\n(?=)`,
		`(?m)[a-z-[b-[c]]]{0}^\n(?=)`,
		`(?m)^(?i:[k-[KÅ]])?\nx(?=)`,
		`(?m)^(?i:[k-[K-Å]])?\nx(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != masked {
			t.Errorf("pattern %q: got %+v, want the masked row %+v", pattern, got, masked)
		}
	}
	// Lazy/unbounded quantifiers: the class stays a maybe-consumed unit
	// and the classifier declines the start proof (documented boundary:
	// only the statically dead {0} erases the class).
	for _, pattern := range []string{
		`(?m)[a-z-[b]]?^\n(?=)`,
		`(?m)[a-z-[b]]*^\n(?=)`,
		`(?m)[a-z-[b]]{0,1}^\n(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got.LineAnchoredStart || got.StringExempt {
			t.Errorf("lazy pattern %q: got %+v, want the undeclared {false false} row", pattern, got)
		}
	}
	// Must-be-last: the fork parse-errors on a trailing sibling after
	// the nested class, so neither form ever masks or revives.
	for _, pattern := range []string{
		`^(?:[a-z-[b]-[c]]|\nx)(?=)`,
		`^(?:[a-z-[b]c]|\nx)(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got.LineAnchoredStart || got.StringExempt {
			t.Errorf("must-be-last pattern %q: got %+v, want the conservative {false false} row", pattern, got)
		}
	}
	// Fork-empty fold subtrahends keep the exemption (engine-equivalent
	// dead optional).
	exempt := Classify(`(?m)^(?:[a-[a]])?\nx(?=)`, DialectRegexp2)
	for _, pattern := range []string{
		`(?m)^(?i:[k-[kK]])?\nx(?=)`,
		`(?m)^(?i:[s-[S]])?\nx(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != exempt {
			t.Errorf("dead optional %q: got %+v, want the exempt row %+v", pattern, got, exempt)
		}
	}
}

// TestScanAnySpellingCardsR9 pins review round 9 finding 4 at the
// event layer: every canonical spelling of Any resolves through the
// single goPropertyTable path on the RE2 dialect, bracketed or
// standalone, folded or not, and the negated forms close empty. The
// fork dialect rejects every Any spelling at parse, so those rows stay
// conservative many.
func TestScanAnySpellingCardsR9(t *testing.T) {
	for _, r := range []struct {
		pattern string
		dialect Dialect
		want    string
		note    string
	}{
		{`[^\p{any}]`, DialectRE2, "class-close card=0", "review example: lowercase spelling"},
		{`[^\p{a_n_y}]`, DialectRE2, "class-close card=0", "canonicalName drops '_'"},
		{`[^\p{ANY}]`, DialectRE2, "class-close card=0", "case-normalized"},
		{`[\p{ANY}]`, DialectRE2, "class-close card=2", "universe"},
		{`[\P{ANY}]`, DialectRE2, "class-close card=0", "negated universe matches nothing"},
		{`[\p{^Any}]`, DialectRE2, "class-close card=0", "ICU caret negation inside braces"},
		{`(?i)[^\p{any}]`, DialectRE2, "class-close card=0", "fold does not change the universe"},
		{`\p{any}`, DialectRE2, "class-close card=2", "standalone many"},
		{`\P{a_n_y}`, DialectRE2, "class-close card=0", "standalone empty"},
		{`[^\p{any}]`, DialectRegexp2, "class-close card=2", "fork rejects any: conservative"},
		{`[\p{Any}]`, DialectRegexp2, "class-close card=2", "recorded atom (round-6 semantics)"},
	} {
		events := DebugEvents(r.pattern, r.dialect)
		if !closeEventIs(events, r.want) {
			t.Errorf("pattern %q (%v): got %v, want %q (%s)", r.pattern, r.dialect, events, r.want, r.note)
		}
	}
	// Guard: any_thing is NOT a spelling of Any and must stay rejected.
	for _, d := range []Dialect{DialectRE2, DialectRegexp2} {
		events := DebugEvents(`[^\p{any_thing}]`, d)
		if !closeEventIs(events, "class-close card=2") {
			t.Errorf(`[^\p{any_thing}] (%v): got %v, want the conservative many close`, d, events)
		}
	}
}

// TestClassifyAnySpellingDescriptorR9 pins finding 4 at the descriptor
// level: on the RE2 route the empty first arm drops out of the
// alternation and the row revives the string exemption exactly like
// ^\nx (ICU rev18 F4 rows: MATCH [2,4) on the CRLF-interior probe);
// the fork route rejects the property at parse, so its rows stay
// conservative masked.
func TestClassifyAnySpellingDescriptorR9(t *testing.T) {
	want := Classify(`(?m)^\nx`, DialectRE2)
	if !want.LineAnchoredStart || !want.StringExempt {
		t.Fatalf("sanity: (?m)^\\nx row = %+v, want {true true}", want)
	}
	for _, pattern := range []string{
		`(?m)^(?:[^\p{any}]|\nx)`,
		`(?m)^(?:\P{a_n_y}|\nx)`,
		`(?m)^(?:[^\p{ANY}]|\nx)`,
		`(?m)^(?:\p{^Any}|\nx)`,
	} {
		got := Classify(pattern, DialectRE2)
		if got != want {
			t.Errorf("pattern %q: got %+v, want the ^\\nx row %+v", pattern, got, want)
		}
	}
	masked := Classify(`(?m)^(?:[ab])?\nx(?=)`, DialectRegexp2)
	if !masked.LineAnchoredStart || masked.StringExempt {
		t.Fatalf("sanity: masked fork row = %+v, want {true false}", masked)
	}
	for _, pattern := range []string{
		`(?m)^(?:[^\p{any}]|\nx)(?=)`,
		`(?m)^(?:\P{a_n_y}|\nx)(?=)`,
	} {
		got := Classify(pattern, DialectRegexp2)
		if got != masked {
			t.Errorf("pattern %q: got %+v, want the conservative masked row %+v", pattern, got, masked)
		}
	}
}

// TestGoPropertyCacheBoundR9 pins review round 9 finding 5: property
// memoization keys the finite resolved identity (table pointers +
// polarity/fold bits), never the raw user-supplied name, and failures
// are never cached. Churning alias and spelling variants of one
// property must not grow the cache, and pattern-sized names must not
// be retained as keys. The matcher-layer regexpSet eviction cannot
// release this process-global cache, so the bound has to live here.
func TestGoPropertyCacheBoundR9(t *testing.T) {
	cacheCount := func() int {
		n := 0
		goPropertyCache.Range(func(_, _ any) bool { n++; return true })
		return n
	}
	// Warm the identity universe fully, then prove a second identical
	// churn allocates nothing: raw-name keys would grow on every round.
	spellings := []string{"L", "L_", "L__", "l", "Ll", "l_l", "lowercaseletter", "Lowercase_Letter",
		"Letter", "Lu", "l_u", "Greek", "g_reek", "Assigned", "a_s_s_i_g_n_e_d", "Any", "any", "a-n y", "Ascii", "a-s cii"}
	groupPrefixes := []string{`(?i)[^\p{%s}\P{Lu}]`, `[^\p{%s}\P{Lu}]`, `[\p{%s}]`, `(?i)[\p{%s}]`}
	churn := func() {
		for _, sp := range spellings {
			for _, gp := range groupPrefixes {
				pat := fmt.Sprintf(gp, sp)
				// Pad the pattern so any raw-name retention would
				// retain this distinct allocation as well.
				pat = pat + strings.Repeat("q", 32)
				_ = DebugEvents(pat, DialectRE2)
			}
		}
	}
	churn()
	churn()
	before := cacheCount()
	churn()
	churn()
	if after := cacheCount(); after != before {
		t.Errorf("goPropertyCache grew under alias churn: %d -> %d entries; keys must be the resolved identity, not the raw name", before, after)
	}
	// Previously unseen valid spellings of an already-warmed identity
	// must stay flat as well: an identity-keyed cache resolves them to
	// the warmed table+polarity/fold entry, while a raw-name cache would
	// insert one entry per new spelling and pass the warm-list churn
	// above. Base properties and normalizations (case, underscore,
	// hyphen, space) are the ones already proven by the warm list;
	// these exact strings never appear in it.
	before = cacheCount()
	unseen := []string{"LETTER", "l_e_t_t_e_r", "A_N_Y", "GREEK", "g-r-e-e-k", "Gr eek"}
	for _, sp := range unseen {
		for _, gp := range groupPrefixes {
			pat := fmt.Sprintf(gp, sp) + strings.Repeat("z", 32)
			_ = DebugEvents(pat, DialectRE2)
		}
	}
	if after := cacheCount(); after != before {
		t.Errorf("goPropertyCache grew under unseen valid spellings of warmed identities: %d -> %d entries; keys must be the resolved identity, not the raw name", before, after)
	}
	// Negative results are never cached: garbage names churn freely.
	before = cacheCount()
	for i := 0; i < 256; i++ {
		_ = DebugEvents(fmt.Sprintf(`(?i)[^\p{bogus_%d}\P{Lu}]`, i), DialectRE2)
	}
	if after := cacheCount(); after != before {
		t.Errorf("goPropertyCache grew under failed resolutions: %d -> %d entries; misses must stay uncached", before, after)
	}
}

// TestStreamingEquivalenceRound9 folds the round-9 constructs into the
// streaming-vs-batch equivalence pin.
func TestStreamingEquivalenceRound9(t *testing.T) {
	corpus := []struct {
		pattern string
		dialect Dialect
	}{
		{`(?m)^(?:(?i:[^\p{Lu}\P{Ll}])|\nx)(?=)`, DialectRegexp2},
		{`(?m)^(?:(?i)[^\p{any}]|\nx)`, DialectRE2},
		{`(?m)^(?:\P{a_n_y}|\nx)`, DialectRE2},
		{`(?m)[a-z-[b]]{0}^\n(?=)`, DialectRegexp2},
		{`(?m)^(?i:[k-[KÅ]])?\nx(?=)`, DialectRegexp2},
		{`(?i)[k-[K-Å]]`, DialectRegexp2},
		{`(?i)[^\p{any}]`, DialectRE2},
		{`(?i)[\P{Ll}]`, DialectRegexp2},
	}
	for _, tc := range corpus {
		want := fold(scan(tc.pattern, tc.dialect))
		got := Classify(tc.pattern, tc.dialect)
		if got != want {
			t.Errorf("pattern %q (%v): streaming Classify = %+v, batch fold = %+v", tc.pattern, tc.dialect, got, want)
		}
	}
}

// BenchmarkClassifyPropertyAliasChurnR9 measures steady-state classify
// cost while churning alias spellings of one property: with resolved
// identity keys the whole churn reuses one memoized set, so ns/op and
// allocs stay flat across the spelling family.
func BenchmarkClassifyPropertyAliasChurnR9(b *testing.B) {
	spellings := []string{"L", "L_", "L__", "l", "Ll", "l_l", "lowercaseletter", "Lowercase_Letter",
		"Letter", "Greek", "g_reek", "Assigned", "a_s_s_i_g_n_e_d", "Any", "any", "Ascii"}
	patterns := make([]string, 0, 2*len(spellings))
	for _, sp := range spellings {
		patterns = append(patterns, `(?i)[^\p{`+sp+`}\P{Lu}]x`, `[^\p{`+sp+`}\P{Lu}]x`)
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Classify(patterns[i%len(patterns)], DialectRE2)
	}
}
