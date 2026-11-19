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
	"testing"
)

// TestForkFoldPropertyEngineSplitR9 pins review round 9 finding 1: the
// structural cardinality of shared folded Unicode property syntax
// derives from the ICU compatibility contract, independently of the
// fork's consuming behavior. ICU 77.1 (rev19 probes) and Go 1.27.1 RE2
// agree at every probed fiber, including the runes where the production
// fork engine diverges with its ToLower fiber (the runner lowercases
// the input, so U+0130 is read as 'i', the cased-letter triple
// expansion pulls U+0131/U+0149/... into (?i)\p{Lu}, and the lcTable
// set-pieces leak Greek fold specials into (?i)\p{Ll}).
//
// Phase 1 proves classifier membership on the fork dialect equals the
// Go RE2 engine (the portable encoding of the ICU contract) over the
// full conformance spread. Phase 2 pins the named fork-engine
// divergence witnesses per construct, so the runtime delta stays
// visible and exact instead of hiding inside an aggregate assertion.
// The RE2-route analogs already live in TestRE2EngineConformanceRound8;
// the pre-round-8 rows asserting fork-engine agreement here promoted a
// compatibility regression and were corrected under the review.
func TestForkFoldPropertyEngineSplitR9(t *testing.T) {
	high := []rune{
		0x00B5, 0x017F, 0x212A, 0x039C, 0x03BC, 0x0130, 0x0131,
		0x0138, 0x0149, 0x0345, 0x03D2, 0x03D3, 0x03D4, 0x2102,
		0xFB00, 0xFB01, 0x1E9E, 0x1E9B, 0x02B0, 0x4E2D, 0x10FFFF,
		0x0666, 0x1D7CE, 0xFEFF, 0x2028, 0x2029, 0x3000,
	}
	for _, tc := range []struct {
		pattern string
		note    string
	}{
		{`(?i)[\p{Lu}]`, "ICU closure of Lu; fiber misses the lcTable-only members"},
		{`(?i)[\P{Lu}]`, "complement after the merge"},
		{`(?i)[\p{Ll}]`, "U+0130 excluded: ToLower(0130)='i' but its SimpleFold orbit is a singleton"},
		{`(?i)[\P{Ll}]`, "U+0130 included in the complement"},
		{`(?i)[\p{Lt}]`, "no fiber-cased uppercase letters on the contract"},
		{`(?i)[\P{Lt}]`, "complement"},
		{`(?i)[\p{L}]`, "0345 joins the letters"},
		{`(?i)[\P{L}]`, "complement"},
		{`(?i)[\p{Greek}]`, "micro and 0345 join Greek via the fold companion"},
		{`(?i)[\P{Greek}]`, "complement"},
		{`(?i)[\p{Lm}]`, "engine-stable (kept as a breadth guard)"},
		{`(?i)[\P{Lm}]`, "engine-stable"},
		{`(?i)[\p{LC}]`, "engine-stable"},
		{`(?i)[\P{LC}]`, "engine-stable"},
		{`(?i)[\p{Nd}]`, "engine-stable"},
		{`(?i)[\P{Nd}]`, "engine-stable"},
		{`(?i)[^\p{Lu}\P{Ll}]`, "review example: stays live (U+FB00, U+0149, U+0131, ...)"},
		{`(?i)[^\p{L}\P{Lu}]`, "complement pair: empty on every reading"},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRegexp2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative, conformance impossible (%s)", tc.pattern, tc.note)
		}
		want := engineMember(t, tc.pattern, false)
		for _, r := range conformanceRunes(high...) {
			if got(r) != want(r) {
				t.Errorf("pattern %q rune U+%04X: classifier %v, ICU/Go contract %v (%s)", tc.pattern, r, got(r), want(r), tc.note)
			}
		}
	}

	// Phase 2: the fork divergence witnesses. For each named rune the
	// classifier carries the ICU/Go contract value while the live fork
	// engine keeps its own consuming verdict. Both values are pinned;
	// the divergence is engine-level and this change only stops
	// deriving masks from the fiber.
	for _, w := range []struct {
		pattern       string
		r             rune
		wantContract  bool
		wantForkFiber bool
		note          string
	}{
		{`(?i)[\p{Lu}]`, 0x0131, false, true, "fork: ToLower-fiber Lu picks up dotless i"},
		{`(?i)[\p{Lu}]`, 0x0138, false, true, "fork: lcTable set-piece"},
		{`(?i)[\p{Lu}]`, 0x0149, false, true, "fork: apostrophe-n set-piece"},
		{`(?i)[\P{Lu}]`, 0x0131, true, false, "complement direction"},
		{`(?i)[\P{Lu}]`, 0x0149, true, false, "complement direction"},
		{`(?i)[\p{Ll}]`, 0x0130, false, true, "fork reads 'i' under the fiber; ICU/Go exclude 0130"},
		{`(?i)[\p{Ll}]`, 0x0345, true, false, "contract includes the combining iota"},
		{`(?i)[\p{Ll}]`, 0x03D2, false, true, "fork fiber includes upsilon hooks"},
		{`(?i)[\p{Ll}]`, 0x03D4, false, true, "fork fiber"},
		{`(?i)[\p{Ll}]`, 0x2102, false, true, "fork reads 'c' under the fiber"},
		{`(?i)[\P{Ll}]`, 0x0130, true, false, "complement direction"},
		{`(?i)[\P{Ll}]`, 0x0345, false, true, "complement direction"},
		{`(?i)[\p{Lt}]`, 0x0041, false, true, "fork triple expansion cased-letters the Lt entry"},
		{`(?i)[\P{Lt}]`, 0x0041, true, false, "complement direction"},
		{`(?i)[\p{L}]`, 0x0345, true, false, "letters include the combining iota"},
		{`(?i)[\P{L}]`, 0x0345, false, true, "complement direction"},
		{`(?i)[\p{Greek}]`, 0x00B5, true, false, "micro joins Greek only on the contract"},
		{`(?i)[\P{Greek}]`, 0x00B5, false, true, "complement direction"},
		{`(?i)[^\p{Lu}\P{Ll}]`, 0xFB00, true, false, "the review's FB00 witness: live on the contract"},
		{`(?i)[^\p{Lu}\P{Ll}]`, 0x0149, true, false, "apostrophe-n stays live"},
		{`(?i)[^\p{Lu}\P{Ll}]`, 0x0131, true, false, "dotless i stays live"},
	} {
		got, conservative := captureClassMembers(t, w.pattern, DialectRegexp2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative (%s)", w.pattern, w.note)
		}
		if got(w.r) != w.wantContract {
			t.Errorf("pattern %q rune U+%04X: classifier %v, want contract %v (%s)", w.pattern, w.r, got(w.r), w.wantContract, w.note)
		}
		if fork := forkEngineMember(t, w.pattern); fork(w.r) != w.wantForkFiber {
			t.Errorf("pattern %q rune U+%04X: fork engine %v, want documented fiber %v (%s)", w.pattern, w.r, fork(w.r), w.wantForkFiber, w.note)
		}
	}
}

// TestSubtractionPredecessorConformanceR9 pins review round 9 finding
// 2: the fork recognizes -[...] subtraction after any completed member
// form (literal, range, shorthand, POSIX class or property) and
// consumes the complete nested class at both nesting levels, matching
// parser.go:1599. Membership on the fork dialect must equal the live
// fork engine over the full conformance spread. The RE2 dialect has no
// subtraction; the forms Go happens to accept are pinned equal to Go's
// own reading, while the forms Go rejects stay conservative atoms.
func TestSubtractionPredecessorConformanceR9(t *testing.T) {
	high := []rune{
		0x00B5, 0x017F, 0x212A, 0x212B, 0x039C, 0x03BC, 0x00C5, 0x00E5,
		0x0130, 0x0131, 0xFB00, 0x4E2D, 0x10FFFF, 0x0666, 0x1D7CE,
	}
	for _, tc := range []struct {
		pattern string
		note    string
	}{
		// Finding 2: subtraction after every member form, nesting included.
		{`[a-z-[b]]`, "review example: range predecessor"},
		{`(?i)[a-z-[b]]`, "folded range predecessor"},
		{`[A-Z-[B]]`, "uppercase range"},
		{`[\d-[5]]`, "shorthand predecessor"},
		{`[\w-[x]]`, "shorthand predecessor"},
		{`[[:alpha:]-[b]]`, "POSIX-class predecessor"},
		{`[\p{L}-[a]]`, "property predecessor"},
		{`[s-[b]]`, "single literal predecessor (round-8 form)"},
		{`[--[b]]`, "literal dash member then subtraction"},
		{`[a-[b-[c]]]`, "nested subtraction at the inner level"},
		{`[a-z-[b-[c]]]`, "nested subtraction after a range"},
		{`[0-[^0]]`, "negated nested operand"},
		{`(?i)[0-[^0]]`, "folded negated nested operand"},
		// Finding 2 round-8 rows kept exact.
		{`(?i)[s-[ſ]]`, "{s,S} live"},
		{`(?i)[µ-[μ]]`, "{µ} singleton"},
		{`(?i)[A-[µ]]`, "{a,A} minus µ"},
		// Finding 3 adjacent fold specials and the interval spelling.
		{`(?i)[k-[KÅ]]`, "{k,K,K} live: subtrahend canonicalizes to [K-Å]"},
		{`(?i)[k-[K-Å]]`, "equivalent interval spelling"},
		{`(?i)[k-[kK]]`, "empty: ToLower(K)=k subtracts out"},
		{`(?i)[KÅ]`, "empty: coalesced [K-Å] has no lcTable image"},
		{`(?i)[K-Å]`, "equivalent interval spelling, empty"},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRegexp2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative (%s)", tc.pattern, tc.note)
		}
		want := forkEngineMember(t, tc.pattern)
		for _, r := range conformanceRunes(high...) {
			if got(r) != want(r) {
				t.Errorf("pattern %q rune U+%04X: classifier %v, fork engine %v (%s)", tc.pattern, r, got(r), want(r), tc.note)
			}
		}
	}

	// RE2 side: Go 1.27.1 has no subtraction; it reads [a-z-[b]] as the
	// class {-, [, a-z} followed by the literal tail "b]" (Concat), so
	// the classifier's first class is pinned against Go's exact
	// concatSub membership dump (syntax.Parse on this toolchain).
	for _, tc := range []struct {
		pattern string
		members []rune
		absent  []rune
		note    string
	}{
		{`[a-z-[b]]`, []rune{'-', '[', 'a', 'z', 'b'}, []rune{']', '5', 'A'}, "Go class {-,[,a-z}; b rides the range"},
		{`(?i)[a-z-[b]]`, []rune{'-', '[', 'A', 'Z', 'a', 'z', 0x017F, 0x212A}, []rune{']', '5', 0x00B5}, "folded reading incl. fold specials"},
		{`[A-Z-[B]]`, []rune{'-', '[', 'A', 'Z'}, []rune{'a', ']', '5'}, "{-,[,A-Z}"},
		{`[\d-[5]]`, []rune{'-', '[', '0', '5', '9'}, []rune{'a', ']'}, "{-,[,Nd}"},
		{`[\w-[x]]`, []rune{'-', '[', '0', 'A', '_', 'a', 'x'}, []rune{']', '!'}, "{-,[,word}"},
		{`[[:alpha:]-[b]]`, []rune{'-', '[', 'A', 'Z', 'a', 'z'}, []rune{'5', ']'}, "{-,[,alpha}"},
		{`[\p{L}-[a]]`, []rune{'-', '[', 'a', 0x4E2D}, []rune{'5', ']'}, "{-,[,L}"},
		{`[--[b]]`, []rune{'-', '.', '/', '0', '3', '5', '[', 'b'}, []rune{'a', ']'}, "Go: range '-' to '[' (incl. digits) plus 'b'"},
		{`[0-[^0]]`, []rune{'0', '3', '5', '@', 'A', '[', '^'}, []rune{'/', ']', 'a'}, "Go: range '0' to '[' plus '^'"},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRE2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative on the RE2 route (%s)", tc.pattern, tc.note)
		}
		for _, r := range tc.members {
			if !got(r) {
				t.Errorf("pattern %q rune U+%04X: classifier false, Go reading true (%s)", tc.pattern, r, tc.note)
			}
		}
		for _, r := range tc.absent {
			if got(r) {
				t.Errorf("pattern %q rune U+%04X: classifier true, Go reading false (%s)", tc.pattern, r, tc.note)
			}
		}
	}
	// RE2 rejects x-[...]] with "invalid character class range" when the
	// lone predecessor sorts above '['; the classifier keeps the
	// conservative atom on the unreachable route.
	for _, pattern := range []string{`[s-[b]]`, `(?i)[µ-[μ]]`, `(?i)[s-[ſ]]`, `(?i)[k-[K]]`} {
		if _, conservative := captureClassMembers(t, pattern, DialectRE2); !conservative {
			t.Errorf("pattern %q: want conservative on the RE2 route (engine parse error)", pattern)
		}
	}
}

// TestAnyCanonicalConformanceR9 pins review round 9 finding 4: every
// canonical spelling of Any (any, a_n_y, ANY, Any - regexp/syntax
// canonicalName drops '_', '-' and ' ' and case-normalizes) resolves
// through the one goPropertyTable path to the same universe table, for
// bracketed and standalone properties, folded or not. Negated forms
// match nothing. The fork rejects every Any spelling at parse, so the
// fork dialect keeps the conservative recorded atom.
func TestAnyCanonicalConformanceR9(t *testing.T) {
	high := []rune{'\n', 0x00B5, 0x4E2D, 0x10FFFF}
	for _, pattern := range []string{
		`[\p{Any}]`, `[\p{any}]`, `[\p{a_n_y}]`, `[\p{ANY}]`,
		`[^\p{Any}]`, `[^\p{any}]`, `[^\p{a_n_y}]`, `[^\p{ANY}]`,
		`[\P{Any}]`, `[\P{ANY}]`, `[\p{^Any}]`,
		`(?i)[\p{ANY}]`, `(?i)[^\p{any}]`, `(?i)[\P{ANY}]`,
	} {
		got, conservative := captureClassMembers(t, pattern, DialectRE2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative on the RE2 route", pattern)
		}
		want := engineMember(t, pattern, false)
		for _, r := range conformanceRunes(high...) {
			if got(r) != want(r) {
				t.Errorf("pattern %q rune U+%04X: classifier %v, RE2 engine %v", pattern, r, got(r), want(r))
			}
		}
	}
	// Fork route: every Any spelling is a parse error there, so the
	// classifier keeps the conservative atom.
	for _, pattern := range []string{`[\p{Any}]`, `[^\p{any}]`, `(?i)[\p{ANY}]`} {
		events := DebugEvents(pattern, DialectRegexp2)
		if !closeEventIs(events, "class-close card=2") {
			t.Errorf("pattern %q: got %v, want the conservative many close (fork parse error)", pattern, events)
		}
	}
	// \p{^Any} rides the round-6 resolved-empty atom semantics: \P{Any}
	// provably covers nothing, so the fork dialect closes empty there
	// as well (engine still parse-errors; deterministic, unreachable).
	for _, pattern := range []string{`[\p{^Any}]`, `[\P{Any}]`} {
		events := DebugEvents(pattern, DialectRegexp2)
		if !closeEventIs(events, "class-close card=0") {
			t.Errorf("pattern %q: got %v, want the resolved-empty close (round-6 atom semantics)", pattern, events)
		}
	}
}
