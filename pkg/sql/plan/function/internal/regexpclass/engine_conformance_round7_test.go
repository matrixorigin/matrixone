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
	"regexp/syntax"
	"testing"
	"unicode"
)

// captureClassMembers installs classMembersHook, scans pattern in the
// dialect, and returns the effective membership predicate of the FIRST
// class (negation already folded in) plus whether it stayed exact.
func captureClassMembers(t *testing.T, pattern string, dialect Dialect) (func(rune) bool, bool) {
	t.Helper()
	type shot struct {
		acc    *classAccumulator
		negate bool
	}
	var shots []shot
	classMembersHook = func(acc *classAccumulator, negate, fold bool) {
		cp := &classAccumulator{conservative: acc.conservative}
		cp.intervals = append(cp.intervals, acc.intervals...)
		cp.atoms = append(cp.atoms, acc.atoms...)
		shots = append(shots, shot{cp, negate})
	}
	defer func() { classMembersHook = nil }()
	DebugEvents(pattern, dialect)
	if len(shots) == 0 {
		t.Fatalf("pattern %q scanned no class", pattern)
	}
	if len(shots) > 1 {
		t.Fatalf("pattern %q scanned %d classes, want one", pattern, len(shots))
	}
	s := shots[0]
	return func(r rune) bool {
		m := s.acc.member(r)
		if s.negate {
			return !m
		}
		return m
	}, s.acc.conservative
}

// engineMember walks a parsed Go RE2 expression and returns the
// membership predicate of its root class. The RE2 parse already folds
// class contents when FoldCase is set, so the parsed rune set is the
// folded set.
func engineMember(t *testing.T, pattern string, fold bool) func(rune) bool {
	t.Helper()
	flags := syntax.Perl
	if fold {
		flags |= syntax.FoldCase
	}
	re, err := syntax.Parse(pattern, flags)
	if err != nil {
		t.Fatalf("pattern %q: RE2 parse error: %v", pattern, err)
	}
	switch re.Op {
	case syntax.OpCharClass:
	case syntax.OpAnyChar:
		return func(rune) bool { return true } // simplified full class
	case syntax.OpAnyCharNotNL:
		return func(r rune) bool { return r != '\n' }
	case syntax.OpNoMatch:
		return func(rune) bool { return false } // simplified empty class
	case syntax.OpLiteral:
		want := re.Rune[0]
		if re.Flags&syntax.FoldCase != 0 {
			return func(r rune) bool {
				for f := unicode.SimpleFold(want); f != want; f = unicode.SimpleFold(f) {
					if r == f {
						return true
					}
				}
				return r == want
			}
		}
		return func(r rune) bool { return r == want }
	default:
		t.Fatalf("pattern %q: RE2 parsed to %v, want a class-shaped op", pattern, re.Op)
	}
	return func(r rune) bool {
		i := 0
		for i = 0; i+1 < len(re.Rune); i += 2 {
			if r < re.Rune[i] {
				return false
			}
			if r <= re.Rune[i+1] {
				return true
			}
		}
		return false
	}
}

// conformanceRunes is the probe universe: the dense low range plus every
// boundary +-1 of both sets under comparison is covered by the dense
// range below 0x3000 for the constructs here; the high sample adds fold
// specials, Unicode digits and property boundary runes.
func conformanceRunes(extra ...rune) []rune {
	out := make([]rune, 0, 0x3000+64)
	for r := rune(0); r < 0x3000; r++ {
		out = append(out, r)
	}
	out = append(out, extra...)
	return out
}

// TestRE2EngineConformanceRound7 compares the classifier's exact member
// sets with the real Go RE2 engine's parsed classes for the round-7
// constructs: mixed POSIX/shorthand/property coverage and operand-level
// folding. Membership must agree on every probe rune.
func TestRE2EngineConformanceRound7(t *testing.T) {
	high := []rune{
		0x017F, 0x212A, 0x00B5, 0x039C, 0x03BC, // fold specials
		0x0660, 0x0666, 0x0669, 0xFF10, 0x1D7CE, 0x1D7FF, // Nd members
		0x00A8, 0x00B7, 0x02C2, 0x203F, 0xE000, 0x10FFFF, // non-members
	}
	for _, tc := range []struct {
		pattern string
		fold    bool
	}{
		{`[^[:digit:]\D]`, false},
		{`[[:digit:]\D]`, false},
		{`[^[:space:]\S]`, false},
		{`[^\s[:^space:]]`, false},
		{`[^[:alnum:][:^alpha:]]`, false},
		{`[^\p{L}\P{Lu}]`, false},
		{`[^\P{Lu}\P{Ll}]`, false},
		{`[^\p{L}\S]`, false},
		{`(?i)[^a-z]`, true},
		{`(?i)[[:^alpha:]]`, true},
		{`(?i)[^k]`, true},
		{`(?i)[^\x00-\x42\x44-\x{10ffff}\d]`, true},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRE2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative, conformance impossible", tc.pattern)
		}
		want := engineMember(t, tc.pattern, tc.fold)
		for _, r := range conformanceRunes(high...) {
			if got(r) != want(r) {
				t.Errorf("pattern %q rune U+%04X: classifier %v, RE2 engine %v", tc.pattern, r, got(r), want(r))
			}
		}
	}
}

// TestRegexp2EngineConformanceRound7 pins the classifier's exact member
// sets against the fork's documented membership for the regexp2 dialect.
// The fork tables come from internal/regexp2/syntax/charclass.go
// (addNamedASCII: digit -> \p{Nd}, space -> the ECMAScript list, word ->
// ECMA word; addDigit/addSpace/addWord with the production RE2-mode
// flags) and its operand-level fold ordering (parser.go:1628
// addLowercase before the CharSet negation at charclass.go:281-288);
// each predicate was probe-verified against the fork itself
// (fork-probe-round7.log).
func TestRegexp2EngineConformanceRound7(t *testing.T) {
	for _, tc := range []struct {
		pattern string
		members []rune
		absent  []rune
		note    string
	}{
		{`[^[:digit:]\D]`, nil, []rune{'5', 'a', 0x0666, 0x1D7CE, '+'}, "empty: Nd unions ~Nd"},
		{`[[:digit:]]`, []rune{'5', 0x0666, 0x1D7CE}, []rune{'a', '+', 0x00B2}, "fork digit is Unicode Nd"},
		{`[[:^digit:]]`, []rune{'a', '+', 0x00B2}, []rune{'5', 0x0666, 0x1D7CE}, "negated Nd"},
		{`[[:space:]]`, []rune{0x09, 0x0B, 0x20, 0xA0, 0x1680, 0x2000, 0x200A, 0x2028, 0x2029, 0x202F, 0x205F, 0x3000, 0xFEFF}, []rune{0x85, 0x00, 'a'}, "fork space is the ECMA list (no NEL)"},
		{`[^[:space:]\S]`, nil, []rune{0x20, 0xA0, 0x85, 'a'}, "empty: ECMA space superset of \\s unions ~\\s covers all"},
		{`[^[:word:]\W]`, nil, []rune{'a', '_', '5', '+'}, "empty: word unions ~word"},
		{`[^\p{L}\P{Lu}]`, nil, []rune{'a', 'A', 0x0391, '5'}, "empty: Lu subset of L"},
		{`[^\P{Lu}\P{Ll}]`, nil, []rune{'a', 'A', '5'}, "empty: Lu and Ll disjoint"},
		{`[^\p{White_Space}\S]`, nil, []rune{0x20, 0x85, 0xA0, 'a'}, "empty: White_Space superset of \\s (fork merges unicode.Properties)"},
		{`(?i)[a-[^A]]`, []rune{'a', 'A'}, []rune{'b', 'B', '5'}, "folded operands leave {a,A} live"},
		{`(?i)[A-[^a]]`, []rune{'a', 'A'}, []rune{'b', 'z'}, "other orientation"},
		{`(?i)[k-[^k]]`, []rune{'k', 'K', 0x212A}, []rune{'j', 'l'}, "Kelvin sign joins the orbit"},
		{`(?i)[ab-[^AB]]`, []rune{'a', 'b', 'A', 'B'}, []rune{'c', 'C'}, "two-rune minuend under fold"},
		{`(?i)[0-[^0]]`, []rune{'0'}, []rune{'1', 'a'}, "digits fold trivially"},
		{`(?i)[a-[^b]]`, nil, []rune{'a', 'A', 'b', 'B'}, "folded empty result"},
		{`[a-[^A]]`, nil, []rune{'a', 'A'}, "unfolded stays empty"},
		{`[a-[^a]]`, []rune{'a'}, []rune{'A', 'b'}, "round-6 truth unchanged"},
		{`(?i)[^a-[b]]`, []rune{'c', 'C', '5'}, []rune{'a', 'A', 'b', 'B'}, "complement before subtraction under fold"},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRegexp2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative (%s)", tc.pattern, tc.note)
		}
		for _, r := range tc.members {
			if !got(r) {
				t.Errorf("pattern %q rune U+%04X: classifier false, fork engine true (%s)", tc.pattern, r, tc.note)
			}
		}
		for _, r := range tc.absent {
			if got(r) {
				t.Errorf("pattern %q rune U+%04X: classifier true, fork engine false (%s)", tc.pattern, r, tc.note)
			}
		}
	}
}
