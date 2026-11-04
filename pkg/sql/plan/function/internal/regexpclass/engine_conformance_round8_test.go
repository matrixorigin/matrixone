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
	"regexp/syntax"
	"testing"

	regexp2 "github.com/matrixorigin/matrixone/pkg/sql/plan/function/internal/regexp2"
)

// TestRE2EngineConformanceRound8 compares the classifier's exact
// member sets with the Go RE2 parser's classes for the round-8
// folded-property constructs: canonical property names and aliases,
// the Any/Assigned/Ascii specials, and the FoldCategory/FoldScript
// companion merges Go applies under FoldCase (Go 1.27.1
// syntax/parse.go). Membership must agree on every probe rune.
func TestRE2EngineConformanceRound8(t *testing.T) {
	high := []rune{
		0x00B5, 0x017F, 0x212A, 0x039C, 0x03BC, // fold specials
		0xFB00, 0xFB01, 0xFB03, 0x1E9E, // ligature/capital-sharp residuals
		0x02B0, 0x4E2D, 0x10FFFF, 0x0130, 0x0131, // modifier, Lo, boundary, dotless
	}
	for _, tc := range []struct {
		pattern string
		fold    bool
		note    string
	}{
		{`[^\p{Letter}\P{Lu}]`, false, "alias L superset of Lu across complements"},
		{`(?i)[^\p{L}\P{Lu}]`, true, "review example: folded empty"},
		{`(?i)[^\p{Letter}\P{Lu}]`, true, "folded alias empty"},
		{`(?i)[^\p{Lu}\P{Ll}]`, true, "FB00 ligature orbit survives in Ll minus Lu"},
		{`(?i)[^\p{Assigned}\p{Cn}]`, true, "Assigned is the sign-flipped Cn complement"},
		{`(?i)[^\p{Assigned}\P{Cn}]`, true, "Assigned == \\P{Cn}: residual Cn"},
		{`(?i)[\p{Ascii}]`, true, "Ascii special case carries its fold table"},
		{`(?i)[\p{Greek}]`, true, "FoldScript companion merge"},
		{`(?i)[^\p{Greek}\P{Greek}]`, true, "self-complement goes empty"},
		{`(?i)[\p{LC}]`, true, "canonical Lc lookup"},
	} {
		got, conservative := captureClassMembers(t, tc.pattern, DialectRE2)
		if conservative {
			t.Fatalf("pattern %q: classifier stayed conservative, conformance impossible (%s)", tc.pattern, tc.note)
		}
		want := engineMember(t, tc.pattern, tc.fold)
		for _, r := range conformanceRunes(high...) {
			if got(r) != want(r) {
				t.Errorf("pattern %q rune U+%04X: classifier %v, RE2 engine %v (%s)", tc.pattern, r, got(r), want(r), tc.note)
			}
		}
	}
}

// forkEngineMember compiles pattern through the production fork path
// (Compile with the RE2 option) and returns its single-rune membership
// predicate.
func forkEngineMember(t *testing.T, pattern string) func(rune) bool {
	t.Helper()
	re, err := regexp2.Compile(fmt.Sprintf("^(?:%s)$", pattern), regexp2.RE2)
	if err != nil {
		t.Fatalf("pattern %q: fork compile error: %v", pattern, err)
	}
	return func(r rune) bool {
		ok, err := re.MatchString(string(r))
		if err != nil {
			t.Fatalf("pattern %q rune U+%04X: fork match error: %v", pattern, r, err)
		}
		return ok
	}
}

// TestRegexp2EngineConformanceRound8 compares the classifier's exact
// member sets with the live fork engine for the round-8 constructs:
// the fork's lowercase fold semantics for subtraction classes and
// folded categories/properties (addLowercase at charclass.go:655-670
// with the lcTable images, the Ll/Lu/Lt case-insensitive triple at
// charclass.go:533-539, and the runner's ToLower-at-input at
// runner.go:1181-1182) - never Go's SimpleFold orbits. Membership must
// agree with the production engine on every probe rune.
func TestRegexp2EngineConformanceRound8(t *testing.T) {
	high := []rune{
		0x00B5, 0x017F, 0x212A, 0x039C, 0x03BC, 0x0130, 0x0131,
		0xFB00, 0xFB01, 0x1E9E, 0x02B0, 0x4E2D, 0x10FFFF,
		0x0666, 0x1D7CE, 0xFEFF, 0x2028, 0x2029, 0x3000,
	}
	for _, tc := range []struct {
		pattern string
		note    string
	}{
		// Finding 2: folded subtraction across differing fold classes.
		{`(?i)[s-[ſ]]`, "review example: {s,S} live"},
		{`(?i)[S-[ſ]]`, "uppercase orientation"},
		{`(?i)[ſ-[s]]`, "{ſ} singleton"},
		{`(?i)[µ-[μ]]`, "{µ} singleton"},
		{`(?i)[μ-[µ]]`, "{Μ,μ} live"},
		{`(?i)[A-[µ]]`, "{a,A} minus µ"},
		{`(?i)[k-[ſ]]`, "{k,K,\\u212A} live"},
		{`(?i)[s-[S]]`, "fork-empty"},
		{`[s-[ſ]]`, "unfolded {s}"},
		{`(?i)[0-[^0]]`, "digits fold trivially"},
		{`(?i)[0-[^1]]`, "empty"},
		{`[0-[^5]]`, "empty unfolded"},
		{`(?i)[^0-[0]]`, "complement minus singleton"},
		{`(?i)[a-[^A]]`, "round-7 pin: {a,A}"},
		{`(?i)[A-[^a]]`, "orientation"},
		{`(?i)[a-[^b]]`, "empty"},
		{`[a-[^A]]`, "empty unfolded"},
		{`[a-[^a]]`, "singleton unfolded"},
		{`(?i)[ab-[^AB]]`, "two-rune minuend"},
		{`(?i)[^a-[b]]`, "complement before subtraction"},
		{`(?i)[k-[^k]]`, "Kelvin orbit partners"},
		{`(?i)[a-[\d]]`, "folded minuend minus digits"},
		// Folded literal classes: fork lowercase, not SimpleFold.
		{`(?i)[s]`, "{s,S} only: ſ is not a fork fold partner of s"},
		{`(?i)[^s]`, "complement excludes ſ"},
		{`(?i)[k]`, "{k,K,\\u212A}"},
		{`(?i)[^k]`, "complement"},
		{`(?i)[ſ]`, "{ſ} only"},
		{`(?i)[a-z]`, "includes U+0130 via ToLower(İ)=i"},
		{`(?i)[^a-z]`, "complement keeps ſ"},
		// Folded POSIX tables on the fork's exact memberships.
		{`(?i)[[:alpha:]]`, "ASCII alpha under fork lowercase"},
		{`(?i)[[:^alpha:]]`, "negated alpha with lcTable set-piece images"},
		{`(?i)[[:digit:]]`, "Nd under fork lowercase"},
		{`(?i)[[:^digit:]]`, "complement of Nd category"},
		{`(?i)[[:space:]]`, "ECMA space list"},
		{`(?i)[[:^space:]]`, "complement"},
		// Finding 1: folded properties. Round 9 re-derives their
		// structural membership from the ICU compatibility contract
		// (per-operand tab U fold-companion closure, complement after
		// the merge), which the fork's consuming behavior diverges
		// from at the fold-special fibers; those rows moved to
		// TestForkFoldPropertyEngineSplitR9 with their documented
		// divergence witnesses. The rows kept below are the cased
		// property families whose closure agrees on both engines.
		{`(?i)[\p{LC}]`, "preimage over LC: LC closure is engine-stable"},
		{`(?i)[\P{LC}]`, "complement of preimage over Ll"},
		{`(?i)[\p{Lm}]`, "modifier letters: no case pairs, engines agree"},
		{`(?i)[\p{White_Space}]`, "fork-only property table"},
		{`(?i)[\P{White_Space}]`, "complement"},
		{`(?i)[^\p{L}\P{Lu}]`, "review example: empty on every reading"},
		{`(?i)[^\p{White_Space}\S]`, "empty: superset pair"},
		// Unfolded guards from earlier rounds. [^\P{Lu}\P{Ll}] is
		// absent on purpose: the fork's first-hit category chain
		// (charclass.go CharIn) evaluates a negative entry before later
		// members are reached, so the fork engine reads that class as
		// \p{Lu} while the classifier keeps ICU's De Morgan reading
		// (empty, round-7 pins). The runtime divergence is
		// engine-level, predates this change, and is unchanged by it,
		// so an engine-membership row here would pin a known
		// divergence, not a repair.
		{`[^\p{L}\P{Lu}]`, "round-7 empty"},
		{`[^[:digit:]\D]`, "round-7 empty"},
		{`[^[:space:]\S]`, "round-7 empty"},
		{`[\p{White_Space}]`, "fork-only property, unfolded"},
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
}

// TestPropertyNameRouteRejectsR8 pins the route-level truth for
// property names one production engine supports and the other rejects:
// the fork compiler (regexp2.RE2) rejects Go RE2's aliases and
// specials, and Go's parser rejects the fork's Properties entries.
// Classifier rows for these names stay conservative atoms, matching the
// unreachable-on-route contract.
func TestPropertyNameRouteRejectsR8(t *testing.T) {
	for _, pattern := range []string{
		`\p{Letter}`, `(?i)\p{Letter}`, `\p{Any}`, `(?i)\p{Any}`,
		`\p{Assigned}`, `(?i)\p{Assigned}`, `\p{WhiteSpace}`, `\p{lowercaseletter}`,
	} {
		if _, err := regexp2.Compile(pattern, regexp2.RE2); err == nil {
			t.Errorf("pattern %q: fork unexpectedly accepted", pattern)
		}
	}
	for _, pattern := range []string{
		`\p{White_Space}`, `(?i)\p{White_Space}`, `\p{WhiteSpace}`,
	} {
		if _, err := syntax.Parse(pattern, syntax.Perl); err == nil {
			t.Errorf("pattern %q: Go RE2 unexpectedly accepted", pattern)
		}
	}
	// Go RE2 accepts exactly these special forms and aliases (the
	// canonicalized name lookup folds lowercaseletter to the
	// Lowercase_Letter alias of Ll).
	for _, pattern := range []string{
		`\p{Any}`, `\p{Assigned}`, `\p{Ascii}`, `\p{Letter}`, `(?i)\p{Letter}`, `\p{LC}`, `\pL`,
		`\p{lowercaseletter}`, `\p{Lowercase_Letter}`,
	} {
		if _, err := syntax.Parse(pattern, syntax.Perl); err != nil {
			t.Errorf("pattern %q: Go RE2 parse error: %v", pattern, err)
		}
	}
}
