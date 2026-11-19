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
	"testing"
)

// TestDescriptorTable pins the invariant-8 classification derived from the
// live ICU 77.1 probes (rows S/R/D/P/Q/C/V/E/X/A/B of the design oracle).
// Production patterns carry the wrapper's (?m) / (?im) prologues; the rows
// spell them explicitly.
func TestDescriptorTable(t *testing.T) {
	type row struct {
		pattern string
		dialect Dialect
		line    bool
		exempt  bool
		note    string
	}
	r2 := DialectRegexp2
	rows := []row{
		// String-exempt rows (S06/S14/C1/R2/R4/D7/D11/D9/C9/E1/E2/X2/B2/B4).
		{`^\nx`, DialectRE2, true, true, "S06 adjacent run"},
		{`^(?:\nx)`, DialectRE2, true, true, "S14 group-adjacent run"},
		{`^[\n]x`, DialectRE2, true, true, "C1 singleton joins run"},
		{`^\nx(?:|y)`, DialectRE2, true, true, "R2 string; arms after run"},
		{`^(?:\nx)+`, DialectRE2, true, true, "R4 plus keeps body first"},
		{`^(?:\nx){2,3}`, DialectRE2, true, true, "D7 group interval body once"},
		{`^(?:\nx){1}`, DialectRE2, true, true, "D11 inline {1}"},
		{`^\n\nx`, DialectRE2, true, true, "D9 adjacent LF run"},
		{`(?s)^\nx`, DialectRE2, true, true, "C9 leading flag seal is a no-op"},
		{`^(?:[^\x00-\x{10ffff}]|\nx)`, DialectRE2, true, true, "E1 dead branch keeps exemption"},
		{`^[^\x00-\x09\x0b-\x{10ffff}]x`, DialectRE2, true, true, "E2 negated singleton joins run"},
		{`^\n\x0b`, DialectRE2, true, true, "X2 hex escape joins run"},
		{`(?=())^\nx`, r2, true, true, "B2 no backreference run"},
		{`(?=(?<e>))^\nx`, r2, true, true, "B4 named group, no backreference"},
		{`^(?:\nxy|\nxz).?`, DialectRE2, false, false, "placeholder overwritten below"},
	}
	// Rows built through the m() wrapper are multiline RE2 rows; adjust the
	// placeholder and append the rest.
	rows = rows[:len(rows)-1]
	add := func(pattern string, dialect Dialect, line, exempt bool, note string) {
		rows = append(rows, row{pattern, dialect, line, exempt, note})
	}
	add(`^(?:\nxy|\nxz)`, DialectRE2, true, false, "D5 branch-forwarded initial units")
	add(`^(?:\nx|\nx)`, DialectRE2, true, false, "D4 two identical arms")
	add(`^\nx(?:y|z)`, DialectRE2, true, true, "D10 run sealed at group open")
	add(`^(?:\nxy|)`, DialectRE2, true, false, "D13 empty arm forces minLen 0")

	// Non-exempt line-anchored rows.
	add(`^\n`, DialectRE2, true, false, "single LF is a 1-char run")
	add(`^b`, DialectRE2, true, false, "issue's basic shape")
	add(`^(?:\n)x`, DialectRE2, true, false, "S13 group open seals the LF")
	add(`^(\n)x`, DialectRE2, true, false, "capture open seals")
	add(`^[\nx]`, DialectRE2, true, false, "non-singleton class consumes +2")
	add(`^[\n\r]x`, DialectRE2, true, false, "S11 verified non-singleton set")
	add(`^(?:\nx|y)`, DialectRE2, true, false, "R1 caret outside alternation")
	add(`^(?:\nx)*`, DialectRE2, true, false, "D6 star guard")
	add(`^\nx{1}`, DialectRE2, true, false, "D1 run split")
	add(`^\nx?`, DialectRE2, true, false, "D15 question guard")
	add(`^\nx+`, DialectRE2, true, false, "D16 plus trailing control")
	add(`^\nx{0}y`, DialectRE2, true, false, "D2 {0} deletes x; LF sealed by split")
	add(`^x{0}y`, DialectRE2, true, false, "R3b {0} delete keeps caret initial")
	add(`^(?:\nx){0}y`, DialectRE2, true, false, "D3 group {0} delete")
	add(`^\n{2}x`, DialectRE2, true, false, "D8 interval split differs from run")
	add(`^\n[x]`, DialectRE2, true, false, "C2 class open seals LF")
	add(`^[\n][x]`, DialectRE2, true, false, "C3 two classes")
	add(`^\n(?s)x`, DialectRE2, true, false, "C4 inline flag seal")
	add(`^\n(?s:x)`, DialectRE2, true, false, "C5 scoped group seals")
	add(`^\n(?i)x`, DialectRE2, true, false, "C6 fold flag seal")
	add(`^\n(?:x)`, DialectRE2, true, false, "C7 noncapture seals")
	add(`^\n\v`, DialectRE2, true, false, "X1 backslash-V is not a run literal")
	add(`^(?:[^\x00-\x{10ffff}]|x)`, DialectRE2, true, false, "E3 dead branch, live 1-char arm")
	add(`^\n$\nx`, DialectRE2, true, false, "A1 dollar seals the LF")
	add(`^\n\B\nx`, DialectRE2, true, false, "A2 non-boundary seals the LF")
	add(`^\n\bx`, DialectRE2, true, false, "word boundary seals the LF")
	add(`^\n\A`, DialectRE2, true, false, "absolute anchor seals the LF")
	add(`^\n\z`, DialectRE2, true, false, "absolute end seals the LF")
	add(`^\n\Zx`, r2, true, false, "EndZ seals the LF")
	add(`^\n\Gx`, r2, true, false, "\\G seals the LF")

	// Line-anchored via preserved atStart.
	add(`(^\n)`, DialectRE2, true, false, "S03 capture preserves atStart")
	add(`(?:^\n)`, DialectRE2, true, false, "S04 noncapture preserves atStart")
	add(`(?m:^\n)`, DialectRE2, true, false, "scoped flag group preserves")
	add(`\B^\n`, DialectRE2, true, false, "S05 boundary preserves atStart")
	add(`(?=b)^\n`, DialectRE2, true, false, "skipped lookahead preserves atStart")
	add(`(?!b)^\n`, DialectRE2, true, false, "negative lookahead skipped")
	add(`()\1^\n`, r2, true, false, "backreference preserves atStart")
	add(`x{0}^\n`, DialectRE2, true, false, "R3/Q1 ordinary {0} deletion")
	add(`(?:^\n)+`, DialectRE2, true, false, "V1 plus control after body")
	add(`(?:^\n)+?`, DialectRE2, true, false, "V3 lazy plus")
	add(`(?:^\n)++`, r2, true, false, "V5 possessive plus")
	add(`^\n1`, DialectRE2, true, true, "m only: LF+1 is one run")

	// Folded runs never exempt.
	add(`(?im)^\nx`, DialectRE2, true, false, "S07 folded run +2")
	add(`(?im)^\n1`, DialectRE2, true, false, "S17 folded run under mi")

	// Not line-anchored (bare mid-pattern carets).
	add(`^\n|z`, DialectRE2, false, false, "arm-start branch precedes the caret")
	add(`x?^\n`, DialectRE2, false, false, "question guard")
	add(`x{0,2}^\n`, DialectRE2, false, false, "D12 optional copies guard")
	add(`(\r)^\n`, DialectRE2, false, false, "consuming CR inside capture")
	add(`(?>(\r))^\n`, r2, false, false, "atomic body consumes")
	add(`(?=\r)\r^\n`, r2, false, false, "consuming CR after lookahead")
	add(`x{0}?^\n`, DialectRE2, false, false, "Q2 lazy keeps the counted loop")
	add(`x{0}+^\n`, r2, false, false, "Q3 possessive keeps the counted loop")
	add(`(?:^\n){1,}`, DialectRE2, false, false, "V2 interval init before body")
	add(`(?:^\n){1,}?`, DialectRE2, false, false, "V4 lazy interval")
	add(`(?:^\n){1,}+`, r2, false, false, "V6 possessive interval")
	add(`(?:^\n)*`, DialectRE2, false, false, "V8 star guard")
	add(`(?:^\n){0,}`, DialectRE2, false, false, "V7 star-spelled interval")
	add(`(?:^\n)?`, DialectRE2, false, false, "V10 question")
	add(`(?:^\n){0,1}`, DialectRE2, false, false, "V9 question-spelled interval")

	// Backreference seals (Rev 10).
	add(`(?=())^\n\1x`, r2, true, false, "B1 numbered backref seals LF")
	add(`(?=(?<e>))^\n\k<e>x`, r2, true, false, "B3 named backref seals LF")

	for _, r := range rows {
		// Production composes the match-type prologue before the user
		// pattern; a leading (?m) plus any pattern-embedded (?im)/(?m)
		// group has the same net classification state.
		pattern := "(?m)" + r.pattern
		got := Classify(pattern, r.dialect)
		if got.LineAnchoredStart != r.line || got.StringExempt != r.exempt {
			t.Errorf("pattern %q (%s): got {line=%v exempt=%v}, want {line=%v exempt=%v} (%s)\nevents: %v",
				pattern, dialectName(r.dialect), got.LineAnchoredStart, got.StringExempt, r.line, r.exempt, r.note, DebugEvents(pattern, r.dialect))
		}
	}
}

func dialectName(d Dialect) string {
	switch d {
	case DialectRegexp2:
		return "regexp2"
	case DialectRegexp2Binary:
		return "regexp2-binary"
	}
	return "re2"
}

// TestEventStream pins the scanner's event sequences for the constructs
// the design validation demands (quantifier FORM/flavor, ICU escape
// identity, group kinds, seals, backreferences, quoted runs).
func TestEventStream(t *testing.T) {
	type row struct {
		pattern string
		dialect Dialect
		want    []string
	}
	rows := []row{
		{`^\n\x0b`, DialectRE2, []string{"anchor 1", `literal U+000A`, `literal U+000B`, "end"}},
		{`^\n\v`, DialectRE2, []string{"anchor 1", `literal U+000A`, "backslash-v", "end"}},
		{`x{0}`, DialectRE2, []string{`literal U+0078`, "quant interval {0,0} ordinary", "end"}},
		{`x{0}?`, DialectRE2, []string{`literal U+0078`, "quant interval {0,0} lazy", "end"}},
		{`x*+`, DialectRegexp2, []string{`literal U+0078`, "quant star {0,-1} possessive", "end"}},
		{`(?U)x*?`, DialectRE2, []string{"flags", `literal U+0078`, "quant star {0,-1} ordinary", "end"}},
		{`(?U)x*`, DialectRE2, []string{"flags", `literal U+0078`, "quant star {0,-1} lazy", "end"}},
		{`x{2,4}?`, DialectRE2, []string{`literal U+0078`, "quant interval {2,4} lazy", "end"}},
		{`x{2,}`, DialectRE2, []string{`literal U+0078`, "quant interval {2,-1} ordinary", "end"}},
		{`(?:^\n){1,}`, DialectRE2, []string{"group-open noncapture", "anchor 1", `literal U+000A`, "group-close noncapture", "quant interval {1,-1} ordinary", "end"}},
		{`(?>a)+`, DialectRegexp2, []string{"group-open atomic", `literal U+0061`, "group-close atomic", "quant plus {1,-1} ordinary", "end"}},
		{`(?i)a`, DialectRE2, []string{"flags", `literal U+0061 fold=true`, "end"}},
		{`a|b`, DialectRE2, []string{`literal U+0061`, "branch", `literal U+0062`, "end"}},
		{`\Q\n\E`, DialectRE2, []string{`literal U+005C`, `literal U+006E`, "end"}},
		{`(?=(?<e>))^\n\k<e>x`, DialectRegexp2, []string{"group-open lookahead", "group-open capture", "group-close capture", "group-close lookahead", "anchor 1", `literal U+000A`, "backref", `literal U+0078`, "end"}},
		{`()\1^\n`, DialectRegexp2, []string{"group-open capture", "group-close capture", "backref", "anchor 1", `literal U+000A`, "end"}},
		{`(?#c)a`, DialectRegexp2, []string{`literal U+0061`, "end"}},
		{`(?x)a b # c` + "\n" + `d`, DialectRegexp2, []string{"flags", `literal U+0061`, `literal U+0062`, `literal U+0064`, "end"}},
		{`[\n]x`, DialectRE2, []string{"class-open", `class-close card=1 U+000A`, `literal U+0078`, "end"}},
		{`[^\n]x`, DialectRE2, []string{"class-open", "class-close card=2", `literal U+0078`, "end"}},
		{`[[:alpha:]]x`, DialectRE2, []string{"class-open", "class-close card=2", `literal U+0078`, "end"}},
		{`\dx`, DialectRE2, []string{"class-open", "class-close card=2", `literal U+0078`, "end"}},
		{`.x`, DialectRE2, []string{"class-open", "class-close card=2", `literal U+0078`, "end"}},
		{`x{`, DialectRE2, []string{`literal U+0078`, `literal U+007B`, "end"}},
	}
	for _, r := range rows {
		events := DebugEvents(r.pattern, r.dialect)
		if len(events) != len(r.want) {
			t.Errorf("pattern %q: got %d events, want %d: %v", r.pattern, len(events), len(r.want), events)
			continue
		}
		for i, w := range r.want {
			got := shortEvent(events[i])
			if got != w {
				t.Errorf("pattern %q event %d: got %q want %q (all: %v)", r.pattern, i, got, w, events)
				break
			}
		}
	}
}

// shortEvent renders the event attributes relevant to the stream contract.
func shortEvent(e Event) string {
	switch e.Kind {
	case KindAnchor:
		return fmt.Sprintf("anchor %d", int(e.Anchor))
	case KindFlags:
		return "flags"
	case KindLiteral:
		if e.Fold {
			return fmt.Sprintf("literal U+%04X fold=true", e.Rune)
		}
		return fmt.Sprintf("literal U+%04X", e.Rune)
	case KindQuant:
		return fmt.Sprintf("quant %s {%d,%d} %s", e.Form, e.Min, e.Max, e.Flavor)
	case KindBranch:
		return "branch"
	case KindBackref:
		return "backref"
	case KindBackslashV:
		return "backslash-v"
	case KindClassOpen:
		return "class-open"
	case KindClassClose:
		if e.Card == CardOne {
			return fmt.Sprintf("class-close card=1 U+%04X", e.Rune)
		}
		return fmt.Sprintf("class-close card=%d", int(e.Card))
	case KindGroupOpen:
		return "group-open " + e.Group.String()
	case KindGroupClose:
		return "group-close " + e.Group.String()
	case KindEnd:
		return "end"
	case KindOpaque:
		return "opaque"
	}
	return "?"
}

// TestDescriptorPrologueEquivalence asserts the wrapper's (?im) prologue
// classifies identically to an embedded (?i) plus leading (?m).
func TestDescriptorPrologueEquivalence(t *testing.T) {
	a := Classify(`(?im)^\nx`, DialectRE2)
	b := Classify(`(?i)(?m)^\nx`, DialectRE2)
	c := Classify(`(?m)(?i)^\nx`, DialectRE2)
	if a != b || a != c {
		t.Errorf("prologue forms diverge: %v %v %v", a, b, c)
	}
}

// TestSuppressCRLFInterior ties the descriptor to the mask policy shape.
func TestSuppressCRLFInterior(t *testing.T) {
	d := Classify(`(?m)^\nx`, DialectRE2)
	if d.SuppressCRLFInterior() {
		t.Error("string-exempt pattern must not mask the CRLF interior")
	}
	d = Classify(`(?m)^b`, DialectRE2)
	if !d.SuppressCRLFInterior() {
		t.Error("line-anchored non-exempt pattern must mask the CRLF interior")
	}
	d = Classify(`(?m)^\n|z`, DialectRE2)
	if d.SuppressCRLFInterior() {
		t.Error("bare arm carets must not mask the CRLF interior")
	}
}
