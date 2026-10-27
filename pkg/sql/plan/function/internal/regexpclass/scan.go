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
	"unicode"
	"unicode/utf8"
)

// scan lexes pattern in the given dialect into the shared pre-lowering
// event vocabulary. The scanner never fails and never parses for match
// semantics: the engines validate patterns before classification, so any
// construct the scanner cannot mirror is emitted conservatively.
func scan(pattern string, dialect Dialect) []Event {
	sc := &scanner{s: pattern, dialect: dialect}
	for i := 0; i < len(pattern); {
		sc.step(i)
		i = sc.pos
	}
	sc.resolveNumericRefs()
	sc.emit(Event{Kind: KindEnd})
	return sc.events
}

// scanner holds the lexing state. Flags tracked: fold (i) and m for the
// caret kind on both dialects, U for the RE2 (?U) laziness swap, and x for
// the regexp2 free-spacing mode. RightToLeft (r) is not mirrored: any RTL
// scope classifies conservatively.
type scanner struct {
	s       string
	dialect Dialect
	pos     int
	events  []Event

	// countOnly runs the capture-count pre-pass: events are discarded and
	// only the counting state matters.
	countOnly bool
	// sink, when set, receives each event as it is produced (streaming
	// classification); otherwise events accumulate in sc.events.
	sink *core
	// totalCaptures is the whole-pattern capture total used to resolve
	// numeric backreference events in streaming mode.
	totalCaptures int

	fold     bool
	mOn      bool
	ungreedy bool
	freesp   bool
	rtl      bool

	flagStack  []flagSet
	groupKinds []GroupKind

	// activeClass is the innermost open class accumulator while a class
	// body is being parsed.
	activeClass *classAccumulator

	captures    int
	numericRefs []numericRef
}

type flagSet struct {
	fold, mOn, ungreedy, freesp, rtl bool
}

// numericRef is a \<digits> escape whose backreference-vs-octal resolution
// needs the pattern's total capture count.
type numericRef struct {
	eventIdx int
	number   int
	fold     bool
}

func (sc *scanner) emit(e Event) {
	if sc.countOnly {
		return
	}
	if sc.sink != nil {
		sc.sink.step(e)
		return
	}
	e.Ordinal = len(sc.events)
	sc.events = append(sc.events, e)
}

func (sc *scanner) flags() flagSet {
	return flagSet{sc.fold, sc.mOn, sc.ungreedy, sc.freesp, sc.rtl}
}

func (sc *scanner) setFlags(f flagSet) {
	sc.fold, sc.mOn, sc.ungreedy, sc.freesp, sc.rtl = f.fold, f.mOn, f.ungreedy, f.freesp, f.rtl
}

// step consumes the construct beginning at byte offset i.
func (sc *scanner) step(i int) {
	s := sc.s
	c, size := utf8.DecodeRuneInString(s[i:])
	sc.pos = i + size

	// Free-spacing whitespace and #-comments (regexp2 dialect, (?x)).
	if sc.dialect.regexp2() && sc.freesp && isForkSpace(c) {
		return
	}
	if sc.dialect.regexp2() && sc.freesp && c == '#' {
		for sc.pos < len(s) {
			c2, sz2 := utf8.DecodeRuneInString(s[sc.pos:])
			sc.pos += sz2
			if c2 == '\n' {
				break
			}
		}
		return
	}

	switch c {
	case '(':
		sc.scanGroup(i)
	case ')':
		if n := len(sc.flagStack); n > 0 {
			sc.setFlags(sc.flagStack[n-1])
			sc.flagStack = sc.flagStack[:n-1]
			sc.emit(Event{Kind: KindGroupClose, Group: sc.groupKinds[n-1]})
			sc.groupKinds = sc.groupKinds[:n-1]
		}
		// Unbalanced ')' cannot occur in an engine-accepted pattern; ignore.
	case '[':
		sc.scanClass(i)
	case '|':
		sc.emit(Event{Kind: KindBranch})
	case '^':
		if sc.mOn {
			sc.emit(Event{Kind: KindAnchor, Anchor: AnchorCaretLine})
		} else {
			sc.emit(Event{Kind: KindAnchor, Anchor: AnchorCaretText})
		}
	case '$':
		sc.emit(Event{Kind: KindAnchor, Anchor: AnchorDollar})
	case '.':
		sc.emit(Event{Kind: KindClassOpen})
		sc.emit(Event{Kind: KindClassClose, Card: CardMany})
	case '*', '+', '?':
		form := FormStar
		if c == '+' {
			form = FormPlus
		} else if c == '?' {
			form = FormQuestion
		}
		sc.finishQuant(form, 0, 0)
	case '{':
		if min, max, end, ok := parseInterval(s, i); ok {
			sc.pos = end
			sc.finishQuant(FormInterval, min, max)
		} else {
			sc.emitLiteral('{')
		}
	case '\\':
		sc.scanEscape(i)
	default:
		sc.emitLiteral(c)
	}
}

func (sc *scanner) emitLiteral(r rune) {
	sc.emit(Event{Kind: KindLiteral, Rune: r, Fold: sc.fold})
}

// finishQuant parses the lazy/possessive suffix, applies the RE2 (?U)
// laziness swap and emits the quantifier event.
func (sc *scanner) finishQuant(form QuantForm, min, max int) {
	if form == FormStar {
		min, max = 0, -1
	} else if form == FormPlus {
		min, max = 1, -1
	} else if form == FormQuestion {
		min, max = 0, 1
	}
	flavor := FlavorOrdinary
	if sc.pos < len(sc.s) {
		c, _ := utf8.DecodeRuneInString(sc.s[sc.pos:])
		if c == '?' {
			sc.pos++
			flavor = FlavorLazy
		} else if c == '+' {
			sc.pos++
			flavor = FlavorPossessive
		}
	}
	if sc.dialect == DialectRE2 && sc.ungreedy && flavor != FlavorPossessive {
		if flavor == FlavorOrdinary {
			flavor = FlavorLazy
		} else {
			flavor = FlavorOrdinary
		}
	}
	sc.emit(Event{Kind: KindQuant, Form: form, Min: min, Max: max, Flavor: flavor})
}

// parseInterval validates Go/.NET style {n} / {n,} / {n,m} repeats. Both
// engines treat a non-valid '{' as a literal, so ok=false means literal.
func parseInterval(s string, i int) (min, max, end int, ok bool) {
	j := i + 1
	n1, j1 := decimal(s, j)
	if j1 == j {
		return 0, 0, 0, false
	}
	j = j1
	if j < len(s) && s[j] == '}' {
		return n1, n1, j + 1, true
	}
	if j >= len(s) || s[j] != ',' {
		return 0, 0, 0, false
	}
	j++
	if j < len(s) && s[j] == '}' {
		if n1 < 0 {
			return 0, 0, 0, false
		}
		return n1, -1, j + 1, true
	}
	n2, j2 := decimal(s, j)
	if j2 == j {
		return 0, 0, 0, false
	}
	j = j2
	if j >= len(s) || s[j] != '}' {
		return 0, 0, 0, false
	}
	return n1, n2, j + 1, true
}

// decimal scans ASCII digits at s[i:], returning value and end offset.
func decimal(s string, i int) (int, int) {
	n := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		n = n*10 + int(s[i]-'0')
		if n > 1<<24 {
			n = 1 << 24 // cap runaway values; treat as huge counted loop
		}
		i++
	}
	return n, i
}

// scanGroup consumes '(' or '(?...' constructs.
func (sc *scanner) scanGroup(i int) {
	s := sc.s
	if sc.pos >= len(s) || s[sc.pos] != '?' {
		sc.openGroup(GroupCapture, true)
		return
	}
	if sc.pos+1 < len(s) && s[sc.pos+1] == ')' {
		// "(?)" is an empty capture per the fork's scanGroupOpen.
		sc.pos++
		sc.openGroup(GroupCapture, true)
		return
	}
	if sc.pos+1 < len(s) && s[sc.pos+1] == '#' {
		// (?# comment): fork scanBlank skips to the first ')'; no events.
		sc.pos += 2
		for sc.pos < len(s) && s[sc.pos] != ')' {
			sc.pos++
		}
		if sc.pos < len(s) {
			sc.pos++
		}
		return
	}
	sc.pos++ // consume '?'
	if sc.pos >= len(s) {
		sc.emit(Event{Kind: KindOpaque})
		return
	}
	c := s[sc.pos]
	switch {
	case c == ':':
		sc.pos++
		sc.openGroup(GroupNonCapture, true)
	case c == '>':
		if sc.dialect.regexp2() {
			sc.pos++
			sc.openGroup(GroupAtomic, true)
			return
		}
		sc.opaqueGroup(i)
	case c == '=' || c == '!':
		sc.pos++
		sc.openGroup(GroupLookahead, true)
	case c == '<' || c == '\'':
		if !sc.dialect.regexp2() {
			sc.opaqueGroup(i)
			return
		}
		if sc.pos+1 < len(s) && (s[sc.pos+1] == '=' || s[sc.pos+1] == '!') {
			sc.pos += 2
			sc.openGroup(GroupLookbehind, true)
			return
		}
		// (?<name> / (?'name' capture.
		close := byte('>')
		if c == '\'' {
			close = '\''
		}
		sc.pos++
		for sc.pos < len(s) && s[sc.pos] != close {
			sc.pos++
		}
		if sc.pos < len(s) {
			sc.pos++
		}
		sc.openGroup(GroupCapture, true)
	case c == 'P' && sc.pos+1 < len(s) && (s[sc.pos+1] == '<' || s[sc.pos+1] == '=') && (sc.dialect.regexp2() || s[sc.pos+1] == '<'):
		if s[sc.pos+1] == '=' {
			// (?P=name) is a named backreference.
			sc.pos += 2
			for sc.pos < len(s) && s[sc.pos] != ')' {
				sc.pos++
			}
			if sc.pos < len(s) {
				sc.pos++
			}
			sc.emit(Event{Kind: KindBackref})
			return
		}
		sc.pos += 2
		for sc.pos < len(s) && s[sc.pos] != '>' {
			sc.pos++
		}
		if sc.pos < len(s) {
			sc.pos++
		}
		sc.openGroup(GroupCapture, true)
	case c == '(' && sc.dialect.regexp2():
		// (?(cond)yes|no): fork Testref; classify conservatively and skip.
		sc.pos++
		sc.skipBalanced(i)
		sc.emit(Event{Kind: KindOpaque})
	case isFlagByte(c) || c == '-':
		// '-' opens a negated-only flag header ((?-i) / (?-i:...)), valid
		// in both dialects; the flag scanner validates the rest.
		sc.scanFlagsGroup()
	default:
		// '(?' followed by anything else is an engine error; opaque.
		sc.opaqueGroup(i)
	}
}

// openGroup pushes flag state and emits the open event.
func (sc *scanner) openGroup(kind GroupKind, counted bool) {
	sc.flagStack = append(sc.flagStack, sc.flags())
	sc.groupKinds = append(sc.groupKinds, kind)
	if kind == GroupCapture && counted {
		sc.captures++
	}
	sc.emit(Event{Kind: KindGroupOpen, Group: kind})
}

// opaqueGroup conservatively classifies an unrecognized group construct
// and skips its balanced body.
func (sc *scanner) opaqueGroup(i int) {
	sc.skipBalanced(i)
	sc.emit(Event{Kind: KindOpaque})
}

// skipBalanced advances past the balanced (...) group whose '(' is at i,
// honoring escapes and class brackets so inner delimiters cannot unbalance
// the walk.
func (sc *scanner) skipBalanced(i int) {
	depth := 0
	for p := i; p < len(sc.s); {
		c, size := utf8.DecodeRuneInString(sc.s[p:])
		p += size
		switch c {
		case '\\':
			if p < len(sc.s) {
				_, sz := utf8.DecodeRuneInString(sc.s[p:])
				p += sz
			}
		case '[':
			p = skipClassBody(sc.s, p)
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				sc.pos = p
				return
			}
		}
	}
	sc.pos = len(sc.s)
}

// scanFlagsGroup parses (?flags) and (?flags: constructs.
func (sc *scanner) scanFlagsGroup() {
	s := sc.s
	neg := false
	changed := false
	newFlags := sc.flags()
	for sc.pos < len(s) {
		c, size := utf8.DecodeRuneInString(s[sc.pos:])
		switch {
		case isFlagByte(byte(c)) && c < 0x80:
			changed = true
			on := !neg
			switch byte(c) {
			case 'i', 'I':
				newFlags.fold = on
			case 'm', 'M':
				newFlags.mOn = on
			case 's', 'S':
				// Dot semantics are invisible to classification.
			case 'U', 'u':
				if sc.dialect == DialectRE2 {
					newFlags.ungreedy = on
				}
				// In the fork 'u' is the Unicode option, not ungreedy.
			case 'x', 'X':
				newFlags.freesp = on
			case 'n', 'N', 'd', 'D', 'e', 'E':
				// Capture numbering/debug/ECMAScript: classification-invisible.
			case 'r', 'R':
				newFlags.rtl = on
			}
			sc.pos += size
		case c == '-':
			neg = true
			sc.pos += size
		case c == ')':
			sc.pos += size
			if changed {
				sc.setFlags(newFlags)
			}
			sc.emit(Event{Kind: KindFlags})
			return
		case c == ':':
			sc.pos += size
			// Scoped flags: sealed open, inner state applies to the body.
			sc.flagStack = append(sc.flagStack, sc.flags())
			sc.groupKinds = append(sc.groupKinds, GroupScoped)
			if changed {
				sc.setFlags(newFlags)
			}
			sc.emit(Event{Kind: KindGroupOpen, Group: GroupScoped})
			return
		default:
			// Invalid flag construct; the engine rejects it. Emit the flags
			// seal for robustness and stop the construct here.
			sc.emit(Event{Kind: KindFlags})
			return
		}
	}
}

// scanEscape consumes the escape starting at the '\' at i.
func (sc *scanner) scanEscape(i int) {
	if sc.pos >= len(sc.s) {
		sc.emitLiteral('\\')
		return
	}
	c, size := utf8.DecodeRuneInString(sc.s[sc.pos:])
	sc.pos += size

	r2 := sc.dialect.regexp2()
	switch c {
	case 'n':
		sc.emitLiteral('\n')
	case 'r':
		sc.emitLiteral('\r')
	case 't':
		sc.emitLiteral('\t')
	case 'f':
		sc.emitLiteral('\f')
	case 'a':
		sc.emitLiteral('\a')
	case 'e':
		if r2 {
			sc.emitLiteral(0x1B)
		} else {
			sc.emitOpaque() // RE2 rejects \e; unreachable on accepted patterns
		}
	case 'v':
		// The classification contract is ICU's: \v is the BACKSLASH_V
		// event on every dialect, never a run literal.
		sc.emit(Event{Kind: KindBackslashV})
	case 'd', 'D', 's', 'S', 'w', 'W':
		sc.emit(Event{Kind: KindClassOpen})
		sc.emit(Event{Kind: KindClassClose, Card: CardMany})
	case 'p', 'P':
		sc.scanProperty(c == 'P')
	case 'x':
		if r2 && sc.pos < len(sc.s) && sc.s[sc.pos] == '{' {
			sc.pos++
			if r, ok := sc.scanHexUntilBrace(); ok {
				sc.emitLiteral(r)
			} else {
				sc.emitOpaque()
			}
			return
		}
		if !r2 && sc.pos < len(sc.s) && sc.s[sc.pos] == '{' {
			sc.pos++
			if r, ok := sc.scanHexUntilBrace(); ok {
				sc.emitLiteral(r)
			} else {
				sc.emitOpaque()
			}
			return
		}
		if r, ok := sc.scanHexDigits(2); ok {
			sc.emitLiteral(r)
		} else if r2 {
			sc.emitLiteral('x')
		} else {
			sc.emitOpaque() // RE2 rejects; unreachable
		}
	case 'u':
		// Fork: \uHHHH; Go rejects \u. \u{...} needs ECMAScript+Unicode.
		if r2 {
			if r, ok := sc.scanHexDigits(4); ok {
				sc.emitLiteral(r)
			} else {
				sc.emitLiteral('u')
			}
		} else {
			sc.emitOpaque() // unreachable on accepted RE2 patterns
		}
	case 'U':
		// The SQL translator rewrites \Uxxxxxxxx to \x{xxxxxxxx} on the
		// text regexp2 route and copies it verbatim on the binary route,
		// where the engine scans the literal char 'U'. Short or invalid
		// digit strings stay verbatim on every route (literal 'U').
		switch sc.dialect {
		case DialectRegexp2:
			if r, ok := sc.scanHexDigits(8); ok {
				if r > 0x10FFFF {
					r = 0x10FFFF
				}
				sc.emitLiteral(r)
			} else {
				sc.emitLiteral('U')
			}
		case DialectRegexp2Binary:
			sc.emitLiteral('U')
		default:
			// RE2 rejects \U and the SQL admission forces the regexp2
			// route for it; unreachable on accepted patterns.
			sc.emitOpaque()
		}
	case 'c':
		if r2 {
			sc.scanControl()
		} else {
			sc.emitOpaque() // unreachable
		}
	case 'A':
		sc.emit(Event{Kind: KindAnchor, Anchor: AnchorAbsoluteStart})
	case 'G':
		if r2 {
			sc.emit(Event{Kind: KindAnchor, Anchor: AnchorG})
		} else {
			sc.emitOpaque()
		}
	case 'Z':
		if r2 {
			sc.emit(Event{Kind: KindAnchor, Anchor: AnchorEndZ})
		} else {
			sc.emitOpaque()
		}
	case 'z':
		sc.emit(Event{Kind: KindAnchor, Anchor: AnchorAbsoluteEnd})
	case 'b':
		sc.emit(Event{Kind: KindAnchor, Anchor: AnchorBoundary})
	case 'B':
		sc.emit(Event{Kind: KindAnchor, Anchor: AnchorNonBoundary})
	case 'l', 'N', 'h', 'H', 'K', 'M', 'R', 'V', 'X':
		sc.emitOpaque()
	case 'Q':
		sc.scanQuote()
	case 'k':
		if r2 {
			if sc.pos < len(sc.s) && (sc.s[sc.pos] == '<' || sc.s[sc.pos] == '\'') {
				open := sc.s[sc.pos]
				sc.pos++
				sc.scanAngleRefBody(open, 'k', true, false)
			} else {
				// Malformed \k: the fork reports ErrMalformedNameRef, so
				// the pattern never reaches classification after engine
				// validation.
				sc.emitOpaque()
			}
		} else {
			sc.emitOpaque() // Go rejects \k; unreachable on accepted patterns
		}
	case 'g':
		if r2 {
			// The SQL translator rewrites \g<name>, \g'name' and \g{name}
			// to \k<name>; any other \g form passes through for the
			// engine's literal 'g'.
			if sc.pos < len(sc.s) && (sc.s[sc.pos] == '<' || sc.s[sc.pos] == '\'' || sc.s[sc.pos] == '{') {
				open := sc.s[sc.pos]
				sc.pos++
				sc.scanAngleRefBody(open, 'g', false, true)
			} else {
				sc.emitLiteral('g')
			}
		} else {
			sc.emitOpaque() // Go rejects \g; unreachable
		}
	case '<', '\'':
		if r2 {
			sc.scanAngleRefBody(byte(c), c, false, false)
		} else {
			sc.emitOpaque()
		}
	case '0':
		v := rune(0)
		n := 0
		for n < 2 && sc.pos < len(sc.s) && sc.s[sc.pos] >= '0' && sc.s[sc.pos] <= '7' {
			v = v*8 + rune(sc.s[sc.pos]-'0')
			sc.pos++
			n++
		}
		sc.emitLiteral(v)
	case '1', '2', '3', '4', '5', '6', '7', '8', '9':
		if !r2 {
			// Backreferences force the regexp2 route; unreachable here.
			sc.emitOpaque()
			return
		}
		num := int(c - '0')
		for sc.pos < len(sc.s) && sc.s[sc.pos] >= '0' && sc.s[sc.pos] <= '9' {
			num = num*10 + int(sc.s[sc.pos]-'0')
			sc.pos++
			if num > 1<<24 {
				num = 1 << 24
				break
			}
		}
		if sc.countOnly {
			return
		}
		if sc.sink != nil {
			// Streaming: the pre-scan already counted every capture.
			sc.emit(numericEvent(num, sc.totalCaptures, sc.fold))
			return
		}
		// Resolved after the whole pattern is scanned and the capture
		// count is known.
		sc.numericRefs = append(sc.numericRefs, numericRef{eventIdx: len(sc.events), number: num, fold: sc.fold})
		sc.emit(Event{Kind: KindOpaque}) // placeholder, rewritten later
	default:
		// Escaped punctuation (\., \\, \/, \- ...) is that literal char on
		// both dialects; unknown letters are literals in the fork and errors
		// (unreachable) in RE2.
		sc.emitLiteral(c)
	}
}

func (sc *scanner) emitOpaque() {
	sc.emit(Event{Kind: KindOpaque})
}

// scanAngleRefBody mirrors the fork's backreference recognition after the
// open delimiter of a \<..., \'...', \k... or \g... construct was
// consumed. Like the fork's scanDecimal/scanCapname, recognition advances
// rune by rune and stops at the first invalid character, so an invalid
// body costs its own length rather than a search through the remaining
// pattern. A terminated, non-empty body of ASCII digits (numbered) or fork
// word chars (named) is a backreference; a digit-led body never takes the
// name path. A strict form (\k) otherwise fails in the engine and
// classifies conservatively; the non-strict forms fall back to the literal
// lead character and rescan the body (rescanAtOpen re-reads the delimiter
// too, matching the engine for \g forms the translator left in place).
func (sc *scanner) scanAngleRefBody(open byte, lead rune, strict bool, rescanAtOpen bool) {
	var close byte
	switch open {
	case '\'':
		close = '\''
	case '{':
		close = '}'
	default:
		close = '>'
	}
	s := sc.s
	bodyStart := sc.pos
	i := bodyStart
	if i < len(s) && s[i] >= '0' && s[i] <= '9' {
		// Numbered reference: decimal digits then the close delimiter.
		for i < len(s) && s[i] >= '0' && s[i] <= '9' {
			i++
		}
		if i < len(s) && s[i] == close {
			sc.pos = i + 1
			sc.emit(Event{Kind: KindBackref})
			return
		}
	} else {
		// Named reference: fork word chars then the close delimiter.
		for i < len(s) {
			r, size := utf8.DecodeRuneInString(s[i:])
			if !forkWordChar(r) {
				break
			}
			i += size
		}
		if i > bodyStart && i < len(s) && s[i] == close {
			sc.pos = i + 1
			sc.emit(Event{Kind: KindBackref})
			return
		}
	}
	if strict {
		// The fork rejects the pattern, which never reaches classification
		// after engine validation; skip the rest conservatively.
		sc.pos = len(sc.s)
		sc.emit(Event{Kind: KindOpaque})
		return
	}
	if rescanAtOpen {
		sc.pos = bodyStart - 1
	} else {
		sc.pos = bodyStart
	}
	sc.emitLiteral(lead)
}

// forkWordChar mirrors the vendored regexp2 syntax.IsWordChar: Unicode
// categories L, Mn, Nd, Pc plus ZWNJ (U+200C) and ZWJ (U+200D).
func forkWordChar(r rune) bool {
	return unicode.Is(unicode.L, r) || unicode.Is(unicode.Mn, r) ||
		unicode.Is(unicode.Nd, r) || unicode.Is(unicode.Pc, r) ||
		r == '\u200C' || r == '\u200D'
}

// scanPropertyName consumes a property body ({Name} or one rune) and
// returns the recorded name; a leading '^' inside braces negates the
// property itself (ICU \p{^X}). Consumption matches the engines' scans.
func (sc *scanner) scanPropertyName() (name string, negated bool) {
	if sc.pos < len(sc.s) && sc.s[sc.pos] == '{' {
		sc.pos++
		start := sc.pos
		for sc.pos < len(sc.s) && sc.s[sc.pos] != '}' {
			sc.pos++
		}
		body := sc.s[start:sc.pos]
		if sc.pos < len(sc.s) {
			sc.pos++
		}
		if len(body) > 0 && body[0] == '^' {
			return body[1:], true
		}
		return body, false
	}
	if sc.pos < len(sc.s) {
		r, sz := utf8.DecodeRuneInString(sc.s[sc.pos:])
		sc.pos += sz
		return string(r), false
	}
	return "", false
}

// scanProperty consumes \pX / \p{Name} (or \P...). \P{Any} (and the
// brace-negated \p{^Any}) matches nothing, the one property whose set is
// closed-form; on the RE2 route every canonical spelling reaches the
// same resolution (\P{a_n_y}, \p{^any}, ...: canonicalName drops '_',
// '-' and ' ' and case-normalizes). Every other property stays a
// conservative CardMany set.
func (sc *scanner) scanProperty(neg bool) {
	name, negBody := sc.scanPropertyName()
	card := CardMany
	isAny := name == "Any" || (!sc.dialect.regexp2() && canonicalNameGo(name) == "Any")
	if isAny && neg != negBody {
		card = CardEmpty
	}
	sc.emit(Event{Kind: KindClassOpen})
	sc.emit(Event{Kind: KindClassClose, Card: card})
}

// scanControl consumes \cX control escapes (regexp2 route).
func (sc *scanner) scanControl() {
	if sc.pos >= len(sc.s) {
		sc.emitOpaque()
		return
	}
	c, size := utf8.DecodeRuneInString(sc.s[sc.pos:])
	sc.pos += size
	if c >= 'a' && c <= 'z' {
		c -= 'a' - 'A'
	}
	sc.emitLiteral(c & 0x1F)
}

// scanQuote consumes \Q...\E quoted literal runs. The SQL translator
// expands quoted literals on the regexp2 routes exactly like Go's source
// syntax, so every dialect expands them here as well.
func (sc *scanner) scanQuote() {
	for sc.pos < len(sc.s) {
		if sc.s[sc.pos] == '\\' && sc.pos+1 < len(sc.s) && sc.s[sc.pos+1] == 'E' {
			sc.pos += 2
			return
		}
		c, size := utf8.DecodeRuneInString(sc.s[sc.pos:])
		sc.pos += size
		sc.emitLiteral(c)
	}
	// Unterminated \Q quotes through the end of the pattern (Go behavior).
}

// scanHexDigits reads exactly n lowercase/uppercase hex digits without
// committing partial scans: on failure the position is unchanged, so the
// caller's fallback rescan sees every digit.
func (sc *scanner) scanHexDigits(n int) (rune, bool) {
	v := rune(0)
	for k := 0; k < n; k++ {
		if sc.pos+k >= len(sc.s) {
			return 0, false
		}
		d, ok := hexVal(sc.s[sc.pos+k])
		if !ok {
			return 0, false
		}
		v = v*16 + d
	}
	sc.pos += n
	return v, true
}

func (sc *scanner) scanHexUntilBrace() (rune, bool) {
	v := rune(0)
	digits := 0
	for sc.pos < len(sc.s) && sc.s[sc.pos] != '}' {
		d, ok := hexVal(sc.s[sc.pos])
		if !ok {
			return 0, false
		}
		v = v*16 + d
		if v > 0x10FFFF {
			v = 0x10FFFF
		}
		sc.pos++
		digits++
	}
	if sc.pos >= len(sc.s) || digits == 0 {
		return 0, false
	}
	sc.pos++ // consume '}'
	return v, true
}

func hexVal(b byte) (rune, bool) {
	switch {
	case b >= '0' && b <= '9':
		return rune(b - '0'), true
	case b >= 'a' && b <= 'f':
		return rune(b-'a') + 10, true
	case b >= 'A' && b <= 'F':
		return rune(b-'A') + 10, true
	}
	return 0, false
}

// numericEvent resolves a \<digits> escape against the pattern's total
// capture count: values that hit a defined capture slot are
// backreferences (the fork keeps those even under RE2 mode); low
// undefined numbers are engine errors and never reach classification on
// the regexp2 route; multi-digit overflow falls back to the fork's
// char-code scan, which lands on an octal or literal char here.
func numericEvent(number, captures int, fold bool) Event {
	switch {
	case number >= 1 && number <= captures:
		return Event{Kind: KindBackref}
	case number <= 9:
		return Event{Kind: KindOpaque}
	default:
		return Event{Kind: KindLiteral, Rune: rune('0' + number%10), Fold: fold}
	}
}

// resolveNumericRefs rewrites pending numeric escapes against the scanned
// total capture count.
func (sc *scanner) resolveNumericRefs() {
	for _, ref := range sc.numericRefs {
		ev := numericEvent(ref.number, sc.captures, ref.fold)
		ev.Ordinal = ref.eventIdx
		sc.events[ref.eventIdx] = ev
	}
}

// isFlagByte reports whether b may appear inside (?...).
func isFlagByte(b byte) bool {
	switch b {
	case 'i', 'I', 'm', 'M', 's', 'S', 'U', 'u', 'x', 'X', 'n', 'N', 'd', 'D', 'e', 'E', 'r', 'R':
		return true
	}
	return false
}

// isForkSpace mirrors the fork's isSpace for free-spacing mode.
func isForkSpace(c rune) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
