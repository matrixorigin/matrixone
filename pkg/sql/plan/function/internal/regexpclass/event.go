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

import "fmt"

// Kind identifies one pre-lowering event kind. Each kind carries ICU's
// three independent effects - run sealing, atStart behavior and
// content/length contribution - per the shared event table of the design.
type Kind uint8

const (
	// KindLiteral is a literal code point with fold state; it joins the
	// pending run.
	KindLiteral Kind = iota
	// KindClassOpen seals the pending run before a class body.
	KindClassOpen
	// KindClassClose resolves a class body into cardinality 0 (failure),
	// 1 (the singleton char joins the new run) or greater (consuming set).
	KindClassClose
	// KindGroupOpen opens a group; it seals the pending run.
	KindGroupOpen
	// KindGroupClose closes a group; it seals the pending run.
	KindGroupClose
	// KindFlags is an inline flag set; it seals the pending run even when
	// no flag actually changes.
	KindFlags
	// KindQuant is a quantifier with parsed form, bounds and flavor.
	KindQuant
	// KindBranch is an alternation branch operator. In ICU's compiled
	// program the branch decision precedes every arm, so the arm-start
	// state-save destroys atStart retroactively for the whole alternation.
	KindBranch
	// KindAnchor is an anchor or boundary assertion of the recorded kind;
	// every kind seals the pending run while staying zero-width, atStart
	// preserving and min-length zero.
	KindAnchor
	// KindBackref is a numbered or named backreference; it seals the
	// pending run, preserves atStart and can match empty.
	KindBackref
	// KindBackslashV is the ICU-identity \v escape: sealing, consuming,
	// never joining a literal run.
	KindBackslashV
	// KindOpaque is a construct outside the mirrored rule set: it seals,
	// destroys atStart and counts conservatively as consuming content.
	KindOpaque
	// KindEnd terminates the pattern; it seals the pending run.
	KindEnd
)

// Card is the resolved class cardinality (the core needs only 0/1/many).
type Card uint8

const (
	CardEmpty Card = iota
	CardOne
	CardMany
)

// GroupKind classifies group boundaries.
type GroupKind uint8

const (
	GroupCapture GroupKind = iota
	GroupNonCapture
	GroupScoped
	GroupAtomic
	GroupLookahead
	GroupLookbehind
)

// QuantForm distinguishes the ICU lowering shapes (plus keeps its body
// first while star/question/interval place their init or guard before).
type QuantForm uint8

const (
	FormStar QuantForm = iota
	FormPlus
	FormQuestion
	FormInterval
)

// Flavor is the quantifier flavor after the RE2 dialect's (?U) laziness
// swap, so the core never needs flag state.
type Flavor uint8

const (
	FlavorOrdinary Flavor = iota
	FlavorLazy
	FlavorPossessive
)

// AnchorKind identifies the assertion; only line-carets feed the
// line-anchored start mark.
type AnchorKind uint8

const (
	AnchorCaretLine AnchorKind = iota
	AnchorCaretText
	AnchorDollar
	AnchorAbsoluteStart
	AnchorG
	AnchorEndZ
	AnchorAbsoluteEnd
	AnchorBoundary
	AnchorNonBoundary
)

// Event is one scanner output token with the attributes the shared core
// folds. Ordinal is the 0-based sequence position assigned by the scanner.
type Event struct {
	Ordinal int
	Kind    Kind

	// KindLiteral carries Rune and Fold.
	Rune rune
	Fold bool

	// KindClassClose carries Card; with CardOne it also carries Rune/Fold.
	Card Card

	// KindGroupOpen/KindGroupClose carry Group.
	Group GroupKind

	// KindQuant carries Form, Min, Max (-1 unbounded) and Flavor.
	Form   QuantForm
	Min    int
	Max    int
	Flavor Flavor

	// KindAnchor carries Anchor.
	Anchor AnchorKind
}

func (k Kind) String() string {
	switch k {
	case KindLiteral:
		return "literal"
	case KindClassOpen:
		return "class-open"
	case KindClassClose:
		return "class-close"
	case KindGroupOpen:
		return "group-open"
	case KindGroupClose:
		return "group-close"
	case KindFlags:
		return "flags"
	case KindQuant:
		return "quant"
	case KindBranch:
		return "branch"
	case KindAnchor:
		return "anchor"
	case KindBackref:
		return "backref"
	case KindBackslashV:
		return "backslash-v"
	case KindOpaque:
		return "opaque"
	case KindEnd:
		return "end"
	}
	return fmt.Sprintf("kind(%d)", int(k))
}

func (g GroupKind) String() string {
	switch g {
	case GroupCapture:
		return "capture"
	case GroupNonCapture:
		return "noncapture"
	case GroupScoped:
		return "scoped"
	case GroupAtomic:
		return "atomic"
	case GroupLookahead:
		return "lookahead"
	case GroupLookbehind:
		return "lookbehind"
	}
	return fmt.Sprintf("group(%d)", int(g))
}

func (f QuantForm) String() string {
	switch f {
	case FormStar:
		return "star"
	case FormPlus:
		return "plus"
	case FormQuestion:
		return "question"
	case FormInterval:
		return "interval"
	}
	return fmt.Sprintf("form(%d)", int(f))
}

func (f Flavor) String() string {
	switch f {
	case FlavorOrdinary:
		return "ordinary"
	case FlavorLazy:
		return "lazy"
	case FlavorPossessive:
		return "possessive"
	}
	return fmt.Sprintf("flavor(%d)", int(f))
}

// String makes the recorded event stream assertable in unit tests.
func (e Event) String() string {
	switch e.Kind {
	case KindLiteral:
		return fmt.Sprintf("%d literal %U fold=%v", e.Ordinal, e.Rune, e.Fold)
	case KindClassClose:
		if e.Card == CardOne {
			return fmt.Sprintf("%d class-close card=1 %U fold=%v", e.Ordinal, e.Rune, e.Fold)
		}
		return fmt.Sprintf("%d class-close card=%d", e.Ordinal, int(e.Card))
	case KindGroupOpen:
		return fmt.Sprintf("%d group-open %s", e.Ordinal, e.Group)
	case KindGroupClose:
		return fmt.Sprintf("%d group-close %s", e.Ordinal, e.Group)
	case KindQuant:
		return fmt.Sprintf("%d quant %s {%d,%d} %s", e.Ordinal, e.Form, e.Min, e.Max, e.Flavor)
	case KindAnchor:
		return fmt.Sprintf("%d anchor %d", e.Ordinal, int(e.Anchor))
	default:
		return fmt.Sprintf("%d %s", e.Ordinal, e.Kind)
	}
}
