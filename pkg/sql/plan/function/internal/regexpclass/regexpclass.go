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

// Package regexpclass implements the structural start classification of
// issue #30210: ICU's matchStartType procedure replicated over a shared
// pre-lowering event vocabulary scanned from the original pattern text.
//
// The scanner emits, for the pattern dialect of one route (Go regexp
// syntax on the RE2 route, the ICU/.NET construct superset on the regexp2
// route), the events ICU's compiler acts on: literals with fold state,
// character classes resolved to cardinality 0/1/greater with complement
// arithmetic, group boundaries, inline flag sets, alternation branches,
// quantifiers with parsed form, bounds and flavor, anchors and boundary
// assertions, backreferences, and the ICU-identity escape \v. The shared
// core folds the events with ICU's quantifier lowering, literal-run
// sealing rules and branch-forwarded initial-string accumulator into the
// pair {lineAnchoredStart, stringExempt}.
//
// The classifier never rejects a pattern: constructs outside the mirrored
// rule set resolve conservatively (no line anchoring, no exemption), which
// degrades matching to the engines' ordinary candidate enumeration.
package regexpclass

// Dialect selects the pattern syntax the scanner lexes.
type Dialect uint8

const (
	// DialectRE2 is the Go regexp syntax accepted by the private RE2
	// engine route.
	DialectRE2 Dialect = iota
	// DialectRegexp2 is the ICU/.NET construct superset accepted by the
	// vendored regexp2 route, exactly as requiresRegexp2Pattern detects it.
	DialectRegexp2
	// DialectRegexp2Binary is the regexp2 route over a binary subject: the
	// SQL translator leaves \Uxxxxxxxx escapes to the engine, which scans
	// them as literal 'U' text. Everything else matches DialectRegexp2.
	DialectRegexp2Binary
)

// regexp2 reports whether the dialect follows the vendored fork's escape
// semantics (both regexp2 dialects do; only \U handling differs).
func (d Dialect) regexp2() bool {
	return d == DialectRegexp2 || d == DialectRegexp2Binary
}

// Descriptor is the immutable compile-time start classification attached
// to each compiled matcher.
type Descriptor struct {
	// LineAnchoredStart reports that a multiline caret is reachable while
	// the match start is still unconstrained (ICU's START_LINE class).
	LineAnchoredStart bool
	// StringExempt reports that ICU's ladder selects START_STRING: one
	// initial case-sensitive literal run of length >= 2 with a non-empty
	// minimum match length.
	StringExempt bool
}

// SuppressCRLFInterior reports whether a match may never begin between CR
// and LF of a CRLF pair in a wide line mode.
func (d Descriptor) SuppressCRLFInterior() bool {
	return d.LineAnchoredStart && !d.StringExempt
}

// Classify scans pattern in the given dialect and folds the events into
// the descriptor as they are produced, without materializing the event
// stream: transient storage stays proportional to the fold state, and the
// call sites run it after the admission size verdicts. It never fails; the
// descriptor is only meaningful for patterns the owning engine accepts,
// which the engines compile before calling this.
func Classify(pattern string, dialect Dialect) Descriptor {
	// Pass 1 counts capture groups; numeric backreference resolution
	// needs the total before the streaming pass.
	counter := &scanner{s: pattern, dialect: dialect, countOnly: true}
	for i := 0; i < len(pattern); i = counter.pos {
		counter.step(i)
	}

	c := &core{atStart: true}
	// A pseudo-frame makes top-level alternations behave exactly like one
	// group's worth of arms.
	c.pushFrame(GroupNonCapture)
	sc := &scanner{s: pattern, dialect: dialect, totalCaptures: counter.captures, sink: c}
	for i := 0; i < len(pattern); i = sc.pos {
		sc.step(i)
	}
	sc.emit(Event{Kind: KindEnd})
	for len(c.frames) > 0 {
		c.popTopFrame()
	}
	return Descriptor{
		LineAnchoredStart: len(c.marks) > 0,
		StringExempt:      c.numInit == 1 && c.minLen() > 0,
	}
}

// DebugEvents returns the event stream recorded for pattern. It powers
// the event-sequence unit tests pinned by the design validation plan.
func DebugEvents(pattern string, dialect Dialect) []Event {
	return scan(pattern, dialect)
}
