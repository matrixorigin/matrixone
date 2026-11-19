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
	"sort"
	"sync"
	"unicode"
	"unicode/utf8"
)

// maxRunes is the size of the Go rune space classes range over.
const maxRunes = 0x110000

// interval is a closed rune range.
type interval struct {
	lo, hi rune
}

// classAtom records a shorthand or property member the interval
// arithmetic cannot represent exactly: name is the atom's identity (d, s,
// w, or the property name inside braces), neg marks the complementary
// form (\D, \P{Lu}, \p{^Lu}).
type classAtom struct {
	name string
	neg  bool
}

// classAccumulator gathers exact member intervals. Insertions append in
// amortized O(1) and the union is normalized lazily (one sort + coalesce
// pass - and, when case folding is active, a single fold settlement -
// before the first query), so tall classes classify in near-linear time
// rather than rescans per member. Shorthand, POSIX and property members
// resolve through the engines' exact membership tables (per dialect),
// so coverage across families - [^[:digit:]\D], [^\p{L}\P{Lu}] - falls
// out of the same interval arithmetic as name pairs. Folded properties
// resolve per engine: Go RE2 merges the property with its fold
// companion table and resolves canonical names plus aliases (syntax
// parse.go unicodeTable), while the fork route derives the structural
// cardinality of shared property syntax from the same ICU per-operand
// closure (tab ∪ fold companion, complement after the merge), keeping
// the membership in lowercase space until the ToLower preimage at
// class close.
// Members the tables cannot settle (\p{Any}, engine-rejected names) are
// recorded as atoms so complementary pairs still resolve to full
// coverage (and their negation to an empty class); anything left
// unresolved flips conservative, which resolves to CardMany.
type classAccumulator struct {
	intervals []interval // unmerged until normalize runs
	atoms     []classAtom
	// pendingRaw holds the raw literal operands of a fork folded class
	// until settleForkFold converts them through the lowercase stored
	// set; keeping them separate from intervals makes insertion
	// amortized O(1) and each settle idempotent.
	pendingRaw []interval
	// pendingProp holds the property operands of a fork folded class
	// as FINAL rune-space membership (the ICU per-operand closure,
	// complemented after the merge for \P{X}): they skip the lowercase
	// pipeline because their contract semantics are rune-space - the
	// runner's ToLower fiber would mis-place U+0130, which ICU 77.1
	// excludes from (?i)\p{Ll} and includes in (?i)\P{Ll}.
	pendingProp []interval
	sorted      bool
	// pendingFold records that a fold-closure operand was inserted
	// without settling; normalize settles it exactly once.
	pendingFold bool
	// foldFork selects the fork lowercase semantics for this class: raw
	// operands accumulate and materializeForkFold converts them to the
	// exact member set at class close.
	foldFork bool
	// foldConsumed reports that materializeForkFold already converted
	// the members to their final form; no further fold may apply.
	foldConsumed bool
	conservative bool
}

// recordAtom registers one shorthand/property member of the class whose
// exact rune set is outside interval arithmetic.
func (a *classAccumulator) recordAtom(name string, neg bool) {
	a.atoms = append(a.atoms, classAtom{name: name, neg: neg})
}

// applyComplement replaces the accumulated members with their exact
// complement, mirroring the fork's negation which applies to the whole
// accumulated set ahead of any subtraction operand (CharSet.CharIn in
// internal/regexp2/syntax/charclass.go flips c.negate before recursing
// into c.sub). A class that provably covers every rune complements to
// nothing; an unresolved member (a shorthand, property or POSIX class)
// cannot be complemented exactly and keeps the conservative answer;
// resolved-empty atoms (\P{Any}) contribute no coverage and complement
// away with the intervals.
func (a *classAccumulator) applyComplement() {
	if a.conservative {
		return
	}
	// A fork folded class complements in lowercase space; the preimage
	// commutes with the complement, so settling first is exact.
	a.settleForkFold()
	if full, unresolved := a.atomCoverage(); full {
		a.intervals = nil
		a.atoms = nil
		a.sorted = true
		return
	} else if unresolved {
		a.conservative = true
		return
	}
	a.normalize()
	var out []interval
	next := rune(0)
	for _, iv := range a.intervals {
		if next < iv.lo {
			out = append(out, interval{next, iv.lo - 1})
		}
		next = iv.hi + 1
	}
	if next <= 0x10FFFF {
		out = append(out, interval{next, 0x10FFFF})
	}
	a.intervals = out
	a.atoms = nil
	a.sorted = true
}

// atomCoverage resolves the recorded atoms that exact arithmetic cannot
// represent. full reports that the union provably covers every rune: an
// atom paired with its own complement in the same body (\s with \S,
// \p{Lu} with \P{Lu}) or a bare \p{Any}. Full coverage subsumes every
// other member. \P{Any} covers nothing and contributes no coverage. Any
// other atom leaves the class unresolved: it must keep the conservative
// CardMany answer rather than being treated as proven nonempty content.
func (a *classAccumulator) atomCoverage() (full bool, unresolved bool) {
	var seen map[string]uint8
	for _, at := range a.atoms {
		if at.name == "Any" {
			if !at.neg {
				return true, false
			}
			continue
		}
		bit := uint8(1)
		if at.neg {
			bit = 2
		}
		if seen == nil {
			seen = make(map[string]uint8, len(a.atoms))
		}
		prev := seen[at.name]
		if prev|bit == 3 {
			return true, false
		}
		seen[at.name] = prev | bit
	}
	return false, len(seen) > 0
}

func (a *classAccumulator) addRune(r rune) {
	a.addInterval(r, r)
}

func (a *classAccumulator) addInterval(lo, hi rune) {
	if lo > hi || lo < 0 || hi > 0x10FFFF {
		// The engines reject inverted or out-of-range limits; when such a
		// pattern somehow reaches classification, fall back conservatively.
		a.conservative = true
		return
	}
	a.intervals = append(a.intervals, interval{lo, hi})
	a.sorted = false
}

// intervalSlice is the concrete sort target for normalize; method-set
// comparison avoids the sort.Slice closure on tall class unions (table
// operands insert hundreds of members per class).
type intervalSlice []interval

func (s intervalSlice) Len() int { return len(s) }
func (s intervalSlice) Less(i, j int) bool {
	if s[i].lo != s[j].lo {
		return s[i].lo < s[j].lo
	}
	return s[i].hi < s[j].hi
}
func (s intervalSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// normalize settles a pending fold closure once, then sorts and
// coalesces. Settling ahead of the sort keeps the fold as one pass over
// the accumulated union instead of one pass per folded operand.
func (a *classAccumulator) normalize() {
	if a.pendingFold {
		a.pendingFold = false
		a.foldClose()
	}
	a.normalizeRaw()
}

// normalizeRaw sorts the recorded intervals once and coalesces
// overlapping or adjacent ranges. Adjacent ranges share no gap, so
// coalescing preserves membership exactly; the result is a sorted list
// of member-disjoint intervals independent of insertion order.
func (a *classAccumulator) normalizeRaw() {
	if a.sorted {
		return
	}
	ivs := a.intervals
	sort.Sort(intervalSlice(ivs))
	out := ivs[:0]
	for _, iv := range ivs {
		if n := len(out); n > 0 && iv.lo <= out[n-1].hi+1 {
			if iv.hi > out[n-1].hi {
				out[n-1].hi = iv.hi
			}
			continue
		}
		out = append(out, iv)
	}
	a.intervals = out
	a.sorted = true
}

func (a *classAccumulator) count() int64 {
	a.normalize()
	var n int64
	for _, iv := range a.intervals {
		n += int64(iv.hi-iv.lo) + 1
	}
	return n
}

func (a *classAccumulator) member(r rune) bool {
	a.normalize()
	i := sort.Search(len(a.intervals), func(i int) bool { return a.intervals[i].hi >= r })
	return i < len(a.intervals) && a.intervals[i].lo <= r
}

// singleton returns the only member when count()==1.
func (a *classAccumulator) singleton() (rune, bool) {
	if a.count() == 1 && len(a.intervals) > 0 {
		return a.intervals[0].lo, true
	}
	return 0, false
}

// subtract removes other's members from a (exact interval difference over
// the normalized sorted lists, one linear sweep). A full-coverage operand
// (\s\S, \p{Lu}\P{Lu}, \p{Any}) empties the class outright; an unresolved
// operand stays conservative like before.
func (a *classAccumulator) subtract(other *classAccumulator) {
	if other.conservative {
		a.conservative = true
		return
	}
	// Both sides subtract in lowercase space on the fork (CharIn
	// recurses into the subtraction with the same lowered input).
	a.settleForkFold()
	other.settleForkFold()
	if full, unresolved := other.atomCoverage(); full {
		a.intervals = nil
		a.atoms = nil
		a.sorted = true
		return
	} else if unresolved {
		a.conservative = true
		return
	}
	a.normalize()
	other.normalize()
	var out []interval
	j := 0
	for _, iv := range a.intervals {
		lo := iv.lo
		for j < len(other.intervals) && other.intervals[j].hi < lo {
			j++
		}
		for k := j; k < len(other.intervals) && other.intervals[k].lo <= iv.hi; k++ {
			ov := other.intervals[k]
			if ov.lo > lo {
				out = append(out, interval{lo, ov.lo - 1})
			}
			if ov.hi >= iv.hi {
				lo = iv.hi + 1
				break
			}
			lo = ov.hi + 1
		}
		if lo <= iv.hi {
			out = append(out, interval{lo, iv.hi})
		}
	}
	a.intervals = out
	a.sorted = true
}

// complementRunes returns up to limit runes outside the member set by
// walking the gaps of the normalized intervals. Only the 0/1/many shape is
// ever needed, so limit 2 keeps every call O(intervals + limit); the rune
// space is never enumerated per class.
func (a *classAccumulator) complementRunes(limit int) []rune {
	a.normalize()
	var out []rune
	next := rune(0)
	for _, iv := range a.intervals {
		for r := next; r < iv.lo; r++ {
			out = append(out, r)
			if len(out) >= limit {
				return out
			}
		}
		next = iv.hi + 1
	}
	for r := next; r <= 0x10FFFF; r++ {
		out = append(out, r)
		if len(out) >= limit {
			return out
		}
	}
	return out
}

// foldComplement returns up to limit runes that live outside the fold
// closure of the member set: a rune survives only when its whole SimpleFold
// orbit avoids membership. Every surviving rune has a fully omitted orbit,
// and runes with a trivial orbit survive on their own, so the gap walk
// ends at limit survivors or inside the small fold-relevant universe; it
// never sweeps the full rune space.
func (a *classAccumulator) foldComplement(limit int) []rune {
	a.normalize()
	var out []rune
	next := rune(0)
	visit := func(g0, g1 rune) bool {
		for r := g0; r <= g1; r++ {
			if a.orbitOutside(r) {
				out = append(out, r)
				if len(out) >= limit {
					return true
				}
			}
		}
		return false
	}
	for _, iv := range a.intervals {
		if next < iv.lo && visit(next, iv.lo-1) {
			return out
		}
		next = iv.hi + 1
	}
	if next <= 0x10FFFF {
		visit(next, 0x10FFFF)
	}
	return out
}

// orbitOutside reports whether r and its entire SimpleFold orbit avoid the
// member set.
func (a *classAccumulator) orbitOutside(r rune) bool {
	if a.member(r) {
		return false
	}
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		if a.member(f) {
			return false
		}
	}
	return true
}

// classMembersHook, when set by tests, observes the exact resolved
// member set of every closed class together with its pending negation
// and the active fold flag, before cardinality resolution. Production
// code never sets it.
var classMembersHook func(acc *classAccumulator, negate, fold bool)

// scanClass consumes the class whose '[' is at i and emits the class-open
// seal plus the resolved close event.
func (sc *scanner) scanClass(i int) {
	sc.emit(Event{Kind: KindClassOpen})
	acc, negate, closed := sc.parseClassBody(i)
	if closed && classMembersHook != nil {
		// Settle the lazy fold before the hook observes the members:
		// its contract is the exact resolved set, and cardinality
		// resolution normalizes identically. Production never sets the
		// hook, so this runs in tests only.
		acc.normalize()
		classMembersHook(acc, negate, sc.fold)
	}
	ev := Event{Kind: KindClassClose, Card: CardMany}
	if closed && !acc.conservative {
		// A materialized fork fold already holds the exact membership;
		// resolve its cardinality without the RE2 fold-closure pass.
		effFold := sc.fold && !acc.foldConsumed
		card, singleton := resolveCardinality(acc, negate, effFold)
		ev.Card = card
		if card == CardOne {
			ev.Rune = singleton
			ev.Fold = effFold
		}
	}
	sc.emit(ev)
}

// parseClassBody consumes one class body ('[' already scanned; sc.pos is
// just after it) and returns the exact members plus the negation flag.
func (sc *scanner) parseClassBody(i int) (*classAccumulator, bool, bool) {
	s := sc.s
	acc := &classAccumulator{foldFork: sc.fold && sc.dialect.regexp2()}
	negate := false
	negatable := true    // '^' negates only as the very first char
	memberAhead := false // ']' is literal until one member was consumed

	prev := sc.activeClass
	sc.activeClass = acc
	defer func() { sc.activeClass = prev }()

	for sc.pos < len(s) {
		c, size := utf8.DecodeRuneInString(s[sc.pos:])
		if c == '^' && negatable && size == 1 {
			negate = true
			negatable = false
			sc.pos += size
			continue
		}
		negatable = false
		if c == ']' && memberAhead && size == 1 {
			sc.pos += size
			// A fork folded class materializes to its exact rune
			// membership at close, before the hook and cardinality
			// resolution observe it.
			acc.materializeForkFold()
			return acc, negate, true
		}

		// regexp2 set subtraction '-[...]' after ANY completed member
		// form - literal, range, class escape, POSIX or property
		// operand. The fork recognizes it whenever a member precedes
		// (internal/regexp2/syntax/parser.go scanCharSet), not only
		// right after a literal atom, so the check sits at the top of
		// the loop where the '-' would otherwise be re-read as a
		// literal member. Go's RE2 keeps '-' literal here.
		if memberAhead && c == '-' && size == 1 && sc.pos+1 < len(s) && s[sc.pos+1] == '[' && sc.dialect.regexp2() {
			// Complement and subtraction run in lowercase space on the
			// fork, exactly like CharSet.CharIn, and a leading '^'
			// negates the whole accumulated set BEFORE the subtraction,
			// so the operand subtracts from the complement.
			acc.settleForkFold()
			if negate {
				acc.applyComplement()
				negate = false
			}
			sc.pos += 2 // consume '-['
			sub := sc.parseNestedSubtraction()
			acc.subtract(sub)
			memberAhead = true
			// The fork requires the class to end right after a
			// subtraction (ErrSubtractionMustBeLast); any other tail
			// is a parse error there and cannot settle into the
			// engine's membership.
			if sc.pos >= len(s) || s[sc.pos] != ']' {
				acc.conservative = true
			}
			continue
		}

		lo, hi, ok := sc.classAtom()
		if !ok {
			// Class-ish escape (\d, [:alpha:], \pL, quoted run): consumed
			// internally; conservatism already recorded where applicable.
			memberAhead = true
			continue
		}
		memberAhead = true

		// Range tail: '-' followed by a non-']' member, or a regexp2
		// nested-set subtraction '-[...]'.
		if sc.pos < len(s) && s[sc.pos] == '-' && sc.pos+1 < len(s) {
			after := sc.pos + 1
			if s[after] == '[' && sc.dialect.regexp2() {
				// The fork adds the pending member (cc.addChar(chPrev)
				// in internal/regexp2/syntax/parser.go) before scanning
				// the nested class; landing it here defers the
				// subtraction to the top of the next iteration, so one
				// path serves every preceding member form.
				acc.addMember(lo, hi, sc.fold)
				continue
			}
			if s[after] != ']' {
				sc.pos = after
				_, hi2, ok2 := sc.classAtom()
				if !ok2 {
					acc.conservative = true
					continue
				}
				acc.addMember(lo, hi2, sc.fold)
				continue
			}
		}
		acc.addMember(lo, hi, sc.fold)
	}
	return acc, negate, false
}

// parseNestedSubtraction parses a regexp2 nested '[...]' subtraction class;
// sc.pos is just past its opening '['. The fork scans the nested class
// with the same routine as the top level, so a leading '^' negates the
// nested operand exactly the same way.
func (sc *scanner) parseNestedSubtraction() *classAccumulator {
	s := sc.s
	acc := &classAccumulator{foldFork: sc.fold && sc.dialect.regexp2()}
	memberAhead := false
	negatable := true // '^' negates only as the very first char
	negate := false
	prev := sc.activeClass
	sc.activeClass = acc
	defer func() { sc.activeClass = prev }()

	for sc.pos < len(s) {
		c, size := utf8.DecodeRuneInString(s[sc.pos:])
		if c == '^' && negatable && size == 1 {
			negate = true
			negatable = false
			sc.pos += size
			continue
		}
		negatable = false
		if c == ']' && memberAhead && size == 1 {
			sc.pos += size
			// The nested operand hands back its lowercase stored set; the
			// outer class materializes the preimage at its own close.
			acc.settleForkFold()
			if negate {
				acc.applyComplement()
			}
			return acc
		}
		// Same after-any-member subtraction recognition as the
		// top-level scanner (the nested class is scanned by the same
		// fork routine at the deeper nesting level).
		if memberAhead && c == '-' && size == 1 && sc.pos+1 < len(s) && s[sc.pos+1] == '[' {
			acc.settleForkFold()
			if negate {
				acc.applyComplement()
				negate = false
			}
			sc.pos += 2 // consume '-['
			sub := sc.parseNestedSubtraction()
			acc.subtract(sub)
			memberAhead = true
			if sc.pos >= len(s) || s[sc.pos] != ']' {
				// ErrSubtractionMustBeLast on the fork; the nested
				// operand cannot settle.
				acc.conservative = true
			}
			continue
		}
		lo, hi, ok := sc.classAtom()
		if !ok {
			memberAhead = true
			continue
		}
		memberAhead = true
		if sc.pos < len(s) && s[sc.pos] == '-' && sc.pos+1 < len(s) {
			after := sc.pos + 1
			if s[after] == '[' {
				// Keep the pending member (folded as its own completed
				// operand); the subtraction is recognized at the top
				// of the next iteration, after any member form.
				acc.addMember(lo, hi, sc.fold)
				continue
			}
			if s[after] != ']' {
				sc.pos = after
				_, hi2, ok2 := sc.classAtom()
				if !ok2 {
					acc.conservative = true
					continue
				}
				acc.addMember(lo, hi2, sc.fold)
				continue
			}
		}
		acc.addMember(lo, hi, sc.fold)
	}
	acc.conservative = true // unterminated nested class
	return acc
}

// resolveCardinality maps exact members (plus negation and fold closure)
// to the 0/1/many answer the core needs. Recorded atoms resolve first: a
// complementary pair (or \p{Any}) covers the whole rune space, so the
// class resolves through the full-set interval arithmetic; anything left
// unresolved keeps the conservative CardMany answer.
func resolveCardinality(acc *classAccumulator, negate bool, fold bool) (Card, rune) {
	if full, unresolved := acc.atomCoverage(); full {
		acc = &classAccumulator{intervals: []interval{{lo: 0, hi: 0x10FFFF}}}
	} else if unresolved {
		// An unresolved member cannot shrink a union the intervals alone
		// already prove to be the whole rune space; only over partial
		// intervals does it keep the conservative CardMany answer.
		if acc.count() < maxRunes {
			return CardMany, 0
		}
	}
	if fold {
		// Fold closure only grows the member set; exact arithmetic matters
		// only below cardinality 2.
		n := acc.count()
		switch {
		case n == 0:
			// Folding the empty set stays empty.
		case n == 1:
			r, _ := acc.singleton()
			closed := &classAccumulator{}
			foldCycleInto(closed, r)
			if closed.count() >= 2 && !negate {
				return CardMany, 0
			}
			acc = closed
		default:
			if !negate {
				return CardMany, 0
			}
			// Fold before complement: a rune survives outside the closure
			// only with its whole fold orbit, so the negated class is the
			// union of fully omitted orbits (ICU: (?i:[^\x00-\x40\x42-...])
			// matches nothing, the 'A' gap folds back through 'a').
			switch gaps := acc.foldComplement(2); len(gaps) {
			case 0:
				return CardEmpty, 0
			case 1:
				return CardOne, gaps[0]
			default:
				return CardMany, 0
			}
		}
	}

	if negate {
		// The complement's 0/1/many shape comes straight from the gaps of
		// the normalized intervals; the rune space is never enumerated.
		switch gaps := acc.complementRunes(2); len(gaps) {
		case 0:
			return CardEmpty, 0
		case 1:
			return CardOne, gaps[0]
		default:
			return CardMany, 0
		}
	}
	switch n := acc.count(); {
	case n == 0:
		return CardEmpty, 0
	case n == 1:
		if r, ok := acc.singleton(); ok {
			return CardOne, r
		}
		return CardMany, 0
	default:
		return CardMany, 0
	}
}

// foldCycleInto adds the SimpleFold closure of r (r included).
func foldCycleInto(acc *classAccumulator, r rune) {
	acc.addRune(r)
	for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
		acc.addRune(f)
	}
}

// maxFoldRune bounds the scan for runes with nontrivial SimpleFold
// orbits. Every case range in the Unicode tables ends far below this
// bound (the highest fold-relevant rune is Adlam U+1E943), so a rune
// above it folds to itself.
const maxFoldRune = 0x1E943

// foldOrbitRunes lists every rune whose SimpleFold orbit is nontrivial,
// in increasing order. SimpleFold partitions the rune space into cycles,
// so the members of one orbit are exactly the runes that list it.
var foldOrbitRunes = buildFoldOrbitRunes()

func buildFoldOrbitRunes() []rune {
	var out []rune
	for r := rune(0); r <= maxFoldRune; r++ {
		if unicode.SimpleFold(r) != r {
			out = append(out, r)
		}
	}
	return out
}

// addMember records one operand interval, closing it over SimpleFold
// orbits when the class scans under case folding. The engines fold each
// completed operand BEFORE any complement or subtraction applies: Go's
// RE2 expands class ranges by fold orbit during parse, and the fork
// lowercases the completed operand charset ahead of the CharIn membership
// negation (internal/regexp2/syntax/parser.go:1628 -> charclass.go
// addLowercase, then charclass.go:281-288). Folding here keeps every
// later exact step (complement, subtraction, cardinality) on the engine's
// ordering.
func (a *classAccumulator) addMember(lo, hi rune, fold bool) {
	a.addIntervals([]interval{{lo, hi}}, fold)
}

// addIntervals records a whole operand set with the same operand-level
// fold semantics as addMember. Growth doubles amortized, so a class of
// n repeated literal members inserts in O(n) total instead of copying
// the accumulated union per member. The fold work settles lazily:
// RE2-mode fold marks a pending closure that normalize runs once over
// the whole union (SimpleFold orbits close over unions operand-wise,
// so one settlement equals folding each operand at insertion); fork
// folded classes hold the operand raw for the lowercase
// materialization.
func (a *classAccumulator) addIntervals(ivs []interval, fold bool) {
	if a.foldFork {
		for _, iv := range ivs {
			if iv.lo > iv.hi || iv.lo < 0 || iv.hi > 0x10FFFF {
				a.conservative = true
				continue
			}
			a.pendingRaw = append(a.pendingRaw, iv)
		}
		return
	}
	if need := len(a.intervals) + len(ivs); need > cap(a.intervals) {
		newCap := cap(a.intervals) * 2
		if newCap < need {
			newCap = need
		}
		if newCap < 8 {
			newCap = 8
		}
		grown := make([]interval, len(a.intervals), newCap)
		copy(grown, a.intervals)
		a.intervals = grown
	}
	for _, iv := range ivs {
		a.addInterval(iv.lo, iv.hi)
	}
	if !fold {
		return
	}
	a.pendingFold = true
}

// foldClose adds every SimpleFold orbit member of every rune already in
// the set. Orbits are equivalence classes, so one pass over the
// orbit-relevant runes that are members closes the set exactly: any orbit
// meeting the operand joins it wholesale, and orbit members already
// inside the operand are detected against the normalized base rather
// than a single interval.
func (a *classAccumulator) foldClose() {
	a.normalizeRaw()
	base := a.intervals
	var extra []rune
	i := 0
	for _, iv := range base {
		for i < len(foldOrbitRunes) && foldOrbitRunes[i] < iv.lo {
			i++
		}
		for j := i; j < len(foldOrbitRunes) && foldOrbitRunes[j] <= iv.hi; j++ {
			r := foldOrbitRunes[j]
			for f := unicode.SimpleFold(r); f != r; f = unicode.SimpleFold(f) {
				if !memberOf(base, f) {
					extra = append(extra, f)
				}
			}
		}
	}
	for _, r := range extra {
		a.addRune(r)
	}
}

// memberOf reports rune membership in normalized intervals.
func memberOf(ivs []interval, r rune) bool {
	i := sort.Search(len(ivs), func(i int) bool { return ivs[i].hi >= r })
	return i < len(ivs) && ivs[i].lo <= r
}

// addTable records engine-table membership for one class atom (each
// interval of the positive table an operand member, folded like any
// other operand). The negated form folds the POSITIVE table before
// complementing, exactly like Go's RE2: (?i)[[:^alpha:]] excludes the
// whole fold closure of alpha (ſ and the Kelvin sign stay outside),
// never the raw complement whose stray fold-special members would drag
// their letter orbits back in.
func (a *classAccumulator) addTable(ivs []interval, neg, fold bool) {
	if neg {
		if fold && !a.foldFork {
			ivs = foldClosedCopy(ivs)
		}
		a.addIntervals(complementIntervals(ivs), false)
		return
	}
	a.addIntervals(ivs, fold)
}

// foldClosedCopy returns the SimpleFold closure of a table's interval
// set on a copy, leaving the shared table untouched.
func foldClosedCopy(ivs []interval) []interval {
	tmp := &classAccumulator{}
	tmp.intervals = append(tmp.intervals, ivs...)
	tmp.foldClose()
	tmp.normalize()
	return tmp.intervals
}

// complementIntervals returns the exact complement of a normalized table
// over the rune space. The receiver table is never mutated.
func complementIntervals(ivs []interval) []interval {
	var out []interval
	next := rune(0)
	for _, iv := range ivs {
		if next < iv.lo {
			out = append(out, interval{next, iv.lo - 1})
		}
		next = iv.hi + 1
	}
	if next <= 0x10FFFF {
		out = append(out, interval{next, 0x10FFFF})
	}
	return out
}

// intervalListDifference returns a minus b over interval lists, using
// the accumulator's exact arithmetic (inputs are each treated as a
// union; the result is sorted and coalesced).
func intervalListDifference(a, b []interval) []interval {
	aa := &classAccumulator{intervals: append([]interval(nil), a...)}
	bb := &classAccumulator{intervals: append([]interval(nil), b...)}
	aa.subtract(bb)
	return aa.intervals
}

// The fork stores a character class in lowercase space for
// case-insensitive matching: at class close CharSet.addLowercase
// replaces every singleton operand with its unicode.ToLower image and
// adds the lowercase image interval of each non-singleton operand,
// while the runner lowercases the input before CharIn
// (internal/regexp2/syntax/charclass.go:655-670 and
// internal/regexp2/runner.go:1180-1182). The lcTable below is a
// verbatim port of the fork's lowercase mapping table: it partitions
// the rune space into intervals on which ToLower is one of four affine
// forms, so a range operand's image is computed block by block instead
// of rune by rune.
const (
	lcSet = 0 // image is the constant data
	lcAdd = 1 // image adds data
	lcBor = 2 // image is ch|1
	lcBad = 3 // image is ch+(ch&1)
)

type lcMap struct {
	chMin, chMax rune
	op, data     int32
}

var lcTable = []lcMap{
	{'A', 'Z', lcAdd, 32},
	{0x00C0, 0x00DE, lcAdd, 32},
	{0x0100, 0x012E, lcBor, 0},
	{0x0130, 0x0130, lcSet, 0x0069},
	{0x0132, 0x0136, lcBor, 0},
	{0x0139, 0x0147, lcBad, 0},
	{0x014A, 0x0176, lcBor, 0},
	{0x0178, 0x0178, lcSet, 0x00FF},
	{0x0179, 0x017D, lcBad, 0},
	{0x0181, 0x0181, lcSet, 0x0253},
	{0x0182, 0x0184, lcBor, 0},
	{0x0186, 0x0186, lcSet, 0x0254},
	{0x0187, 0x0187, lcSet, 0x0188},
	{0x0189, 0x018A, lcAdd, 205},
	{0x018B, 0x018B, lcSet, 0x018C},
	{0x018E, 0x018E, lcSet, 0x01DD},
	{0x018F, 0x018F, lcSet, 0x0259},
	{0x0190, 0x0190, lcSet, 0x025B},
	{0x0191, 0x0191, lcSet, 0x0192},
	{0x0193, 0x0193, lcSet, 0x0260},
	{0x0194, 0x0194, lcSet, 0x0263},
	{0x0196, 0x0196, lcSet, 0x0269},
	{0x0197, 0x0197, lcSet, 0x0268},
	{0x0198, 0x0198, lcSet, 0x0199},
	{0x019C, 0x019C, lcSet, 0x026F},
	{0x019D, 0x019D, lcSet, 0x0272},
	{0x019F, 0x019F, lcSet, 0x0275},
	{0x01A0, 0x01A4, lcBor, 0},
	{0x01A7, 0x01A7, lcSet, 0x01A8},
	{0x01A9, 0x01A9, lcSet, 0x0283},
	{0x01AC, 0x01AC, lcSet, 0x01AD},
	{0x01AE, 0x01AE, lcSet, 0x0288},
	{0x01AF, 0x01AF, lcSet, 0x01B0},
	{0x01B1, 0x01B2, lcAdd, 217},
	{0x01B3, 0x01B5, lcBad, 0},
	{0x01B7, 0x01B7, lcSet, 0x0292},
	{0x01B8, 0x01B8, lcSet, 0x01B9},
	{0x01BC, 0x01BC, lcSet, 0x01BD},
	{0x01C4, 0x01C5, lcSet, 0x01C6},
	{0x01C7, 0x01C8, lcSet, 0x01C9},
	{0x01CA, 0x01CB, lcSet, 0x01CC},
	{0x01CD, 0x01DB, lcBad, 0},
	{0x01DE, 0x01EE, lcBor, 0},
	{0x01F1, 0x01F2, lcSet, 0x01F3},
	{0x01F4, 0x01F4, lcSet, 0x01F5},
	{0x01FA, 0x0216, lcBor, 0},
	{0x0386, 0x0386, lcSet, 0x03AC},
	{0x0388, 0x038A, lcAdd, 37},
	{0x038C, 0x038C, lcSet, 0x03CC},
	{0x038E, 0x038F, lcAdd, 63},
	{0x0391, 0x03AB, lcAdd, 32},
	{0x03E2, 0x03EE, lcBor, 0},
	{0x0401, 0x040F, lcAdd, 80},
	{0x0410, 0x042F, lcAdd, 32},
	{0x0460, 0x0480, lcBor, 0},
	{0x0490, 0x04BE, lcBor, 0},
	{0x04C1, 0x04C3, lcBad, 0},
	{0x04C7, 0x04C7, lcSet, 0x04C8},
	{0x04CB, 0x04CB, lcSet, 0x04CC},
	{0x04D0, 0x04EA, lcBor, 0},
	{0x04EE, 0x04F4, lcBor, 0},
	{0x04F8, 0x04F8, lcSet, 0x04F9},
	{0x0531, 0x0556, lcAdd, 48},
	{0x10A0, 0x10C5, lcAdd, 48},
	{0x1E00, 0x1EF8, lcBor, 0},
	{0x1F08, 0x1F0F, lcAdd, -8},
	{0x1F18, 0x1F1F, lcAdd, -8},
	{0x1F28, 0x1F2F, lcAdd, -8},
	{0x1F38, 0x1F3F, lcAdd, -8},
	{0x1F48, 0x1F4D, lcAdd, -8},
	{0x1F59, 0x1F59, lcSet, 0x1F51},
	{0x1F5B, 0x1F5B, lcSet, 0x1F53},
	{0x1F5D, 0x1F5D, lcSet, 0x1F55},
	{0x1F5F, 0x1F5F, lcSet, 0x1F57},
	{0x1F68, 0x1F6F, lcAdd, -8},
	{0x1F88, 0x1F8F, lcAdd, -8},
	{0x1F98, 0x1F9F, lcAdd, -8},
	{0x1FA8, 0x1FAF, lcAdd, -8},
	{0x1FB8, 0x1FB9, lcAdd, -8},
	{0x1FBA, 0x1FBB, lcAdd, -74},
	{0x1FBC, 0x1FBC, lcSet, 0x1FB3},
	{0x1FC8, 0x1FCB, lcAdd, -86},
	{0x1FCC, 0x1FCC, lcSet, 0x1FC3},
	{0x1FD8, 0x1FD9, lcAdd, -8},
	{0x1FDA, 0x1FDB, lcAdd, -100},
	{0x1FE8, 0x1FE9, lcAdd, -8},
	{0x1FEA, 0x1FEB, lcAdd, -112},
	{0x1FEC, 0x1FEC, lcSet, 0x1FE5},
	{0x1FF8, 0x1FF9, lcAdd, -128},
	{0x1FFA, 0x1FFB, lcAdd, -126},
	{0x1FFC, 0x1FFC, lcSet, 0x1FF3},
	{0x2160, 0x216F, lcAdd, 16},
	{0x24B6, 0x24D0, lcAdd, 26},
	{0xFF21, 0xFF3A, lcAdd, 32},
}

// lcImageIntervals computes the lowercase image intervals of one
// non-singleton operand exactly like the fork's addLowercaseRange: walk
// the mapping table from the first entry reaching the operand, clamp to
// the operand bounds, apply the block transform, and keep an image
// interval only when the transformed bounds escape the original
// operand's bounds (an image inside the operand adds no member).
func lcImageIntervals(lo, hi rune) []interval {
	var out []interval
	i := sort.Search(len(lcTable), func(j int) bool { return lcTable[j].chMax >= lo })
	for ; i < len(lcTable); i++ {
		lc := lcTable[i]
		if lc.chMin > hi {
			break
		}
		loT, hiT := lc.chMin, lc.chMax
		if loT < lo {
			loT = lo
		}
		if hiT > hi {
			hiT = hi
		}
		switch lc.op {
		case lcSet:
			loT, hiT = rune(lc.data), rune(lc.data)
		case lcAdd:
			loT += rune(lc.data)
			hiT += rune(lc.data)
		case lcBor:
			loT |= 1
			hiT |= 1
		case lcBad:
			loT += loT & 1
			hiT += hiT & 1
		}
		if loT < lo || hiT > hi {
			out = append(out, interval{loT, hiT})
		}
	}
	return out
}

// forkLowerRunes lists every rune whose unicode.ToLower image differs
// from itself, with forkLowerImages holding the parallel images and
// forkNonIdentityIntervals the same set coalesced: the ToLower
// preimage of a fork folded class maps the stored lowercase set
// through these.
var forkLowerRunes, forkLowerImages, forkNonIdentityIntervals = buildForkLowerMaps()

func buildForkLowerMaps() (runes, images []rune, ivs []interval) {
	for r := rune(0); r <= 0x10FFFF; r++ {
		if l := unicode.ToLower(r); l != r {
			runes = append(runes, r)
			images = append(images, l)
		}
	}
	for _, r := range runes {
		if n := len(ivs); n > 0 && r == ivs[n-1].hi+1 {
			ivs[n-1].hi = r
		} else {
			ivs = append(ivs, interval{r, r})
		}
	}
	return runes, images, ivs
}

// forkPreimage maps a sorted stored lowercase set to the rune set whose
// ToLower images land inside it: identity-lowering members pass
// through, and each rune with a nontrivial lowercase image joins
// exactly when its image is stored.
func forkPreimage(stored []interval) []interval {
	out := intervalListDifference(stored, forkNonIdentityIntervals)
	for i, r := range forkLowerRunes {
		if memberOf(stored, forkLowerImages[i]) {
			out = append(out, interval{r, r})
		}
	}
	merged := &classAccumulator{intervals: out}
	merged.normalizeRaw()
	return merged.intervals
}

// settleForkFold converts the pending operands of a fork folded class
// into their rune-space membership. The literal operands first
// canonicalize (overlapping AND abutting ranges merge, mirroring the
// fork's canonicalize as members arrive in
// internal/regexp2/syntax/charclass.go:605-650) BEFORE the lowering
// pass (charclass.go:655 addLowercase): merging first keeps adjacent
// fold-special literals as one range whose lcTable image the fork
// never supplies ([KÅ] settles to U+212A-U+212B, not to two separate
// ToLower singletons). The lowered stored set then maps through the
// ToLower preimage immediately: the preimage is a Boolean
// homomorphism, so lifting here plus merging the rune-space property
// operands verbatim equals lifting the whole class expression at
// close, and every later complement, subtraction or nested-scope
// transfer stays exact in rune space. Only the pending lists
// transform, so settling twice never re-lowers or re-lifts.
func (a *classAccumulator) settleForkFold() {
	if !a.foldFork {
		return
	}
	if len(a.pendingRaw) == 0 && len(a.pendingProp) == 0 {
		return
	}
	if len(a.pendingRaw) != 0 {
		a.pendingRaw = coalesceRaw(a.pendingRaw)
		var fresh []interval
		for _, iv := range a.pendingRaw {
			if iv.lo == iv.hi {
				l := unicode.ToLower(iv.lo)
				fresh = append(fresh, interval{l, l})
				continue
			}
			fresh = append(fresh, iv)
			fresh = append(fresh, lcImageIntervals(iv.lo, iv.hi)...)
		}
		a.pendingRaw = a.pendingRaw[:0]
		tmp := &classAccumulator{intervals: fresh}
		tmp.normalizeRaw()
		a.intervals = append(a.intervals, forkPreimage(tmp.intervals)...)
	}
	a.intervals = append(a.intervals, a.pendingProp...)
	a.pendingProp = nil
	a.sorted = false
}

// coalesceRaw sorts one owned operand list and merges overlapping or
// abutting intervals, matching the fork's canonicalize (a range whose
// first rune is at most one past the previous last rune joins it).
func coalesceRaw(ivs []interval) []interval {
	tmp := &classAccumulator{intervals: ivs}
	tmp.normalizeRaw()
	return tmp.intervals
}

// materializeForkFold finishes a fork folded class at its close: the
// pending operands settle (literals through the lowercase stored set
// and its ToLower preimage; properties as their rune-space contract
// closure), so the recorded intervals hold the exact membership.
// foldConsumed then shields the set from any later fold handling;
// recorded atoms stay recorded, keeping classes with an
// engine-rejected member conservative.
func (a *classAccumulator) materializeForkFold() {
	if !a.foldFork || a.foldConsumed {
		return
	}
	a.settleForkFold()
	a.foldFork = false
	a.foldConsumed = true
	if len(a.atoms) != 0 {
		return
	}
	a.normalizeRaw()
}

// addForkProperty records one \p/\P member of a fork folded class.
// Structural membership for shared Unicode property syntax derives
// from the ICU compatibility contract, independently of the fork's
// consuming behavior: each operand closes to tab ∪ fold companion
// (the FoldCategory/FoldScript merge Go RE2 and ICU 77.1 apply per
// operand, rev19-confirmed at every fold-special fiber including
// U+0130) and a negated operand complements AFTER that merge. The
// resolved set is FINAL rune-space membership and joins the class at
// settle verbatim; it never enters the fork's lowercase pipeline,
// whose ToLower preimage would mis-place U+0130 (fork input lowering
// treats Ÿ as 'i'; ICU and Go RE2 close over SimpleFold orbits and
// exclude it from (?i)\p{Ll}). The name resolves against the fork's
// exact-name namespace (Categories+Scripts+Properties,
// internal/regexp2/syntax charclass.go unicodeCategories); names the
// fork rejects stay conservative atoms.
func (a *classAccumulator) addForkProperty(name string, neg bool) {
	tab := propertyTable(name, DialectRegexp2)
	if tab == nil {
		a.recordAtom(name, neg)
		return
	}
	ftab := unicode.FoldCategory[name]
	if f := unicode.FoldScript[name]; f != nil {
		ftab = f
	}
	a.pendingProp = append(a.pendingProp, resolvedPropertyIntervals(tab, ftab, neg, true)...)
}

// canonicalNameGo ports regexp/syntax canonicalName (Go 1.27.1): the
// first letter uppercases, the rest lowercase, and '_', '-' and ' '
// drop out.
func canonicalNameGo(name string) string {
	var b []byte
	first := true
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c == '_' || c == '-' || c == ' ':
			continue
		case first:
			if 'a' <= c && c <= 'z' {
				c -= 'a' - 'A'
			}
			first = false
		default:
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
		}
		b = append(b, c)
	}
	return string(b)
}

// goASCIIPropTable and goASCIIPropFoldTable port the \p{Ascii} special
// case tables from regexp/syntax (Go 1.27.1): the fold companion adds
// U+017F and U+212A, which fold into the ASCII letters.
var goASCIIPropTable = &unicode.RangeTable{
	R16: []unicode.Range16{{Lo: 0, Hi: 0x7F, Stride: 1}},
}
var goASCIIPropFoldTable = &unicode.RangeTable{
	R16: []unicode.Range16{
		{Lo: 0, Hi: 0x7F, Stride: 1},
		{Lo: 0x017F, Hi: 0x017F, Stride: 1},
		{Lo: 0x212A, Hi: 0x212A, Stride: 1},
	},
}

// goAliases mirrors the canonicalized alias lookup regexp/syntax
// builds from unicode.CategoryAliases plus every script name: the map
// is keyed by canonicalName so that, for example, lowercaseletter
// resolves through the Lowercase_Letter alias of Ll.
var goAliases struct {
	once       sync.Once
	categories map[string]string
	scripts    map[string]string
}

func initGoAliases() {
	goAliases.categories = make(map[string]string)
	for name, actual := range unicode.CategoryAliases {
		goAliases.categories[canonicalNameGo(name)] = actual
	}
	goAliases.scripts = make(map[string]string)
	for name := range unicode.Scripts {
		goAliases.scripts[canonicalNameGo(name)] = name
	}
}

// goAnyTable is the full-range table of Go RE2's \p{Any}: every
// canonical spelling (any, a_n_y, ANY - canonicalName drops '_', '-'
// and ' ' and case-normalizes) resolves to the same universe, folded
// or not, and the negated forms match nothing. The fork rejects every
// Any spelling at parse, so the fork paths keep recording atoms.
var goAnyTable = &unicode.RangeTable{
	R32: []unicode.Range32{{Lo: 0, Hi: 0x10FFFF, Stride: 1}},
}

// goPropertyTable ports regexp/syntax unicodeTable (Go 1.27.1): the
// canonicalized name resolves to a membership table plus its fold
// companion and sign. The Any, Assigned, Ascii and Lc specials come
// first, then categories and scripts, then the canonicalized alias
// maps. Names Go rejects report ok=false, which keeps the
// conservative atom.
func goPropertyTable(name string) (tab, fold *unicode.RangeTable, sign int, ok bool) {
	cname := canonicalNameGo(name)
	switch cname {
	case "Any":
		return goAnyTable, goAnyTable, +1, true
	case "Assigned":
		return unicode.Categories["Cn"], unicode.Categories["Cn"], -1, true
	case "Ascii":
		return goASCIIPropTable, goASCIIPropFoldTable, +1, true
	case "Lc":
		return unicode.Categories["LC"], unicode.FoldCategory["LC"], +1, true
	}
	if t := unicode.Categories[cname]; t != nil {
		return t, unicode.FoldCategory[cname], +1, true
	}
	if t := unicode.Scripts[cname]; t != nil {
		return t, unicode.FoldScript[cname], +1, true
	}
	goAliases.once.Do(initGoAliases)
	if actual := goAliases.categories[cname]; actual != "" {
		return unicode.Categories[actual], unicode.FoldCategory[actual], +1, true
	}
	if actual := goAliases.scripts[cname]; actual != "" {
		return unicode.Scripts[actual], unicode.FoldScript[actual], +1, true
	}
	return nil, nil, 0, false
}

// goPropertyKey identifies one RESOLVED \p/\P computation for the
// memoization in goPropertyCache: the finite table identity plus the
// effective polarity and fold mode, never the raw user-supplied name.
// Alias and spelling variants of one property (L, L_, L__,
// lowercaseletter, ...) collapse to a single entry, so the cache
// stays bounded by the property universe and retains no pattern
// allocations; names that fail to resolve are never cached.
type goPropertyKey struct {
	tab, ftab *unicode.RangeTable
	neg, fold bool
}

var goPropertyCache sync.Map // map[goPropertyKey][]interval

// resolvedPropertyIntervals computes (and memoizes) one resolved
// property set: tab merged with its FoldCategory/FoldScript companion
// under fold, complemented after the merge when negated
// (regexp/syntax parseUnicodeClass ordering). Both dialect paths
// share it; the cached slices are treated as read-only by every
// consumer.
func resolvedPropertyIntervals(tab, ftab *unicode.RangeTable, neg, fold bool) []interval {
	key := goPropertyKey{tab: tab, ftab: ftab, neg: neg, fold: fold}
	if v, hit := goPropertyCache.Load(key); hit {
		return v.([]interval)
	}
	merged := &classAccumulator{}
	merged.addIntervals(cachedExpand(tab, false), false)
	if fold && ftab != nil {
		merged.addIntervals(cachedExpand(ftab, false), false)
	}
	merged.normalizeRaw()
	if neg {
		merged.applyComplement()
	}
	ivs := merged.intervals
	goPropertyCache.Store(key, ivs)
	return ivs
}

// goPropertyIntervals resolves a \p/\P member against Go RE2's
// production semantics: the Unicode tables through the canonicalized
// name and aliases, folding by merging the fold companion table when
// FoldCase is active, with the table sign applied after the merge
// (regexp/syntax parseUnicodeClass). ok=false marks the names Go's
// parse rejects; the rejection is map-miss work only and stays
// uncached, which keeps the conservative atom.
func goPropertyIntervals(name string, neg, fold bool) ([]interval, bool) {
	tab, ftab, sign, ok := goPropertyTable(name)
	if !ok {
		return nil, false
	}
	if sign < 0 {
		neg = !neg
	}
	return resolvedPropertyIntervals(tab, ftab, neg, fold), true
}

// expandedTables memoizes positive and complement expansions of the
// Unicode tables so repeated property classes in one pattern (and across
// classifications) share one flattening. The cached slices are treated
// as read-only by every consumer.
var expandedTables sync.Map // map[*unicode.RangeTable]*expansionPair

type expansionPair struct {
	posOnce, negOnce sync.Once
	pos, neg         []interval
}

// cachedExpand returns the table's interval expansion, or its exact
// complement when neg is set, flattening at most once per table.
func cachedExpand(t *unicode.RangeTable, neg bool) []interval {
	v, _ := expandedTables.LoadOrStore(t, &expansionPair{})
	p := v.(*expansionPair)
	if neg {
		p.negOnce.Do(func() {
			p.neg = complementIntervals(cachedExpand(t, false))
		})
		return p.neg
	}
	p.posOnce.Do(func() {
		p.pos = expandTable(t)
	})
	return p.pos
}

// expandTable flattens a Unicode range table into intervals. Strided
// ranges (stride above one is unused by the tables carried here) expand
// to their member runes.
func expandTable(t *unicode.RangeTable) []interval {
	var out []interval
	add := func(lo, hi, stride uint32) {
		if stride == 1 {
			out = append(out, interval{rune(lo), rune(hi)})
			return
		}
		for v := lo; v <= hi; v += stride {
			out = append(out, interval{rune(v), rune(v)})
		}
	}
	for _, r := range t.R16 {
		add(uint32(r.Lo), uint32(r.Hi), uint32(r.Stride))
	}
	for _, r := range t.R32 {
		add(r.Lo, r.Hi, r.Stride)
	}
	return out
}

// Shorthand membership is identical on both production engines (Go's
// perl_groups code1-3 and the fork's RE2-mode addDigit/addSpace/addWord
// all agree): \d is [0-9], \s is {\t \n \f \r space} without VT, and \w
// is [0-9A-Za-z_].
var shorthandTables = map[string][]interval{
	"d": {{'0', '9'}},
	"s": {{0x09, 0x0A}, {0x0C, 0x0D}, {0x20, 0x20}},
	"w": {{'0', '9'}, {'A', 'Z'}, {'_', '_'}, {'a', 'z'}},
}

// POSIX membership most names share: Go's posixGroup ranges and the
// fork's addNamedASCII ASCII entries are identical for these classes.
var posixASCIITables = map[string][]interval{
	"alnum":  {{'0', '9'}, {'A', 'Z'}, {'a', 'z'}},
	"alpha":  {{'A', 'Z'}, {'a', 'z'}},
	"ascii":  {{0x00, 0x7F}},
	"blank":  {{0x09, 0x09}, {0x20, 0x20}},
	"cntrl":  {{0x00, 0x1F}, {0x7F, 0x7F}},
	"graph":  {{0x21, 0x7E}},
	"lower":  {{'a', 'z'}},
	"print":  {{0x20, 0x7E}},
	"punct":  {{0x21, 0x2F}, {0x3A, 0x40}, {0x5B, 0x60}, {0x7B, 0x7E}},
	"upper":  {{'A', 'Z'}},
	"word":   {{'0', '9'}, {'A', 'Z'}, {'_', '_'}, {'a', 'z'}},
	"xdigit": {{'0', '9'}, {'A', 'F'}, {'a', 'f'}},
}

// The two POSIX names the engines disagree on: the fork reads
// [[:digit:]] as \p{Nd} and [[:space:]] as the ECMAScript space list
// (charclass.go addNamedASCII -> addDigit/addSpace with the production
// RE2-mode flags), while Go's RE2 keeps both ASCII.
var (
	posixDigitRE2 = []interval{{'0', '9'}}
	posixSpaceRE2 = []interval{{0x09, 0x0D}, {0x20, 0x20}}

	posixDigitRegexp2 = expandTable(unicode.Nd)
	posixSpaceRegexp2 = []interval{
		{0x09, 0x0D}, {0x20, 0x20}, {0xA0, 0xA0}, {0x1680, 0x1680},
		{0x2000, 0x200A}, {0x2028, 0x2029}, {0x202F, 0x202F},
		{0x205F, 0x205F}, {0x3000, 0x3000}, {0xFEFF, 0xFEFF},
	}
)

// posixIntervals returns POSIX named-class membership for the dialect,
// nil when the name is not a POSIX class the engines know.
func posixIntervals(name string, d Dialect) []interval {
	switch name {
	case "digit":
		if d.regexp2() {
			return posixDigitRegexp2
		}
		return posixDigitRE2
	case "space":
		if d.regexp2() {
			return posixSpaceRegexp2
		}
		return posixSpaceRE2
	}
	return posixASCIITables[name]
}

// propertyTable resolves a property name the way the production engine
// does: both accept the Unicode general categories and scripts, and the
// fork additionally merges unicode.Properties (regexp2/syntax
// charclass.go unicodeCategories); Go's RE2 rejects other names at
// parse, so they stay unresolved here. The fork's unfolded properties
// and its folded property operands resolve through this table; Go
// RE2's own resolution (canonical names, aliases, Any and fold-merge)
// lives in goPropertyIntervals.
func propertyTable(name string, d Dialect) *unicode.RangeTable {
	if t := unicode.Categories[name]; t != nil {
		return t
	}
	if t := unicode.Scripts[name]; t != nil {
		return t
	}
	if d.regexp2() {
		return unicode.Properties[name]
	}
	return nil
}

// skipClassBody advances past a class body starting just after '['.
// Used only by the balanced-group skipper; it is approximate on purpose.
func skipClassBody(s string, i int) int {
	if i < len(s) && s[i] == '^' {
		i++
	}
	for i < len(s) {
		c := s[i]
		i++
		if c == ']' {
			return i
		}
		if c == '\\' && i < len(s) {
			i++
		}
	}
	return i
}

// classAtom parses one member atom starting at sc.pos. ok=false means the
// atom was a class escape or POSIX/property/quoted construct consumed
// internally (conservatism recorded on sc.activeClass where applicable).
func (sc *scanner) classAtom() (lo, hi rune, ok bool) {
	s := sc.s
	c, size := utf8.DecodeRuneInString(s[sc.pos:])

	if c == '[' && size == 1 && sc.pos+1 < len(s) && s[sc.pos+1] == ':' {
		// POSIX named class [:alpha:]. The engines honor the caret's
		// polarity (the fork reads it in production RE2 mode at
		// internal/regexp2/syntax/parser.go:1545-1563 and Go's RE2
		// supports the [:^name:] form natively), and each name now
		// resolves against the engine's exact membership table, so
		// coverage with a complementary shorthand or property in the
		// same body goes through the same interval arithmetic as any
		// other member. Only names the engines reject stay recorded
		// atoms with the conservative CardMany answer.
		nameStart := sc.pos + 2
		end := sc.pos
		for end+1 < len(s) && !(s[end] == ':' && s[end+1] == ']') {
			end++
		}
		name := s[nameStart:end]
		next := end + 2
		if end+1 >= len(s) {
			name = s[nameStart:]
			next = len(s)
		}
		neg := false
		if len(name) > 0 && name[0] == '^' {
			neg = true
			name = name[1:]
		}
		// Resolve the exact per-dialect membership (Go's posixGroup
		// tables; the fork's addNamedASCII with its two Unicode
		// entries) so complementary pairs across families still prove
		// coverage - [^[:digit:]\D] is empty on both engines, and an
		// unresolved atom could never show it. Names the engines reject
		// keep the conservative recorded atom.
		if ivs := posixIntervals(name, sc.dialect); ivs != nil {
			sc.activeClass.addTable(ivs, neg, sc.fold)
		} else {
			sc.activeClass.recordAtom("posix:"+name, neg)
		}
		sc.pos = next
		return 0, 0, false
	}

	if c != '\\' {
		sc.pos += size
		return c, c, true
	}

	// Escape inside a class.
	sc.pos += size
	if sc.pos >= len(s) {
		sc.activeClass.conservative = true
		return 0, 0, false
	}
	e, esize := utf8.DecodeRuneInString(s[sc.pos:])
	sc.pos += esize
	switch e {
	case 'n':
		return '\n', '\n', true
	case 'r':
		return '\r', '\r', true
	case 't':
		return '\t', '\t', true
	case 'f':
		return '\f', '\f', true
	case 'a':
		return '\a', '\a', true
	case 'v':
		return '\v', '\v', true
	case 'b':
		return '\b', '\b', true // in-class \b is backspace on both engines
	case 'e':
		if sc.dialect.regexp2() {
			return 0x1B, 0x1B, true
		}
		sc.activeClass.conservative = true
		return 0, 0, false
	case 'd', 'D', 's', 'S', 'w', 'W':
		// Both production engines agree on the ASCII shorthand tables,
		// so record the exact membership: complementary pairs still
		// resolve to full coverage, and mixed coverage with POSIX or
		// property members now resolves through the same arithmetic.
		sc.activeClass.addTable(shorthandTables[string(unicode.ToLower(e))], e != unicode.ToLower(e), sc.fold)
		return 0, 0, false
	case 'p', 'P':
		// Same coverage treatment for properties; \p{^X} flips
		// polarity. Every name resolves per dialect, folded or not: Go
		// RE2 merges the property with its FoldCategory/FoldScript
		// companion and resolves canonical names, aliases and every
		// spelling of Any, while a fork folded class materializes the
		// ICU per-operand closure in its lowercase pipeline. The fork
		// rejects the names outside its exact-name namespace (Any
		// included) at parse, so they keep the conservative recorded
		// atom there.
		name, negBody := sc.scanPropertyName()
		neg := negBody != (e == 'P')
		if sc.fold && sc.dialect.regexp2() {
			sc.activeClass.addForkProperty(name, neg)
			return 0, 0, false
		}
		if !sc.dialect.regexp2() {
			if ivs, ok := goPropertyIntervals(name, neg, sc.fold); ok {
				sc.activeClass.addIntervals(ivs, false)
			} else {
				sc.activeClass.recordAtom(name, neg)
			}
			return 0, 0, false
		}
		if t := propertyTable(name, sc.dialect); t != nil {
			// The cached complement is already the exact negated set.
			sc.activeClass.addIntervals(cachedExpand(t, neg), false)
		} else {
			sc.activeClass.recordAtom(name, neg)
		}
		return 0, 0, false
	case '0':
		v := rune(0)
		n := 0
		for n < 2 && sc.pos < len(s) && s[sc.pos] >= '0' && s[sc.pos] <= '7' {
			v = v*8 + rune(s[sc.pos]-'0')
			sc.pos++
			n++
		}
		return v, v, true
	case 'x':
		if sc.pos < len(s) && s[sc.pos] == '{' {
			sc.pos++
			if r, okH := sc.scanHexUntilBrace(); okH {
				return r, r, true
			}
			sc.activeClass.conservative = true
			return 0, 0, false
		}
		if r, okH := sc.scanHexDigits(2); okH {
			return r, r, true
		}
		if sc.dialect.regexp2() {
			return 'x', 'x', true
		}
		sc.activeClass.conservative = true
		return 0, 0, false
	case 'u':
		if sc.dialect.regexp2() {
			if r, okH := sc.scanHexDigits(4); okH {
				return r, r, true
			}
			return 'u', 'u', true
		}
		sc.activeClass.conservative = true
		return 0, 0, false
	case 'U':
		// The SQL translator rewrites a valid \Uxxxxxxxx inside classes to
		// \x{xxxxxxxx} on the text regexp2 route; the binary route and
		// short or invalid forms pass through for the engine's literal 'U'.
		if sc.dialect == DialectRegexp2 {
			if r, okH := sc.scanHexDigits(8); okH {
				if r > 0x10FFFF {
					r = 0x10FFFF
				}
				return r, r, true
			}
		}
		return 'U', 'U', true
	case 'c':
		if sc.dialect.regexp2() && sc.pos < len(s) {
			cc, csz := utf8.DecodeRuneInString(s[sc.pos:])
			sc.pos += csz
			if cc >= 'a' && cc <= 'z' {
				cc -= 'a' - 'A'
			}
			v := cc & 0x1F
			return v, v, true
		}
		sc.activeClass.conservative = true
		return 0, 0, false
	case 'Q':
		if sc.dialect.regexp2() {
			// The class translator keeps \Q verbatim; the fork scans the
			// literal char 'Q'.
			return 'Q', 'Q', true
		}
		// \Q...\E inside classes is literal text on the RE2 route; each
		// quoted rune is its own operand (Go folds quoted literals under
		// (?i) like any other member).
		for sc.pos < len(s) {
			if sc.s[sc.pos] == '\\' && sc.pos+1 < len(s) && sc.s[sc.pos+1] == 'E' {
				sc.pos += 2
				break
			}
			q, qsz := utf8.DecodeRuneInString(s[sc.pos:])
			sc.pos += qsz
			sc.activeClass.addMember(q, q, sc.fold)
		}
		return 0, 0, false
	default:
		// Escaped punctuation or unknown letter: literal char per the fork;
		// unsupported escapes in RE2 are engine errors and unreachable.
		return e, e, true
	}
}
