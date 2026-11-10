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

// fold walks the event stream applying ICU's quantifier lowering,
// literal-run sealing rules and the branch-forwarded initial-string
// accumulator, and derives the descriptor. It is the single shared
// classification core consumed by both engine dialects.
func fold(events []Event) Descriptor {
	c := &core{atStart: true}
	// A pseudo-frame makes top-level alternations behave exactly like one
	// group's worth of arms.
	c.pushFrame(GroupNonCapture)
	for i := range events {
		c.step(events[i])
	}
	for len(c.frames) > 0 {
		c.popTopFrame()
	}
	return Descriptor{
		LineAnchoredStart: len(c.marks) > 0,
		StringExempt:      c.numInit == 1 && c.minLen() > 0,
	}
}

// snapshot captures the state a deletion or retroactive truncation
// restores.
type snapshot struct {
	atStart bool
	dead    bool
	curLen  int64
	numInit int
	marks   int
}

// unitKind identifies what a quantifier binds to.
type unitKind uint8

const (
	unitNone unitKind = iota
	unitGroup
	unitAtomic
)

// unit is a quantifiable non-run target (a closed group or a non-literal
// consuming/zero-width atom).
type unit struct {
	kind    unitKind
	snap    snapshot // pre-unit state for {0} deletion and truncation
	bodyMin int64    // closed group's content minimum; 0 for zero-width atoms
}

// runChar is one pending literal with the state needed to restore a
// deletion of a suffix binding.
type runChar struct {
	r             rune
	fold          bool
	atStartBefore bool
}

// frame is one open alternation scope (a group, or the top-level pseudo
// frame).
type frame struct {
	kind     GroupKind
	snap     snapshot // state at the frame's content start
	entryLen int64
	altSeen  bool
	armLens  []int64 // absolute curLen at each finished arm's end
	armDead  []bool
}

// core is the fold state.
type core struct {
	atStart bool
	dead    bool // current path killed by an empty-class failure
	curLen  int64
	numInit int
	marks   []int

	run     []runChar
	runBase snapshot

	frames []frame
	last   unit

	// classSnap is the state captured at class open; an empty (never
	// matching) class uses it to stay a quantifiable unit, so a following
	// zero-minimum quantifier can restore the live bypass path.
	classSnap snapshot

	skipDepth int
	skipSave  snapshot
}

func (c *core) snap() snapshot {
	return snapshot{atStart: c.atStart, dead: c.dead, curLen: c.curLen, numInit: c.numInit, marks: len(c.marks)}
}

func (c *core) restore(s snapshot) {
	c.atStart = s.atStart
	c.dead = s.dead
	c.curLen = s.curLen
	c.numInit = s.numInit
	c.marks = c.marks[:s.marks]
}

// minLen is the pattern's minimum match length over live paths.
func (c *core) minLen() int64 {
	if c.dead {
		return 0
	}
	return c.curLen
}

func (c *core) step(e Event) {
	// Whole lookaround bodies are skipped with the pre-open state restored
	// at the matching close (ICU's LA/LB skip).
	if c.skipDepth > 0 {
		switch e.Kind {
		case KindGroupOpen:
			c.skipDepth++
		case KindGroupClose:
			c.skipDepth--
			if c.skipDepth == 0 {
				c.restore(c.skipSave)
				c.last = unit{}
				// Pop the frame the skipped lookaround pushed at its open,
				// so a later alternation resolves against the enclosing
				// scope, not the abandoned lookaround scope.
				if n := len(c.frames); n > 0 {
					c.frames = c.frames[:n-1]
				}
			}
		}
		return
	}

	switch e.Kind {
	case KindLiteral:
		c.appendLiteral(e.Rune, e.Fold)
	case KindClassOpen:
		c.seal()
		c.classSnap = c.snap()
	case KindClassClose:
		c.classClose(e)
	case KindGroupOpen:
		c.groupOpen(e.Group)
	case KindGroupClose:
		c.groupClose()
	case KindFlags:
		c.seal()
	case KindQuant:
		c.quantify(e)
	case KindBranch:
		c.branch()
	case KindAnchor:
		c.anchor(e.Anchor)
	case KindBackref:
		c.seal()
		c.last = unit{kind: unitAtomic, snap: c.snap(), bodyMin: 0}
	case KindBackslashV:
		c.seal()
		c.consume()
	case KindOpaque:
		c.seal()
		c.consume()
	case KindEnd:
		c.seal()
	}
}

// consume handles one non-run consuming atom (set, any-char, \v, opaque):
// +2 initial content, min +1, atStart destroyed. It also records the atom
// as a quantifiable unit.
func (c *core) consume() {
	snap := c.snap()
	if !c.dead {
		if c.curLen == 0 {
			c.numInit += 2
		}
		c.curLen++
	}
	c.atStart = false
	c.last = unit{kind: unitAtomic, snap: snap, bodyMin: 1}
}

// appendLiteral joins a literal to the pending run.
func (c *core) appendLiteral(r rune, fold bool) {
	if c.dead {
		c.atStart = false
		c.last = unit{}
		return
	}
	if len(c.run) == 0 {
		c.runBase = c.snap()
	}
	c.run = append(c.run, runChar{r: r, fold: fold, atStartBefore: c.atStart})
	c.atStart = false
	c.last = unit{}
}

// seal emits the pending run as one unit (ICU fixLiterals(false)).
func (c *core) seal() {
	if len(c.run) == 0 {
		return
	}
	c.emitRun(len(c.run))
}

// emitRun emits the first n pending run chars as one unit and advances the
// minimum length. A case-sensitive run of >= 2 code points counts as one
// initial string; a length-1 or folded run counts as two.
func (c *core) emitRun(n int) {
	part := c.run[:n]
	folded := false
	for _, rc := range part {
		if rc.fold {
			folded = true
			break
		}
	}
	if !c.dead {
		if len(part) >= 2 && !folded {
			if c.curLen == 0 {
				c.numInit++
			}
		} else {
			if c.curLen == 0 {
				c.numInit += 2
			}
		}
		c.curLen += int64(len(part))
	}
	c.run = c.run[n:]
}

// classClose resolves a class body per its cardinality.
func (c *core) classClose(e Event) {
	switch e.Card {
	case CardEmpty:
		// Failure event: no initial content; this path is killed while the
		// class is mandatory. The class still stands as a quantifiable
		// zero-content unit so a zero-minimum quantifier over it (star,
		// question, {0,n} of any flavor) can restore the live bypass path
		// like the engines do; a mandatory quantifier keeps the path dead.
		c.dead = true
		c.atStart = false
		c.last = unit{kind: unitAtomic, snap: c.classSnap, bodyMin: 0}
	case CardOne:
		// The singleton char joins the new run the open seal just started.
		c.appendLiteral(e.Rune, e.Fold)
	default:
		c.consume()
	}
}

func (c *core) groupOpen(kind GroupKind) {
	c.seal()
	c.pushFrame(kind)
	c.last = unit{}
}

func (c *core) pushFrame(kind GroupKind) {
	c.frames = append(c.frames, frame{
		kind:     kind,
		snap:     c.snap(),
		entryLen: c.curLen,
	})
	if kind == GroupLookahead || kind == GroupLookbehind {
		c.skipSave = c.snap()
		c.skipDepth = 1
	}
}

// mergeArmMin folds a finished arm's end into the running minimum over
// live arms.
func mergeArmMin(bestLen int64, bestDead, live bool, armLen int64, armDead bool) (int64, bool, bool) {
	if armDead {
		return bestLen, bestDead, live
	}
	if !live || armLen < bestLen {
		return armLen, false, true
	}
	return bestLen, bestDead, live
}

// groupClose handles capture/noncapture/scoped/atomic closes; lookaround
// closes arrive only via the skip walker.
func (c *core) groupClose() {
	c.seal()
	if len(c.frames) == 0 {
		return
	}
	f := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]

	if f.altSeen {
		bestLen, bestDead, live := int64(0), true, false
		for i := range f.armLens {
			bestLen, bestDead, live = mergeArmMin(bestLen, bestDead, live, f.armLens[i], f.armDead[i])
		}
		bestLen, bestDead, _ = mergeArmMin(bestLen, bestDead, live, c.curLen, c.dead)
		c.curLen, c.dead = bestLen, bestDead
	}
	bodyMin := c.curLen - f.entryLen
	if c.dead {
		bodyMin = 0
	}
	c.last = unit{kind: unitGroup, snap: f.snap, bodyMin: bodyMin}
}

// popTopFrame unwinds one frame left open at pattern end (the pseudo-frame
// plus any engine-rejected leftovers, which cannot occur in practice).
func (c *core) popTopFrame() {
	if len(c.frames) == 0 {
		return
	}
	f := c.frames[len(c.frames)-1]
	c.frames = c.frames[:len(c.frames)-1]
	if f.altSeen {
		bestLen, bestDead, live := int64(0), true, false
		for i := range f.armLens {
			bestLen, bestDead, live = mergeArmMin(bestLen, bestDead, live, f.armLens[i], f.armDead[i])
		}
		bestLen, bestDead, _ = mergeArmMin(bestLen, bestDead, live, c.curLen, c.dead)
		c.curLen, c.dead = bestLen, bestDead
	}
}

// branch mirrors ICU's compiled program order: the branch state-save
// precedes every arm, so carets recorded in this alternation's earlier
// arms are bare, atStart is destroyed, and the minimum is forwarded into
// the new arm. The sibling arm starts with the frame's ENTRY
// reachability: an alternation nested inside an already-dead path can
// never match, so its arms must not revive on their own (only a
// zero-minimum quantifier over the enclosing unit restores the bypass).
func (c *core) branch() {
	c.seal()
	if len(c.frames) == 0 {
		return
	}
	f := &c.frames[len(c.frames)-1]
	f.armLens = append(f.armLens, c.curLen)
	f.armDead = append(f.armDead, c.dead)
	if !f.altSeen {
		f.altSeen = true
		// Discard carets recorded inside this alternation's earlier arm.
		c.marks = c.marks[:f.snap.marks]
	}
	c.atStart = false
	c.curLen = f.entryLen
	c.dead = f.snap.dead
	c.last = unit{}
}

// anchor seals the run; a line caret reached while atStart records the
// line-anchored start mark.
func (c *core) anchor(kind AnchorKind) {
	c.seal()
	if kind == AnchorCaretLine && c.atStart && !c.dead {
		c.marks = append(c.marks, len(c.marks))
	}
	c.last = unit{}
}

// quantify applies ICU's form- and flavor-dispatched quantifier lowering
// to the last unit.
func (c *core) quantify(e Event) {
	q := quant{form: e.Form, min: e.Min, max: e.Max, flavor: e.Flavor}

	// Char target: the quantifier binds to the last pending run code point
	// (the run split of fixLiterals(true)).
	if len(c.run) > 0 {
		lastCh := c.run[len(c.run)-1]
		c.run = c.run[:len(c.run)-1]
		if len(c.run) > 0 {
			c.emitRun(len(c.run))
		}
		// The prefix is sealed before the unit, so the snapshot for a later
		// deletion is taken after its emission.
		snap := snapshot{atStart: lastCh.atStartBefore, dead: c.dead, curLen: c.curLen, numInit: c.numInit, marks: len(c.marks)}
		c.quantSingular(q, snap, 1, false)
		c.last = unit{}
		return
	}

	switch c.last.kind {
	case unitGroup, unitAtomic:
		c.quantSingular(q, c.last.snap, c.last.bodyMin, true)
		c.last = unit{}
	default:
		// Quantifier with nothing to bind: engines reject; conservative.
		c.atStart = false
		c.last = unit{}
	}
}

// quant is the lowered quantifier view.
type quant struct {
	form   QuantForm
	min    int
	max    int
	flavor Flavor
}

// quantSingular lowers the quantifier over a unit whose content has been
// folded once: a pending char (alreadyCounted=false; the char itself still
// needs its presence counted), a single atom or a closed group body
// (alreadyCounted=true; the presence was counted when folded). snap is the
// state before the unit's own effects; bodyMin is the unit's minimum
// content length.
func (c *core) quantSingular(q quant, snap snapshot, bodyMin int64, alreadyCounted bool) {
	// truncate mirrors the loop init/guard placed BEFORE the body by star,
	// question and interval forms: carets inside the body are bare and
	// atStart is destroyed.
	truncate := func() {
		c.marks = c.marks[:snap.marks]
		c.atStart = false
	}
	// countPresence records one body presence's initial content (chars
	// only; atoms and groups were counted when folded).
	countPresence := func() {
		if alreadyCounted {
			return
		}
		if !c.dead && c.curLen == 0 {
			c.numInit += 2
		}
	}
	// revive restores the live/killed flag for a zero-minimum bypass: the
	// unit's emptiness (dead class, dead group body) cannot kill a path the
	// quantifier can skip. Only dead is forwarded; curLen and numInit stay
	// under the branch's own accounting (a live body's contributions
	// already make a following run non-initial, matching ICU's masking).
	revive := func(s snapshot) {
		c.dead = s.dead
	}
	setLen := func(contrib int64) {
		if !c.dead {
			c.curLen = snap.curLen + contrib
		}
	}

	switch q.form {
	case FormPlus:
		// The body stays first (atStart preserved into it); the loop
		// control follows the body and destroys atStart afterwards.
		countPresence()
		setLen(bodyMin)
		c.atStart = false
		return
	case FormStar, FormQuestion:
		// A leading guard, form-equivalent to {0,} and {0,1} for every
		// flavor. Zero repetitions are always allowed, so a dead unit
		// (empty class or group) cannot kill the path: restore the pre-unit
		// live state.
		truncate()
		revive(snap)
		countPresence()
		setLen(0)
		return
	}

	// Interval form.
	switch q.flavor {
	case FlavorOrdinary:
		if q.max == 0 {
			// {0} / {0,0}: the unit is physically deleted; restore state.
			c.restore(snap)
			return
		}
		if q.max == 1 {
			if q.min == 1 {
				// {1} / {1,1}: the body once, plain.
				countPresence()
				setLen(bodyMin)
				return
			}
			// {0,1}: optional guarded copy.
			truncate()
			revive(snap)
			countPresence()
			setLen(0)
			return
		}
		if q.min == 0 {
			// Optional copies or counted loop with a zero minimum: the
			// minimum is forwarded past the loop, and the bypass is live
			// again for a dead unit.
			truncate()
			revive(snap)
			countPresence()
			setLen(0)
			return
		}
		// min > 0 with max > 1 or unbounded: the body once wrapped in loop
		// controls (small single-char mandatory/optional copies classify
		// identically).
		truncate()
		countPresence()
		setLen(int64(q.min) * bodyMin)
	case FlavorLazy, FlavorPossessive:
		// Always a counted loop (state-saves are atStart-neutral); never
		// inlined, never deleted, even at a zero minimum. The bypass is
		// still live at a zero minimum, so a dead unit revives.
		truncate()
		if q.min == 0 {
			revive(snap)
		}
		countPresence()
		setLen(int64(q.min) * bodyMin)
	}
}
