// Copyright 2021 - 2024 Matrix Origin
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

package geo

// Polygon Boolean operations via the Martinez-Rueda-Feito sweep-line algorithm
// ("A new algorithm for computing Boolean operations on polygons", 2009). This
// is a native Go port of the canonical implementation (as used by the widely
// adopted martinez / polygon-clipping libraries). It computes the union,
// intersection, difference and symmetric difference of areal geometries.

import (
	"sort"
)

// BoolOp selects which Boolean operation overlay computes.
type BoolOp int

const (
	OpIntersection BoolOp = iota
	OpUnion
	OpDifference
	OpXOR
)

type edgeKind int

const (
	edgeNormal edgeKind = iota
	edgeNonContributing
	edgeSameTransition
	edgeDifferentTransition
)

// ovEvent is a sweep event (an endpoint of a polygon edge).
type ovEvent struct {
	p            Coord
	left         bool
	other        *ovEvent
	subject      bool // true: subject polygon, false: clipping polygon
	kind         edgeKind
	inOut        bool
	otherInOut   bool
	prevInResult *ovEvent
	inResult     bool
	contourID    int
	pos          int
}

func ovSignedArea(p0, p1, p2 Coord) float64 {
	return (p0.X-p2.X)*(p1.Y-p2.Y) - (p1.X-p2.X)*(p0.Y-p2.Y)
}

// snapScale snaps coordinates to a fixed grid so that points which should
// coincide do so exactly. The sweep-line algorithm relies on exact equality;
// integer-valued inputs are robust, while irrational coordinates (e.g. from
// circle approximations) are not. Snap-rounding both inputs and computed
// intersection points to this grid restores robustness at ~1e-9 precision.
const snapScale = 1e9

func snapCoord(c Coord) Coord {
	return Coord{
		X: ovRound(c.X*snapScale) / snapScale,
		Y: ovRound(c.Y*snapScale) / snapScale,
	}
}

// ovRound is math.Round inlined to avoid an import cycle of intent; rounds half
// away from zero.
func ovRound(x float64) float64 {
	if x >= 0 {
		return float64(int64(x + 0.5))
	}
	return float64(int64(x - 0.5))
}

func ovEqual(a, b Coord) bool { return a.X == b.X && a.Y == b.Y }

// below reports whether this event's edge lies below point p.
func (e *ovEvent) below(p Coord) bool {
	if e.left {
		return ovSignedArea(e.p, e.other.p, p) > 0
	}
	return ovSignedArea(e.other.p, e.p, p) > 0
}

func (e *ovEvent) above(p Coord) bool { return !e.below(p) }

// compareEvents returns -1 if e1 is processed before e2, +1 otherwise.
func compareEvents(e1, e2 *ovEvent) int {
	if e1.p.X > e2.p.X {
		return 1
	}
	if e1.p.X < e2.p.X {
		return -1
	}
	if e1.p.Y != e2.p.Y {
		if e1.p.Y > e2.p.Y {
			return 1
		}
		return -1
	}
	// Same point: the right endpoint is processed first.
	if e1.left != e2.left {
		if e1.left {
			return 1
		}
		return -1
	}
	// Same point and endpoint type: order by edge angle.
	if ovSignedArea(e1.p, e1.other.p, e2.other.p) != 0 {
		if e1.above(e2.other.p) {
			return 1
		}
		return -1
	}
	// Collinear: subject before clipping.
	if !e1.subject && e2.subject {
		return 1
	}
	return -1
}

// compareSegments orders edges in the sweep-line status (le1 below le2 -> -1).
func compareSegments(le1, le2 *ovEvent) int {
	if le1 == le2 {
		return 0
	}
	if ovSignedArea(le1.p, le1.other.p, le2.p) != 0 ||
		ovSignedArea(le1.p, le1.other.p, le2.other.p) != 0 {
		// Not collinear.
		if ovEqual(le1.p, le2.p) {
			if le1.below(le2.other.p) {
				return -1
			}
			return 1
		}
		if compareEvents(le1, le2) == 1 {
			if le2.above(le1.p) {
				return -1
			}
			return 1
		}
		if le1.below(le2.p) {
			return -1
		}
		return 1
	}
	// Collinear segments.
	if le1.subject == le2.subject {
		if ovEqual(le1.p, le2.p) {
			if le1.contourID < le2.contourID {
				return -1
			}
			if le1.contourID > le2.contourID {
				return 1
			}
			return 0
		}
		if compareEvents(le1, le2) == 1 {
			return 1
		}
		return -1
	}
	if le1.subject {
		return -1
	}
	return 1
}

// eventQueue is a sorted insertion priority queue keyed by compareEvents.
type eventQueue struct {
	events []*ovEvent
}

func (q *eventQueue) push(e *ovEvent) {
	// Insert keeping the slice sorted so pop takes the front (earliest).
	i := sort.Search(len(q.events), func(i int) bool {
		return compareEvents(q.events[i], e) >= 0
	})
	q.events = append(q.events, nil)
	copy(q.events[i+1:], q.events[i:])
	q.events[i] = e
}

func (q *eventQueue) pop() *ovEvent {
	e := q.events[0]
	q.events = q.events[1:]
	return e
}

func (q *eventQueue) empty() bool { return len(q.events) == 0 }

// statusLine is the ordered set of edges currently crossing the sweep line.
type statusLine struct {
	items []*ovEvent
}

func (s *statusLine) insert(e *ovEvent) int {
	i := sort.Search(len(s.items), func(i int) bool {
		return compareSegments(s.items[i], e) >= 0
	})
	s.items = append(s.items, nil)
	copy(s.items[i+1:], s.items[i:])
	s.items[i] = e
	return i
}

func (s *statusLine) removeAt(i int) {
	s.items = append(s.items[:i], s.items[i+1:]...)
}

func (s *statusLine) indexOf(e *ovEvent) int {
	for i, it := range s.items {
		if it == e {
			return i
		}
	}
	return -1
}

type overlay struct {
	q  eventQueue
	op BoolOp
}

func newEvent(p Coord, left bool, subject bool) *ovEvent {
	return &ovEvent{p: p, left: left, subject: subject, contourID: 0}
}

// addEdge enqueues the two endpoints of one polygon edge.
func (o *overlay) addEdge(p1, p2 Coord, subject bool) {
	p1, p2 = snapCoord(p1), snapCoord(p2)
	if ovEqual(p1, p2) {
		return // skip zero-length edges
	}
	e1 := newEvent(p1, true, subject)
	e2 := newEvent(p2, true, subject)
	e1.other = e2
	e2.other = e1
	if compareEvents(e1, e2) < 0 {
		e2.left = false
	} else {
		e1.left = false
	}
	o.q.push(e1)
	o.q.push(e2)
}

func (o *overlay) addRing(ring []Coord, subject bool) {
	n := len(ring)
	if n < 2 {
		return
	}
	for i := 1; i < n; i++ {
		o.addEdge(ring[i-1], ring[i], subject)
	}
	if !ovEqual(ring[0], ring[n-1]) {
		o.addEdge(ring[n-1], ring[0], subject)
	}
}

// computeFields sets the in/out transition flags and result membership.
func (o *overlay) computeFields(e *ovEvent, prev *ovEvent) {
	if prev == nil {
		e.inOut = false
		e.otherInOut = true
	} else if e.subject == prev.subject {
		e.inOut = !prev.inOut
		e.otherInOut = prev.otherInOut
	} else {
		e.inOut = !prev.otherInOut
		if prev.vertical() {
			e.otherInOut = !prev.inOut
		} else {
			e.otherInOut = prev.inOut
		}
	}
	if prev != nil {
		if !o.inResult(prev) || prev.vertical() {
			e.prevInResult = prev.prevInResult
		} else {
			e.prevInResult = prev
		}
	}
	e.inResult = o.inResult(e)
}

func (e *ovEvent) vertical() bool { return e.p.X == e.other.p.X }

func (o *overlay) inResult(e *ovEvent) bool {
	switch e.kind {
	case edgeNormal:
		switch o.op {
		case OpIntersection:
			return !e.otherInOut
		case OpUnion:
			return e.otherInOut
		case OpDifference:
			return (e.subject && e.otherInOut) || (!e.subject && !e.otherInOut)
		case OpXOR:
			return true
		}
	case edgeSameTransition:
		return o.op == OpIntersection || o.op == OpUnion
	case edgeDifferentTransition:
		return o.op == OpDifference
	case edgeNonContributing:
		return false
	}
	return false
}

// possibleIntersection subdivides edges e1 and e2 at their intersection.
func (o *overlay) possibleIntersection(e1, e2 *ovEvent) int {
	p1, p2 := e1.p, e1.other.p
	p3, p4 := e2.p, e2.other.p
	nInter, ip1, _ := segmentIntersection(p1, p2, p3, p4)

	if nInter == 0 {
		return 0
	}
	if nInter == 1 && (ovEqual(e1.p, e2.p) || ovEqual(e1.other.p, e2.other.p)) {
		return 0 // shared endpoint only
	}
	if nInter == 2 && e1.subject == e2.subject {
		return 0 // overlapping edges of the same polygon
	}

	if nInter == 1 {
		if !ovEqual(e1.p, ip1) && !ovEqual(e1.other.p, ip1) {
			o.divideSegment(e1, ip1)
		}
		if !ovEqual(e2.p, ip1) && !ovEqual(e2.other.p, ip1) {
			o.divideSegment(e2, ip1)
		}
		return 1
	}

	// Overlapping collinear segments (two intersection points).
	var sortedEvents []*ovEvent
	leftCoincide := ovEqual(e1.p, e2.p)
	rightCoincide := ovEqual(e1.other.p, e2.other.p)
	if leftCoincide {
		sortedEvents = append(sortedEvents, nil)
	} else if compareEvents(e1, e2) == 1 {
		sortedEvents = append(sortedEvents, e2, e1)
	} else {
		sortedEvents = append(sortedEvents, e1, e2)
	}
	if rightCoincide {
		sortedEvents = append(sortedEvents, nil)
	} else if compareEvents(e1.other, e2.other) == 1 {
		sortedEvents = append(sortedEvents, e2.other, e1.other)
	} else {
		sortedEvents = append(sortedEvents, e1.other, e2.other)
	}

	if leftCoincide {
		e2.kind = edgeNonContributing
		if e1.inOut == e2.inOut {
			e1.kind = edgeSameTransition
		} else {
			e1.kind = edgeDifferentTransition
		}
		if leftCoincide && !rightCoincide {
			o.divideSegment(sortedEvents[2].other, sortedEvents[1].p)
		}
		return 2
	}
	if rightCoincide {
		o.divideSegment(sortedEvents[0], sortedEvents[1].p)
		return 3
	}
	if sortedEvents[0] != sortedEvents[3].other {
		o.divideSegment(sortedEvents[0], sortedEvents[1].p)
		o.divideSegment(sortedEvents[1], sortedEvents[2].p)
		return 3
	}
	o.divideSegment(sortedEvents[0], sortedEvents[1].p)
	o.divideSegment(sortedEvents[3].other, sortedEvents[2].p)
	return 3
}

// divideSegment splits edge e at point p, producing two edges.
func (o *overlay) divideSegment(e *ovEvent, p Coord) {
	p = snapCoord(p)
	if ovEqual(p, e.p) || ovEqual(p, e.other.p) {
		return // snapped onto an existing endpoint; nothing to split
	}
	r := newEvent(p, false, e.subject)
	r.other = e
	r.kind = e.kind
	r.contourID = e.contourID

	l := newEvent(p, true, e.subject)
	l.other = e.other
	l.kind = e.other.kind
	l.contourID = e.contourID

	if compareEvents(l, e.other) > 0 {
		e.other.left = true
		l.left = false
	}
	e.other.other = l
	e.other = r

	o.q.push(l)
	o.q.push(r)
}

// run executes the sweep and returns the result events that are in the output.
func (o *overlay) run() []*ovEvent {
	var status statusLine
	var sortedResult []*ovEvent

	for !o.q.empty() {
		e := o.q.pop()

		if e.left {
			pos := status.insert(e)
			var prev *ovEvent
			if pos > 0 {
				prev = status.items[pos-1]
			}
			o.computeFields(e, prev)
			if pos+1 < len(status.items) {
				next := status.items[pos+1]
				if o.possibleIntersection(e, next) == 2 {
					o.computeFields(e, prev)
					o.computeFields(next, e)
				}
			}
			if prev != nil {
				if o.possibleIntersection(prev, e) == 2 {
					var prevPrev *ovEvent
					ppos := status.indexOf(prev)
					if ppos > 0 {
						prevPrev = status.items[ppos-1]
					}
					o.computeFields(prev, prevPrev)
					o.computeFields(e, prev)
				}
			}
		} else {
			// Right endpoint: locate the matching left event and remove it.
			le := e.other
			pos := status.indexOf(le)
			if pos < 0 {
				continue
			}
			var prev, next *ovEvent
			if pos > 0 {
				prev = status.items[pos-1]
			}
			if pos+1 < len(status.items) {
				next = status.items[pos+1]
			}
			sortedResult = append(sortedResult, le)
			status.removeAt(pos)
			if prev != nil && next != nil {
				o.possibleIntersection(prev, next)
			}
		}
	}
	return sortedResult
}
