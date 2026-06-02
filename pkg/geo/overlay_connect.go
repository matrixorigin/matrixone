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

import (
	"fmt"
	"math"
)

// segmentIntersection computes the intersection of segments a1a2 and b1b2.
// It returns the number of intersection points (0, 1, or 2 for collinear
// overlap) and the points themselves.
func segmentIntersection(a1, a2, b1, b2 Coord) (int, Coord, Coord) {
	va := Coord{X: a2.X - a1.X, Y: a2.Y - a1.Y}
	vb := Coord{X: b2.X - b1.X, Y: b2.Y - b1.Y}
	e := Coord{X: b1.X - a1.X, Y: b1.Y - a1.Y}
	cross := func(u, v Coord) float64 { return u.X*v.Y - u.Y*v.X }
	dot := func(u, v Coord) float64 { return u.X*v.X + u.Y*v.Y }

	kross := cross(va, vb)
	if kross != 0 {
		s := cross(e, vb) / kross
		if s < 0 || s > 1 {
			return 0, Coord{}, Coord{}
		}
		t := cross(e, va) / kross
		if t < 0 || t > 1 {
			return 0, Coord{}, Coord{}
		}
		return 1, snapCoord(Coord{X: a1.X + s*va.X, Y: a1.Y + s*va.Y}), Coord{}
	}
	// Parallel segments.
	if cross(e, va) != 0 {
		return 0, Coord{}, Coord{} // parallel, not collinear
	}
	sqrLenA := dot(va, va)
	if sqrLenA == 0 {
		return 0, Coord{}, Coord{}
	}
	sa := dot(e, va) / sqrLenA
	sb := sa + dot(vb, va)/sqrLenA
	smin := math.Min(sa, sb)
	smax := math.Max(sa, sb)
	if smin > 1 || smax < 0 {
		return 0, Coord{}, Coord{}
	}
	pt := func(s float64) Coord { return Coord{X: a1.X + s*va.X, Y: a1.Y + s*va.Y} }
	lo := math.Max(smin, 0)
	hi := math.Min(smax, 1)
	if lo == hi {
		return 1, snapCoord(pt(lo)), Coord{}
	}
	return 2, snapCoord(pt(lo)), snapCoord(pt(hi))
}

// orderEvents filters the swept events to those contributing to the result and
// orders them, assigning the cross-link positions used by connectEdges.
func orderEvents(sortedEvents []*ovEvent) []*ovEvent {
	var resultEvents []*ovEvent
	for _, e := range sortedEvents {
		if (e.left && e.inResult) || (!e.left && e.other.inResult) {
			resultEvents = append(resultEvents, e)
		}
	}
	// Overlapping edges may leave the list not fully sorted; settle it.
	sorted := false
	for !sorted {
		sorted = true
		for i := 0; i+1 < len(resultEvents); i++ {
			if compareEvents(resultEvents[i], resultEvents[i+1]) == 1 {
				resultEvents[i], resultEvents[i+1] = resultEvents[i+1], resultEvents[i]
				sorted = false
			}
		}
	}
	for i, e := range resultEvents {
		e.pos = i
	}
	for _, e := range resultEvents {
		if !e.left {
			e.pos, e.other.pos = e.other.pos, e.pos
		}
	}
	return resultEvents
}

func nextPos(pos int, resultEvents []*ovEvent, processed []bool, origIndex int) int {
	newPos := pos + 1
	length := len(resultEvents)
	p := resultEvents[pos].p
	for newPos < length && ovEqual(resultEvents[newPos].p, p) {
		if !processed[newPos] {
			return newPos
		}
		newPos++
	}
	newPos = pos - 1
	for newPos >= origIndex && processed[newPos] {
		newPos--
	}
	return newPos
}

// connectEdges walks the ordered result events to assemble closed rings.
func connectEdges(sortedEvents []*ovEvent) [][]Coord {
	resultEvents := orderEvents(sortedEvents)
	processed := make([]bool, len(resultEvents))
	var result [][]Coord

	for i := range resultEvents {
		if processed[i] {
			continue
		}
		var contour []Coord
		contour = append(contour, resultEvents[i].p)
		pos := i
		for pos >= i {
			processed[pos] = true
			pos = resultEvents[pos].pos
			if pos < 0 || pos >= len(resultEvents) {
				break
			}
			processed[pos] = true
			contour = append(contour, resultEvents[pos].p)
			pos = nextPos(pos, resultEvents, processed, i)
		}
		if len(contour) >= 3 {
			if !ovEqual(contour[0], contour[len(contour)-1]) {
				contour = append(contour, contour[0])
			}
			result = append(result, contour)
		}
	}
	return result
}

// ringContainsStrict reports whether p is strictly inside the closed ring.
func ringContainsStrict(ring []Coord, p Coord) bool {
	n := len(ring) - 1 // last == first
	if n < 3 {
		return false
	}
	in := false
	for i, j := 0, n-1; i < n; j, i = i, i+1 {
		if pointOnSegment(p, ring[i], ring[j]) {
			return false
		}
		yi, yj := ring[i].Y, ring[j].Y
		if (yi > p.Y) != (yj > p.Y) {
			xint := (ring[j].X-ring[i].X)*(p.Y-yi)/(yj-yi) + ring[i].X
			if p.X < xint {
				in = !in
			}
		}
	}
	return in
}

// ringInsideRing reports whether ring a lies inside ring b (non-crossing rings).
func ringInsideRing(a, b []Coord) bool {
	for _, v := range a[:len(a)-1] {
		if ringContainsStrict(b, v) {
			return true
		}
	}
	return false
}

// assembleResult groups rings into shells and holes by nesting depth.
func assembleResult(rings [][]Coord) Geometry {
	if len(rings) == 0 {
		return Polygon{}
	}
	n := len(rings)
	depth := make([]int, n)
	parent := make([]int, n)
	for i := range parent {
		parent[i] = -1
	}
	for i := 0; i < n; i++ {
		bestDepthContainer := -1
		bestContainerDepth := -1
		count := 0
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if ringInsideRing(rings[i], rings[j]) {
				count++
			}
		}
		depth[i] = count
		// Immediate parent: the container with the largest container-count.
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			if ringInsideRing(rings[i], rings[j]) {
				cj := 0
				for k := 0; k < n; k++ {
					if k != j && ringInsideRing(rings[j], rings[k]) {
						cj++
					}
				}
				if cj > bestContainerDepth {
					bestContainerDepth = cj
					bestDepthContainer = j
				}
			}
		}
		parent[i] = bestDepthContainer
	}

	var polys []Polygon
	for i := 0; i < n; i++ {
		if depth[i]%2 != 0 {
			continue // hole
		}
		shell := normalizeRing(rings[i], true)
		var holes [][]Coord
		for j := 0; j < n; j++ {
			if depth[j]%2 == 1 && parent[j] == i {
				holes = append(holes, normalizeRing(rings[j], false))
			}
		}
		ringsOut := append([][]Coord{shell}, holes...)
		polys = append(polys, Polygon{Rings: ringsOut})
	}

	if len(polys) == 0 {
		return Polygon{}
	}
	if len(polys) == 1 {
		return polys[0]
	}
	return MultiPolygon{Polygons: polys}
}

// normalizeRing orients a ring CCW for shells (ccw=true) or CW for holes.
func normalizeRing(r []Coord, ccw bool) []Coord {
	area := ringSignedArea(r)
	isCCW := area > 0
	if isCCW != ccw {
		out := make([]Coord, len(r))
		for i := range r {
			out[i] = r[len(r)-1-i]
		}
		return out
	}
	return r
}

// polygonRings returns the rings of a polygonal geometry, or an error if g is
// not areal.
func polygonRings(g Geometry) ([][]Coord, error) {
	switch v := g.(type) {
	case Polygon:
		return v.Rings, nil
	case MultiPolygon:
		var rings [][]Coord
		for _, p := range v.Polygons {
			rings = append(rings, p.Rings...)
		}
		return rings, nil
	case GeometryCollection:
		var rings [][]Coord
		for _, sub := range v.Geometries {
			r, err := polygonRings(sub)
			if err != nil {
				return nil, err
			}
			rings = append(rings, r...)
		}
		return rings, nil
	default:
		return nil, fmt.Errorf("overlay requires POLYGON or MULTIPOLYGON input, got %s", subtypeName(g.Type()))
	}
}

// Overlay computes a Boolean operation between two areal geometries.
func Overlay(a, b Geometry, op BoolOp) (Geometry, error) {
	ra, err := polygonRings(a)
	if err != nil {
		return nil, err
	}
	rb, err := polygonRings(b)
	if err != nil {
		return nil, err
	}

	o := &overlay{op: op}
	for _, r := range ra {
		o.addRing(r, true)
	}
	for _, r := range rb {
		o.addRing(r, false)
	}
	// Trivial cases: an empty operand.
	if len(ra) == 0 || len(rb) == 0 {
		switch op {
		case OpUnion, OpXOR:
			if len(ra) == 0 {
				return b, nil
			}
			return a, nil
		case OpDifference:
			return a, nil
		case OpIntersection:
			return Polygon{}, nil
		}
	}

	processed := o.run()
	var sortedEvents []*ovEvent
	for _, e := range processed {
		sortedEvents = append(sortedEvents, e, e.other)
	}
	rings := connectEdges(sortedEvents)
	return assembleResult(rings), nil
}

// subtypeName is a small helper for error messages.
func subtypeName(s Subtype) string {
	names := map[Subtype]string{
		POINT: "POINT", LINESTRING: "LINESTRING", POLYGON: "POLYGON",
		MULTIPOINT: "MULTIPOINT", MULTILINESTRING: "MULTILINESTRING",
		MULTIPOLYGON: "MULTIPOLYGON", GEOMETRYCOLLECTION: "GEOMETRYCOLLECTION",
	}
	if n, ok := names[s]; ok {
		return n
	}
	return "GEOMETRY"
}
