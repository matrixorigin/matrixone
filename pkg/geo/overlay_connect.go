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
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
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

type overlayBoundaryVertex struct {
	firstRay  int
	secondRay int
	degree    int
}

// resultEdgeForward reports whether a selected canonical left event should be
// traversed from its lexicographically smaller endpoint to its larger endpoint
// so that the result interior remains on the left.
func resultEdgeForward(e *ovEvent, op BoolOp) (bool, error) {
	switch e.kind {
	case edgeNormal:
		switch op {
		case OpIntersection, OpUnion:
			return !e.inOut, nil
		case OpDifference:
			return e.subject != e.inOut, nil
		case OpXOR:
			return e.inOut != e.otherInOut, nil
		}
	case edgeSameTransition:
		if op == OpIntersection || op == OpUnion {
			return !e.inOut, nil
		}
	case edgeDifferentTransition:
		if op == OpDifference {
			return e.subject != e.inOut, nil
		}
	}
	return false, moerr.NewInternalErrorNoCtxf("unexpected overlay result edge kind %d for operation %d", e.kind, op)
}

func overlayRayHalf(origin, point Coord) int {
	dx, dy := point.X-origin.X, point.Y-origin.Y
	if dy > 0 || (dy == 0 && dx >= 0) {
		return 0
	}
	return 1
}

func overlayRayCross(origin, a, b Coord) float64 {
	ax, ay := a.X-origin.X, a.Y-origin.Y
	bx, by := b.X-origin.X, b.Y-origin.Y
	return ax*by - ay*bx
}

// overlayRayLess orders rays counterclockwise around origin. Equal rays are
// ordered deterministically and rejected by connectEdges as duplicate
// incidences; the endpoint tie-breaker keeps sort.Slice's order strict.
func overlayRayLess(origin, a, b Coord) bool {
	ha, hb := overlayRayHalf(origin, a), overlayRayHalf(origin, b)
	if ha != hb {
		return ha < hb
	}
	if cross := overlayRayCross(origin, a, b); cross != 0 {
		return cross > 0
	}
	ax, ay := a.X-origin.X, a.Y-origin.Y
	bx, by := b.X-origin.X, b.Y-origin.Y
	if ax != bx {
		return ax < bx
	}
	return ay < by
}

func sameOverlayRay(origin, a, b Coord) bool {
	return overlayRayHalf(origin, a) == overlayRayHalf(origin, b) && overlayRayCross(origin, a, b) == 0
}

func overlayBoundaryRayCode(edge int, outgoing bool) int {
	code := edge << 1
	if outgoing {
		code |= 1
	}
	return code
}

func overlayBoundaryRayEdge(code int) int {
	return code >> 1
}

func overlayBoundaryRayOutgoing(code int) bool {
	return code&1 != 0
}

func overlayBoundaryRayPoint(code int, edges []*ovEvent, forwardEdges []bool) Coord {
	edge := overlayBoundaryRayEdge(code)
	if overlayBoundaryRayOutgoing(code) == forwardEdges[edge] {
		return edges[edge].other.p
	}
	return edges[edge].p
}

func overlayGraphError(format string, args ...any) error {
	return moerr.NewInternalErrorNoCtxf("invalid overlay boundary graph: "+format, args...)
}

// splitRepeatedRing decomposes a closed boundary walk into simple rings at
// repeated branch vertices. A degree-two vertex has only one incoming and one
// outgoing edge, so revisiting it would already repeat an edge and be rejected
// by the traversal. The common simple-ring case therefore needs no per-vertex
// path/map allocation.
func splitRepeatedRing(ring []Coord, branchVertices map[Coord]struct{}) ([][]Coord, error) {
	if len(ring) < 4 || !ovEqual(ring[0], ring[len(ring)-1]) {
		return nil, overlayGraphError("boundary walk is not a closed ring")
	}

	var branchPositions map[Coord]int
	for i, p := range ring[:len(ring)-1] {
		if _, isBranch := branchVertices[p]; !isBranch {
			continue
		}
		if branchPositions == nil {
			branchPositions = make(map[Coord]int)
		}
		if _, ok := branchPositions[p]; ok {
			return splitRepeatedRingWithRepeatedVertices(ring)
		}
		branchPositions[p] = i
	}
	return [][]Coord{ring}, nil
}

// splitRepeatedRingWithRepeatedVertices handles the uncommon case where a
// validly noded boundary walk revisits a branch vertex. Each resulting cycle
// retains its input segments exactly once.
func splitRepeatedRingWithRepeatedVertices(ring []Coord) ([][]Coord, error) {
	positions := make(map[Coord]int, len(ring)-1)
	path := make([]Coord, 1, len(ring))
	path[0] = ring[0]
	positions[ring[0]] = 0
	var result [][]Coord

	for _, p := range ring[1:] {
		if pos, ok := positions[p]; ok {
			cycle := append([]Coord(nil), path[pos:]...)
			cycle = append(cycle, p)
			if len(cycle) < 4 {
				return nil, overlayGraphError("boundary walk contains a degenerate cycle")
			}
			result = append(result, cycle)
			for _, removed := range path[pos+1:] {
				delete(positions, removed)
			}
			path = path[:pos+1]
			continue
		}
		positions[p] = len(path)
		path = append(path, p)
	}

	if len(path) != 1 || !ovEqual(path[0], ring[0]) {
		return nil, overlayGraphError("repeated-vertex split left an open boundary")
	}
	return result, nil
}

// connectEdges directs selected sweep edges with the result interior on the
// left, then follows the clockwise predecessor at each vertex to preserve the
// filled result sector. sweptEdges contains the canonical left event for every
// noded segment emitted by run.
func connectEdges(sweptEdges []*ovEvent, op BoolOp) ([][]Coord, error) {
	edges := make([]*ovEvent, 0, len(sweptEdges))
	forwardEdges := make([]bool, 0, len(sweptEdges))
	for _, e := range sweptEdges {
		if e == nil || e.other == nil || !e.left {
			return nil, overlayGraphError("sweep returned a noncanonical segment event")
		}
		if !e.inResult {
			continue
		}
		forward, err := resultEdgeForward(e, op)
		if err != nil {
			return nil, err
		}
		if ovEqual(e.p, e.other.p) {
			return nil, overlayGraphError("selected a zero-length boundary edge")
		}
		// pos is only used by the legacy connector after the sweep has completed.
		// Reuse it for the successor index instead of copying endpoint coordinates
		// into a parallel result-edge graph.
		e.pos = -1
		edges = append(edges, e)
		forwardEdges = append(forwardEdges, forward)
	}
	if len(edges) == 0 {
		return nil, nil
	}

	vertexIDs := make(map[Coord]int, len(edges))
	vertices := make([]overlayBoundaryVertex, 0, len(edges))
	var branchRaysByVertex map[int][]int
	appendRay := func(vertex Coord, edge int, outgoing bool) {
		vertexID, ok := vertexIDs[vertex]
		if !ok {
			vertexID = len(vertices)
			vertexIDs[vertex] = vertexID
			vertices = append(vertices, overlayBoundaryVertex{})
		}
		state := &vertices[vertexID]
		ray := overlayBoundaryRayCode(edge, outgoing)
		switch state.degree {
		case 0:
			state.firstRay = ray
		case 1:
			state.secondRay = ray
		case 2:
			if branchRaysByVertex == nil {
				branchRaysByVertex = make(map[int][]int)
			}
			branchRays := make([]int, 3, 4)
			branchRays[0] = state.firstRay
			branchRays[1] = state.secondRay
			branchRays[2] = ray
			branchRaysByVertex[vertexID] = branchRays
		default:
			branchRaysByVertex[vertexID] = append(branchRaysByVertex[vertexID], ray)
		}
		state.degree++
	}
	for i, edge := range edges {
		from, to := edge.p, edge.other.p
		if !forwardEdges[i] {
			from, to = to, from
		}
		appendRay(from, i, true)
		appendRay(to, i, false)
	}

	predecessors := make([]bool, len(edges))
	var branchVertices map[Coord]struct{}
	for vertex, vertexID := range vertexIDs {
		state := vertices[vertexID]
		if state.degree > 2 {
			if branchVertices == nil {
				branchVertices = make(map[Coord]struct{})
			}
			branchVertices[vertex] = struct{}{}
		}
		if state.degree == 2 {
			first, second := state.firstRay, state.secondRay
			firstOutgoing, secondOutgoing := overlayBoundaryRayOutgoing(first), overlayBoundaryRayOutgoing(second)
			if firstOutgoing == secondOutgoing {
				return nil, overlayGraphError("unbalanced boundary at (%g,%g): 1 incoming, 1 outgoing required", vertex.X, vertex.Y)
			}
			if sameOverlayRay(
				vertex,
				overlayBoundaryRayPoint(first, edges, forwardEdges),
				overlayBoundaryRayPoint(second, edges, forwardEdges),
			) {
				return nil, overlayGraphError("duplicate collinear boundary rays at (%g,%g)", vertex.X, vertex.Y)
			}
			incoming, outgoing := first, second
			if firstOutgoing {
				incoming, outgoing = second, first
			}
			outgoingEdge := overlayBoundaryRayEdge(outgoing)
			if predecessors[outgoingEdge] {
				return nil, overlayGraphError("boundary edge %d has multiple predecessors", outgoingEdge)
			}
			edges[overlayBoundaryRayEdge(incoming)].pos = outgoingEdge
			predecessors[outgoingEdge] = true
			continue
		}

		branchRays := branchRaysByVertex[vertexID]
		if len(branchRays) != state.degree {
			return nil, overlayGraphError("corrupt incidence list at (%g,%g)", vertex.X, vertex.Y)
		}
		incoming, outgoing := 0, 0
		for _, ray := range branchRays {
			if overlayBoundaryRayOutgoing(ray) {
				outgoing++
			} else {
				incoming++
			}
		}
		if incoming != outgoing {
			return nil, overlayGraphError("unbalanced boundary at (%g,%g): %d incoming, %d outgoing", vertex.X, vertex.Y, incoming, outgoing)
		}
		sort.Slice(branchRays, func(i, j int) bool {
			return overlayRayLess(
				vertex,
				overlayBoundaryRayPoint(branchRays[i], edges, forwardEdges),
				overlayBoundaryRayPoint(branchRays[j], edges, forwardEdges),
			)
		})
		for i := range branchRays {
			j := (i + 1) % len(branchRays)
			if sameOverlayRay(
				vertex,
				overlayBoundaryRayPoint(branchRays[i], edges, forwardEdges),
				overlayBoundaryRayPoint(branchRays[j], edges, forwardEdges),
			) {
				return nil, overlayGraphError("duplicate collinear boundary rays at (%g,%g)", vertex.X, vertex.Y)
			}
		}
		for i, ray := range branchRays {
			if overlayBoundaryRayOutgoing(ray) {
				continue
			}
			previous := branchRays[(i+len(branchRays)-1)%len(branchRays)]
			if !overlayBoundaryRayOutgoing(previous) {
				return nil, overlayGraphError("boundary directions do not alternate at (%g,%g)", vertex.X, vertex.Y)
			}
			previousEdge := overlayBoundaryRayEdge(previous)
			if predecessors[previousEdge] {
				return nil, overlayGraphError("boundary edge %d has multiple predecessors", previousEdge)
			}
			incomingEdge := overlayBoundaryRayEdge(ray)
			edges[incomingEdge].pos = previousEdge
			predecessors[previousEdge] = true
		}
	}
	for i, hasPredecessor := range predecessors {
		if !hasPredecessor || edges[i].pos < 0 {
			return nil, overlayGraphError("boundary edge %d has no predecessor", i)
		}
	}

	visited := make([]bool, len(edges))
	var result [][]Coord
	for start := range edges {
		if visited[start] {
			continue
		}
		cycleEdges := 0
		current := start
		for {
			if current < 0 || current >= len(edges) {
				return nil, overlayGraphError("boundary successor index %d is out of range", current)
			}
			if visited[current] {
				return nil, overlayGraphError("boundary traversal entered an already consumed ring")
			}
			if cycleEdges >= len(edges) {
				return nil, overlayGraphError("boundary traversal did not close")
			}
			cycleEdges++
			current = edges[current].pos
			if current == start {
				break
			}
		}

		startPoint := edges[start].p
		if !forwardEdges[start] {
			startPoint = edges[start].other.p
		}
		contour := make([]Coord, 1, cycleEdges+1)
		contour[0] = startPoint
		current = start
		for steps := 0; steps < cycleEdges; steps++ {
			if visited[current] {
				return nil, overlayGraphError("boundary traversal entered an already consumed ring")
			}
			visited[current] = true
			edge := edges[current]
			from, to := edge.p, edge.other.p
			if !forwardEdges[current] {
				from, to = to, from
			}
			if !ovEqual(contour[len(contour)-1], from) {
				return nil, overlayGraphError("successor edges do not share an endpoint")
			}
			contour = append(contour, to)
			current = edge.pos
		}
		if current != start || !ovEqual(contour[0], contour[len(contour)-1]) {
			return nil, overlayGraphError("boundary traversal did not close")
		}
		rings, err := splitRepeatedRing(contour, branchVertices)
		if err != nil {
			return nil, err
		}
		result = append(result, rings...)
	}
	for i, consumed := range visited {
		if !consumed {
			return nil, overlayGraphError("boundary edge %d was not consumed", i)
		}
	}
	return result, nil
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
		return nil, moerr.NewInvalidInputNoCtxf("overlay requires POLYGON or MULTIPOLYGON input, got %s", subtypeName(g.Type()))
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

	sweptEdges := o.run()
	rings, err := connectEdges(sweptEdges, op)
	if err != nil {
		return nil, err
	}
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
