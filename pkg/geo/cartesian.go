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

import "math"

// cartesian.go implements planar (SRID 0) geometric primitives: measures
// (area, length, perimeter, distance), bounding box, centroid, and the
// predicate helpers (point-in-polygon, segment intersection) used by the
// spatial relationship functions. All results are in the coordinate system's
// own units; there is no notion of meters here (that is geodetic.go).

// epsilon is the tolerance used for orientation / on-segment sign tests. Inputs
// to the planar kernel are unitless coordinates; 1e-12 distinguishes genuine
// zero from floating-point noise for the coordinate magnitudes we expect.
const epsilon = 1e-12

func sign(x float64) int {
	switch {
	case x > epsilon:
		return 1
	case x < -epsilon:
		return -1
	default:
		return 0
	}
}

// eachCoord visits every coordinate of g, recursing into collections.
func eachCoord(g Geometry, fn func(Coord)) {
	switch v := g.(type) {
	case Point:
		if !v.IsEmpty {
			fn(Coord{X: v.X, Y: v.Y})
		}
	case LineString:
		for _, c := range v.Points {
			fn(c)
		}
	case Polygon:
		for _, r := range v.Rings {
			for _, c := range r {
				fn(c)
			}
		}
	case MultiPoint:
		for _, p := range v.Points {
			if !p.IsEmpty {
				fn(Coord{X: p.X, Y: p.Y})
			}
		}
	case MultiLineString:
		for _, l := range v.Lines {
			for _, c := range l.Points {
				fn(c)
			}
		}
	case MultiPolygon:
		for _, p := range v.Polygons {
			for _, r := range p.Rings {
				for _, c := range r {
					fn(c)
				}
			}
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			eachCoord(sub, fn)
		}
	}
}

// BBox is an axis-aligned bounding box.
type BBox struct {
	MinX, MinY, MaxX, MaxY float64
}

// Envelope returns the bounding box of g. ok is false when g has no
// coordinates (empty geometry).
func Envelope(g Geometry) (bb BBox, ok bool) {
	first := true
	eachCoord(g, func(c Coord) {
		if first {
			bb = BBox{MinX: c.X, MinY: c.Y, MaxX: c.X, MaxY: c.Y}
			first = false
			return
		}
		bb.MinX = math.Min(bb.MinX, c.X)
		bb.MinY = math.Min(bb.MinY, c.Y)
		bb.MaxX = math.Max(bb.MaxX, c.X)
		bb.MaxY = math.Max(bb.MaxY, c.Y)
	})
	return bb, !first
}

// --- Area -----------------------------------------------------------------

// ringSignedArea returns the signed area of a ring via the shoelace formula.
// The ring may or may not repeat its first point as the last; both are handled.
func ringSignedArea(ring []Coord) float64 {
	n := len(ring)
	if n < 3 {
		return 0
	}
	sum := 0.0
	for i := range n {
		j := (i + 1) % n
		sum += ring[i].X*ring[j].Y - ring[j].X*ring[i].Y
	}
	return sum / 2
}

func polygonArea(p Polygon) float64 {
	if len(p.Rings) == 0 {
		return 0
	}
	area := math.Abs(ringSignedArea(p.Rings[0]))
	for _, hole := range p.Rings[1:] {
		area -= math.Abs(ringSignedArea(hole))
	}
	if area < 0 {
		return 0
	}
	return area
}

// CartesianArea returns the planar area of g (0 for non-areal geometries).
func CartesianArea(g Geometry) float64 {
	switch v := g.(type) {
	case Polygon:
		return polygonArea(v)
	case MultiPolygon:
		a := 0.0
		for _, p := range v.Polygons {
			a += polygonArea(p)
		}
		return a
	case GeometryCollection:
		a := 0.0
		for _, sub := range v.Geometries {
			a += CartesianArea(sub)
		}
		return a
	default:
		return 0
	}
}

// --- Length / Perimeter ---------------------------------------------------

func segLen(a, b Coord) float64 { return math.Hypot(b.X-a.X, b.Y-a.Y) }

func lineLength(pts []Coord) float64 {
	l := 0.0
	for i := 1; i < len(pts); i++ {
		l += segLen(pts[i-1], pts[i])
	}
	return l
}

// CartesianLength returns the total length of all line components of g (0 for
// points and polygons).
func CartesianLength(g Geometry) float64 {
	switch v := g.(type) {
	case LineString:
		return lineLength(v.Points)
	case MultiLineString:
		l := 0.0
		for _, ls := range v.Lines {
			l += lineLength(ls.Points)
		}
		return l
	case GeometryCollection:
		l := 0.0
		for _, sub := range v.Geometries {
			l += CartesianLength(sub)
		}
		return l
	default:
		return 0
	}
}

func polygonPerimeter(p Polygon) float64 {
	per := 0.0
	for _, r := range p.Rings {
		per += lineLength(r)
	}
	return per
}

// CartesianPerimeter returns the total ring length of all areal components of g
// (0 for points and lines).
func CartesianPerimeter(g Geometry) float64 {
	switch v := g.(type) {
	case Polygon:
		return polygonPerimeter(v)
	case MultiPolygon:
		per := 0.0
		for _, p := range v.Polygons {
			per += polygonPerimeter(p)
		}
		return per
	case GeometryCollection:
		per := 0.0
		for _, sub := range v.Geometries {
			per += CartesianPerimeter(sub)
		}
		return per
	default:
		return 0
	}
}

// --- Point / segment predicates ------------------------------------------

// orient returns twice the signed area of triangle (a, b, c): >0 left turn,
// <0 right turn, ~0 collinear.
func orient(a, b, c Coord) float64 {
	return (b.X-a.X)*(c.Y-a.Y) - (b.Y-a.Y)*(c.X-a.X)
}

// onSegment reports whether c lies on segment a-b, assuming a, b, c are
// collinear.
func onSegment(a, b, c Coord) bool {
	return c.X >= math.Min(a.X, b.X)-epsilon && c.X <= math.Max(a.X, b.X)+epsilon &&
		c.Y >= math.Min(a.Y, b.Y)-epsilon && c.Y <= math.Max(a.Y, b.Y)+epsilon
}

// pointOnSegment reports whether c lies on segment a-b (collinear and within
// bounds).
func pointOnSegment(c, a, b Coord) bool {
	if sign(orient(a, b, c)) != 0 {
		return false
	}
	return onSegment(a, b, c)
}

// SegmentsIntersect reports whether segments p1-p2 and p3-p4 share any point,
// including collinear overlap and shared endpoints.
func SegmentsIntersect(p1, p2, p3, p4 Coord) bool {
	d1 := sign(orient(p3, p4, p1))
	d2 := sign(orient(p3, p4, p2))
	d3 := sign(orient(p1, p2, p3))
	d4 := sign(orient(p1, p2, p4))
	if d1 != d2 && d3 != d4 {
		return true
	}
	if d1 == 0 && onSegment(p3, p4, p1) {
		return true
	}
	if d2 == 0 && onSegment(p3, p4, p2) {
		return true
	}
	if d3 == 0 && onSegment(p1, p2, p3) {
		return true
	}
	if d4 == 0 && onSegment(p1, p2, p4) {
		return true
	}
	return false
}

// pointInRing reports whether c is strictly inside the ring (boundary excluded),
// via ray casting.
func pointInRing(c Coord, ring []Coord) bool {
	inside := false
	n := len(ring)
	if n < 3 {
		return false
	}
	j := n - 1
	for i := range n {
		yi, yj := ring[i].Y, ring[j].Y
		if (yi > c.Y) != (yj > c.Y) {
			xint := (ring[j].X-ring[i].X)*(c.Y-yi)/(yj-yi) + ring[i].X
			if c.X < xint {
				inside = !inside
			}
		}
		j = i
	}
	return inside
}

func pointOnRing(c Coord, ring []Coord) bool {
	for i := 1; i < len(ring); i++ {
		if pointOnSegment(c, ring[i-1], ring[i]) {
			return true
		}
	}
	return false
}

// PointInPolygon classifies c against polygon p: 1 = interior, 0 = on the
// boundary, -1 = exterior (including inside a hole).
func PointInPolygon(c Coord, p Polygon) int {
	if len(p.Rings) == 0 {
		return -1
	}
	for _, ring := range p.Rings {
		if pointOnRing(c, ring) {
			return 0
		}
	}
	if !pointInRing(c, p.Rings[0]) {
		return -1
	}
	for _, hole := range p.Rings[1:] {
		if pointInRing(c, hole) {
			return -1
		}
	}
	return 1
}

// --- Distance -------------------------------------------------------------

// pointSegDist returns the minimum distance from c to segment a-b.
func pointSegDist(c, a, b Coord) float64 {
	dx := b.X - a.X
	dy := b.Y - a.Y
	if dx == 0 && dy == 0 {
		return segLen(c, a)
	}
	t := ((c.X-a.X)*dx + (c.Y-a.Y)*dy) / (dx*dx + dy*dy)
	if t < 0 {
		t = 0
	} else if t > 1 {
		t = 1
	}
	return segLen(c, Coord{X: a.X + t*dx, Y: a.Y + t*dy})
}

// primitives decomposes a geometry into the lowest-level pieces distance needs:
// isolated points, segments, and polygons (for containment tests).
type primitives struct {
	points   []Coord
	segments [][2]Coord
	polygons []Polygon
}

func (p primitives) empty() bool {
	return len(p.points) == 0 && len(p.segments) == 0 && len(p.polygons) == 0
}

func collect(g Geometry, out *primitives) {
	switch v := g.(type) {
	case Point:
		if !v.IsEmpty {
			out.points = append(out.points, Coord{X: v.X, Y: v.Y})
		}
	case LineString:
		appendSegments(out, v.Points)
	case Polygon:
		if len(v.Rings) > 0 {
			out.polygons = append(out.polygons, v)
			for _, r := range v.Rings {
				appendSegments(out, r)
			}
		}
	case MultiPoint:
		for _, pt := range v.Points {
			if !pt.IsEmpty {
				out.points = append(out.points, Coord{X: pt.X, Y: pt.Y})
			}
		}
	case MultiLineString:
		for _, ls := range v.Lines {
			appendSegments(out, ls.Points)
		}
	case MultiPolygon:
		for _, poly := range v.Polygons {
			collect(poly, out)
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			collect(sub, out)
		}
	}
}

func appendSegments(out *primitives, pts []Coord) {
	switch len(pts) {
	case 0:
		return
	case 1:
		out.points = append(out.points, pts[0])
	default:
		for i := 1; i < len(pts); i++ {
			out.segments = append(out.segments, [2]Coord{pts[i-1], pts[i]})
		}
	}
}

// representativeCoords returns at least one coordinate per primitive group, used
// for polygon-containment short-circuits in distance.
func (p primitives) representativeCoords() []Coord {
	cs := make([]Coord, 0, len(p.points)+len(p.segments))
	cs = append(cs, p.points...)
	for _, s := range p.segments {
		cs = append(cs, s[0])
	}
	return cs
}

// CartesianDistance returns the minimum planar distance between g1 and g2. ok is
// false when either geometry is empty. The distance is 0 when the geometries
// intersect (touch, cross, or one contains the other).
func CartesianDistance(g1, g2 Geometry) (dist float64, ok bool) {
	var a, b primitives
	collect(g1, &a)
	collect(g2, &b)
	if a.empty() || b.empty() {
		return 0, false
	}

	// Containment: a representative coordinate of one geometry lying within a
	// polygon of the other means distance 0.
	if anyPointInPolygons(a.representativeCoords(), b.polygons) ||
		anyPointInPolygons(b.representativeCoords(), a.polygons) {
		return 0, true
	}

	best := math.MaxFloat64
	upd := func(d float64) {
		if d < best {
			best = d
		}
	}

	// point/point
	for _, p := range a.points {
		for _, q := range b.points {
			upd(segLen(p, q))
		}
	}
	// point/segment (both directions)
	for _, p := range a.points {
		for _, s := range b.segments {
			upd(pointSegDist(p, s[0], s[1]))
		}
	}
	for _, q := range b.points {
		for _, s := range a.segments {
			upd(pointSegDist(q, s[0], s[1]))
		}
	}
	// segment/segment
	for _, s := range a.segments {
		for _, r := range b.segments {
			if SegmentsIntersect(s[0], s[1], r[0], r[1]) {
				return 0, true
			}
			upd(segSegDist(s[0], s[1], r[0], r[1]))
		}
	}
	return best, true
}

func anyPointInPolygons(coords []Coord, polys []Polygon) bool {
	for _, p := range polys {
		for _, c := range coords {
			if PointInPolygon(c, p) >= 0 {
				return true
			}
		}
	}
	return false
}

func segSegDist(a1, a2, b1, b2 Coord) float64 {
	return math.Min(
		math.Min(pointSegDist(a1, b1, b2), pointSegDist(a2, b1, b2)),
		math.Min(pointSegDist(b1, a1, a2), pointSegDist(b2, a1, a2)),
	)
}

// --- Simplicity / ring properties ----------------------------------------

// LineIsSimple reports whether a line's segments do not self-intersect except
// at shared endpoints of consecutive segments (and the closing point of a ring).
func LineIsSimple(pts []Coord) bool {
	n := len(pts)
	if n < 2 {
		return true
	}
	closed := pts[0] == pts[n-1]
	for i := 0; i+1 < n; i++ {
		for j := i + 1; j+1 < n; j++ {
			if j == i {
				continue
			}
			adjacent := j == i+1
			// First and last segment of a closed ring share the closing point.
			wraps := closed && i == 0 && j == n-2
			if adjacent || wraps {
				// Allowed to touch only at the single shared endpoint.
				if segmentsOverlapBeyondEndpoint(pts[i], pts[i+1], pts[j], pts[j+1]) {
					return false
				}
				continue
			}
			if SegmentsIntersect(pts[i], pts[i+1], pts[j], pts[j+1]) {
				return false
			}
		}
	}
	return true
}

// segmentsOverlapBeyondEndpoint reports whether two segments that already share
// exactly one endpoint (because they are consecutive in a line) touch anywhere
// beyond that point. Two consecutive segments that are not collinear meet only
// at their shared vertex, which is always simple; the only non-simple case is a
// collinear backtrack where they overlap along a sub-span.
func segmentsOverlapBeyondEndpoint(a1, a2, b1, b2 Coord) bool {
	if sign(orient(a1, a2, b1)) == 0 && sign(orient(a1, a2, b2)) == 0 {
		return overlap1D(a1, a2, b1, b2)
	}
	return false
}

func overlap1D(a1, a2, b1, b2 Coord) bool {
	// Project onto the dominant axis and check for more-than-point overlap.
	if math.Abs(a2.X-a1.X) >= math.Abs(a2.Y-a1.Y) {
		lo, hi := math.Min(a1.X, a2.X), math.Max(a1.X, a2.X)
		blo, bhi := math.Min(b1.X, b2.X), math.Max(b1.X, b2.X)
		return math.Min(hi, bhi)-math.Max(lo, blo) > epsilon
	}
	lo, hi := math.Min(a1.Y, a2.Y), math.Max(a1.Y, a2.Y)
	blo, bhi := math.Min(b1.Y, b2.Y), math.Max(b1.Y, b2.Y)
	return math.Min(hi, bhi)-math.Max(lo, blo) > epsilon
}

// --- Centroid -------------------------------------------------------------

// Centroid returns the centroid of g using the OGC dimension rule: the centroid
// of the highest-dimension components present (areal, then linear, then
// puntal). ok is false for empty geometries.
func Centroid(g Geometry) (Coord, bool) {
	if cx, cy, w := areaCentroid(g); w != 0 {
		return Coord{X: cx / w, Y: cy / w}, true
	}
	if cx, cy, w := lineCentroid(g); w != 0 {
		return Coord{X: cx / w, Y: cy / w}, true
	}
	if cx, cy, n := pointCentroid(g); n != 0 {
		return Coord{X: cx / float64(n), Y: cy / float64(n)}, true
	}
	return Coord{}, false
}

// ringCentroid returns the centroid and signed area of a single ring.
func ringCentroid(ring []Coord) (cx, cy, area float64) {
	n := len(ring)
	if n < 3 {
		return 0, 0, 0
	}
	for i := range n {
		j := (i + 1) % n
		cross := ring[i].X*ring[j].Y - ring[j].X*ring[i].Y
		area += cross
		cx += (ring[i].X + ring[j].X) * cross
		cy += (ring[i].Y + ring[j].Y) * cross
	}
	area /= 2
	if area == 0 {
		return 0, 0, 0
	}
	cx /= 6 * area
	cy /= 6 * area
	return cx, cy, area
}

// areaCentroid accumulates the area-weighted centroid contributions of g.
func areaCentroid(g Geometry) (cx, cy, w float64) {
	switch v := g.(type) {
	case Polygon:
		for idx, ring := range v.Rings {
			rcx, rcy, a := ringCentroid(ring)
			weight := math.Abs(a)
			if idx > 0 {
				weight = -weight // holes subtract
			}
			cx += rcx * weight
			cy += rcy * weight
			w += weight
		}
	case MultiPolygon:
		for _, p := range v.Polygons {
			pcx, pcy, pw := areaCentroid(p)
			cx += pcx
			cy += pcy
			w += pw
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			scx, scy, sw := areaCentroid(sub)
			cx += scx
			cy += scy
			w += sw
		}
	}
	return cx, cy, w
}

// lineCentroid accumulates the length-weighted centroid of segment midpoints.
func lineCentroid(g Geometry) (cx, cy, w float64) {
	addLine := func(pts []Coord) {
		for i := 1; i < len(pts); i++ {
			l := segLen(pts[i-1], pts[i])
			mx := (pts[i-1].X + pts[i].X) / 2
			my := (pts[i-1].Y + pts[i].Y) / 2
			cx += mx * l
			cy += my * l
			w += l
		}
	}
	switch v := g.(type) {
	case LineString:
		addLine(v.Points)
	case MultiLineString:
		for _, ls := range v.Lines {
			addLine(ls.Points)
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			scx, scy, sw := lineCentroid(sub)
			cx += scx
			cy += scy
			w += sw
		}
	}
	return cx, cy, w
}

// pointCentroid accumulates the simple average of point components.
func pointCentroid(g Geometry) (cx, cy float64, n int) {
	switch v := g.(type) {
	case Point:
		if !v.IsEmpty {
			return v.X, v.Y, 1
		}
	case MultiPoint:
		for _, p := range v.Points {
			if !p.IsEmpty {
				cx += p.X
				cy += p.Y
				n++
			}
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			scx, scy, sn := pointCentroid(sub)
			cx += scx
			cy += scy
			n += sn
		}
	}
	return cx, cy, n
}
