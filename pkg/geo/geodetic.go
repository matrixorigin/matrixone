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

import "github.com/golang/geo/s2"

// geodetic.go implements spherical (SRID 4326, WGS 84 lon/lat) measures and
// predicates using the S2 geometry library. Distances are returned in meters
// and areas in square meters, using EarthRadiusMeters as the sphere radius.
//
// Coordinate convention: for SRID 4326 a Coord is (X=longitude, Y=latitude) in
// degrees, matching WKT/WKB ordering and MySQL's lon-lat axis order.

// EarthRadiusMeters is the IUGG mean Earth radius (R1). S2 computes on a unit
// sphere; multiplying angular results by this radius yields meters. Because S2
// is spherical (not ellipsoidal), geodetic results differ slightly from
// MySQL/PostGIS ellipsoidal values — callers compare with tolerance.
const EarthRadiusMeters = 6371008.8

func s2Point(c Coord) s2.Point {
	return s2.PointFromLatLng(s2.LatLngFromDegrees(c.Y, c.X))
}

// geomEmpty reports whether g has no coordinates.
func geomEmpty(g Geometry) bool {
	_, ok := Envelope(g)
	return !ok
}

// --- Length ---------------------------------------------------------------

func polylineMeters(pts []Coord) float64 {
	total := 0.0
	for i := 1; i < len(pts); i++ {
		total += s2Point(pts[i-1]).Distance(s2Point(pts[i])).Radians()
	}
	return total * EarthRadiusMeters
}

// LengthMeters returns the geodesic length of all line components of g.
func LengthMeters(g Geometry) float64 {
	switch v := g.(type) {
	case LineString:
		return polylineMeters(v.Points)
	case MultiLineString:
		total := 0.0
		for _, ls := range v.Lines {
			total += polylineMeters(ls.Points)
		}
		return total
	case GeometryCollection:
		total := 0.0
		for _, sub := range v.Geometries {
			total += LengthMeters(sub)
		}
		return total
	default:
		return 0
	}
}

// --- Area -----------------------------------------------------------------

// loopFromRing builds a normalized S2 loop from a WKB ring (dropping the
// repeated closing vertex). Normalize selects the smaller of the two regions
// the loop bounds, so containment and area are correct regardless of the input
// winding order for sub-hemisphere polygons.
func loopFromRing(ring []Coord) *s2.Loop {
	end := len(ring)
	if end > 1 && ring[0] == ring[end-1] {
		end--
	}
	pts := make([]s2.Point, 0, end)
	for i := 0; i < end; i++ {
		pts = append(pts, s2Point(ring[i]))
	}
	loop := s2.LoopFromPoints(pts)
	loop.Normalize()
	return loop
}

func polygonMeters2(p Polygon) float64 {
	if len(p.Rings) == 0 {
		return 0
	}
	area := loopFromRing(p.Rings[0]).Area()
	for _, hole := range p.Rings[1:] {
		area -= loopFromRing(hole).Area()
	}
	if area < 0 {
		area = 0
	}
	return area * EarthRadiusMeters * EarthRadiusMeters
}

// AreaSquareMeters returns the geodesic area of all areal components of g.
func AreaSquareMeters(g Geometry) float64 {
	switch v := g.(type) {
	case Polygon:
		return polygonMeters2(v)
	case MultiPolygon:
		total := 0.0
		for _, p := range v.Polygons {
			total += polygonMeters2(p)
		}
		return total
	case GeometryCollection:
		total := 0.0
		for _, sub := range v.Geometries {
			total += AreaSquareMeters(sub)
		}
		return total
	default:
		return 0
	}
}

// --- Point-in-polygon -----------------------------------------------------

// GeodeticContainsPoint reports whether c lies within polygon p on the sphere
// (interior; a point exactly on the boundary is treated as contained by S2's
// loop containment).
func GeodeticContainsPoint(c Coord, p Polygon) bool {
	if len(p.Rings) == 0 {
		return false
	}
	pt := s2Point(c)
	if !loopFromRing(p.Rings[0]).ContainsPoint(pt) {
		return false
	}
	for _, hole := range p.Rings[1:] {
		if loopFromRing(hole).ContainsPoint(pt) {
			return false
		}
	}
	return true
}

func geodeticAnyContained(g, container Geometry) bool {
	polys := polygonsOf(container)
	if len(polys) == 0 {
		return false
	}
	contained := false
	eachCoord(g, func(c Coord) {
		if contained {
			return
		}
		for _, p := range polys {
			if GeodeticContainsPoint(c, p) {
				contained = true
				return
			}
		}
	})
	return contained
}

func polygonsOf(g Geometry) []Polygon {
	var out []Polygon
	switch v := g.(type) {
	case Polygon:
		if len(v.Rings) > 0 {
			out = append(out, v)
		}
	case MultiPolygon:
		out = append(out, v.Polygons...)
	case GeometryCollection:
		for _, sub := range v.Geometries {
			out = append(out, polygonsOf(sub)...)
		}
	}
	return out
}

// --- Distance -------------------------------------------------------------

// DistanceMeters returns the minimum geodesic distance in meters between g1 and
// g2. ok is false when either geometry is empty. The distance is 0 when the
// geometries intersect or one contains a point of the other.
func DistanceMeters(g1, g2 Geometry) (dist float64, ok bool) {
	if geomEmpty(g1) || geomEmpty(g2) {
		return 0, false
	}
	if geodeticAnyContained(g1, g2) || geodeticAnyContained(g2, g1) {
		return 0, true
	}
	idx1 := boundaryIndex(g1)
	idx2 := boundaryIndex(g2)
	query := s2.NewClosestEdgeQuery(idx1, s2.NewClosestEdgeQueryOptions())
	target := s2.NewMinDistanceToShapeIndexTarget(idx2)
	chord := query.Distance(target)
	return chord.Angle().Radians() * EarthRadiusMeters, true
}

// boundaryIndex builds an S2 shape index over the boundary edges (and isolated
// points) of g. Polygons contribute their ring boundaries; interior containment
// is handled separately in DistanceMeters.
func boundaryIndex(g Geometry) *s2.ShapeIndex {
	idx := s2.NewShapeIndex()
	addBoundary(idx, g)
	return idx
}

func addBoundary(idx *s2.ShapeIndex, g Geometry) {
	switch v := g.(type) {
	case Point:
		if !v.IsEmpty {
			addPoints(idx, []Coord{{X: v.X, Y: v.Y}})
		}
	case LineString:
		addPolyline(idx, v.Points)
	case Polygon:
		for _, ring := range v.Rings {
			addPolyline(idx, ring)
		}
	case MultiPoint:
		cs := make([]Coord, 0, len(v.Points))
		for _, p := range v.Points {
			if !p.IsEmpty {
				cs = append(cs, Coord{X: p.X, Y: p.Y})
			}
		}
		addPoints(idx, cs)
	case MultiLineString:
		for _, ls := range v.Lines {
			addPolyline(idx, ls.Points)
		}
	case MultiPolygon:
		for _, p := range v.Polygons {
			addBoundary(idx, p)
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			addBoundary(idx, sub)
		}
	}
}

func addPolyline(idx *s2.ShapeIndex, pts []Coord) {
	switch len(pts) {
	case 0:
		return
	case 1:
		addPoints(idx, pts)
	default:
		pl := make(s2.Polyline, len(pts))
		for i, c := range pts {
			pl[i] = s2Point(c)
		}
		idx.Add(&pl)
	}
}

func addPoints(idx *s2.ShapeIndex, cs []Coord) {
	if len(cs) == 0 {
		return
	}
	pv := make(s2.PointVector, len(cs))
	for i, c := range cs {
		pv[i] = s2Point(c)
	}
	idx.Add(&pv)
}
