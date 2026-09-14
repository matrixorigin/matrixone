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

	"github.com/golang/geo/s2"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

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

// ValidateGeodeticCoordinates verifies that every non-empty coordinate is a
// finite WGS 84 longitude/latitude pair. Call it at the effective SRID-4326
// measurement boundary before invoking the kernels below: S2 normalizes
// out-of-range coordinates instead of rejecting them. SRID-0 Cartesian callers
// must not apply this check.
func ValidateGeodeticCoordinates(g Geometry) error {
	return validateGeodeticCoordinates(g, 0)
}

func validateGeodeticCoordinates(g Geometry, depth int) error {
	if g == nil {
		return moerr.NewInvalidInputNoCtx("invalid geometry payload")
	}
	if depth > maxGeometryNestingDepth {
		return moerr.NewInvalidInputNoCtxf("geometry collection nesting depth exceeds %d", maxGeometryNestingDepth)
	}
	switch v := g.(type) {
	case Point:
		if v.IsEmpty {
			return nil
		}
		return validateGeodeticCoordinate(Coord{X: v.X, Y: v.Y})
	case LineString:
		return validateGeodeticCoordinatesInSlice(v.Points)
	case Polygon:
		for _, ring := range v.Rings {
			if err := validateGeodeticCoordinatesInSlice(ring); err != nil {
				return err
			}
		}
	case MultiPoint:
		for _, point := range v.Points {
			if point.IsEmpty {
				continue
			}
			if err := validateGeodeticCoordinate(Coord{X: point.X, Y: point.Y}); err != nil {
				return err
			}
		}
	case MultiLineString:
		for _, line := range v.Lines {
			if err := validateGeodeticCoordinatesInSlice(line.Points); err != nil {
				return err
			}
		}
	case MultiPolygon:
		for _, polygon := range v.Polygons {
			for _, ring := range polygon.Rings {
				if err := validateGeodeticCoordinatesInSlice(ring); err != nil {
					return err
				}
			}
		}
	case GeometryCollection:
		for _, sub := range v.Geometries {
			if err := validateGeodeticCoordinates(sub, depth+1); err != nil {
				return err
			}
		}
	default:
		// Avoid formatting the interface value here: retaining a %T diagnostic
		// makes otherwise-valid concrete geometries escape to the heap.
		return moerr.NewInvalidInputNoCtx("unsupported geometry type for geodetic calculation")
	}
	return nil
}

func validateGeodeticCoordinatesInSlice(coords []Coord) error {
	for _, coord := range coords {
		if err := validateGeodeticCoordinate(coord); err != nil {
			return err
		}
	}
	return nil
}

func validateGeodeticCoordinate(coord Coord) error {
	if math.IsNaN(coord.X) || math.IsInf(coord.X, 0) {
		return moerr.NewInvalidInputNoCtx("SRID 4326 longitude must be finite")
	}
	if coord.X < -180 || coord.X > 180 {
		return moerr.NewInvalidInputNoCtxf("SRID 4326 longitude %v is out of range [-180, 180]", coord.X)
	}
	if math.IsNaN(coord.Y) || math.IsInf(coord.Y, 0) {
		return moerr.NewInvalidInputNoCtx("SRID 4326 latitude must be finite")
	}
	if coord.Y < -90 || coord.Y > 90 {
		return moerr.NewInvalidInputNoCtxf("SRID 4326 latitude %v is out of range [-90, 90]", coord.Y)
	}
	return nil
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
// Callers must validate effective-SRID-4326 coordinates with
// ValidateGeodeticCoordinates first; this numeric-only kernel does not return
// coordinate-validation errors.
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

// loopFromRing builds an S2 loop from a WKB ring (dropping the repeated closing
// vertex). The loop's winding follows the ring's: by OGC convention the enclosed
// region lies to the left of a counter-clockwise ring, so we orient the S2 loop
// counter-clockwise (positive planar signed area) and trust the input winding to
// decide which side is the interior.
//
// We deliberately avoid s2.Loop.Normalize(), which unconditionally selects the
// smaller of the two regions a loop bounds: that would silently turn a
// larger-than-hemisphere polygon into its complement, inverting its area and
// containment. (Polygons spanning a pole or the antimeridian remain a deferred
// corner case — planar winding is ill-defined there.)
func loopFromRing(ring []Coord) *s2.Loop {
	end := len(ring)
	if end > 1 && ring[0] == ring[end-1] {
		end--
	}
	ccw := ringSignedArea(ring) >= 0
	pts := make([]s2.Point, 0, end)
	if ccw {
		for i := 0; i < end; i++ {
			pts = append(pts, s2Point(ring[i]))
		}
	} else {
		for i := end - 1; i >= 0; i-- {
			pts = append(pts, s2Point(ring[i]))
		}
	}
	return s2.LoopFromPoints(pts)
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
// Callers must validate effective-SRID-4326 coordinates with
// ValidateGeodeticCoordinates first; this numeric-only kernel does not return
// coordinate-validation errors.
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
// geometries intersect or one contains a point of the other. Callers must
// validate both geometries according to their operation's coordinate
// contract first (for effective SRID 4326, use
// ValidateGeodeticCoordinates); this kernel's boolean reports emptiness, not
// coordinate-validation errors.
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
