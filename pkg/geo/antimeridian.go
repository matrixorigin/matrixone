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

package geo

import (
	"math"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// A gnomonic projection maps minor great-circle arcs to straight lines. This
// lets the existing robust Cartesian topology and polygon-overlay kernels be
// used for a bounded WGS84 domain without treating longitude/latitude as a
// Cartesian coordinate system.
//
// The projection is only numerically stable in the open hemisphere facing its
// center. Keep a small margin from the horizon: as cos(c) approaches zero,
// projected coordinates become unbounded and a float64 planar kernel can no
// longer preserve topology reliably.
const (
	geodeticProjectionMinCos = 1e-10
	// The raw gnomonic ordinate is tan(angle) and is therefore dimensionless.
	// Scale it into degree-equivalent units before handing it to the existing
	// Cartesian kernel. Its 1e-9 snap/predicate tolerance then remains about
	// 1e-9 degrees near the frame center instead of accepting distinct points
	// separated by several 1e-8 degrees.
	geodeticProjectionScale = 180 / math.Pi
)

type sphericalVector struct {
	x, y, z float64
}

// GeodeticProjector is a common local spherical-to-planar frame for a pair of
// geometries. It is intentionally created from both operands so swapping
// operands or reordering a collection cannot select a different longitude
// branch or projection center.
type GeodeticProjector struct {
	center sphericalVector
	east   sphericalVector
	north  sphericalVector
	// projectedVertices restores source vertices when the overlay output keeps
	// an exact projected input coordinate.
	projectedVertices map[Coord]Coord
	// snappedVertices contains only unambiguous source vertices whose projected
	// coordinates share one overlay snap cell. Ambiguous cells are deliberately
	// omitted: restoring an arbitrary source coordinate would make output depend
	// on operand/member order.
	snappedVertices          map[Coord]Coord
	ambiguousSnappedVertices map[Coord]struct{}
	// ambiguousProjectedVertices records exact-coordinate collisions. An exact
	// float64 match is not enough to restore a source vertex when two distinct
	// WGS84 inputs rounded to it; those coordinates must use the deterministic
	// inverse projection instead.
	ambiguousProjectedVertices map[Coord]struct{}
	valid                      bool
}

// ProjectGeodeticPair projects a WGS84 pair into one common gnomonic frame.
// The frame represents the spherical minor great-circle edges exactly as
// straight lines, subject to floating-point roundoff. The returned projector
// must be used to Unproject an overlay result before encoding it as WGS84.
//
// Inputs whose vertices do not fit in one stable open hemisphere are rejected.
// This is a deliberate applicability boundary: returning Cartesian results
// for such input would silently change the meaning of a large or ambiguous
// spherical region. Empty geometries are copied and do not constrain the
// frame. Vertices at a pole are rejected because longitude is not unique
// there, so an inverse WKT/WKB representation could not preserve the input
// boundary unambiguously.
func ProjectGeodeticPair(left, right Geometry) (GeodeticProjector, Geometry, Geometry, error) {
	if err := ValidateGeodeticCoordinates(left); err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	if err := ValidateGeodeticCoordinates(right); err != nil {
		return GeodeticProjector{}, nil, nil, err
	}

	coords := collectUniqueGeodeticCoordinates(left, right)
	if len(coords) == 0 {
		return GeodeticProjector{}, cloneGeometry(left), cloneGeometry(right), nil
	}
	for _, c := range coords {
		if math.Abs(c.Y) >= 90 {
			return GeodeticProjector{}, nil, nil, moerr.NewInvalidInputNoCtx(
				"SRID 4326 topology does not support geometry vertices at the poles")
		}
	}

	center, err := commonProjectionCenter(coords)
	if err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	projector := makeGeodeticProjector(center)
	projector.projectedVertices = make(map[Coord]Coord, len(coords))
	projector.snappedVertices = make(map[Coord]Coord, len(coords))
	projector.ambiguousSnappedVertices = make(map[Coord]struct{})
	projector.ambiguousProjectedVertices = make(map[Coord]struct{})
	projectedBySource := make(map[Coord]Coord, len(coords))
	exactCounts := make(map[Coord]int, len(coords))
	snapCandidates := make(map[Coord]Coord, len(coords))
	snapCounts := make(map[Coord]int, len(coords))
	for _, c := range coords {
		if projector.dot(toSphericalVector(c)) <= geodeticProjectionMinCos {
			return GeodeticProjector{}, nil, nil, moerr.NewInvalidInputNoCtx(
				"SRID 4326 topology requires all vertices in one stable gnomonic hemisphere")
		}
		projected, err := projector.projectCoord(c)
		if err != nil {
			return GeodeticProjector{}, nil, nil, err
		}
		projectedBySource[c] = projected
		exactCounts[projected]++
		key := snapCoord(projected)
		snapCandidates[key] = c
		snapCounts[key]++
	}
	for source, projected := range projectedBySource {
		key := snapCoord(projected)
		if exactCounts[projected] == 1 && snapCounts[key] == 1 {
			projector.projectedVertices[projected] = source
		} else if exactCounts[projected] > 1 {
			projector.ambiguousProjectedVertices[projected] = struct{}{}
		}
		if snapCounts[key] == 1 {
			projector.snappedVertices[key] = snapCandidates[key]
		} else if snapCounts[key] > 1 {
			projector.ambiguousSnappedVertices[key] = struct{}{}
		}
	}

	projectedLeft, err := projector.projectGeometry(left)
	if err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	projectedRight, err := projector.projectGeometry(right)
	if err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	projector.valid = true
	return projector, projectedLeft, projectedRight, nil
}

// Unproject maps a geometry from this projector's local plane back to
// canonical WGS84 longitude/latitude. It always returns an independent
// geometry, including for an invalid projector used with empty inputs.
func (p GeodeticProjector) Unproject(g Geometry) Geometry {
	if !p.valid {
		return cloneGeometry(g)
	}
	return p.unprojectGeometry(g)
}

func commonProjectionCenter(coords []Coord) (sphericalVector, error) {
	vectors := make([]sphericalVector, len(coords))
	for i, c := range coords {
		vectors[i] = toSphericalVector(c)
	}
	sort.Slice(vectors, func(i, j int) bool {
		if vectors[i].x != vectors[j].x {
			return vectors[i].x < vectors[j].x
		}
		if vectors[i].y != vectors[j].y {
			return vectors[i].y < vectors[j].y
		}
		return vectors[i].z < vectors[j].z
	})

	// Kahan summation makes the commutative aggregate independent of the
	// original operand/member order after the canonical sort above, without
	// making the geo package depend on a numerical helper library.
	xs := make([]float64, len(vectors))
	ys := make([]float64, len(vectors))
	zs := make([]float64, len(vectors))
	for i, v := range vectors {
		xs[i], ys[i], zs[i] = v.x, v.y, v.z
	}
	center := sphericalVector{sumSorted(xs), sumSorted(ys), sumSorted(zs)}
	norm := math.Sqrt(center.x*center.x + center.y*center.y + center.z*center.z)
	if norm <= geodeticProjectionMinCos {
		return sphericalVector{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology has no unambiguous common gnomonic hemisphere")
	}
	return sphericalVector{center.x / norm, center.y / norm, center.z / norm}, nil
}

func sumSorted(values []float64) float64 {
	sum, correction := 0.0, 0.0
	for _, value := range values {
		y := value - correction
		total := sum + y
		correction = (total - sum) - y
		sum = total
	}
	return sum
}

func makeGeodeticProjector(center sphericalVector) GeodeticProjector {
	east := sphericalVector{-center.y, center.x, 0}
	eastNorm := math.Sqrt(east.x*east.x + east.y*east.y)
	if eastNorm <= geodeticProjectionMinCos {
		// The center is at a pole. This basis is still valid for projection; pole
		// vertices themselves are rejected by ProjectGeodeticPair because their
		// longitude cannot be represented uniquely.
		east = sphericalVector{0, 1, 0}
	} else {
		east.x /= eastNorm
		east.y /= eastNorm
	}
	north := sphericalVector{
		center.y*east.z - center.z*east.y,
		center.z*east.x - center.x*east.z,
		center.x*east.y - center.y*east.x,
	}
	return GeodeticProjector{center: center, east: east, north: north}
}

func (p GeodeticProjector) dot(v sphericalVector) float64 {
	return p.center.x*v.x + p.center.y*v.y + p.center.z*v.z
}

func (p GeodeticProjector) projectCoord(c Coord) (Coord, error) {
	if math.Abs(c.Y) >= 90 {
		return Coord{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology does not support geometry vertices at the poles")
	}
	v := toSphericalVector(c)
	cosC := p.dot(v)
	if cosC <= geodeticProjectionMinCos {
		return Coord{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology requires all vertices in one stable gnomonic hemisphere")
	}
	return Coord{
		X: geodeticProjectionScale * (p.east.x*v.x + p.east.y*v.y + p.east.z*v.z) / cosC,
		Y: geodeticProjectionScale * (p.north.x*v.x + p.north.y*v.y + p.north.z*v.z) / cosC,
	}, nil
}

func (p GeodeticProjector) unprojectCoord(c Coord) Coord {
	if original, ok := p.projectedVertices[c]; ok {
		return original
	}
	if original, ok := p.snappedVertices[c]; ok {
		return original
	}
	projectedX := c.X / geodeticProjectionScale
	projectedY := c.Y / geodeticProjectionScale
	v := sphericalVector{
		p.center.x + projectedX*p.east.x + projectedY*p.north.x,
		p.center.y + projectedX*p.east.y + projectedY*p.north.y,
		p.center.z + projectedX*p.east.z + projectedY*p.north.z,
	}
	norm := math.Sqrt(v.x*v.x + v.y*v.y + v.z*v.z)
	if norm != 0 {
		v.x, v.y, v.z = v.x/norm, v.y/norm, v.z/norm
	}
	latitude := math.Asin(math.Max(-1, math.Min(1, v.z))) * 180 / math.Pi
	longitude := math.Atan2(v.y, v.x) * 180 / math.Pi
	return Coord{X: wrapLongitude(longitude), Y: latitude}
}

func (p GeodeticProjector) projectGeometry(g Geometry) (Geometry, error) {
	switch v := g.(type) {
	case Point:
		if v.IsEmpty {
			return v, nil
		}
		c, err := p.projectCoord(Coord{X: v.X, Y: v.Y})
		if err != nil {
			return nil, err
		}
		return Point{X: c.X, Y: c.Y}, nil
	case LineString:
		points, err := p.projectCoords(v.Points)
		return LineString{Points: points}, err
	case Polygon:
		rings, err := p.projectRings(v.Rings)
		return Polygon{Rings: rings}, err
	case MultiPoint:
		points := append([]Point(nil), v.Points...)
		for i := range points {
			if points[i].IsEmpty {
				continue
			}
			c, err := p.projectCoord(Coord{X: points[i].X, Y: points[i].Y})
			if err != nil {
				return nil, err
			}
			points[i].X, points[i].Y = c.X, c.Y
		}
		return MultiPoint{Points: points}, nil
	case MultiLineString:
		lines := make([]LineString, len(v.Lines))
		for i, line := range v.Lines {
			points, err := p.projectCoords(line.Points)
			if err != nil {
				return nil, err
			}
			lines[i] = LineString{Points: points}
		}
		return MultiLineString{Lines: lines}, nil
	case MultiPolygon:
		polygons := make([]Polygon, len(v.Polygons))
		for i, polygon := range v.Polygons {
			rings, err := p.projectRings(polygon.Rings)
			if err != nil {
				return nil, err
			}
			polygons[i] = Polygon{Rings: rings}
		}
		return MultiPolygon{Polygons: polygons}, nil
	case GeometryCollection:
		geometries := make([]Geometry, len(v.Geometries))
		for i, sub := range v.Geometries {
			projected, err := p.projectGeometry(sub)
			if err != nil {
				return nil, err
			}
			geometries[i] = projected
		}
		return GeometryCollection{Geometries: geometries}, nil
	default:
		return nil, moerr.NewInvalidInputNoCtx("unsupported geometry type for geodetic calculation")
	}
}

func (p GeodeticProjector) projectCoords(coords []Coord) ([]Coord, error) {
	if coords == nil {
		return nil, nil
	}
	out := make([]Coord, len(coords))
	for i, c := range coords {
		projected, err := p.projectCoord(c)
		if err != nil {
			return nil, err
		}
		out[i] = projected
	}
	return out, nil
}

func (p GeodeticProjector) projectRings(rings [][]Coord) ([][]Coord, error) {
	if rings == nil {
		return nil, nil
	}
	out := make([][]Coord, len(rings))
	for i, ring := range rings {
		projected, err := p.projectCoords(ring)
		if err != nil {
			return nil, err
		}
		out[i] = projected
	}
	return out, nil
}

func (p GeodeticProjector) unprojectGeometry(g Geometry) Geometry {
	switch v := g.(type) {
	case Point:
		if v.IsEmpty {
			return v
		}
		c := p.unprojectCoord(Coord{X: v.X, Y: v.Y})
		return Point{X: c.X, Y: c.Y}
	case LineString:
		v.Points = p.unprojectCoords(v.Points)
		return v
	case Polygon:
		v.Rings = p.unprojectRings(v.Rings)
		return v
	case MultiPoint:
		v.Points = append([]Point(nil), v.Points...)
		for i := range v.Points {
			if !v.Points[i].IsEmpty {
				c := p.unprojectCoord(Coord{X: v.Points[i].X, Y: v.Points[i].Y})
				v.Points[i].X, v.Points[i].Y = c.X, c.Y
			}
		}
		return v
	case MultiLineString:
		lines := v.Lines
		v.Lines = make([]LineString, len(lines))
		for i := range lines {
			v.Lines[i].Points = p.unprojectCoords(lines[i].Points)
		}
		return v
	case MultiPolygon:
		polygons := v.Polygons
		v.Polygons = make([]Polygon, len(polygons))
		for i := range polygons {
			v.Polygons[i].Rings = p.unprojectRings(polygons[i].Rings)
		}
		return v
	case GeometryCollection:
		geometries := v.Geometries
		v.Geometries = make([]Geometry, len(geometries))
		for i := range geometries {
			v.Geometries[i] = p.unprojectGeometry(geometries[i])
		}
		return v
	default:
		return g
	}
}

func (p GeodeticProjector) unprojectCoords(coords []Coord) []Coord {
	if coords == nil {
		return nil
	}
	out := make([]Coord, len(coords))
	for i, c := range coords {
		out[i] = p.unprojectCoord(c)
	}
	return out
}

func (p GeodeticProjector) unprojectRings(rings [][]Coord) [][]Coord {
	if rings == nil {
		return nil
	}
	out := make([][]Coord, len(rings))
	for i, ring := range rings {
		out[i] = p.unprojectCoords(ring)
	}
	return out
}

func collectUniqueGeodeticCoordinates(geometries ...Geometry) []Coord {
	seen := make(map[Coord]struct{})
	coords := make([]Coord, 0)
	for _, g := range geometries {
		eachCoord(g, func(c Coord) {
			c.X = canonicalLongitude(c.X)
			if c.X == 0 {
				c.X = 0
			}
			if _, ok := seen[c]; ok {
				return
			}
			seen[c] = struct{}{}
			coords = append(coords, c)
		})
	}
	return coords
}

func canonicalLongitude(longitude float64) float64 {
	if longitude == 180 {
		return -180
	}
	return longitude
}

func toSphericalVector(c Coord) sphericalVector {
	lon := canonicalLongitude(c.X) * math.Pi / 180
	lat := c.Y * math.Pi / 180
	cosLat := math.Cos(lat)
	return sphericalVector{
		x: cosLat * math.Cos(lon),
		y: cosLat * math.Sin(lon),
		z: math.Sin(lat),
	}
}

func wrapLongitude(longitude float64) float64 {
	for longitude > 180 {
		longitude -= 360
	}
	for longitude < -180 {
		longitude += 360
	}
	return longitude
}

func cloneGeometry(g Geometry) Geometry {
	switch v := g.(type) {
	case Point:
		return v
	case LineString:
		v.Points = cloneCoords(v.Points)
		return v
	case Polygon:
		v.Rings = cloneRings(v.Rings)
		return v
	case MultiPoint:
		v.Points = append([]Point(nil), v.Points...)
		return v
	case MultiLineString:
		lines := v.Lines
		v.Lines = make([]LineString, len(lines))
		for i := range lines {
			v.Lines[i].Points = cloneCoords(lines[i].Points)
		}
		return v
	case MultiPolygon:
		polygons := v.Polygons
		v.Polygons = make([]Polygon, len(polygons))
		for i := range polygons {
			v.Polygons[i].Rings = cloneRings(polygons[i].Rings)
		}
		return v
	case GeometryCollection:
		geometries := v.Geometries
		v.Geometries = make([]Geometry, len(geometries))
		for i := range geometries {
			v.Geometries[i] = cloneGeometry(geometries[i])
		}
		return v
	default:
		return g
	}
}

func cloneRings(rings [][]Coord) [][]Coord {
	if rings == nil {
		return nil
	}
	out := make([][]Coord, len(rings))
	for i := range rings {
		out[i] = cloneCoords(rings[i])
	}
	return out
}

func cloneCoords(coords []Coord) []Coord {
	if coords == nil {
		return nil
	}
	return append([]Coord(nil), coords...)
}
