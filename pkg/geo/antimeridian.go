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
	"strings"

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
	// At larger ordinates a float64 ULP is wider than the Cartesian kernel's
	// geodetic snap resolution, so exact noding is no longer meaningful. Keep
	// the supported domain bounded by the numerical precision of the projected
	// representation rather than accepting a mathematically valid but
	// topologically unstable near-horizon result.
	geodeticProjectionMaxAbs = 1e6
	// Geodetic overlays use a finer projected-plane grid than the Cartesian
	// kernel. The projection and overlay therefore preserve shared great-circle
	// boundaries before the source coordinates are encoded back to WGS84. The
	// geodetic noding path still applies its explicit one-cell orientation bound.
	geodeticOverlaySnapScale = 1e13
	// The raw gnomonic ordinate is tan(angle) and is therefore dimensionless.
	// Scale it into degree-equivalent units before handing it to the existing
	// Cartesian kernel.
	geodeticProjectionScale = 180 / math.Pi
)

var geodeticOverlayScales = [...]float64{1e13, 1e12, 1e11, 1e10, 1e9}

type sphericalVector struct {
	x, y, z float64
}

type geodeticProjectedSegment struct {
	source1, source2       Coord
	projected1, projected2 Coord
	lineKey                geodeticLineKey
}

type geodeticLineKey struct {
	x, y, z int64
}

// geodeticLineQuantization is deliberately much finer than a user-visible
// coordinate. It only groups independently projected representations of the
// same great-circle plane; a real bend of 1e-6 degrees changes the normalized
// plane normal by roughly 1e-8 and remains in a different group.
const geodeticLineQuantization = 1e12

const geodeticLineNormalTolerance = 1e-12

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
	// snappedVertices contains only unambiguous source vertices for every grid
	// that the bounded geodetic overlay recovery path may use. Ambiguous cells
	// are deliberately omitted: restoring an arbitrary source coordinate would
	// make output depend on operand/member order.
	snappedVertices          map[Coord]Coord
	ambiguousSnappedVertices map[Coord]struct{}
	// ambiguousProjectedVertices records exact-coordinate collisions. An exact
	// float64 match is not enough to restore a source vertex when two distinct
	// WGS84 inputs rounded to it; those coordinates must use the deterministic
	// inverse projection instead.
	ambiguousProjectedVertices map[Coord]struct{}
	valid                      bool
}

// Overlay applies a polygon Boolean operation in this projector's local
// frame. It uses the geodetic noding grid required to keep shared great-circle
// boundaries connected. Call Unproject on the returned geometry before
// encoding it as WGS84.
func (p GeodeticProjector) Overlay(left, right Geometry, op BoolOp) (Geometry, error) {
	// The primary grid is selected to retain the smallest source-level
	// projection residuals. A rare input can still land exactly on a rounding
	// boundary and make that grid produce an invalid boundary graph. Retry with
	// progressively coarser, bounded grids; this path is entered only after the
	// normal overlay has failed, so valid-input performance is unchanged while
	// numerical recovery remains deterministic and allocation-bounded.
	var firstErr error
	for _, scale := range geodeticOverlayScales {
		result, err := overlayWithOptions(left, right, op, scale, true)
		if err == nil {
			return result, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if !isRecoverableGeodeticOverlayError(err) {
			return nil, err
		}
	}
	return nil, firstErr
}

func isRecoverableGeodeticOverlayError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "invalid overlay boundary graph")
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
	}

	projectedLeft, err := projector.projectGeometry(left)
	if err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	projectedRight, err := projector.projectGeometry(right)
	if err != nil {
		return GeodeticProjector{}, nil, nil, err
	}
	// A gnomonic projection maps every great-circle edge to a line, but the
	// two endpoints are evaluated independently in float64. When another
	// geometry introduces a vertex in the middle of that same edge, the tiny
	// round-off can leave a dangling overlay node. Canonicalize only edges that
	// are identified as the same source great-circle plane within the explicit
	// normal tolerance; this preserves
	// genuinely bent edges and keeps the ordinary Cartesian overlay untouched.
	canonicalProjected := canonicalizeProjectedGeodesicLines(left, projectedLeft, right, projectedRight)
	if len(canonicalProjected) != 0 {
		rewriteProjectedGeometry(left, projectedLeft, canonicalProjected)
		rewriteProjectedGeometry(right, projectedRight, canonicalProjected)
	}
	exactCounts := make(map[Coord]int, len(coords))
	snapCandidates := make(map[Coord]Coord, len(coords))
	for source, projected := range projectedBySource {
		if canonical, ok := canonicalProjected[canonicalSourceCoord(source)]; ok {
			projected = canonical
		}
		exactCounts[projected]++
		for _, scale := range geodeticOverlayScales {
			key := snapCoordAtScale(projected, scale)
			if previous, exists := snapCandidates[key]; exists && previous != source {
				delete(snapCandidates, key)
				projector.ambiguousSnappedVertices[key] = struct{}{}
				continue
			}
			if _, ambiguous := projector.ambiguousSnappedVertices[key]; !ambiguous {
				snapCandidates[key] = source
			}
		}
	}
	for source, projected := range projectedBySource {
		if canonical, ok := canonicalProjected[canonicalSourceCoord(source)]; ok {
			projected = canonical
		}
		if exactCounts[projected] == 1 {
			projector.projectedVertices[projected] = source
		} else if exactCounts[projected] > 1 {
			projector.ambiguousProjectedVertices[projected] = struct{}{}
		}
	}
	for key, source := range snapCandidates {
		if _, ambiguous := projector.ambiguousSnappedVertices[key]; !ambiguous {
			projector.snappedVertices[key] = source
		}
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
	deterministicShuffleSphericalVectors(vectors)

	// The normalized vertex sum is not a valid center selection rule: repeated
	// vertices can move the sum across the horizon even when the geometry is
	// unchanged. Find the smallest spherical cap containing all vertices
	// instead. Its center maximizes the minimum dot product with the vertices,
	// so it is invariant under vertex density and remains inside every valid
	// common hemisphere.
	cap := smallestSphericalCap(vectors)
	if !cap.valid {
		return sphericalVector{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology has no unambiguous common gnomonic hemisphere")
	}
	minDot := math.Inf(1)
	for _, v := range vectors {
		minDot = math.Min(minDot, sphericalDot(cap.center, v))
	}
	if minDot <= geodeticProjectionMinCos {
		return sphericalVector{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology has no unambiguous common gnomonic hemisphere")
	}
	return cap.center, nil
}

const geodeticProjectionCenterTolerance = 1e-14
const geodeticProjectionCenterRelativeTolerance = 1e-12

type sphericalCap struct {
	center sphericalVector
	minDot float64
	radius float64 // chord distance from center to the boundary
	valid  bool
}

func sphericalDot(a, b sphericalVector) float64 {
	return a.x*b.x + a.y*b.y + a.z*b.z
}

func sphericalNorm(v sphericalVector) float64 {
	return math.Sqrt(sphericalDot(v, v))
}

func sphericalCapContains(cap sphericalCap, point sphericalVector) bool {
	if !cap.valid {
		return false
	}
	delta := sphericalVector{
		x: cap.center.x - point.x,
		y: cap.center.y - point.y,
		z: cap.center.z - point.z,
	}
	return sphericalNorm(delta) <= cap.radius+geodeticProjectionCenterTolerance
}

// deterministicShuffleSphericalVectors keeps the incremental cap solver's
// expected linear behavior without making the result depend on geometry
// member order. The input is canonically sorted first, so this fixed-seed
// permutation is stable for the same set of coordinates.
func deterministicShuffleSphericalVectors(points []sphericalVector) {
	state := uint64(0x9e3779b97f4a7c15)
	for i := len(points) - 1; i > 0; i-- {
		state += 0x9e3779b97f4a7c15
		z := state
		z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
		z = (z ^ (z >> 27)) * 0x94d049bb133111eb
		z ^= z >> 31
		j := int(z % uint64(i+1))
		points[i], points[j] = points[j], points[i]
	}
}

// smallestSphericalCap uses the fixed-dimensional incremental algorithm for a
// smallest enclosing circle, with a spherical cap as the circle. A minimum
// cap on S2 has at most three boundary points; the deterministic permutation
// keeps the expected cost linear while avoiding any vertex-density weighting.
func smallestSphericalCap(points []sphericalVector) sphericalCap {
	var cap sphericalCap
	for i, point := range points {
		if sphericalCapContains(cap, point) {
			continue
		}
		cap = sphericalCapFromBoundary([]sphericalVector{point})
		for j := 0; j < i; j++ {
			if sphericalCapContains(cap, points[j]) {
				continue
			}
			cap = sphericalCapFromBoundary([]sphericalVector{point, points[j]})
			for k := 0; k < j; k++ {
				if sphericalCapContains(cap, points[k]) {
					continue
				}
				cap = sphericalCapFromBoundary([]sphericalVector{point, points[j], points[k]})
			}
		}
	}
	return cap
}

func sphericalCapFromBoundary(points []sphericalVector) sphericalCap {
	if len(points) == 0 {
		return sphericalCap{}
	}
	if len(points) == 1 {
		return sphericalCapForCenter(points[0], points)
	}

	var best sphericalCap
	consider := func(candidate sphericalCap) {
		if !candidate.valid || !sphericalCapContainsAll(candidate, points) {
			return
		}
		if !best.valid || candidate.minDot > best.minDot+geodeticProjectionCenterTolerance {
			best = candidate
		}
	}

	pairCap := func(a, b sphericalVector) sphericalCap {
		sum := sphericalVector{x: a.x + b.x, y: a.y + b.y, z: a.z + b.z}
		norm := sphericalNorm(sum)
		if norm <= geodeticProjectionCenterTolerance {
			return sphericalCap{}
		}
		center := sphericalVector{sum.x / norm, sum.y / norm, sum.z / norm}
		return sphericalCapForCenter(center, []sphericalVector{a, b})
	}

	if len(points) == 2 {
		return pairCap(points[0], points[1])
	}

	// A pair cap can be used as a degenerate three-point boundary only when
	// every forced boundary point is actually on that cap. In particular, do
	// not replace a three-point boundary with a pair cap that puts one of its
	// constraints strictly in the interior; the incremental solver relies on
	// those constraints remaining active.
	for i := range len(points) {
		for j := i + 1; j < len(points); j++ {
			candidate := pairCap(points[i], points[j])
			if sphericalCapHasBoundaryPoints(candidate, points) {
				consider(candidate)
			}
		}
	}

	// A non-degenerate three-point boundary is the normal to the affine plane
	// through the points. Choose the side with the smaller cap and let
	// sphericalCapForCenter compute the radius from all three points so small
	// floating-point errors cannot reject an otherwise valid boundary.
	a, b, c := points[0], points[1], points[2]
	u := sphericalVector{b.x - a.x, b.y - a.y, b.z - a.z}
	v := sphericalVector{c.x - a.x, c.y - a.y, c.z - a.z}
	normal := sphericalVector{
		x: u.y*v.z - u.z*v.y,
		y: u.z*v.x - u.x*v.z,
		z: u.x*v.y - u.y*v.x,
	}
	norm := sphericalNorm(normal)
	scale := math.Max(sphericalNorm(u), sphericalNorm(v))
	if scale > 0 && norm > geodeticProjectionCenterRelativeTolerance*scale*scale {
		center := sphericalVector{normal.x / norm, normal.y / norm, normal.z / norm}
		orientation := sphericalDot(center, a) + sphericalDot(center, b) + sphericalDot(center, c)
		if orientation < 0 {
			center.x, center.y, center.z = -center.x, -center.y, -center.z
		}
		consider(sphericalCapForCenter(center, points))
	}
	return best
}

func sphericalCapHasBoundaryPoints(cap sphericalCap, points []sphericalVector) bool {
	if !cap.valid {
		return false
	}
	for _, point := range points {
		delta := sphericalVector{
			x: cap.center.x - point.x,
			y: cap.center.y - point.y,
			z: cap.center.z - point.z,
		}
		if cap.radius-sphericalNorm(delta) > geodeticProjectionCenterTolerance {
			return false
		}
	}
	return sphericalCapContainsAll(cap, points)
}

func sphericalCapForCenter(center sphericalVector, points []sphericalVector) sphericalCap {
	minDot := math.Inf(1)
	radius := 0.0
	for _, point := range points {
		minDot = math.Min(minDot, sphericalDot(center, point))
		radius = math.Max(radius, sphericalNorm(sphericalVector{
			x: center.x - point.x,
			y: center.y - point.y,
			z: center.z - point.z,
		}))
	}
	return sphericalCap{center: center, minDot: minDot, radius: radius, valid: true}
}

func sphericalCapContainsAll(cap sphericalCap, points []sphericalVector) bool {
	for _, point := range points {
		if !sphericalCapContains(cap, point) {
			return false
		}
	}
	return true
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

func canonicalSourceCoord(c Coord) Coord {
	c.X = canonicalLongitude(c.X)
	if c.X == 0 {
		c.X = 0
	}
	return c
}

func collectProjectedGeodeticSegments(source, projected Geometry, out *[]geodeticProjectedSegment) {
	appendRing := func(sourceRing, projectedRing []Coord) {
		n := len(sourceRing)
		if n < 2 || len(projectedRing) != n {
			return
		}
		for i := 1; i < n; i++ {
			appendProjectedGeodeticSegment(sourceRing[i-1], sourceRing[i], projectedRing[i-1], projectedRing[i], out)
		}
		if sourceRing[0] != sourceRing[n-1] {
			appendProjectedGeodeticSegment(sourceRing[n-1], sourceRing[0], projectedRing[n-1], projectedRing[0], out)
		}
	}
	var walk func(Geometry, Geometry)
	walk = func(src, dst Geometry) {
		switch s := src.(type) {
		case Point:
			return
		case LineString:
			d, ok := dst.(LineString)
			if !ok || len(s.Points) != len(d.Points) {
				return
			}
			for i := 1; i < len(s.Points); i++ {
				appendProjectedGeodeticSegment(s.Points[i-1], s.Points[i], d.Points[i-1], d.Points[i], out)
			}
		case Polygon:
			d, ok := dst.(Polygon)
			if !ok || len(s.Rings) != len(d.Rings) {
				return
			}
			for i := range s.Rings {
				appendRing(s.Rings[i], d.Rings[i])
			}
		case MultiPoint:
			return
		case MultiLineString:
			d, ok := dst.(MultiLineString)
			if !ok || len(s.Lines) != len(d.Lines) {
				return
			}
			for i := range s.Lines {
				walk(s.Lines[i], d.Lines[i])
			}
		case MultiPolygon:
			d, ok := dst.(MultiPolygon)
			if !ok || len(s.Polygons) != len(d.Polygons) {
				return
			}
			for i := range s.Polygons {
				walk(s.Polygons[i], d.Polygons[i])
			}
		case GeometryCollection:
			d, ok := dst.(GeometryCollection)
			if !ok || len(s.Geometries) != len(d.Geometries) {
				return
			}
			for i := range s.Geometries {
				walk(s.Geometries[i], d.Geometries[i])
			}
		}
	}
	walk(source, projected)
}

func appendProjectedGeodeticSegment(source1, source2, projected1, projected2 Coord, out *[]geodeticProjectedSegment) {
	if source1 == source2 || projected1 == projected2 {
		return
	}
	key, ok := geodeticGreatCircleLineKey(source1, source2)
	if !ok {
		return
	}
	*out = append(*out, geodeticProjectedSegment{
		source1: canonicalSourceCoord(source1), source2: canonicalSourceCoord(source2),
		projected1: projected1, projected2: projected2, lineKey: key,
	})
}

func geodeticGreatCircleLineKey(a, b Coord) (geodeticLineKey, bool) {
	n, ok := geodeticGreatCircleNormal(a, b)
	if !ok {
		return geodeticLineKey{}, false
	}
	return geodeticLineKey{
		x: int64(math.Round(n.x * geodeticLineQuantization)),
		y: int64(math.Round(n.y * geodeticLineQuantization)),
		z: int64(math.Round(n.z * geodeticLineQuantization)),
	}, true
}

func geodeticGreatCircleNormal(a, b Coord) (sphericalVector, bool) {
	va, vb := toSphericalVector(a), toSphericalVector(b)
	n := sphericalVector{
		x: va.y*vb.z - va.z*vb.y,
		y: va.z*vb.x - va.x*vb.z,
		z: va.x*vb.y - va.y*vb.x,
	}
	norm := math.Sqrt(n.x*n.x + n.y*n.y + n.z*n.z)
	if norm <= geodeticProjectionMinCos {
		return sphericalVector{}, false
	}
	n.x, n.y, n.z = n.x/norm, n.y/norm, n.z/norm
	// A plane normal has no orientation. Canonicalize its sign before
	// quantization so reversing an edge selects the same group.
	if n.x < 0 || (n.x == 0 && (n.y < 0 || (n.y == 0 && n.z < 0))) {
		n.x, n.y, n.z = -n.x, -n.y, -n.z
	}
	return n, true
}

func sameGeodeticGreatCircle(a, b geodeticProjectedSegment) bool {
	na, ok := geodeticGreatCircleNormal(a.source1, a.source2)
	if !ok {
		return false
	}
	nb, ok := geodeticGreatCircleNormal(b.source1, b.source2)
	if !ok {
		return false
	}
	// Normals are sign-canonicalized. Compare the vector difference rather than
	// 1-dot: the latter loses the distinction when dot rounds to exactly one and
	// can even become negative when arithmetic puts dot just above one. The
	// quantized key above is only a cheap candidate filter; this explicit bound
	// is the actual source-line compatibility contract.
	dx, dy, dz := na.x-nb.x, na.y-nb.y, na.z-nb.z
	return math.Sqrt(dx*dx+dy*dy+dz*dz) <= geodeticLineNormalTolerance
}

func canonicalizeProjectedGeodesicLines(geometries ...Geometry) map[Coord]Coord {
	if len(geometries)%2 != 0 {
		return nil
	}
	segments := make([]geodeticProjectedSegment, 0)
	for i := 0; i < len(geometries); i += 2 {
		collectProjectedGeodeticSegments(geometries[i], geometries[i+1], &segments)
	}
	if len(segments) < 2 {
		return nil
	}
	groups := make(map[geodeticLineKey][]int)
	for i := range segments {
		groups[segments[i].lineKey] = append(groups[segments[i].lineKey], i)
	}
	type candidate struct {
		point Coord
		key   geodeticLineKey
	}
	candidates := make(map[Coord]candidate)
	for key, indexes := range groups {
		if len(indexes) < 2 {
			continue
		}
		sort.Slice(indexes, func(i, j int) bool {
			a, b := segments[indexes[i]], segments[indexes[j]]
			amin, amax := minCoord(a.source1, a.source2), maxCoord(a.source1, a.source2)
			bmin, bmax := minCoord(b.source1, b.source2), maxCoord(b.source1, b.source2)
			if amin != bmin {
				return coordLess(amin, bmin)
			}
			return coordLess(amax, bmax)
		})
		reference := segments[indexes[0]]
		dx, dy := reference.projected2.X-reference.projected1.X, reference.projected2.Y-reference.projected1.Y
		lengthSquared := dx*dx + dy*dy
		if lengthSquared == 0 {
			continue
		}
		for _, index := range indexes {
			segment := segments[index]
			if !sameGeodeticGreatCircle(reference, segment) {
				continue
			}
			for _, endpoint := range []struct {
				source, projected Coord
			}{
				{segment.source1, segment.projected1},
				{segment.source2, segment.projected2},
			} {
				dxToPoint, dyToPoint := endpoint.projected.X-reference.projected1.X, endpoint.projected.Y-reference.projected1.Y
				t := (dxToPoint*dx + dyToPoint*dy) / lengthSquared
				projected := Coord{X: reference.projected1.X + t*dx, Y: reference.projected1.Y + t*dy}
				if !withinGeodeticProjectionRoundoff(endpoint.projected, projected) {
					continue
				}
				source := canonicalSourceCoord(endpoint.source)
				old, exists := candidates[source]
				if !exists || geodeticLineKeyLess(key, old.key) {
					candidates[source] = candidate{point: projected, key: key}
				}
			}
		}
	}
	result := make(map[Coord]Coord, len(candidates))
	for source, value := range candidates {
		result[source] = value.point
	}
	return result
}

func withinGeodeticProjectionRoundoff(a, b Coord) bool {
	maxAbs := math.Max(1, math.Max(math.Abs(a.X), math.Abs(a.Y)))
	maxAbs = math.Max(maxAbs, math.Max(math.Abs(b.X), math.Abs(b.Y)))
	tolerance := 64*float64Epsilon*maxAbs + 8/geodeticOverlaySnapScale
	return math.Hypot(a.X-b.X, a.Y-b.Y) <= tolerance
}

func geodeticLineKeyLess(a, b geodeticLineKey) bool {
	if a.x != b.x {
		return a.x < b.x
	}
	if a.y != b.y {
		return a.y < b.y
	}
	return a.z < b.z
}

func coordLess(a, b Coord) bool {
	if a.X != b.X {
		return a.X < b.X
	}
	return a.Y < b.Y
}

func minCoord(a, b Coord) Coord {
	if coordLess(b, a) {
		return b
	}
	return a
}

func maxCoord(a, b Coord) Coord {
	if coordLess(a, b) {
		return b
	}
	return a
}

func rewriteProjectedGeometry(source, projected Geometry, canonical map[Coord]Coord) {
	lookup := func(sourceCoord, projectedCoord Coord) Coord {
		if replacement, ok := canonical[canonicalSourceCoord(sourceCoord)]; ok {
			return replacement
		}
		return projectedCoord
	}
	var walk func(Geometry, Geometry)
	walk = func(src, dst Geometry) {
		switch s := src.(type) {
		case LineString:
			d, ok := dst.(LineString)
			if !ok || len(s.Points) != len(d.Points) {
				return
			}
			for i := range s.Points {
				d.Points[i] = lookup(s.Points[i], d.Points[i])
			}
		case Polygon:
			d, ok := dst.(Polygon)
			if !ok || len(s.Rings) != len(d.Rings) {
				return
			}
			for i := range s.Rings {
				if len(s.Rings[i]) != len(d.Rings[i]) {
					continue
				}
				for j := range s.Rings[i] {
					d.Rings[i][j] = lookup(s.Rings[i][j], d.Rings[i][j])
				}
			}
		case MultiLineString:
			d, ok := dst.(MultiLineString)
			if !ok || len(s.Lines) != len(d.Lines) {
				return
			}
			for i := range s.Lines {
				walk(s.Lines[i], d.Lines[i])
			}
		case MultiPolygon:
			d, ok := dst.(MultiPolygon)
			if !ok || len(s.Polygons) != len(d.Polygons) {
				return
			}
			for i := range s.Polygons {
				walk(s.Polygons[i], d.Polygons[i])
			}
		case GeometryCollection:
			d, ok := dst.(GeometryCollection)
			if !ok || len(s.Geometries) != len(d.Geometries) {
				return
			}
			for i := range s.Geometries {
				walk(s.Geometries[i], d.Geometries[i])
			}
		}
	}
	walk(source, projected)
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
	projected := Coord{
		X: geodeticProjectionScale * (p.east.x*v.x + p.east.y*v.y + p.east.z*v.z) / cosC,
		Y: geodeticProjectionScale * (p.north.x*v.x + p.north.y*v.y + p.north.z*v.z) / cosC,
	}
	if math.Abs(projected.X) > geodeticProjectionMaxAbs ||
		math.Abs(projected.Y) > geodeticProjectionMaxAbs {
		return Coord{}, moerr.NewInvalidInputNoCtx(
			"SRID 4326 topology is numerically unstable near the gnomonic hemisphere boundary")
	}
	return projected, nil
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
