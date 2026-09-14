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
	"strconv"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestProjectGeodeticPairAcrossAntimeridian(t *testing.T) {
	outer := mustParse(t, "POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))")
	inner := mustParse(t, "POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))")

	projector, projectedOuter, projectedInner, err := ProjectGeodeticPair(outer, inner)
	require.NoError(t, err)
	result, err := projector.Overlay(projectedOuter, projectedInner, OpIntersection)
	require.NoError(t, err)
	got := projector.Unproject(result).(Polygon)

	want := [][]Coord{{
		{179.5, 0.5}, {179.5, -0.5}, {-179.5, -0.5}, {-179.5, 0.5}, {179.5, 0.5},
	}}
	requirePolygonCoordinatesNear(t, want, got.Rings, 1e-12)
}

func TestProjectGeodeticPairHandlesHighLatitudeGreatCircleDomain(t *testing.T) {
	outer := mustParse(t, "POLYGON((20 79,-20 79,-20 81,20 81,20 79))")
	inner := mustParse(t, "POLYGON((10 79.5,-10 79.5,-10 80.5,10 80.5,10 79.5))")

	projector, projectedOuter, projectedInner, err := ProjectGeodeticPair(outer, inner)
	require.NoError(t, err)
	result, err := projector.Overlay(projectedOuter, projectedInner, OpIntersection)
	require.NoError(t, err)
	got := projector.Unproject(result).(Polygon)
	require.NotEmpty(t, got.Rings)
	// The result must remain a valid local WGS84 polygon. In particular, the
	// projected overlay must not be allowed to produce an out-of-range point.
	require.NoError(t, ValidateGeodeticCoordinates(got))
	for _, ring := range got.Rings {
		for _, c := range ring {
			require.LessOrEqual(t, math.Abs(c.Y), 90.0)
		}
	}
}

func TestProjectGeodeticPairIsStableUnderUnevenVertexDensification(t *testing.T) {
	other := mustParse(t, "POINT(1 0)")
	sparse := mustParse(t, "LINESTRING(-80 0,82 0)")
	dense := mustParse(t, "LINESTRING(-80 0,80 0,81 0,82 0)")

	sparseProjector, _, _, err := ProjectGeodeticPair(sparse, other)
	require.NoError(t, err)
	denseProjector, _, _, err := ProjectGeodeticPair(dense, other)
	require.NoError(t, err)

	// Adding collinear vertices changes neither the great-circle arc nor its
	// valid projection domain. The selected center must not depend on vertex
	// density, otherwise the averaged center can cross the horizon.
	require.Equal(t, sparseProjector.center, denseProjector.center)
	for _, c := range []Coord{{X: -80, Y: 0}, {X: 80, Y: 0}, {X: 81, Y: 0}, {X: 82, Y: 0}} {
		require.Greater(t, sparseProjector.dot(toSphericalVector(c)), geodeticProjectionMinCos)
	}
}

func TestProjectGeodeticPairAcceptsSmallNonCollinearDomain(t *testing.T) {
	for _, line := range []string{
		"LINESTRING(0 0,0.000001 0,0.0000005 0.0000008660254)",
		"LINESTRING(0 0,0.0001 0,0.00005 0.00008660254)",
	} {
		t.Run(line, func(t *testing.T) {
			left := mustParse(t, line)
			right := mustParse(t, "POINT(0 0)")
			_, _, _, err := ProjectGeodeticPair(left, right)
			require.NoError(t, err)
		})
	}
}

func TestProjectGeodeticPairKeepsAllActiveCapConstraints(t *testing.T) {
	left := mustParse(t, "MULTIPOINT((-7 -80),(-70 -77),(9 60),(25 32),(62 -13),(-12 60),(-8 39),(56 55))")
	right := mustParse(t, "POINT(-7 -80)")

	_, _, _, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
}

func TestProjectGeodeticPairIsIndependentOfOperandAndMemberOrder(t *testing.T) {
	left := mustParse(t, "GEOMETRYCOLLECTION(POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1)),POINT(179.2 0))")
	right := mustParse(t, "POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))")

	p1, l1, r1, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	p2, r2, l2, err := ProjectGeodeticPair(right, left)
	require.NoError(t, err)
	require.Equal(t, p1, p2)
	require.Equal(t, l1, l2)
	require.Equal(t, r1, r2)

	rotated := mustParse(t, "POLYGON((-179 -1,-179 1,179 1,179 -1,-179 -1))")
	_, _, _, err = ProjectGeodeticPair(rotated, right)
	require.NoError(t, err)
}

func TestProjectGeodeticPairDoesNotMutateNestedInputs(t *testing.T) {
	left := GeometryCollection{Geometries: []Geometry{
		MultiPolygon{Polygons: []Polygon{{Rings: [][]Coord{{
			{179, -1}, {-179, -1}, {-179, 1}, {179, 1}, {179, -1},
		}}}}},
	}}
	right := Point{X: 180, Y: 0}
	original := cloneGeometry(left)
	_, _, _, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	require.Equal(t, original, left)
}

func TestProjectGeodeticPairKeepsDistinctNearbyPointsDistinct(t *testing.T) {
	left := mustParse(t, "POINT(0 0)")
	right := mustParse(t, "POINT(0.00000001 0)")
	_, projectedLeft, projectedRight, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	leftPoint := projectedLeft.(Point)
	rightPoint := projectedRight.(Point)
	require.Greater(t, math.Abs(leftPoint.X-rightPoint.X), 1e-9)
}

func TestProjectGeodeticPairKeepsSmallShapesDistinct(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  string
		right string
		check func(t *testing.T, left, right Geometry)
	}{
		{
			name:  "short line",
			left:  "LINESTRING(0 0,0.00000001 0)",
			right: "LINESTRING(0.00000002 0,0.00000003 0)",
			check: func(t *testing.T, left, right Geometry) {
				l := left.(LineString)
				r := right.(LineString)
				require.Greater(t, math.Abs(l.Points[1].X-l.Points[0].X), 1e-9)
				require.Greater(t, math.Abs(r.Points[1].X-r.Points[0].X), 1e-9)
				require.Greater(t, r.Points[0].X-l.Points[1].X, 1e-9)
			},
		},
		{
			name:  "small polygon",
			left:  "POLYGON((0 0,0.00000001 0,0.00000001 0.00000001,0 0.00000001,0 0))",
			right: "POLYGON((0.00000002 0,0.00000003 0,0.00000003 0.00000001,0.00000002 0.00000001,0.00000002 0))",
			check: func(t *testing.T, left, right Geometry) {
				l := left.(Polygon).Rings[0]
				r := right.(Polygon).Rings[0]
				require.Greater(t, math.Abs(l[1].X-l[0].X), 1e-9)
				require.Greater(t, math.Abs(l[2].Y-l[1].Y), 1e-9)
				require.Greater(t, r[0].X-l[1].X, 1e-9)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, left, right, err := ProjectGeodeticPair(
				mustParse(t, tc.left), mustParse(t, tc.right))
			require.NoError(t, err)
			tc.check(t, left, right)
		})
	}
}

func TestProjectGeodeticPairDoesNotRestoreAmbiguousSnapCell(t *testing.T) {
	left := GeometryCollection{Geometries: []Geometry{
		Point{X: 0, Y: 0},
		Point{X: 0.0000000004, Y: 0},
	}}
	right := Point{X: 0, Y: 0}

	first, _, _, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	second, _, _, err := ProjectGeodeticPair(right, left)
	require.NoError(t, err)

	// Both source points round into the same overlay snap cell. No source
	// coordinate is uniquely recoverable, so Unproject must use the common
	// inverse frame rather than whichever point was visited last.
	firstPoint := first.Unproject(Point{}).(Point)
	secondPoint := second.Unproject(Point{}).(Point)
	require.Equal(t, firstPoint, secondPoint)
	require.InDelta(t, 0.0000000002, firstPoint.X, 1e-12)

	// The same collision must remain deterministic when the points are carried
	// by a polygon and its ring is cyclically rotated.
	ring := Polygon{Rings: [][]Coord{{
		{X: 0, Y: 0},
		{X: 0.0000000004, Y: 0},
		{X: 1, Y: 1},
		{X: 0, Y: 1},
		{X: 0, Y: 0},
	}}}
	rotated := Polygon{Rings: [][]Coord{{
		{X: 0.0000000004, Y: 0},
		{X: 1, Y: 1},
		{X: 0, Y: 1},
		{X: 0, Y: 0},
		{X: 0.0000000004, Y: 0},
	}}}
	first, _, _, err = ProjectGeodeticPair(ring, right)
	require.NoError(t, err)
	second, _, _, err = ProjectGeodeticPair(rotated, right)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, first.Unproject(Point{}), second.Unproject(Point{}))
}

func TestProjectGeodeticPairDoesNotRestoreExactCollision(t *testing.T) {
	a := Point{X: 179.00000000000003, Y: 0}
	b := Point{X: 179.00000000000006, Y: 0}

	first, projectedA, _, err := ProjectGeodeticPair(a, b)
	require.NoError(t, err)
	second, _, projectedAAfterSwap, err := ProjectGeodeticPair(b, a)
	require.NoError(t, err)
	require.Equal(t, projectedA, projectedAAfterSwap)

	// The two distinct source coordinates round to the same projected float64
	// coordinate. No last-writer source vertex may be restored after swapping
	// the operands; both projectors must use the same inverse result.
	require.Equal(t, first.Unproject(projectedA), second.Unproject(projectedAAfterSwap))
}

func TestProjectGeodeticPairRestoresEveryRecoveryGrid(t *testing.T) {
	left, right := Point{X: 10.123456789, Y: 20.234567891}, Point{X: 11.123456789, Y: 21.234567891}
	projector, projectedLeft, _, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	raw := projectedLeft.(Point).Coord()
	for _, scale := range geodeticOverlayScales {
		key := snapCoordAtScale(raw, scale)
		got := projector.Unproject(Point{X: key.X, Y: key.Y}).(Point)
		require.InDelta(t, left.X, got.X, 1e-12)
		require.InDelta(t, left.Y, got.Y, 1e-12)
	}
}

func TestProjectGeodeticPairDoesNotCanonicalizeDistinctGreatCircles(t *testing.T) {
	left := mustParse(t, "LINESTRING(0 0,10 10)")
	right := mustParse(t, "LINESTRING(0 0.000001,10 10.000001)")
	projector, projectedLeft, projectedRight, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	got := projectedRight.(LineString).Points
	for i, source := range right.(LineString).Points {
		want, err := projector.projectCoord(source)
		require.NoError(t, err)
		require.Equal(t, want, got[i], "distinct source great-circle edge was canonicalized")
	}
	// Keep the left edge live as well; this test is about the source-line
	// grouping and must not rely on an empty operand shortcut.
	require.NotEmpty(t, projectedLeft.(LineString).Points)
}

func TestProjectGeodeticPairRejectsAmbiguousDomains(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  string
		right string
		want  string
	}{
		{name: "antipodal", left: "POINT(0 0)", right: "POINT(180 0)", want: "unambiguous common gnomonic hemisphere"},
		{name: "pole", left: "POINT(0 90)", right: "POINT(0 0)", want: "vertices at the poles"},
		{name: "near horizon", left: "POINT(0 0)", right: "POINT(179.9999 0)", want: "numerically unstable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, _, _, err := ProjectGeodeticPair(mustParse(t, tc.left), mustParse(t, tc.right))
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
			require.Contains(t, err.Error(), tc.want)
		})
	}
}

func TestProjectGeodeticPairOverlayCreatesGreatCircleIntersections(t *testing.T) {
	a := mustParse(t, "POLYGON((179 -2,-179 -2,-179 2,179 2,179 -2))")
	b := mustParse(t, "POLYGON((178 0,-178 0,-178 3,178 3,178 0))")
	want := [][]Coord{{
		{179, 2}, {179, 0}, {-179, 0}, {-179, 2}, {179, 2},
	}}
	run := func(left, right Geometry) Geometry {
		projector, projectedLeft, projectedRight, err := ProjectGeodeticPair(left, right)
		require.NoError(t, err)
		result, err := projector.Overlay(projectedLeft, projectedRight, OpIntersection)
		require.NoError(t, err)
		return projector.Unproject(result)
	}

	got := run(a, b).(Polygon)
	requirePolygonCoordinatesNear(t, want, got.Rings, 1e-8)
	got = run(b, a).(Polygon)
	requirePolygonCoordinatesNear(t, want, got.Rings, 1e-8)
}

func TestProjectGeodeticPairOverlayHandlesSharedEdges(t *testing.T) {
	// The second polygon shares a meridian edge with the first one. Projection
	// round-off must not turn the shared edge into a dangling result boundary.
	a := mustParse(t, "POLYGON((0 0,2 0,2 2,0 2,0 0))")
	b := mustParse(t, "POLYGON((2 0,3 0,3 1,2 1,2 0))")
	projector, projectedA, projectedB, err := ProjectGeodeticPair(a, b)
	require.NoError(t, err)
	result, err := projector.Overlay(projectedA, projectedB, OpUnion)
	require.NoError(t, err)

	got, ok := projector.Unproject(result).(Polygon)
	require.True(t, ok)
	requirePolygonCoordinatesNear(t, [][]Coord{{
		{0, 2}, {0, 0}, {2, 0}, {3, 0}, {3, 1}, {2, 1}, {2, 2}, {0, 2},
	}}, got.Rings, 1e-8)
}

func TestProjectGeodeticPairOverlayHandlesPartialSharedEdges(t *testing.T) {
	for _, tc := range []struct {
		name, left, right string
	}{
		{
			name:  "equator",
			left:  "POLYGON((0 0,2 0,2 2,0 2,0 0))",
			right: "POLYGON((2 0.5,3 0.5,3 1.5,2 1.5,2 0.5))",
		},
		{
			name:  "high latitude",
			left:  "POLYGON((-170 -78,-168 -78,-168 -76,-170 -76,-170 -78))",
			right: "POLYGON((-168 -77.5,-166 -77.5,-166 -76.5,-168 -76.5,-168 -77.5))",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The shared boundary starts and ends inside the first polygon edge.
			// This exercises near-collinear noding without an exact endpoint
			// coincidence.
			a, b := mustParse(t, tc.left), mustParse(t, tc.right)
			projector, projectedA, projectedB, err := ProjectGeodeticPair(a, b)
			require.NoError(t, err)
			result, err := projector.Overlay(projectedA, projectedB, OpUnion)
			require.NoError(t, err)
			got, ok := projector.Unproject(result).(Polygon)
			require.True(t, ok)
			require.NoError(t, ValidateGeodeticCoordinates(got))
		})
	}
}

func TestProjectGeodeticPairOverlaySharedEdgeInvariants(t *testing.T) {
	const (
		baseA = "POLYGON((0 0,2 0,2 1,2 2,0 2,0 0))"
		baseB = "POLYGON((2 0,3 0,3 1,2 1,2 0))"
	)
	cases := []struct {
		name  string
		left  string
		right string
	}{
		{name: "split edge", left: baseA, right: "POLYGON((2 0,2 0.5,2 1,3 1,3 0,2 0))"},
		{name: "reversed rings", left: "POLYGON((0 0,0 2,2 2,2 1,2 0,0 0))", right: "POLYGON((2 0,2 1,3 1,3 0,2 0))"},
		{name: "translated frame", left: "POLYGON((100 0,102 0,102 1,102 2,100 2,100 0))", right: "POLYGON((102 0,103 0,103 1,102 1,102 0))"},
	}
	ops := []BoolOp{OpIntersection, OpUnion, OpDifference, OpXOR}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right := mustParse(t, tc.left), mustParse(t, tc.right)
			for _, op := range ops {
				t.Run(strconv.Itoa(int(op)), func(t *testing.T) {
					projector, projectedLeft, projectedRight, err := ProjectGeodeticPair(left, right)
					require.NoError(t, err)
					result, err := projector.Overlay(projectedLeft, projectedRight, op)
					require.NoError(t, err)
					require.NoError(t, ValidateGeodeticCoordinates(projector.Unproject(result)))
				})
			}
		})
	}
}

func TestProjectGeodeticPairOverlayDoesNotMergeSeparatedEdges(t *testing.T) {
	left := mustParse(t, "POLYGON((0 0,2 0,2 2,0 2,0 0))")
	right := mustParse(t, "POLYGON((0 2.00001,2 2.00001,2 4.00001,0 4.00001,0 2.00001))")
	projector, projectedLeft, projectedRight, err := ProjectGeodeticPair(left, right)
	require.NoError(t, err)
	result, err := projector.Overlay(projectedLeft, projectedRight, OpUnion)
	require.NoError(t, err)
	require.Equal(t, MULTIPOLYGON, projector.Unproject(result).Type())
}

func TestCartesianSegmentIntersectionKeepsDistinctSnapRows(t *testing.T) {
	// The scale-aware orientation tolerance is reserved for geodetic overlays.
	// Cartesian callers must not turn two already distinct snap rows into a
	// collinear overlap.
	n, _, _ := segmentIntersection(
		Coord{X: 0, Y: 0}, Coord{X: 1, Y: 0},
		Coord{X: 0, Y: 1e-9}, Coord{X: 1, Y: 1e-9})
	require.Equal(t, 0, n)
}

func TestGeodeticSegmentIntersectionKeepsAdjacentSnapRows(t *testing.T) {
	// Geodetic noding may tolerate projection round-off, but neighboring snap
	// rows are still distinct topology. Recompute near-zero orientation at high
	// precision before applying the strict half-cell bound.
	n, _, _ := segmentIntersectionAtScale(
		Coord{X: 0, Y: 0}, Coord{X: 1, Y: 0},
		Coord{X: 0, Y: 1e-12}, Coord{X: 1, Y: 1e-12},
		geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
}

func TestGeodeticSegmentIntersectionIsInvariantToSegmentDirection(t *testing.T) {
	// The near-collinear predicate must use the complete second segment when
	// estimating its snap-rounding error. A bound based only on b1-a1 changes
	// when B is reversed and can turn two distinct parallel rows into an
	// overlap in one direction.
	a1, a2 := Coord{X: 0, Y: 0}, Coord{X: 1, Y: 0}
	b1, b2 := Coord{X: 0.9, Y: 1e-12}, Coord{X: 2, Y: 1e-12}
	n, _, _ := segmentIntersectionAtScale(a1, a2, b1, b2, geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
	n, _, _ = segmentIntersectionAtScale(a1, a2, b2, b1, geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
	// Swapping the operands must preserve the same no-intersection result.
	n, _, _ = segmentIntersectionAtScale(b1, b2, a1, a2, geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
	n, _, _ = segmentIntersectionAtScale(b2, b1, a1, a2, geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
}

func TestGeodeticSegmentIntersectionDoesNotTrustZeroFloatCross(t *testing.T) {
	// Use large, nearly cancelling ordinates so float64 sees a zero cross
	// product while the exact 128-bit recomputation still sees a separated
	// line. The fallback must not classify it as an overlap.
	a1, a2 := Coord{X: 1e8, Y: 1e8}, Coord{X: 1e8 + 1, Y: 1e8 + 1}
	b1, b2 := Coord{X: 1e8, Y: 1e8 + 1e-6}, Coord{X: 1e8 + 1, Y: 1e8 + 1 + 1e-6}
	n, _, _ := segmentIntersectionAtScale(a1, a2, b1, b2, geodeticOverlaySnapScale, true)
	require.Equal(t, 0, n)
}

func TestGeodeticSegmentIntersectionKeepsNearParallelEndpoint(t *testing.T) {
	// A shallow angle does not make a shared endpoint disappear. The lines are
	// too far apart at B's far endpoint to be treated as a snapped overlap, but
	// their exact common origin remains a real intersection.
	scale := geodeticOverlaySnapScale
	n, point, _ := segmentIntersectionAtScale(
		Coord{X: 0, Y: 0}, Coord{X: 1, Y: 0},
		Coord{X: 0, Y: 0}, Coord{X: 100, Y: 2 / scale}, scale, true)
	require.Equal(t, 1, n)
	require.Equal(t, Coord{}, point)
}

func TestSegmentIntersectionRecoversCancelledOrientation(t *testing.T) {
	// The float64 cross of these vectors is zero, while their exact binary
	// products differ. The Cartesian (strict) path must still retain the shared
	// endpoint instead of taking the parallel/overlap branch.
	n, point, _ := segmentIntersectionAtScale(
		Coord{X: 0, Y: 0}, Coord{X: 1e16, Y: 1},
		Coord{X: 0, Y: 0}, Coord{X: 1, Y: 1e-16}, snapScale, false)
	require.Equal(t, 1, n)
	require.Equal(t, Coord{}, point)
}

func TestSegmentIntersectionRecoversCancelledMidpoint(t *testing.T) {
	// These exactly representable binary coordinates make the float64 cross
	// denominator cancel to zero while the true intersection is the midpoint
	// of A. The high-precision denominator and numerators must be evaluated as
	// one ratio; recovering only the denominator would return A's origin.
	const (
		n = 1 << 27
		k = 1.0 / (1 << 10)
	)
	va := Coord{X: float64(n) * k, Y: float64(n-1) * k}
	vb := Coord{X: float64(n+1) * k, Y: float64(n) * k}
	a1 := Coord{}
	a2 := va
	half := Coord{X: va.X / 2, Y: va.Y / 2}
	b1 := half
	b2 := Coord{X: half.X + vb.X, Y: half.Y + vb.Y}
	want := snapCoordAtScale(half, snapScale)

	cases := [][4]Coord{
		{a1, a2, b1, b2},
		{a2, a1, b1, b2},
		{a1, a2, b2, b1},
		{a2, a1, b2, b1},
		{b1, b2, a1, a2},
		{b2, b1, a1, a2},
		{b1, b2, a2, a1},
		{b2, b1, a2, a1},
	}
	for _, tc := range cases {
		n, point, _ := segmentIntersectionAtScale(tc[0], tc[1], tc[2], tc[3], snapScale, false)
		require.Equal(t, 1, n)
		require.Equal(t, want, point)
	}
}

func requirePolygonCoordinatesNear(t *testing.T, want, got [][]Coord, delta float64) {
	t.Helper()
	require.Len(t, got, len(want))
	for i := range want {
		require.Len(t, got[i], len(want[i]))
		for j := range want[i] {
			require.InDelta(t, want[i][j].X, got[i][j].X, delta)
			require.InDelta(t, want[i][j].Y, got[i][j].Y, delta)
		}
	}
}
