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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestProjectGeodeticPairAcrossAntimeridian(t *testing.T) {
	outer := mustParse(t, "POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))")
	inner := mustParse(t, "POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))")

	projector, projectedOuter, projectedInner, err := ProjectGeodeticPair(outer, inner)
	require.NoError(t, err)
	result, err := Overlay(projectedOuter, projectedInner, OpIntersection)
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
	result, err := Overlay(projectedOuter, projectedInner, OpIntersection)
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

func TestProjectGeodeticPairRejectsAmbiguousDomains(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  string
		right string
		want  string
	}{
		{name: "antipodal", left: "POINT(0 0)", right: "POINT(180 0)", want: "unambiguous common gnomonic hemisphere"},
		{name: "pole", left: "POINT(0 90)", right: "POINT(0 0)", want: "vertices at the poles"},
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
		result, err := Overlay(projectedLeft, projectedRight, OpIntersection)
		require.NoError(t, err)
		return projector.Unproject(result)
	}

	got := run(a, b).(Polygon)
	requirePolygonCoordinatesNear(t, want, got.Rings, 1e-8)
	got = run(b, a).(Polygon)
	requirePolygonCoordinatesNear(t, want, got.Rings, 1e-8)
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
