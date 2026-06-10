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
	"testing"

	"github.com/stretchr/testify/require"
)

func overlayArea(t *testing.T, a, b Geometry, op BoolOp) (Geometry, float64) {
	t.Helper()
	g, err := Overlay(a, b, op)
	require.NoError(t, err)
	return g, CartesianArea(g)
}

func TestOverlayTwoOverlappingSquares(t *testing.T) {
	// A: [0,4]^2 (area 16). B: [2,6]x[2,6] (area 16). Overlap [2,4]^2 (area 4).
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((2 2, 6 2, 6 6, 2 6, 2 2))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 4.0, inter, 1e-9)

	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 28.0, union, 1e-9) // 16 + 16 - 4

	_, diff := overlayArea(t, a, b, OpDifference)
	require.InDelta(t, 12.0, diff, 1e-9) // 16 - 4

	_, xor := overlayArea(t, a, b, OpXOR)
	require.InDelta(t, 24.0, xor, 1e-9) // union - inter
}

func TestOverlayInvariants(t *testing.T) {
	a := wkt(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	b := wkt(t, "POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))")
	areaA, areaB := CartesianArea(a), CartesianArea(b)

	_, inter := overlayArea(t, a, b, OpIntersection)
	_, union := overlayArea(t, a, b, OpUnion)
	_, diffAB := overlayArea(t, a, b, OpDifference)
	_, diffBA := overlayArea(t, b, a, OpDifference)
	_, xor := overlayArea(t, a, b, OpXOR)

	// area(A) + area(B) == area(A∪B) + area(A∩B)
	require.InDelta(t, areaA+areaB, union+inter, 1e-9)
	// area(A\B) == area(A) - area(A∩B)
	require.InDelta(t, areaA-inter, diffAB, 1e-9)
	require.InDelta(t, areaB-inter, diffBA, 1e-9)
	// area(A xor B) == area(A∪B) - area(A∩B)
	require.InDelta(t, union-inter, xor, 1e-9)
}

func TestOverlayDisjoint(t *testing.T) {
	a := wkt(t, "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))")
	b := wkt(t, "POLYGON((5 5, 6 5, 6 6, 5 6, 5 5))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 0.0, inter, 1e-9)

	uni, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 2.0, union, 1e-9)
	require.Equal(t, MULTIPOLYGON, uni.Type())
}

func TestOverlayContained(t *testing.T) {
	// B fully inside A: union == A, intersection == B, difference is a ring.
	a := wkt(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	b := wkt(t, "POLYGON((3 3, 7 3, 7 7, 3 7, 3 3))")

	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 100.0, union, 1e-9)

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 16.0, inter, 1e-9)

	diff, dArea := overlayArea(t, a, b, OpDifference)
	require.InDelta(t, 84.0, dArea, 1e-9) // polygon with a hole
	require.Equal(t, POLYGON, diff.Type())
	require.Len(t, diff.(Polygon).Rings, 2) // shell + hole
}

func TestOverlaySharedEdge(t *testing.T) {
	// Two unit-tall squares sharing the x=4 edge -> union is one 8x4 rectangle.
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((4 0, 8 0, 8 4, 4 4, 4 0))")

	_, inter := overlayArea(t, a, b, OpIntersection)
	require.InDelta(t, 0.0, inter, 1e-9) // touching only along an edge

	uni, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 32.0, union, 1e-9)
	require.Equal(t, POLYGON, uni.Type()) // merged into a single polygon
}

func TestOverlayHalfOverlapInvariants(t *testing.T) {
	// Partial overlap with a shared partial edge.
	a := wkt(t, "POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))")
	b := wkt(t, "POLYGON((4 1, 8 1, 8 3, 4 3, 4 1))")
	areaA, areaB := CartesianArea(a), CartesianArea(b)
	_, inter := overlayArea(t, a, b, OpIntersection)
	_, union := overlayArea(t, a, b, OpUnion)
	require.InDelta(t, 0.0, inter, 1e-9)
	require.InDelta(t, areaA+areaB, union+inter, 1e-9)
}

func TestOverlayNonConvexFloat(t *testing.T) {
	// A non-convex "cross" polygon unioned with a float-coordinate disc must
	// keep the area invariant (snap-rounding makes near-coincident points
	// coincide exactly). Regression for the buffer union path.
	cross := wkt(t, "POLYGON((-1 0,0 0,0 -1,10 -1,10 0,11 0,11 10,10 10,10 11,0 11,0 10,-1 10,-1 0))")
	disc := Polygon(circlePolygon(Coord{X: 0, Y: 0}, 1, 16))

	areaCross, areaDisc := CartesianArea(cross), CartesianArea(disc)
	_, inter := overlayArea(t, cross, disc, OpIntersection)
	_, union := overlayArea(t, cross, disc, OpUnion)
	require.InDelta(t, areaCross+areaDisc, union+inter, 1e-6)
	require.Greater(t, union, areaCross) // the disc pokes outside the cross
}

func TestOverlayNonAreal(t *testing.T) {
	_, err := Overlay(wkt(t, "POINT(0 0)"), wkt(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))"), OpUnion)
	require.Error(t, err)
}
