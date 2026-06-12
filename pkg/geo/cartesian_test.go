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

const delta = 1e-9

func TestCartesianArea(t *testing.T) {
	cases := []struct {
		wkt  string
		want float64
	}{
		{"POLYGON((0 0,4 0,4 4,0 4,0 0))", 16},
		{"POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))", 96},
		{"MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((10 10,13 10,13 10,10 13,10 10)))", 4 + 4.5},
		{"POINT(1 2)", 0},
		{"LINESTRING(0 0,1 1)", 0},
		{"GEOMETRYCOLLECTION(POLYGON((0 0,2 0,2 2,0 2,0 0)),POINT(9 9))", 4},
	}
	for _, c := range cases {
		require.InDelta(t, c.want, CartesianArea(mustParse(t, c.wkt)), delta, c.wkt)
	}
}

func TestCartesianLength(t *testing.T) {
	cases := []struct {
		wkt  string
		want float64
	}{
		{"LINESTRING(0 0,3 4)", 5},
		{"LINESTRING(0 0,3 0,3 4)", 7},
		{"MULTILINESTRING((0 0,0 1),(0 0,1 0))", 2},
		{"POINT(1 1)", 0},
		{"POLYGON((0 0,1 0,1 1,0 1,0 0))", 0},
	}
	for _, c := range cases {
		require.InDelta(t, c.want, CartesianLength(mustParse(t, c.wkt)), delta, c.wkt)
	}
}

func TestCartesianPerimeter(t *testing.T) {
	require.InDelta(t, 16.0, CartesianPerimeter(mustParse(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")), delta)
	require.InDelta(t, 0.0, CartesianPerimeter(mustParse(t, "LINESTRING(0 0,1 1)")), delta)
}

func TestEnvelope(t *testing.T) {
	bb, ok := Envelope(mustParse(t, "LINESTRING(1 2,3 4,0 5)"))
	require.True(t, ok)
	require.Equal(t, BBox{MinX: 0, MinY: 2, MaxX: 3, MaxY: 5}, bb)

	_, ok = Envelope(mustParse(t, "LINESTRING EMPTY"))
	require.False(t, ok)
}

func TestCartesianDistance(t *testing.T) {
	cases := []struct {
		a, b string
		want float64
	}{
		{"POINT(0 0)", "POINT(3 4)", 5},
		{"POINT(0 0)", "LINESTRING(1 0,1 1)", 1},
		{"LINESTRING(0 0,0 1)", "LINESTRING(2 0,2 1)", 2},
		{"POINT(5 5)", "POLYGON((0 0,10 0,10 10,0 10,0 0))", 0}, // inside
		{"POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((5 0,7 0,7 2,5 2,5 0))", 3},
		{"LINESTRING(0 0,2 2)", "LINESTRING(0 2,2 0)", 0}, // crossing
	}
	for _, c := range cases {
		d, ok := CartesianDistance(mustParse(t, c.a), mustParse(t, c.b))
		require.True(t, ok)
		require.InDelta(t, c.want, d, delta, "%s <-> %s", c.a, c.b)
	}

	_, ok := CartesianDistance(mustParse(t, "POINT EMPTY"), mustParse(t, "POINT(0 0)"))
	require.False(t, ok)
}

func TestPointInPolygon(t *testing.T) {
	p := mustParse(t, "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))").(Polygon)
	require.Equal(t, 1, PointInPolygon(Coord{5, 5}, p))   // interior
	require.Equal(t, 0, PointInPolygon(Coord{0, 5}, p))   // on boundary
	require.Equal(t, -1, PointInPolygon(Coord{20, 5}, p)) // exterior
	require.Equal(t, -1, PointInPolygon(Coord{3, 3}, p))  // inside the hole
}

func TestSegmentsIntersect(t *testing.T) {
	require.True(t, SegmentsIntersect(Coord{0, 0}, Coord{2, 2}, Coord{0, 2}, Coord{2, 0}))  // cross
	require.True(t, SegmentsIntersect(Coord{0, 0}, Coord{1, 1}, Coord{1, 1}, Coord{2, 0}))  // touch endpoint
	require.True(t, SegmentsIntersect(Coord{0, 0}, Coord{4, 0}, Coord{2, 0}, Coord{6, 0}))  // collinear overlap
	require.False(t, SegmentsIntersect(Coord{0, 0}, Coord{1, 0}, Coord{0, 1}, Coord{1, 1})) // parallel
	require.False(t, SegmentsIntersect(Coord{0, 0}, Coord{1, 0}, Coord{2, 0}, Coord{3, 0})) // collinear disjoint
}

func TestCentroid(t *testing.T) {
	cases := []struct {
		wkt    string
		wx, wy float64
	}{
		{"POLYGON((0 0,2 0,2 2,0 2,0 0))", 1, 1},
		{"POINT(3 4)", 3, 4},
		{"MULTIPOINT(0 0,2 0,2 2,0 2)", 1, 1},
		{"LINESTRING(0 0,2 0)", 1, 0},
		// Areal dominates the point in a mixed collection.
		{"GEOMETRYCOLLECTION(POLYGON((0 0,2 0,2 2,0 2,0 0)),POINT(100 100))", 1, 1},
	}
	for _, c := range cases {
		got, ok := Centroid(mustParse(t, c.wkt))
		require.True(t, ok, c.wkt)
		require.InDelta(t, c.wx, got.X, delta, c.wkt)
		require.InDelta(t, c.wy, got.Y, delta, c.wkt)
	}

	_, ok := Centroid(mustParse(t, "POINT EMPTY"))
	require.False(t, ok)
}

func TestLineIsSimple(t *testing.T) {
	require.True(t, LineIsSimple([]Coord{{0, 0}, {1, 1}, {2, 2}}))
	require.True(t, LineIsSimple([]Coord{{0, 0}, {1, 0}, {1, 1}, {0, 1}, {0, 0}})) // closed ring
	require.False(t, LineIsSimple([]Coord{{0, 0}, {2, 2}, {2, 0}, {0, 2}}))        // self-crossing
	require.False(t, LineIsSimple([]Coord{{0, 0}, {2, 0}, {1, 0}, {3, 0}}))        // collinear backtrack
}
