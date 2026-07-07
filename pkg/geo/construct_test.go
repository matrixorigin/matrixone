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

func wkt(t *testing.T, s string) Geometry {
	t.Helper()
	g, err := ParseWKT(s)
	require.NoError(t, err, s)
	return g
}

func TestConvexHull(t *testing.T) {
	// A square with an interior point -> the square (CCW exterior ring).
	g := wkt(t, "MULTIPOINT(0 0, 4 0, 4 4, 0 4, 2 2)")
	hull := ConvexHull(g)
	require.Equal(t, POLYGON, hull.Type())
	require.Equal(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))", WriteWKT(hull))

	// Collinear points collapse to a line.
	line := ConvexHull(wkt(t, "MULTIPOINT(0 0,1 1,2 2)"))
	require.Equal(t, LINESTRING, line.Type())
	require.Equal(t, "LINESTRING(0 0,2 2)", WriteWKT(line))

	// A single distinct point.
	pt := ConvexHull(wkt(t, "MULTIPOINT(5 5, 5 5)"))
	require.Equal(t, POINT, pt.Type())
	require.Equal(t, "POINT(5 5)", WriteWKT(pt))
}

func TestSimplify(t *testing.T) {
	// The middle point is ~0.0 off the straight line -> removed.
	g := wkt(t, "LINESTRING(0 0, 5 0.0001, 10 0)")
	s := Simplify(g, 0.001)
	require.Equal(t, "LINESTRING(0 0,10 0)", WriteWKT(s))

	// A real bend is preserved.
	g2 := wkt(t, "LINESTRING(0 0,5 5,10 0)")
	require.Equal(t, "LINESTRING(0 0,5 5,10 0)", WriteWKT(Simplify(g2, 0.001)))

	// Points are unchanged.
	require.Equal(t, "POINT(1 2)", WriteWKT(Simplify(wkt(t, "POINT(1 2)"), 1)))
}

func TestCollect(t *testing.T) {
	mp := Collect(wkt(t, "POINT(0 0)"), wkt(t, "POINT(1 1)"))
	require.Equal(t, "MULTIPOINT(0 0,1 1)", WriteWKT(mp))

	mixed := Collect(wkt(t, "POINT(0 0)"), wkt(t, "LINESTRING(0 0, 1 1)"))
	require.Equal(t, GEOMETRYCOLLECTION, mixed.Type())

	// Flatten a multipoint and a point into one multipoint.
	flat := Collect(wkt(t, "MULTIPOINT(0 0,1 1)"), wkt(t, "POINT(2 2)"))
	require.Equal(t, "MULTIPOINT(0 0,1 1,2 2)", WriteWKT(flat))
}
