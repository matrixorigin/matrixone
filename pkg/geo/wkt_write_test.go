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

func TestWriteWKTGolden(t *testing.T) {
	cases := []struct {
		name string
		g    Geometry
		want string
	}{
		{"point", Point{X: 1, Y: 2}, "POINT(1 2)"},
		{"point decimal", Point{X: -1.5, Y: 2.25}, "POINT(-1.5 2.25)"},
		{"point empty", Point{IsEmpty: true}, "POINT EMPTY"},
		{"linestring", LineString{Points: []Coord{{0, 0}, {1, 1}, {2, 3}}}, "LINESTRING(0 0,1 1,2 3)"},
		{"linestring empty", LineString{}, "LINESTRING EMPTY"},
		{
			"polygon with hole",
			Polygon{Rings: [][]Coord{
				{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}},
				{{2, 2}, {2, 4}, {4, 4}, {4, 2}, {2, 2}},
			}},
			"POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))",
		},
		{
			"multipoint bare form",
			MultiPoint{Points: []Point{{X: 1, Y: 1}, {X: 2, Y: 2}}},
			"MULTIPOINT(1 1,2 2)",
		},
		{
			"multilinestring",
			MultiLineString{Lines: []LineString{
				{Points: []Coord{{0, 0}, {1, 1}}},
				{Points: []Coord{{2, 2}, {3, 3}}},
			}},
			"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		},
		{
			"multipolygon",
			MultiPolygon{Polygons: []Polygon{
				{Rings: [][]Coord{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}},
				{Rings: [][]Coord{{{2, 2}, {3, 2}, {3, 3}, {2, 2}}}},
			}},
			"MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
		},
		{
			"geometrycollection",
			GeometryCollection{Geometries: []Geometry{
				Point{X: 1, Y: 1},
				LineString{Points: []Coord{{0, 0}, {1, 1}}},
			}},
			"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
		},
		{"geometrycollection empty", GeometryCollection{}, "GEOMETRYCOLLECTION EMPTY"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, WriteWKT(c.g))
		})
	}
}

// TestWKTRoundTrip parses each WKT string, writes it back, and re-parses,
// asserting the geometry is stable across Parse -> Write -> Parse.
func TestWKTRoundTrip(t *testing.T) {
	inputs := []string{
		"POINT(1 2)",
		"POINT(-1.5 2.25)",
		"POINT EMPTY",
		"LINESTRING(0 0,1 1,2 3)",
		"LINESTRING EMPTY",
		"POLYGON((0 0,4 0,4 4,0 4,0 0))",
		"POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))",
		"MULTIPOINT(1 1,2 2)",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
		"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)),POINT(1 1))",
		"GEOMETRYCOLLECTION EMPTY",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			g1, err := ParseWKT(in)
			require.NoError(t, err)
			out := WriteWKT(g1)
			g2, err := ParseWKT(out)
			require.NoError(t, err)
			require.Equal(t, g1, g2, "round-trip changed geometry; wrote %q", out)
		})
	}
}
