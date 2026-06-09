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

func TestParseWKTValid(t *testing.T) {
	cases := []struct {
		name string
		wkt  string
		want Geometry
	}{
		{
			"point",
			"POINT(1 2)",
			Point{X: 1, Y: 2},
		},
		{
			"point negative/decimal",
			"POINT(-1.5 2.25)",
			Point{X: -1.5, Y: 2.25},
		},
		{
			"point scientific",
			"POINT(1e2 -3.5e-1)",
			Point{X: 100, Y: -0.35},
		},
		{
			"point lowercase + extra spaces",
			"  point (  3   4 ) ",
			Point{X: 3, Y: 4},
		},
		{
			"point empty",
			"POINT EMPTY",
			Point{IsEmpty: true},
		},
		{
			"linestring",
			"LINESTRING(0 0, 1 1, 2 3)",
			LineString{Points: []Coord{{0, 0}, {1, 1}, {2, 3}}},
		},
		{
			"linestring empty",
			"LINESTRING EMPTY",
			LineString{},
		},
		{
			"polygon no holes",
			"POLYGON((0 0, 4 0, 4 4, 0 4, 0 0))",
			Polygon{Rings: [][]Coord{{{0, 0}, {4, 0}, {4, 4}, {0, 4}, {0, 0}}}},
		},
		{
			"polygon with hole",
			"POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))",
			Polygon{Rings: [][]Coord{
				{{0, 0}, {10, 0}, {10, 10}, {0, 10}, {0, 0}},
				{{2, 2}, {2, 4}, {4, 4}, {4, 2}, {2, 2}},
			}},
		},
		{
			"multipoint bare",
			"MULTIPOINT(1 2, 3 4)",
			MultiPoint{Points: []Point{{X: 1, Y: 2}, {X: 3, Y: 4}}},
		},
		{
			"multipoint parenthesized",
			"MULTIPOINT((1 2),(3 4))",
			MultiPoint{Points: []Point{{X: 1, Y: 2}, {X: 3, Y: 4}}},
		},
		{
			"multilinestring",
			"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
			MultiLineString{Lines: []LineString{
				{Points: []Coord{{0, 0}, {1, 1}}},
				{Points: []Coord{{2, 2}, {3, 3}}},
			}},
		},
		{
			"multipolygon",
			"MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
			MultiPolygon{Polygons: []Polygon{
				{Rings: [][]Coord{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}},
				{Rings: [][]Coord{{{2, 2}, {3, 2}, {3, 3}, {2, 2}}}},
			}},
		},
		{
			"geometrycollection",
			"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
			GeometryCollection{Geometries: []Geometry{
				Point{X: 1, Y: 1},
				LineString{Points: []Coord{{0, 0}, {1, 1}}},
			}},
		},
		{
			"nested geometrycollection",
			"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)),POINT(1 1))",
			GeometryCollection{Geometries: []Geometry{
				GeometryCollection{Geometries: []Geometry{Point{X: 0, Y: 0}}},
				Point{X: 1, Y: 1},
			}},
		},
		{
			"geometrycollection empty",
			"GEOMETRYCOLLECTION EMPTY",
			GeometryCollection{},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, err := ParseWKT(c.wkt)
			require.NoError(t, err)
			require.Equal(t, c.want, g)
		})
	}
}

func TestParseWKTInvalid(t *testing.T) {
	cases := []struct {
		name string
		wkt  string
	}{
		{"empty string", ""},
		{"unknown type", "CIRCLE(0 0, 1)"},
		{"missing paren", "POINT 1 2"},
		{"unclosed paren", "POINT(1 2"},
		{"missing ordinate", "POINT(1)"},
		{"z ordinate rejected", "POINT(1 2 3)"},
		{"trailing junk", "POINT(1 2) garbage"},
		{"comma instead of space", "POINT(1,2)"},
		{"polygon missing inner parens", "POLYGON(0 0, 1 1, 1 0, 0 0)"},
		{"non numeric", "LINESTRING(a b, c d)"},
		{"dangling comma", "LINESTRING(0 0,)"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ParseWKT(c.wkt)
			require.Error(t, err)
		})
	}
}
