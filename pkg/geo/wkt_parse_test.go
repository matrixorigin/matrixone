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
	"strings"
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
			_, err = ParseWKTForSerialization(c.wkt)
			require.Error(t, err)
		})
	}
}

func TestPolygonRingAdmissionStructure(t *testing.T) {
	for _, tc := range []struct{ wkt, json string }{
		{"POLYGON((0 0,1 0,1 1))", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1]]]}`},
		{"POLYGON((0 0,1 0,0 0))", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[0,0]]]}`},
		{"POLYGON((0 0,1 0,1 1,0 0.0000000001))", `{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1],[0,0.0000000001]]]}`},
		{"MULTIPOLYGON(((0 0,1 0,1 1)))", `{"type":"MultiPolygon","coordinates":[[[[0,0],[1,0],[1,1]]]]}`},
		{"GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1)))", `{"type":"GeometryCollection","geometries":[{"type":"Polygon","coordinates":[[[0,0],[1,0],[1,1]]]}]}`},
	} {
		_, err := ParseWKT(tc.wkt)
		require.Error(t, err, tc.wkt)
		_, err = ParseGeoJSON([]byte(tc.json))
		require.Error(t, err, tc.json)
	}
	for _, text := range []string{"POLYGON EMPTY", "MULTIPOLYGON(EMPTY,((0 0,1 0,0 1,0 0)))", "POLYGON((0 0,1 1,1 0,0 1,0 0))"} {
		_, err := ParseWKT(text)
		require.NoError(t, err, text)
	}
	_, err := ParseGeoJSON([]byte(`{"type":"Polygon","coordinates":[[]]}`))
	require.Error(t, err)
	_, err = ParseGeoJSON([]byte(`{"type":"Polygon","coordinates":[]}`))
	require.NoError(t, err)
	// Codecs still expose old malformed WKB for inspection/export. Admission is
	// the constructor's responsibility, not a new stored-data reader policy.
	historical := Polygon{Rings: [][]Coord{{{0, 0}, {1, 0}, {1, 1}}}}
	g, err := ReadWKB(WriteWKB(historical))
	require.NoError(t, err)
	require.Equal(t, historical, g)
}

func TestParseWKTForSerializationPreservesHistoricalRings(t *testing.T) {
	three := []Coord{{0, 0}, {1, 0}, {1, 1}}
	four := []Coord{{0, 0}, {1, 0}, {1, 1}, {0, 1}}
	polygon := Polygon{Rings: [][]Coord{four}}
	for _, tc := range []struct {
		text string
		want Geometry
	}{
		{"POLYGON((0 0,1 0,1 1))", Polygon{Rings: [][]Coord{three}}},
		{"POLYGON((0 0,1 0,1 1,0 1))", polygon},
		{"POLYGON((0 0,4 0,4 4,0 0),(0 0,1 0,1 1))", Polygon{Rings: [][]Coord{{{0, 0}, {4, 0}, {4, 4}, {0, 0}}, three}}},
		{"MULTIPOLYGON(EMPTY,((0 0,1 0,1 1,0 1)))", MultiPolygon{Polygons: []Polygon{{}, polygon}}},
		{"GEOMETRYCOLLECTION(POINT EMPTY,GEOMETRYCOLLECTION(POLYGON((0 0,1 0,1 1,0 1))))", GeometryCollection{Geometries: []Geometry{Point{IsEmpty: true}, GeometryCollection{Geometries: []Geometry{polygon}}}}},
	} {
		t.Run(tc.text, func(t *testing.T) {
			_, err := ParseWKT(tc.text)
			require.Error(t, err)
			g, err := ParseWKTForSerialization(tc.text)
			require.NoError(t, err)
			require.Equal(t, tc.want, g)
		})
	}
	for _, text := range []string{"POLYGON EMPTY", "MULTIPOLYGON(EMPTY)", "POLYGON((0 0,1 1,1 0,0 1,0 0))"} {
		strict, err := ParseWKT(text)
		require.NoError(t, err)
		stored, err := ParseWKTForSerialization(text)
		require.NoError(t, err)
		require.Equal(t, strict, stored)
	}
	deeplyNested := strings.Repeat("GEOMETRYCOLLECTION(", maxGeometryNestingDepth+1) + "POINT EMPTY" + strings.Repeat(")", maxGeometryNestingDepth+1)
	_, err := ParseWKTForSerialization(deeplyNested)
	require.ErrorContains(t, err, "nesting depth")
}
