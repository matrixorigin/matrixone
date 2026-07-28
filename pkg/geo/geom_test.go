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

func TestGeometryType(t *testing.T) {
	cases := []struct {
		name string
		g    Geometry
		want Subtype
	}{
		{"point", Point{X: 1, Y: 2}, POINT},
		{"linestring", LineString{Points: []Coord{{0, 0}, {1, 1}}}, LINESTRING},
		{"polygon", Polygon{Rings: [][]Coord{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}}, POLYGON},
		{"multipoint", MultiPoint{Points: []Point{{X: 1, Y: 2}}}, MULTIPOINT},
		{"multilinestring", MultiLineString{Lines: []LineString{{Points: []Coord{{0, 0}, {1, 1}}}}}, MULTILINESTRING},
		{"multipolygon", MultiPolygon{Polygons: []Polygon{{Rings: [][]Coord{{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}}}}, MULTIPOLYGON},
		{"collection", GeometryCollection{Geometries: []Geometry{Point{X: 1, Y: 2}}}, GEOMETRYCOLLECTION},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			require.Equal(t, c.want, c.g.Type())
			require.False(t, c.g.Empty())
		})
	}
}

func TestGeometryEmpty(t *testing.T) {
	empties := []Geometry{
		Point{IsEmpty: true},
		LineString{},
		Polygon{},
		MultiPoint{},
		MultiLineString{},
		MultiPolygon{},
		GeometryCollection{},
	}
	for _, g := range empties {
		require.True(t, g.Empty(), "%s should be empty", g.Type())
	}

	// A point with coordinates is not empty.
	require.False(t, Point{X: 0, Y: 0}.Empty())
}

func TestLineStringClosed(t *testing.T) {
	require.True(t, LineString{Points: []Coord{{0, 0}, {1, 0}, {1, 1}, {0, 0}}}.Closed())
	require.False(t, LineString{Points: []Coord{{0, 0}, {1, 0}, {1, 1}}}.Closed())
	require.False(t, LineString{}.Closed())
}

func TestSubtypeString(t *testing.T) {
	require.Equal(t, "GEOMETRY", GENERIC.String())
	require.Equal(t, "POINT", POINT.String())
	require.Equal(t, "GEOMETRYCOLLECTION", GEOMETRYCOLLECTION.String())
}

func TestSubtypeValid(t *testing.T) {
	require.True(t, POINT.Valid())
	require.True(t, GEOMETRYCOLLECTION.Valid())
	require.False(t, Subtype(8).Valid())
}

func TestSupportedSRID(t *testing.T) {
	require.True(t, SupportedSRID(SRIDPlanar))
	require.True(t, SupportedSRID(SRIDWGS84))
	require.False(t, SupportedSRID(3857))
}
