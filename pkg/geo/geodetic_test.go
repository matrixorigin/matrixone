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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// oneDegreeMeters is the great-circle length of one degree of arc on the mean
// sphere, the reference value for the equator/meridian distance tests.
var oneDegreeMeters = (math.Pi / 180) * EarthRadiusMeters // ~111195 m

func TestDistanceMetersGreatCircle(t *testing.T) {
	// One degree of longitude along the equator.
	d, ok := DistanceMeters(mustParse(t, "POINT(0 0)"), mustParse(t, "POINT(1 0)"))
	require.True(t, ok)
	require.InDelta(t, oneDegreeMeters, d, 1.0)

	// One degree of latitude along a meridian.
	d, ok = DistanceMeters(mustParse(t, "POINT(0 0)"), mustParse(t, "POINT(0 1)"))
	require.True(t, ok)
	require.InDelta(t, oneDegreeMeters, d, 1.0)
}

func TestDistanceMetersKnownCities(t *testing.T) {
	// London (-0.1278, 51.5074) to Paris (2.3522, 48.8566). The great-circle
	// distance on the mean sphere is ~343.5 km; allow a few km of tolerance.
	london := "POINT(-0.1278 51.5074)"
	paris := "POINT(2.3522 48.8566)"
	d, ok := DistanceMeters(mustParse(t, london), mustParse(t, paris))
	require.True(t, ok)
	require.InDelta(t, 343556, d, 3000)
}

func TestDistanceMetersContainment(t *testing.T) {
	// A point inside a polygon is distance 0.
	d, ok := DistanceMeters(
		mustParse(t, "POINT(0.5 0.5)"),
		mustParse(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))"),
	)
	require.True(t, ok)
	require.Equal(t, 0.0, d)

	// Empty operand -> not ok.
	_, ok = DistanceMeters(mustParse(t, "POINT EMPTY"), mustParse(t, "POINT(0 0)"))
	require.False(t, ok)
}

func TestLengthMeters(t *testing.T) {
	// A one-degree equatorial segment.
	l := LengthMeters(mustParse(t, "LINESTRING(0 0,1 0)"))
	require.InDelta(t, oneDegreeMeters, l, 1.0)

	// Two one-degree segments.
	l = LengthMeters(mustParse(t, "LINESTRING(0 0,1 0,2 0)"))
	require.InDelta(t, 2*oneDegreeMeters, l, 2.0)

	require.Equal(t, 0.0, LengthMeters(mustParse(t, "POINT(0 0)")))
}

func TestAreaSquareMeters(t *testing.T) {
	// A 1deg x 1deg cell near the equator. Compare against the analytic spherical
	// cap-band approximation R^2 * dLon * (sin(lat2)-sin(lat1)); S2 uses geodesic
	// edges, so allow ~1% tolerance.
	got := AreaSquareMeters(mustParse(t, "POLYGON((0 0,1 0,1 1,0 1,0 0))"))
	dLon := math.Pi / 180
	band := math.Sin(math.Pi/180) - math.Sin(0)
	want := EarthRadiusMeters * EarthRadiusMeters * dLon * band
	require.InEpsilon(t, want, got, 0.01)

	require.Equal(t, 0.0, AreaSquareMeters(mustParse(t, "LINESTRING(0 0,1 1)")))
}

func TestValidateGeodeticCoordinates(t *testing.T) {
	for _, tc := range []struct {
		name string
		g    Geometry
		want string
	}{
		{name: "point", g: Point{X: 181, Y: 0}, want: "longitude 181"},
		{name: "line string", g: LineString{Points: []Coord{{X: 0, Y: 0}, {X: 0, Y: 91}}}, want: "latitude 91"},
		{name: "polygon hole", g: Polygon{Rings: [][]Coord{{{X: 0, Y: 0}, {X: 1, Y: 0}, {X: 1, Y: 1}, {X: 0, Y: 0}}, {{X: -181, Y: 0}}}}, want: "longitude -181"},
		{name: "later multipoint member", g: MultiPoint{Points: []Point{{X: 0, Y: 0}, {X: 0, Y: -91}}}, want: "latitude -91"},
		{name: "later multiline member", g: MultiLineString{Lines: []LineString{{Points: []Coord{{X: 0, Y: 0}}}, {Points: []Coord{{X: 181, Y: 0}}}}}, want: "longitude 181"},
		{name: "later multipolygon member", g: MultiPolygon{Polygons: []Polygon{{Rings: [][]Coord{{{X: 0, Y: 0}}}}, {Rings: [][]Coord{{{X: 0, Y: 91}}}}}}, want: "latitude 91"},
		{name: "nested geometry collection", g: GeometryCollection{Geometries: []Geometry{GeometryCollection{Geometries: []Geometry{Point{X: 0, Y: 90.1}}}}}, want: "latitude 90.1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateGeodeticCoordinates(tc.g)
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
			require.Contains(t, err.Error(), tc.want)
		})
	}

	for _, coord := range []Coord{
		{X: -180, Y: -90},
		{X: 180, Y: 90},
	} {
		require.NoError(t, ValidateGeodeticCoordinates(Point{X: coord.X, Y: coord.Y}))
	}

	for _, tc := range []struct {
		name  string
		coord Coord
		want  string
	}{
		{name: "longitude above maximum", coord: Coord{X: 180.0001}, want: "longitude 180.0001"},
		{name: "longitude below minimum", coord: Coord{X: -180.0001}, want: "longitude -180.0001"},
		{name: "latitude above maximum", coord: Coord{Y: 90.0001}, want: "latitude 90.0001"},
		{name: "latitude below minimum", coord: Coord{Y: -90.0001}, want: "latitude -90.0001"},
		{name: "NaN longitude", coord: Coord{X: math.NaN()}, want: "longitude must be finite"},
		{name: "positive infinity longitude", coord: Coord{X: math.Inf(1)}, want: "longitude must be finite"},
		{name: "negative infinity longitude", coord: Coord{X: math.Inf(-1)}, want: "longitude must be finite"},
		{name: "NaN latitude", coord: Coord{Y: math.NaN()}, want: "latitude must be finite"},
		{name: "positive infinity latitude", coord: Coord{Y: math.Inf(1)}, want: "latitude must be finite"},
		{name: "negative infinity latitude", coord: Coord{Y: math.Inf(-1)}, want: "latitude must be finite"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateGeodeticCoordinates(Point{X: tc.coord.X, Y: tc.coord.Y})
			require.Error(t, err)
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
			require.Contains(t, err.Error(), tc.want)
		})
	}

	t.Run("empty point has no coordinate to validate", func(t *testing.T) {
		g := GeometryCollection{Geometries: []Geometry{
			Point{X: math.NaN(), Y: math.Inf(1), IsEmpty: true},
			MultiPoint{Points: []Point{{X: math.Inf(-1), IsEmpty: true}}},
		}}
		require.NoError(t, ValidateGeodeticCoordinates(g))
	})
	t.Run("nil geometry", func(t *testing.T) {
		err := ValidateGeodeticCoordinates(nil)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
	})
	t.Run("unsupported geometry", func(t *testing.T) {
		err := ValidateGeodeticCoordinates(unsupportedGeometry{})
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
	})
	t.Run("excessive collection nesting", func(t *testing.T) {
		var g Geometry = Point{X: 0, Y: 0}
		for range maxGeometryNestingDepth + 1 {
			g = GeometryCollection{Geometries: []Geometry{g}}
		}
		err := ValidateGeodeticCoordinates(g)
		require.Error(t, err)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput), err)
		require.Contains(t, err.Error(), "geometry collection nesting depth")
	})
}

func TestGeodeticContainsPoint(t *testing.T) {
	p := mustParse(t, "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))").(Polygon)
	require.True(t, GeodeticContainsPoint(Coord{5, 5}, p))
	require.False(t, GeodeticContainsPoint(Coord{20, 5}, p)) // outside
	require.False(t, GeodeticContainsPoint(Coord{3, 3}, p))  // in hole
}
