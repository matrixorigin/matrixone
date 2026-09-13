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
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeoJSONRoundTrip(t *testing.T) {
	cases := []string{
		"POINT(1 2)",
		"POINT EMPTY",
		"LINESTRING(0 0, 1 1, 2 2)",
		"POLYGON((0 0, 4 0, 4 4, 0 4, 0 0), (1 1, 2 1, 2 2, 1 2, 1 1))",
		"MULTIPOINT(0 0, 1 1)",
		"MULTILINESTRING((0 0, 1 1), (2 2, 3 3))",
		"MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), ((2 2, 3 2, 3 3, 2 3, 2 2)))",
		"GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 1 1))",
	}
	for _, wkt := range cases {
		g, err := ParseWKT(wkt)
		require.NoError(t, err, wkt)
		gj := WriteGeoJSON(g, -1)
		back, err := ParseGeoJSON([]byte(gj))
		require.NoError(t, err, gj)
		require.Equal(t, WriteWKT(g), WriteWKT(back), "roundtrip %s via %s", wkt, gj)
	}
}

func TestGeoJSONWrite(t *testing.T) {
	g, err := ParseWKT("POINT(1.23456 2.34567)")
	require.NoError(t, err)
	require.Equal(t, `{"type":"Point","coordinates":[1.23456,2.34567]}`, WriteGeoJSON(g, -1))
	require.Equal(t, `{"type":"Point","coordinates":[1.23,2.35]}`, WriteGeoJSON(g, 2))
}

func TestGeoJSONWriteLargePrecisionIsValidJSON(t *testing.T) {
	cases := []struct {
		name   string
		point  Point
		maxDec int64
		wantX  float64
	}{
		{name: "reported boundary", point: Point{X: 1.23456789}, maxDec: 308, wantX: 1.23456789},
		{name: "scale multiplication overflow", point: Point{X: 2}, maxDec: 308, wantX: 2},
		{name: "negative scale multiplication overflow", point: Point{X: -2}, maxDec: 308, wantX: -2},
		{name: "first infinite power of ten", point: Point{X: 1.23456789}, maxDec: 309, wantX: 1.23456789},
		{name: "large precision", point: Point{X: 1.23456789}, maxDec: 1000, wantX: 1.23456789},
		{name: "max accepted SQL precision", point: Point{X: 1.23456789}, maxDec: 1<<32 - 1, wantX: 1.23456789},
		{name: "max int64 precision", point: Point{X: 1.23456789}, maxDec: int64(^uint64(0) >> 1), wantX: 1.23456789},
		{name: "max float precision zero", point: Point{X: math.MaxFloat64}, maxDec: 0, wantX: math.MaxFloat64},
		{name: "max float precision one", point: Point{X: math.MaxFloat64}, maxDec: 1, wantX: math.MaxFloat64},
		{name: "max float precision 308", point: Point{X: -math.MaxFloat64}, maxDec: 308, wantX: -math.MaxFloat64},
		{name: "smallest normal rounds at 309", point: Point{X: math.SmallestNonzeroFloat64 * float64(uint64(1)<<52)}, maxDec: 309, wantX: 2.2e-308},
		{name: "subnormal rounds to zero at 323", point: Point{X: math.SmallestNonzeroFloat64}, maxDec: 323, wantX: 0},
		{name: "subnormal is preserved at 324", point: Point{X: math.SmallestNonzeroFloat64}, maxDec: 324, wantX: math.SmallestNonzeroFloat64},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := WriteGeoJSONWithMaxDecimalDigits(tc.point, tc.maxDec)
			require.True(t, json.Valid([]byte(out)), "maxDec=%d: %s", tc.maxDec, out)
			var decoded struct {
				Coordinates []float64 `json:"coordinates"`
			}
			require.NoError(t, json.Unmarshal([]byte(out), &decoded), out)
			require.Len(t, decoded.Coordinates, 2, out)
			require.Equal(t, tc.wantX, decoded.Coordinates[0], out)
			require.Zero(t, decoded.Coordinates[1], out)
		})
	}
}

func TestGeoJSONWriteLargePrecisionInNestedGeometry(t *testing.T) {
	g := GeometryCollection{Geometries: []Geometry{
		Point{X: 2, Y: -2},
		LineString{Points: []Coord{{X: 1.23456789, Y: -1.2345}}},
	}}
	want := `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[2,-2]},{"type":"LineString","coordinates":[[1.23456789,-1.2345]]}]}`
	out := WriteGeoJSONWithMaxDecimalDigits(g, 308)
	require.True(t, json.Valid([]byte(out)), out)
	require.JSONEq(t, want, out)
}

func TestGeoJSONDecimalRoundingRetainsFastPath(t *testing.T) {
	// Keep the established float-scaling behavior for ordinary precisions while
	// the high-precision overflow path is hardened.
	require.Equal(t, "2.68", fmtGeoJSONNum(2.675, 2))
	require.Equal(t, "-2.68", fmtGeoJSONNum(-2.675, 2))
	require.Equal(t, "1", fmtGeoJSONNum(1.005, 2))
	require.Equal(t, "1200", fmtGeoJSONNum(1200, 1000))
	require.Equal(t, "1.23", fmtGeoJSONNum(1.23, 1000))

	g := Point{X: 1.23456789, Y: -2.3456789}
	require.Equal(t, WriteGeoJSON(g, -1), WriteGeoJSONWithMaxDecimalDigits(g, -1))
}

func TestGeoJSONParseErrors(t *testing.T) {
	bad := []string{
		`{"type":"Point"}`,                  // missing coordinates
		`{"type":"Bogus","coordinates":[]}`, // unknown type
		`not json`,
		`{"coordinates":[1,2]}`, // missing type
	}
	for _, s := range bad {
		_, err := ParseGeoJSON([]byte(s))
		require.Error(t, err, s)
	}
}
