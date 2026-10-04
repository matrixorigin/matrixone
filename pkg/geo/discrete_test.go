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

	"github.com/stretchr/testify/require"
)

func TestHausdorffDistance(t *testing.T) {
	// Two parallel segments 1 unit apart.
	a := wkt(t, "LINESTRING(0 0, 10 0)")
	b := wkt(t, "LINESTRING(0 1, 10 1)")
	d, ok := HausdorffDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, 1.0, d, 1e-9)

	// Identical geometries -> 0.
	d2, ok := HausdorffDistance(a, a)
	require.True(t, ok)
	require.InDelta(t, 0.0, d2, 1e-9)
}

func TestHausdorffDistanceDirection(t *testing.T) {
	for _, tc := range []struct {
		name   string
		a, b   string
		ab, ba float64
	}{
		{
			name: "asymmetric lines",
			a:    "LINESTRING(0 0,1 4,4 4)",
			b:    "LINESTRING(0 0,4 0,4 4)",
			ab:   3,
			ba:   4,
		},
		{
			name: "MySQL manual example",
			a:    "LINESTRING(0 0,0 5,5 5)",
			b:    "LINESTRING(0 1,0 6,3 3,5 6)",
			ab:   1,
			ba:   math.Sqrt(8),
		},
		{
			name: "point and multipoint",
			a:    "POINT(0 0)",
			b:    "MULTIPOINT(0 0,3 4)",
			ab:   0,
			ba:   5,
		},
		{
			name: "linestring and multiline string",
			a:    "LINESTRING(0 0,10 0)",
			b:    "MULTILINESTRING((0 1,10 1),(0 0,10 0))",
			ab:   0,
			ba:   1,
		},
		{
			name: "multipoint and multipoint",
			a:    "MULTIPOINT(0 0,5 0)",
			b:    "MULTIPOINT(0 0)",
			ab:   5,
			ba:   0,
		},
		{
			name: "multiline components all participate",
			a:    "MULTILINESTRING((0 0,0 1),(10 0,10 1))",
			b:    "MULTILINESTRING((0 0,0 1),(10 0,10 1),(11 0,11 1))",
			ab:   0,
			ba:   1,
		},
		{
			name: "discrete vertices do not become segment distances",
			a:    "LINESTRING(0 0,1 1,2 0)",
			b:    "LINESTRING(0 0,2 0)",
			ab:   math.Sqrt2,
			ba:   0,
		},
		{
			name: "duplicate vertices do not change the directed result",
			a:    "LINESTRING(0 0,0 0,2 0)",
			b:    "LINESTRING(2 0,0 0)",
			ab:   0,
			ba:   0,
		},
		{
			name: "previously accepted polygon pair remains accepted",
			a:    "POLYGON((0 0,2 0,2 2,0 2,0 0))",
			b:    "LINESTRING(0 0,0 1)",
			ab:   math.Sqrt(5),
			ba:   1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := wkt(t, tc.a), wkt(t, tc.b)
			forward, ok := DirectedHausdorffDistance(a, b)
			require.True(t, ok)
			require.InDelta(t, tc.ab, forward, 1e-9)

			reverse, ok := DirectedHausdorffDistance(b, a)
			require.True(t, ok)
			require.InDelta(t, tc.ba, reverse, 1e-9)

			symmetricForward, ok := HausdorffDistance(a, b)
			require.True(t, ok)
			symmetricReverse, ok := HausdorffDistance(b, a)
			require.True(t, ok)
			wantSymmetric := math.Max(tc.ab, tc.ba)
			require.InDelta(t, wantSymmetric, symmetricForward, 1e-9)
			require.InDelta(t, wantSymmetric, symmetricReverse, 1e-9)
		})
	}
}

func TestDirectedHausdorffDistanceEmpty(t *testing.T) {
	empty := wkt(t, "LINESTRING EMPTY")
	line := wkt(t, "LINESTRING(0 0,1 0)")

	for _, pair := range []struct {
		name string
		a, b Geometry
	}{
		{name: "empty source", a: empty, b: line},
		{name: "empty target", a: line, b: empty},
	} {
		t.Run(pair.name, func(t *testing.T) {
			_, ok := DirectedHausdorffDistance(pair.a, pair.b)
			require.False(t, ok)
		})
	}
}

func TestFrechetDistance(t *testing.T) {
	a := wkt(t, "LINESTRING(0 0, 10 0)")
	b := wkt(t, "LINESTRING(0 1, 10 1)")
	d, ok := FrechetDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, 1.0, d, 1e-9)

	// A perpendicular offset at one end raises the Fréchet distance.
	c := wkt(t, "LINESTRING(0 0, 10 0)")
	e := wkt(t, "LINESTRING(0 0, 10 5)")
	d2, ok := FrechetDistance(c, e)
	require.True(t, ok)
	require.InDelta(t, 5.0, d2, 1e-9)
}

func TestFrechetDistanceMatchesFullMatrixOracle(t *testing.T) {
	for _, tc := range []struct {
		name string
		a, b string
	}{
		{name: "non-square sequences", a: "LINESTRING(0 0,2 1,4 0)", b: "LINESTRING(0 1,4 1)"},
		{name: "point and line", a: "POINT(1 2)", b: "LINESTRING(0 0,1 2,3 4,5 6)"},
		{name: "line and point", a: "LINESTRING(0 0,1 2,3 4,5 6)", b: "POINT(1 2)"},
		{name: "repeated vertices", a: "LINESTRING(0 0,0 0,2 0,2 0)", b: "LINESTRING(0 0,1 0,2 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := wkt(t, tc.a), wkt(t, tc.b)
			got, ok := FrechetDistance(a, b)
			require.True(t, ok)
			want := fullMatrixFrechetOracle(coordsOf(a), coordsOf(b))
			require.InDelta(t, want, got, 1e-12)
		})
	}
}

// fullMatrixFrechetOracle deliberately keeps the reference recurrence
// independent from frechetDistance's rolling-row storage. The inputs stay
// small so this is a correctness oracle, not a production implementation.
func fullMatrixFrechetOracle(a, b []Coord) float64 {
	ca := make([][]float64, len(a))
	for i := range ca {
		ca[i] = make([]float64, len(b))
		for j := range ca[i] {
			d := math.Hypot(a[i].X-b[j].X, a[i].Y-b[j].Y)
			switch {
			case i == 0 && j == 0:
				ca[i][j] = d
			case i == 0:
				ca[i][j] = math.Max(ca[i][j-1], d)
			case j == 0:
				ca[i][j] = math.Max(ca[i-1][j], d)
			default:
				previous := math.Min(ca[i-1][j], math.Min(ca[i-1][j-1], ca[i][j-1]))
				ca[i][j] = math.Max(previous, d)
			}
		}
	}
	return ca[len(a)-1][len(b)-1]
}

func TestGeodeticDiscreteDistances(t *testing.T) {
	a := wkt(t, "LINESTRING(0 0, 1 0)")
	b := wkt(t, "LINESTRING(0 1, 1 1)")

	d, ok := GeodeticFrechetDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, oneDegreeMeters, d, 1e-6)

	d, ok = GeodeticDirectedHausdorffDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, oneDegreeMeters, d, 1e-6)

	_, ok = GeodeticFrechetDistance(wkt(t, "LINESTRING EMPTY"), b)
	require.False(t, ok)
	_, ok = GeodeticDirectedHausdorffDistance(wkt(t, "LINESTRING EMPTY"), b)
	require.False(t, ok)
}

func TestGeodeticDiscreteDistanceAnalyticBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		a, b                   string
		lon1, lat1, lon2, lat2 float64
	}{
		{name: "antimeridian", a: "POINT(179 0)", b: "POINT(-179 0)", lon1: 179, lat1: 0, lon2: -179, lat2: 0},
		{name: "high latitude", a: "POINT(0 80)", b: "POINT(90 80)", lon1: 0, lat1: 80, lon2: 90, lat2: 80},
		{name: "north pole", a: "POINT(0 90)", b: "POINT(180 90)", lon1: 0, lat1: 90, lon2: 180, lat2: 90},
		{name: "antipodal", a: "POINT(0 0)", b: "POINT(180 0)", lon1: 0, lat1: 0, lon2: 180, lat2: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b := wkt(t, tc.a), wkt(t, tc.b)
			want := analyticSphericalDistance(tc.lon1, tc.lat1, tc.lon2, tc.lat2)

			frechet, ok := GeodeticFrechetDistance(a, b)
			require.True(t, ok)
			require.InDelta(t, want, frechet, 1e-5)

			hausdorff, ok := GeodeticDirectedHausdorffDistance(a, b)
			require.True(t, ok)
			require.InDelta(t, want, hausdorff, 1e-5)
		})
	}
}

func analyticSphericalDistance(lon1, lat1, lon2, lat2 float64) float64 {
	toRadians := math.Pi / 180
	lat1 *= toRadians
	lat2 *= toRadians
	deltaLon := (lon2 - lon1) * toRadians
	cosAngle := math.Sin(lat1)*math.Sin(lat2) +
		math.Cos(lat1)*math.Cos(lat2)*math.Cos(deltaLon)
	cosAngle = math.Max(-1, math.Min(1, cosAngle))
	return math.Acos(cosAngle) * EarthRadiusMeters
}
