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

func TestGeodeticContainsPoint(t *testing.T) {
	p := mustParse(t, "POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))").(Polygon)
	require.True(t, GeodeticContainsPoint(Coord{5, 5}, p))
	require.False(t, GeodeticContainsPoint(Coord{20, 5}, p)) // outside
	require.False(t, GeodeticContainsPoint(Coord{3, 3}, p))  // in hole
}
