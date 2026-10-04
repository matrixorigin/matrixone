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
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeoHash(t *testing.T) {
	// Classic reference: "ezs42" denotes a cell near (lon -5.6, lat 42.6).
	lon, lat, err := DecodeGeoHash("ezs42")
	require.NoError(t, err)
	require.InDelta(t, -5.6, lon, 0.1)
	require.InDelta(t, 42.6, lat, 0.1)

	// Encode then decode round-trips at high precision.
	h, err := EncodeGeoHash(-5.603, 42.605, 12)
	require.NoError(t, err)
	require.Equal(t, "ezs42", h[:5])
	lon2, lat2, err := DecodeGeoHash(h)
	require.NoError(t, err)
	require.InDelta(t, -5.603, lon2, 1e-5)
	require.InDelta(t, 42.605, lat2, 1e-5)

	// Origin and length handling.
	h, err = EncodeGeoHash(0, 0, 11)
	require.NoError(t, err)
	require.Equal(t, "s0000000000", h)
	h, err = EncodeGeoHash(10, 20, 8)
	require.NoError(t, err)
	require.Len(t, h, 8)

	// Invalid characters (a, i, l, o are not in the alphabet) are rejected.
	_, _, err = DecodeGeoHash("ail")
	require.Error(t, err)
}

func TestEncodeGeoHashRejectsInvalidInputs(t *testing.T) {
	valid := []struct {
		name       string
		lon, lat   float64
		length     int64
		wantLength int
	}{
		{name: "southwest endpoint", lon: -180, lat: -90, length: 1, wantLength: 1},
		{name: "northeast endpoint", lon: 180, lat: 90, length: 100, wantLength: 100},
		{name: "ordinary point", lon: 12.5, lat: -7.25, length: 12, wantLength: 12},
	}
	for _, tc := range valid {
		t.Run(tc.name, func(t *testing.T) {
			got, err := EncodeGeoHash(tc.lon, tc.lat, tc.length)
			require.NoError(t, err)
			require.Len(t, got, tc.wantLength)
		})
	}

	invalidCoordinates := []struct {
		name     string
		lon, lat float64
	}{
		{name: "longitude below range", lon: math.Nextafter(-180, math.Inf(-1)), lat: 0},
		{name: "longitude above range", lon: math.Nextafter(180, math.Inf(1)), lat: 0},
		{name: "latitude below range", lon: 0, lat: math.Nextafter(-90, math.Inf(-1))},
		{name: "latitude above range", lon: 0, lat: math.Nextafter(90, math.Inf(1))},
		{name: "nan longitude", lon: math.NaN(), lat: 0},
		{name: "positive infinity longitude", lon: math.Inf(1), lat: 0},
		{name: "negative infinity longitude", lon: math.Inf(-1), lat: 0},
		{name: "nan latitude", lon: 0, lat: math.NaN()},
		{name: "positive infinity latitude", lon: 0, lat: math.Inf(1)},
		{name: "negative infinity latitude", lon: 0, lat: math.Inf(-1)},
	}
	for _, tc := range invalidCoordinates {
		t.Run(tc.name, func(t *testing.T) {
			_, err := EncodeGeoHash(tc.lon, tc.lat, 12)
			require.Error(t, err)
		})
	}

	for _, length := range []int64{math.MinInt64, -1, 0, 101, 1000, math.MaxInt64} {
		t.Run("length/"+strconv.FormatInt(length, 10), func(t *testing.T) {
			_, err := EncodeGeoHash(0, 0, length)
			require.Error(t, err)
		})
	}
}

func TestDecodeGeoHashRejectsEmptyAndBoundsWork(t *testing.T) {
	_, _, err := DecodeGeoHash("")
	require.Error(t, err)

	wantLon, wantLat, err := DecodeGeoHash("ezs42")
	require.NoError(t, err)
	gotLon, gotLat, err := DecodeGeoHash("EZS42")
	require.NoError(t, err)
	require.Equal(t, wantLon, gotLon)
	require.Equal(t, wantLat, gotLat)

	validPrefix := strings.Repeat("s", maxGeoHashDecodeLength)
	_, _, err = DecodeGeoHash(validPrefix[:maxGeoHashDecodeLength-1] + "!")
	require.Error(t, err, "invalid characters inside the consumed prefix are rejected")

	_, _, err = DecodeGeoHash(validPrefix + "!")
	require.NoError(t, err, "suffix after the documented 433-character prefix is ignored")
	_, _, err = DecodeGeoHash(strings.Repeat("S", maxGeoHashDecodeLength) + "!invalid suffix")
	require.NoError(t, err, "decoding work and validation stop at the documented prefix")

	for _, hash := range []string{"a", "i", "l", "o", " ", "\x00", "é"} {
		t.Run("invalid/"+strconv.Quote(hash), func(t *testing.T) {
			_, _, err := DecodeGeoHash(hash)
			require.Error(t, err)
		})
	}
}
