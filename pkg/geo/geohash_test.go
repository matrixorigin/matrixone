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

func TestGeoHash(t *testing.T) {
	// Classic reference: "ezs42" denotes a cell near (lon -5.6, lat 42.6).
	lon, lat, err := DecodeGeoHash("ezs42")
	require.NoError(t, err)
	require.InDelta(t, -5.6, lon, 0.1)
	require.InDelta(t, 42.6, lat, 0.1)

	// Encode then decode round-trips at high precision.
	h := EncodeGeoHash(-5.603, 42.605, 12)
	require.Equal(t, "ezs42", h[:5])
	lon2, lat2, err := DecodeGeoHash(h)
	require.NoError(t, err)
	require.InDelta(t, -5.603, lon2, 1e-5)
	require.InDelta(t, 42.605, lat2, 1e-5)

	// Origin and length handling.
	require.Equal(t, "s0000000000", EncodeGeoHash(0, 0, 11))
	require.Len(t, EncodeGeoHash(10, 20, 8), 8)

	// Invalid characters (a, i, l, o are not in the alphabet) are rejected.
	_, _, err = DecodeGeoHash("ail")
	require.Error(t, err)
}
