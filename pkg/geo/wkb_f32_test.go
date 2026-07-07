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

// For coordinates that are exactly representable in float32, the float32 WKB
// round-trip is lossless.
func TestWKBFloat32RoundTripExact(t *testing.T) {
	inputs := []string{
		"POINT(1 2)",
		"POINT(-1.5 2.25)",
		"POINT EMPTY",
		"LINESTRING(0 0,1 1,2 3)",
		"POLYGON((0 0,4 0,4 4,0 4,0 0))",
		"MULTIPOINT(1 1,2 2)",
		"MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
		"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			g1 := mustParse(t, in)
			b := WriteWKBFloat32(g1)
			g2, err := ReadWKBFloat32(b)
			require.NoError(t, err)
			require.Equal(t, g1, g2)
		})
	}
}

// A float32 ordinate is 4 bytes; a 2D point payload is 1 (order) + 4 (type) +
// 2*4 (coords) = 13 bytes, versus 21 for float64.
func TestWKBFloat32Size(t *testing.T) {
	g := mustParse(t, "POINT(1 2)")
	require.Len(t, WriteWKBFloat32(g), 13)
	require.Len(t, WriteWKB(g), 21)
}

// A coordinate not exactly representable in float32 loses precision, and the
// round-trip value equals the float32-rounded value (not the original float64).
func TestWKBFloat32PrecisionLoss(t *testing.T) {
	g1 := Point{X: 0.1, Y: 0.2}
	b := WriteWKBFloat32(g1)
	g2, err := ReadWKBFloat32(b)
	require.NoError(t, err)

	p := g2.(Point)
	require.Equal(t, float64(float32(0.1)), p.X)
	require.Equal(t, float64(float32(0.2)), p.Y)
	require.NotEqual(t, 0.1, p.X)
}

// WKBFloat32ToStandard yields standard float64 WKB equal to encoding the
// float32-rounded geometry directly.
func TestWKBFloat32ToStandard(t *testing.T) {
	g1 := Point{X: 0.1, Y: 0.2}
	std, err := WKBFloat32ToStandard(WriteWKBFloat32(g1))
	require.NoError(t, err)

	rounded := Point{X: float64(float32(0.1)), Y: float64(float32(0.2))}
	require.Equal(t, WriteWKB(rounded), std)

	// And the result is valid standard WKB.
	back, err := ReadWKB(std)
	require.NoError(t, err)
	require.Equal(t, rounded, back)
}
