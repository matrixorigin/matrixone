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
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func nestedGCWKT(depth int) string {
	return strings.Repeat("GEOMETRYCOLLECTION(", depth) + "POINT(0 0)" + strings.Repeat(")", depth)
}

// nestedGCWKB builds `depth` nested little-endian GEOMETRYCOLLECTIONs around a
// single point. Each collection level is 9 bytes (byte order + type + count=1).
func nestedGCWKB(depth int) []byte {
	var b []byte
	for i := 0; i < depth; i++ {
		level := make([]byte, 9)
		level[0] = wkbNDR
		binary.LittleEndian.PutUint32(level[1:], wkbGeometryCollection)
		binary.LittleEndian.PutUint32(level[5:], 1)
		b = append(b, level...)
	}
	pt := make([]byte, 21)
	pt[0] = wkbNDR
	binary.LittleEndian.PutUint32(pt[1:], wkbPoint)
	b = append(b, pt...)
	return b
}

// TestParseWKTNestingDepthBounded ensures deeply nested GEOMETRYCOLLECTION WKT
// is rejected with an error rather than recursing to a fatal stack overflow.
func TestParseWKTNestingDepthBounded(t *testing.T) {
	_, err := ParseWKT(nestedGCWKT(maxGeometryNestingDepth))
	require.NoError(t, err, "nesting at the limit must parse")

	_, err = ParseWKT(nestedGCWKT(maxGeometryNestingDepth + 1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "geometry collection nesting depth exceeds")

	// Pathological depth must still return an error, not crash the process.
	_, err = ParseWKT(nestedGCWKT(200000))
	require.Error(t, err)
}

// TestReadWKBNestingDepthBounded mirrors the WKT case for the binary reader.
func TestReadWKBNestingDepthBounded(t *testing.T) {
	_, err := ReadWKB(nestedGCWKB(maxGeometryNestingDepth))
	require.NoError(t, err, "nesting at the limit must decode")

	_, err = ReadWKB(nestedGCWKB(maxGeometryNestingDepth + 1))
	require.Error(t, err)
	require.Contains(t, err.Error(), "geometry collection nesting depth exceeds")

	_, err = ReadWKB(nestedGCWKB(200000))
	require.Error(t, err)
}

// TestSimplifyDoesNotMutateInput guards against douglasPeucker overwriting the
// caller's coordinate backing array.
func TestSimplifyDoesNotMutateInput(t *testing.T) {
	ring := []Coord{
		{X: 0, Y: 0}, {X: 1, Y: 0.1}, {X: 2, Y: -0.1}, {X: 3, Y: 0.05},
		{X: 4, Y: 0}, {X: 4, Y: 4}, {X: 0, Y: 4}, {X: 0, Y: 0},
	}
	orig := append([]Coord(nil), ring...)
	_ = Simplify(Polygon{Rings: [][]Coord{ring}}, 0.5)
	require.Equal(t, orig, ring, "Simplify must not mutate the input coordinates")
}
