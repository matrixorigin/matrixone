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
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Golden WKB hex strings produced by PostGIS ST_AsBinary / MySQL ST_AsWKB for
// the same geometries. These pin byte-level interoperability.
func TestWriteWKBGolden(t *testing.T) {
	cases := []struct {
		name string
		wkt  string
		hex  string
	}{
		{
			"point",
			"POINT(1 2)",
			"0101000000000000000000f03f0000000000000040",
		},
		{
			"linestring",
			"LINESTRING(0 0,1 1)",
			"0102000000020000000000000000000000000000000000000" +
				"0000000000000f03f000000000000f03f",
		},
		{
			"polygon unit square",
			"POLYGON((0 0,1 0,1 1,0 1,0 0))",
			"01030000000100000005000000" +
				"00000000000000000000000000000000" + // 0 0
				"000000000000f03f0000000000000000" + // 1 0
				"000000000000f03f000000000000f03f" + // 1 1
				"0000000000000000000000000000f03f" + // 0 1
				"00000000000000000000000000000000", // 0 0
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			g, err := ParseWKT(c.wkt)
			require.NoError(t, err)
			got := hex.EncodeToString(WriteWKB(g))
			require.Equal(t, c.hex, got)
		})
	}
}

func TestWKBRoundTrip(t *testing.T) {
	inputs := []string{
		"POINT(1 2)",
		"POINT(-1.5 2.25)",
		"POINT EMPTY",
		"LINESTRING(0 0,1 1,2 3)",
		"LINESTRING EMPTY",
		"POLYGON((0 0,4 0,4 4,0 4,0 0))",
		"POLYGON((0 0,10 0,10 10,0 10,0 0),(2 2,2 4,4 4,4 2,2 2))",
		"MULTIPOINT(1 1,2 2)",
		"MULTILINESTRING((0 0,1 1),(2 2,3 3))",
		"MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 2,3 2,3 3,2 2)))",
		"GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(0 0,1 1))",
		"GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)),POINT(1 1))",
		"GEOMETRYCOLLECTION EMPTY",
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			g1, err := ParseWKT(in)
			require.NoError(t, err)
			b := WriteWKB(g1)
			g2, err := ReadWKB(b)
			require.NoError(t, err)
			require.Equal(t, g1, g2)
			// WKB -> WKT -> WKB is also stable.
			require.Equal(t, b, WriteWKB(mustParse(t, WriteWKT(g2))))
		})
	}
}

// A big-endian (XDR) POINT(1 2) must decode identically to the little-endian
// form.
func TestReadWKBBigEndian(t *testing.T) {
	be := mustHex(t, "00000000013ff00000000000004000000000000000")
	g, err := ReadWKB(be)
	require.NoError(t, err)
	require.Equal(t, Point{X: 1, Y: 2}, g)
}

func TestReadWKBInvalid(t *testing.T) {
	cases := []struct {
		name string
		hex  string
	}{
		{"empty", ""},
		{"bad byte order", "0201000000000000000000f03f0000000000000040"},
		{"truncated point", "010100000000"},
		{"unknown type", "0163000000000000000000f03f0000000000000040"},
		{"trailing bytes", "0101000000000000000000f03f0000000000000040ff"},
		// LINESTRING with an absurd point count and no coordinate data.
		{"huge count", "0102000000ffffffff"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ReadWKB(mustHex(t, c.hex))
			require.Error(t, err)
		})
	}
}

func mustParse(t *testing.T, wkt string) Geometry {
	t.Helper()
	g, err := ParseWKT(wkt)
	require.NoError(t, err)
	return g
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}
