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
