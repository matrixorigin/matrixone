// Copyright 2021 - 2026 Matrix Origin
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

// parseGeoJSONOrFatal parses one GeoJSON geometry object, failing the test when
// the input is rejected, and returns the geometry for WKT comparisons.
func parseGeoJSONOrFatal(t *testing.T, input string) Geometry {
	t.Helper()
	g, err := ParseGeoJSON([]byte(input))
	require.NoError(t, err, input)
	return g
}

// rejectGeoJSON requires input to be an invalid GeoJSON geometry. When the
// parser wrongly accepts it, the failure message carries the parsed WKT so the
// wrongly produced geometry (for example an EMPTY one) is visible.
func rejectGeoJSON(t *testing.T, input string) {
	t.Helper()
	g, err := ParseGeoJSON([]byte(input))
	if err != nil {
		return
	}
	t.Fatalf("invalid GeoJSON %s was accepted as geometry %s", input, WriteWKT(g))
}

// TestGeoJSONRequiredMember pins the RFC 7946 section-3.1 rule that every
// Geometry object carries its type's required array member: coordinates for
// the six coordinate-bearing types, geometries for a GeometryCollection. An
// absent or JSON null member is an invalid geometry, while an explicit empty
// array keeps the established EMPTY geometry behavior. The rule applies to
// child Geometry objects at any depth, and optional metadata stays inert.
func TestGeoJSONRequiredMember(t *testing.T) {
	coordinateTypes := []struct {
		typ         string
		nonEmpty    string
		nonEmptyWKT string
	}{
		{"Point", `[1,2]`, "POINT(1 2)"},
		{"LineString", `[[0,0],[1,1]]`, "LINESTRING(0 0,1 1)"},
		{"Polygon", `[[[0,0],[1,0],[1,1],[0,0]]]`, "POLYGON((0 0,1 0,1 1,0 0))"},
		{"MultiPoint", `[[0,0],[1,1]]`, "MULTIPOINT(0 0,1 1)"},
		{"MultiLineString", `[[[0,0],[1,1]],[[2,2],[3,3]]]`, "MULTILINESTRING((0 0,1 1),(2 2,3 3))"},
		{"MultiPolygon", `[[[[1,1],[3,1],[3,3],[1,3],[1,1]]]]`, "MULTIPOLYGON(((1 1,3 1,3 3,1 3,1 1)))"},
	}
	emptyWKT := map[string]string{
		"Point":           "POINT EMPTY",
		"LineString":      "LINESTRING EMPTY",
		"Polygon":         "POLYGON EMPTY",
		"MultiPoint":      "MULTIPOINT EMPTY",
		"MultiLineString": "MULTILINESTRING EMPTY",
		"MultiPolygon":    "MULTIPOLYGON EMPTY",
	}
	for _, ct := range coordinateTypes {
		t.Run(ct.typ, func(t *testing.T) {
			// The coordinates member is required: neither absent...
			rejectGeoJSON(t, `{"type":"`+ct.typ+`"}`)
			// ...nor JSON null is a valid geometry.
			rejectGeoJSON(t, `{"type":"`+ct.typ+`","coordinates":null}`)
			// An explicit empty array stays a valid EMPTY geometry.
			require.Equal(t, emptyWKT[ct.typ],
				WriteWKT(parseGeoJSONOrFatal(t, `{"type":"`+ct.typ+`","coordinates":[]}`)))
			// A valid non-empty member round-trips to the expected geometry.
			require.Equal(t, ct.nonEmptyWKT,
				WriteWKT(parseGeoJSONOrFatal(t, `{"type":"`+ct.typ+`","coordinates":`+ct.nonEmpty+`}`)))
		})
	}

	t.Run("GeometryCollection", func(t *testing.T) {
		// The geometries member is required: neither absent...
		rejectGeoJSON(t, `{"type":"GeometryCollection"}`)
		// ...nor JSON null is a valid geometry.
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":null}`)
		// An explicit empty array stays a valid EMPTY collection, and a
		// non-empty member keeps parsing its children.
		require.Equal(t, "GEOMETRYCOLLECTION EMPTY",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[]}`)))
		require.Equal(t, "GEOMETRYCOLLECTION(POINT(1 2))",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}]}`)))
	})

	t.Run("ChildGeometryObjects", func(t *testing.T) {
		// Child coordinate-bearing objects require their coordinates member.
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"Point"}]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":null}]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"LineString","coordinates":null}]}`)
		// Child collections require their geometries member at any depth.
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection"}]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":null}]}`)
		// Explicit empty arrays keep producing EMPTY child geometries,
		// including nested empty collections.
		require.Equal(t, "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION EMPTY)",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":[]}]}`)))
		require.Equal(t, "GEOMETRYCOLLECTION(POINT EMPTY)",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[]}]}`)))
	})

	t.Run("OptionalMetadata", func(t *testing.T) {
		// bbox and other foreign members are optional (RFC 7946 section 6.1);
		// the required member stays the only structural requirement.
		require.Equal(t, "POINT(1 2)",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"Point","bbox":[0,0,1,1],"coordinates":[1,2],"score":7}`)))
	})

	t.Run("MalformedUnrelatedMember", func(t *testing.T) {
		// A wrongly typed geometries member is rejected everywhere — on
		// non-collection types at the top level and inside collections —
		// preserving the original typed shape decoding of that member.
		rejectGeoJSON(t, `{"type":"Point","coordinates":[1,2],"geometries":{}}`)
		rejectGeoJSON(t, `{"type":"Point","coordinates":[1,2],"geometries":7}`)
		rejectGeoJSON(t, `{"type":"Point","coordinates":[1,2],"geometries":"none"}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2],"geometries":{"a":1}}]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"LineString","coordinates":[[0,0],[1,1]],"geometries":false}]}`)
	})

	// The required member names are exact, case-sensitive names. A member that
	// differs only in case is a foreign member: it never substitutes the
	// canonical value, never turns a canonical null or missing required member
	// into a success, at any depth and whatever the member order. Duplicate
	// exact member names keep the decoder's own duplicate-name behavior; this
	// test adds no separate policy for them.
	t.Run("ExactCaseMemberNames", func(t *testing.T) {
		// A null required member stays invalid even when a differently-cased
		// member case-folds onto the same field, before or after it.
		rejectGeoJSON(t, `{"type":"Point","coordinates":null,"Coordinates":[1,2]}`)
		rejectGeoJSON(t, `{"type":"Point","Coordinates":[1,2],"coordinates":null}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":null,"Geometries":[{"type":"Point","coordinates":[1,2]}]}`)
		// A missing required member is not supplied by a differently-cased
		// member, and a differently-cased type member is not the type either.
		rejectGeoJSON(t, `{"type":"Point","Coordinates":[1,2]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","Geometries":[{"type":"Point","coordinates":[1,2]}]}`)
		rejectGeoJSON(t, `{"Type":"Point","coordinates":[1,2]}`)
		rejectGeoJSON(t, `{"type":"Bogus","Type":"Point","coordinates":[1,2]}`)

		// A valid canonical member wins over a differently-cased member.
		require.Equal(t, "POINT(5 6)",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"Point","coordinates":[5,6],"Coordinates":[9,9]}`)))
		// Exact-member EMPTY acceptance is not shadowed by a differently-cased
		// member either, at the top level or inside a collection.
		require.Equal(t, "POINT EMPTY",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"Point","coordinates":[],"Coordinates":[1,2]}`)))
		require.Equal(t, "GEOMETRYCOLLECTION EMPTY",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[],"Geometries":[{"type":"Point","coordinates":[1,2]}]}`)))
		require.Equal(t, "GEOMETRYCOLLECTION(POINT(1 2))",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2]}],"Geometries":[]}`)))

		// The same rule holds for nested child objects, including collections
		// inside collections.
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":null,"Coordinates":[1,2]}]}`)
		rejectGeoJSON(t, `{"type":"GeometryCollection","geometries":[{"type":"GeometryCollection","geometries":null,"Geometries":[]}]}`)
		require.Equal(t, "GEOMETRYCOLLECTION(POINT(1 2))",
			WriteWKT(parseGeoJSONOrFatal(t, `{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[1,2],"Coordinates":[0,0]}]}`)))
	})
}
