// Copyright 2026 Matrix Origin
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

package function

import (
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func derivedPayload(t *testing.T, wkt string) []byte {
	t.Helper()
	g, err := geo.ParseWKT(wkt)
	require.NoError(t, err)
	return geo.WriteWKB(g)
}

func TestDerivedGeometrySimplePayloadMatrix(t *testing.T) {
	cases := []struct {
		name string
		wkt  string
		want bool
	}{
		{"point", "POINT(1 2)", true},
		{"point_empty", "POINT EMPTY", true},
		{"line_empty", "LINESTRING EMPTY", true},
		{"line_simple", "LINESTRING(0 0,2 0,2 2)", true},
		{"line_crossing", "LINESTRING(0 0,2 2,0 2,2 0)", false},
		{"polygon_empty", "POLYGON EMPTY", true},
		{"polygon_simple", "POLYGON((0 0,4 0,4 4,0 0))", true},
		{"polygon_self_intersecting", "POLYGON((0 0,4 4,0 4,4 0,0 0))", false},
		{"multipoint_unique", "MULTIPOINT(0 0,1 1,2 2)", true},
		{"multipoint_duplicate", "MULTIPOINT(0 0,1 1,0 0)", false},
		{"multipoint_empty", "MULTIPOINT EMPTY", true},
		{"multiline_disjoint", "MULTILINESTRING((0 0,1 0),(0 1,1 1))", true},
		{"multiline_crossing", "MULTILINESTRING((0 0,2 2),(0 2,2 0))", false},
		{"multiline_shared_endpoint", "MULTILINESTRING((0 0,1 0),(1 0,2 0))", true},
		{"multiline_non_simple_member", "MULTILINESTRING((0 0,2 2,0 2,2 0),(4 0,5 0))", false},
		{"multipolygon_disjoint", "MULTIPOLYGON(((0 0,1 0,1 1,0 0)),((2 0,3 0,3 1,2 0)))", true},
		{"multipolygon_overlap", "MULTIPOLYGON(((0 0,2 0,2 2,0 0)),((1 0,3 0,3 2,1 0)))", false},
		{"multipolygon_non_simple_member", "MULTIPOLYGON(((0 0,2 2,0 2,2 0,0 0)),((4 0,5 0,5 1,4 0)))", false},
		{"collection_empty", "GEOMETRYCOLLECTION EMPTY", true},
		{"collection_disjoint", "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 0,2 0))", true},
		{"collection_point_on_line", "GEOMETRYCOLLECTION(POINT(1 0),LINESTRING(0 0,2 0))", false},
		{"collection_nested", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(POINT(0 0)),POINT(2 2))", true},
		{"collection_non_simple_member", "GEOMETRYCOLLECTION(MULTIPOINT(0 0,0 0),POINT(2 2))", false},
		{"collection_nested_non_simple_member", "GEOMETRYCOLLECTION(GEOMETRYCOLLECTION(MULTIPOINT(0 0,0 0)),POINT(2 2))", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := isSimpleFromPayload(derivedPayload(t, tc.wkt))
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSimpleGeometryMemberPointLocationMatrix(t *testing.T) {
	point := geometryPoint2D{x: 1, y: 1}
	cases := []struct {
		name   string
		member simpleGeometryMember
		point  geometryPoint2D
		want   geometryPointLocation
	}{
		{"point_interior", simpleGeometryMember{"POINT", derivedPayload(t, "POINT(1 1)")}, point, geometryPointInterior},
		{"point_exterior", simpleGeometryMember{"POINT", derivedPayload(t, "POINT(2 2)")}, point, geometryPointExterior},
		{"point_empty", simpleGeometryMember{"POINT", derivedPayload(t, "POINT EMPTY")}, point, geometryPointExterior},
		{"multipoint_interior", simpleGeometryMember{"MULTIPOINT", derivedPayload(t, "MULTIPOINT(EMPTY,1 1,2 2)")}, point, geometryPointInterior},
		{"multipoint_exterior", simpleGeometryMember{"MULTIPOINT", derivedPayload(t, "MULTIPOINT(EMPTY,2 2)")}, point, geometryPointExterior},
		{"line_interior", simpleGeometryMember{"LINESTRING", derivedPayload(t, "LINESTRING(0 0,2 0,2 2)")}, geometryPoint2D{x: 2, y: 1}, geometryPointInterior},
		{"line_boundary", simpleGeometryMember{"LINESTRING", derivedPayload(t, "LINESTRING(0 0,2 0,2 2)")}, geometryPoint2D{x: 0, y: 0}, geometryPointBoundary},
		{"line_exterior", simpleGeometryMember{"LINESTRING", derivedPayload(t, "LINESTRING(0 0,2 0,2 2)")}, point, geometryPointExterior},
		{"line_empty", simpleGeometryMember{"LINESTRING", derivedPayload(t, "LINESTRING EMPTY")}, point, geometryPointExterior},
		{"multiline_boundary_parity", simpleGeometryMember{"MULTILINESTRING", derivedPayload(t, "MULTILINESTRING((0 0,1 0),(1 0,2 0))")}, geometryPoint2D{x: 1, y: 0}, geometryPointInterior},
		{"multiline_boundary", simpleGeometryMember{"MULTILINESTRING", derivedPayload(t, "MULTILINESTRING((0 0,1 0),(1 0,2 0))")}, geometryPoint2D{x: 0, y: 0}, geometryPointBoundary},
		{"multiline_exterior", simpleGeometryMember{"MULTILINESTRING", derivedPayload(t, "MULTILINESTRING(EMPTY,(0 0,1 0))")}, point, geometryPointExterior},
		{"polygon_interior", simpleGeometryMember{"POLYGON", derivedPayload(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")}, point, geometryPointInterior},
		{"polygon_boundary", simpleGeometryMember{"POLYGON", derivedPayload(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")}, geometryPoint2D{x: 0, y: 1}, geometryPointBoundary},
		{"polygon_hole", simpleGeometryMember{"POLYGON", derivedPayload(t, "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))")}, geometryPoint2D{x: 2, y: 2}, geometryPointExterior},
		{"polygon_exterior", simpleGeometryMember{"POLYGON", derivedPayload(t, "POLYGON((0 0,4 0,4 4,0 4,0 0))")}, geometryPoint2D{x: 5, y: 5}, geometryPointExterior},
		{"multipolygon_interior", simpleGeometryMember{"MULTIPOLYGON", derivedPayload(t, "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((4 0,6 0,6 2,4 2,4 0)))")}, geometryPoint2D{x: 1, y: 1}, geometryPointInterior},
		{"multipolygon_boundary", simpleGeometryMember{"MULTIPOLYGON", derivedPayload(t, "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)))")}, geometryPoint2D{x: 0, y: 1}, geometryPointBoundary},
		{"multipolygon_exterior", simpleGeometryMember{"MULTIPOLYGON", derivedPayload(t, "MULTIPOLYGON(EMPTY,((0 0,2 0,2 2,0 2,0 0)))")}, geometryPoint2D{x: 5, y: 5}, geometryPointExterior},
		{"collection_interior_priority", simpleGeometryMember{"GEOMETRYCOLLECTION", derivedPayload(t, "GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(0 0))")}, geometryPoint2D{x: 0, y: 0}, geometryPointInterior},
		{"collection_boundary", simpleGeometryMember{"GEOMETRYCOLLECTION", derivedPayload(t, "GEOMETRYCOLLECTION(LINESTRING(0 0,2 0),POINT(5 5))")}, geometryPoint2D{x: 0, y: 0}, geometryPointBoundary},
		{"collection_exterior", simpleGeometryMember{"GEOMETRYCOLLECTION", derivedPayload(t, "GEOMETRYCOLLECTION(POINT(5 5))")}, point, geometryPointExterior},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := simpleGeometryMemberPointLocation(tc.member, tc.point)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	reversed := simpleGeometryMember{typeName: "GEOMETRYCOLLECTION", payload: derivedPayload(t, "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(0 0,2 0))")}
	got, err := simpleGeometryMemberPointLocation(reversed, geometryPoint2D{x: 0, y: 0})
	require.NoError(t, err)
	require.Equal(t, geometryPointInterior, got)
	_, err = simpleGeometryMemberPointLocation(simpleGeometryMember{typeName: "UNKNOWN", payload: []byte("UNKNOWN")}, point)
	require.Error(t, err)
}

func TestSimpleGeometryIntersectionMatrix(t *testing.T) {
	member := func(typeName, wkt string) simpleGeometryMember {
		return simpleGeometryMember{typeName: typeName, payload: derivedPayload(t, wkt)}
	}
	cases := []struct {
		name        string
		left, right simpleGeometryMember
		want        bool
	}{
		{"point_point", member("POINT", "POINT(0 0)"), member("POINT", "POINT(0 0)"), true},
		{"point_line", member("POINT", "POINT(1 0)"), member("LINESTRING", "LINESTRING(0 0,2 0)"), true},
		{"point_polygon", member("POINT", "POINT(1 1)"), member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
		{"line_line_cross", member("LINESTRING", "LINESTRING(0 0,2 2)"), member("LINESTRING", "LINESTRING(0 2,2 0)"), true},
		{"line_line_disjoint", member("LINESTRING", "LINESTRING(0 0,1 0)"), member("LINESTRING", "LINESTRING(0 2,1 2)"), false},
		{"line_polygon_cross", member("LINESTRING", "LINESTRING(-1 1,3 1)"), member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
		{"polygon_polygon_touch", member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), member("POLYGON", "POLYGON((2 0,4 0,4 2,2 2,2 0))"), false},
		{"polygon_polygon_disjoint", member("POLYGON", "POLYGON((0 0,1 0,1 1,0 1,0 0))"), member("POLYGON", "POLYGON((3 0,4 0,4 1,3 1,3 0))"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := simpleGeometryMembersInteriorIntersect(tc.left, tc.right)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}

	line := simpleGeometryPart{typeName: "LINESTRING", payload: derivedPayload(t, "LINESTRING(0 0,2 0)")}
	point := simpleGeometryPart{typeName: "POINT", payload: derivedPayload(t, "POINT(1 0)")}
	require.True(t, func() bool {
		got, err := simpleGeometryPartsIntersect(point, line)
		return err == nil && got
	}())
	require.True(t, func() bool {
		got, err := simplePartsTouchOnlyAtBoundary(
			point,
			simpleGeometryPart{typeName: "LINESTRING", payload: derivedPayload(t, "LINESTRING(0 0,2 0)")},
		)
		return err == nil && !got
	}())
	leftLine := simpleGeometryPart{typeName: "LINESTRING", payload: derivedPayload(t, "LINESTRING(0 0,1 0)")}
	rightLine := simpleGeometryPart{typeName: "LINESTRING", payload: derivedPayload(t, "LINESTRING(1 0,2 0)")}
	require.True(t, func() bool {
		got, err := simplePartsTouchAtBothBoundaries(leftLine, rightLine)
		return err == nil && got
	}())
}

func TestSimplePartsBoundaryTouchMatrix(t *testing.T) {
	part := func(typeName, wkt string) simpleGeometryPart {
		return simpleGeometryPart{typeName: typeName, payload: derivedPayload(t, wkt)}
	}
	cases := []struct {
		name        string
		left, right simpleGeometryPart
		want        bool
	}{
		{"point_point", part("POINT", "POINT(0 0)"), part("POINT", "POINT(0 0)"), false},
		{"point_line_endpoint", part("POINT", "POINT(0 0)"), part("LINESTRING", "LINESTRING(0 0,2 0)"), true},
		{"point_polygon_boundary", part("POINT", "POINT(0 1)"), part("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
		{"line_point_endpoint", part("LINESTRING", "LINESTRING(0 0,2 0)"), part("POINT", "POINT(0 0)"), true},
		{"line_line_endpoint", part("LINESTRING", "LINESTRING(0 0,1 0)"), part("LINESTRING", "LINESTRING(1 0,2 0)"), true},
		{"line_line_cross", part("LINESTRING", "LINESTRING(0 0,2 2)"), part("LINESTRING", "LINESTRING(0 2,2 0)"), false},
		{"line_polygon_boundary", part("LINESTRING", "LINESTRING(0 0,2 0)"), part("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
		{"polygon_line_boundary", part("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), part("LINESTRING", "LINESTRING(0 0,2 0)"), true},
		{"polygon_polygon_touch", part("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), part("POLYGON", "POLYGON((2 0,4 0,4 2,2 2,2 0))"), true},
		{"polygon_polygon_overlap", part("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), part("POLYGON", "POLYGON((1 0,3 0,3 2,1 2,1 0))"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := simplePartsTouchOnlyAtBoundary(tc.left, tc.right)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestSimpleGeometryInteriorIntersectionEdges(t *testing.T) {
	member := func(typeName, wkt string) simpleGeometryMember {
		return simpleGeometryMember{typeName: typeName, payload: derivedPayload(t, wkt)}
	}
	cases := []struct {
		name        string
		left, right simpleGeometryMember
		want        bool
	}{
		{"line_line_overlap", member("LINESTRING", "LINESTRING(0 0,2 0)"), member("LINESTRING", "LINESTRING(1 0,3 0)"), true},
		{"line_line_endpoint_only", member("LINESTRING", "LINESTRING(0 0,1 0)"), member("LINESTRING", "LINESTRING(1 0,2 0)"), false},
		{"line_polygon_inside", member("LINESTRING", "LINESTRING(0.5 1,1.5 1)"), member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
		{"line_polygon_boundary_only", member("LINESTRING", "LINESTRING(0 0,2 0)"), member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), false},
		{"polygon_polygon_overlap", member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), member("POLYGON", "POLYGON((1 0,3 0,3 2,1 2,1 0))"), true},
		{"polygon_polygon_reverse_overlap", member("POLYGON", "POLYGON((1 0,3 0,3 2,1 2,1 0))"), member("POLYGON", "POLYGON((0 0,2 0,2 2,0 2,0 0))"), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := simpleGeometryMembersInteriorIntersect(tc.left, tc.right)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDerivedGeometryValidationAndDimensionMatrix(t *testing.T) {
	coord := func(x, y float64) geo.Coord { return geo.Coord{X: x, Y: y} }
	valid := []geo.Geometry{
		geo.Point{X: 1, Y: 2},
		geo.Point{IsEmpty: true},
		geo.LineString{Points: []geo.Coord{coord(0, 0), coord(1, 1)}},
		geo.LineString{},
		geo.Polygon{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0), coord(0, 1), coord(0, 0)}}},
		geo.Polygon{},
		geo.MultiPoint{Points: []geo.Point{{X: 1, Y: 2}, {IsEmpty: true}}},
		geo.MultiLineString{Lines: []geo.LineString{{Points: []geo.Coord{coord(0, 0), coord(1, 0)}}}},
		geo.MultiPolygon{Polygons: []geo.Polygon{{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0), coord(0, 1), coord(0, 0)}}}}},
		geo.GeometryCollection{Geometries: []geo.Geometry{geo.Point{X: 1, Y: 1}}},
	}
	for _, g := range valid {
		require.NoError(t, validateDerivedGeometry(g))
	}
	invalid := []geo.Geometry{
		geo.Point{X: math.NaN(), Y: 0},
		geo.LineString{Points: []geo.Coord{coord(0, 0)}},
		geo.LineString{Points: []geo.Coord{coord(0, 0), coord(math.Inf(1), 0)}},
		geo.Polygon{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0)}}},
		geo.Polygon{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0), coord(math.NaN(), 1)}}},
		geo.MultiPoint{Points: []geo.Point{{X: math.Inf(1), Y: 0}}},
		geo.MultiLineString{Lines: []geo.LineString{{Points: []geo.Coord{coord(0, 0)}}}},
		geo.MultiPolygon{Polygons: []geo.Polygon{{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0)}}}}},
		geo.GeometryCollection{Geometries: []geo.Geometry{nil}},
	}
	for _, g := range invalid {
		require.Error(t, validateDerivedGeometry(g))
	}

	dimensionCases := []struct {
		g    geo.Geometry
		want int
		ok   bool
	}{
		{geo.Point{X: 1, Y: 1}, 0, true},
		{geo.Point{IsEmpty: true}, 0, false},
		{geo.LineString{Points: []geo.Coord{coord(0, 0), coord(1, 0)}}, 1, true},
		{geo.LineString{}, 1, false},
		{geo.Polygon{Rings: [][]geo.Coord{{coord(0, 0), coord(1, 0), coord(0, 1), coord(0, 0)}}}, 2, true},
		{geo.Polygon{}, 2, false},
		{geo.MultiPoint{Points: []geo.Point{{IsEmpty: true}}}, 0, false},
		{geo.MultiLineString{Lines: []geo.LineString{{}}}, 0, false},
		{geo.MultiPolygon{Polygons: []geo.Polygon{{}}}, 0, false},
		{geo.GeometryCollection{Geometries: []geo.Geometry{geo.Point{X: 1, Y: 1}, geo.LineString{Points: []geo.Coord{coord(0, 0), coord(1, 0)}}}}, 1, true},
	}
	for _, tc := range dimensionCases {
		got, ok := derivedGeometryDimension(tc.g)
		require.Equal(t, tc.want, got)
		require.Equal(t, tc.ok, ok)
	}
}

func TestDerivedEnvelopeAndCentroidPayloadEdges(t *testing.T) {
	cases := []struct {
		name             string
		wkt              string
		wantEnvelope     string
		wantCentroid     string
		wantCentroidNull bool
	}{
		{"point", "POINT(1 2)", "POINT(1 2)", "POINT(1 2)", false},
		{"vertical", "LINESTRING(1 0,1 2)", "LINESTRING(1 0,1 2)", "POINT(1 1)", false},
		{"horizontal", "LINESTRING(0 1,2 1)", "LINESTRING(0 1,2 1)", "POINT(1 1)", false},
		{"area", "POLYGON((0 0,2 0,2 2,0 2,0 0))", "POLYGON((0 0,2 0,2 2,0 2,0 0))", "POINT(1 1)", false},
		{"collection", "GEOMETRYCOLLECTION(POINT(0 0),LINESTRING(1 1,2 3))", "POLYGON((0 0,2 0,2 3,0 3,0 0))", "POINT(1.5 2)", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := derivedPayload(t, tc.wkt)
			envelope, err := envelopeFromPayload(payload)
			require.NoError(t, err)
			gotEnvelope, err := geo.ReadWKB(envelope)
			require.NoError(t, err)
			require.Equal(t, tc.wantEnvelope, geo.WriteWKT(gotEnvelope))

			centroid, isNull, err := centroidFromPayload(payload)
			require.NoError(t, err)
			require.Equal(t, tc.wantCentroidNull, isNull)
			gotCentroid, err := geo.ReadWKB(centroid)
			require.NoError(t, err)
			require.Equal(t, tc.wantCentroid, geo.WriteWKT(gotCentroid))
		})
	}
	_, isNull, err := centroidFromPayload(derivedPayload(t, "GEOMETRYCOLLECTION EMPTY"))
	require.NoError(t, err)
	require.True(t, isNull)
	_, err = envelopeFromPayload(derivedPayload(t, "LINESTRING(0 0)"))
	require.Error(t, err)
}

func TestDerivedGeometryMalformedPayloadErrors(t *testing.T) {
	bad := []byte("not a geometry")
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"simple", func() error { _, err := isSimpleFromPayload(bad); return err }},
		{"polygon_simple", func() error { _, err := polygonIsSimpleFromPayload(bad); return err }},
		{"envelope", func() error { _, err := envelopeFromPayload(bad); return err }},
		{"centroid", func() error { _, _, err := centroidFromPayload(bad); return err }},
		{"parts", func() error { _, err := simpleGeometryPartsFromPayload(bad, "MULTIPOINT"); return err }},
		{"members", func() error { _, err := simpleGeometryMembersFromPayload(bad, "GEOMETRYCOLLECTION"); return err }},
		{"member_point", func() error {
			_, err := simpleGeometryMemberPointLocation(simpleGeometryMember{typeName: "POINT", payload: bad}, geometryPoint2D{})
			return err
		}},
		{"part_point", func() error {
			_, err := simpleGeometryPartPoint(simpleGeometryPart{typeName: "POINT", payload: bad})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Error(t, tc.fn()) })
	}

	_, err := geometryDimensionFromTextWithDepth("GEOMETRYCOLLECTION(bad)", 0)
	require.Error(t, err)
	_, err = geometryDimensionFromTextWithDepth("GEOMETRYCOLLECTION(POINT(0 0))", maxGeometryCollectionNestingDepth)
	require.Error(t, err)
	require.False(t, polygonRingsAreSimple(nil))
	require.True(t, ringContainedByPolygonShell(nil, geometryPolygon2D{}))
}

func TestSimpleGeometryIntersectionMalformedOperands(t *testing.T) {
	bad := simpleGeometryPart{typeName: "POINT", payload: []byte("not a geometry")}
	goodPoint := simpleGeometryPart{typeName: "POINT", payload: derivedPayload(t, "POINT(0 0)")}
	goodLine := simpleGeometryPart{typeName: "LINESTRING", payload: derivedPayload(t, "LINESTRING(0 0,1 0)")}
	goodPolygon := simpleGeometryPart{typeName: "POLYGON", payload: derivedPayload(t, "POLYGON((0 0,1 0,1 1,0 0))")}
	for _, tc := range []struct {
		name string
		fn   func() error
	}{
		{"parts_point", func() error { _, err := simpleGeometryPartsIntersect(bad, goodPoint); return err }},
		{"parts_line", func() error { _, err := simpleGeometryPartsIntersect(goodLine, bad); return err }},
		{"parts_polygon", func() error { _, err := simpleGeometryPartsIntersect(goodPolygon, bad); return err }},
		{"touch_point", func() error { _, err := simplePartsTouchOnlyAtBoundary(bad, goodPoint); return err }},
		{"touch_line", func() error { _, err := simplePartsTouchOnlyAtBoundary(goodLine, bad); return err }},
		{"touch_polygon", func() error { _, err := simplePartsTouchOnlyAtBoundary(goodPolygon, bad); return err }},
		{"members", func() error {
			_, err := simpleGeometryMembersInteriorIntersect(simpleGeometryMember{typeName: "POINT", payload: bad.payload}, simpleGeometryMember{typeName: "POINT", payload: goodPoint.payload})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Error(t, tc.fn()) })
	}
}

func TestStCentroidResultNullTemplateModes(t *testing.T) {
	proc := testutil.NewProcess(t)
	point := "POINT(1 2)"
	empty := "GEOMETRYCOLLECTION EMPTY"
	run := func(t *testing.T, input FunctionTestInput, expected FunctionTestResult, selectList *FunctionSelectList) {
		t.Helper()
		tc := NewFunctionTestCase(proc, []FunctionTestInput{input}, expected, StCentroid).WithSelectList(selectList)
		ok, info := tc.Run()
		require.True(t, ok, info)
	}
	run(t, NewFunctionTestInput(types.T_geometry.ToType(), []string{}, nil), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{}, nil), nil)
	run(t, NewFunctionTestConstInput(types.T_geometry.ToType(), []string{point}, nil), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)"}, []bool{false}), nil)
	run(t, NewFunctionTestConstInput(types.T_geometry.ToType(), []string{point}, []bool{true}), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}), nil)
	run(t, NewFunctionTestConstInput(types.T_geometry.ToType(), []string{empty}, nil), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}), nil)
	run(t, NewFunctionTestInput(types.T_geometry.ToType(), []string{point, empty, point}, []bool{false, false, true}), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"POINT(1 2)", "", ""}, []bool{false, true, true}), nil)
	run(t, NewFunctionTestInput(types.T_geometry.ToType(), []string{point, point}, nil), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{"", "POINT(1 2)"}, []bool{true, false}), &FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	run(t, NewFunctionTestInput(types.T_geometry.ToType(), []string{"LINESTRING(0 0)"}, nil), NewFunctionTestResult(types.T_geometry.ToType(), false, []string{""}, []bool{true}), &FunctionSelectList{AnyNull: true, AllNull: true})
}

func TestStCentroidResultNullTemplateConstBroadcastAndMaskedMalformed(t *testing.T) {
	proc := testutil.NewProcess(t)
	in, err := vector.NewConstBytes(types.T_geometry.ToType(), []byte("POINT(1 2)"), 3, proc.Mp())
	require.NoError(t, err)
	defer in.Free(proc.Mp())
	out := vector.NewFunctionResultWrapper(types.T_geometry.ToType(), proc.Mp())
	defer out.Free()
	require.NoError(t, out.PreExtendAndReset(3))
	require.NoError(t, StCentroid([]*vector.Vector{in}, out, proc, 3, nil))
	for i := uint64(0); i < 3; i++ {
		got := out.GetResultVector().GetBytesAt(int(i))
		require.False(t, out.GetResultVector().GetNulls().Contains(i))
		g, err := geo.ReadWKB(got)
		require.NoError(t, err)
		require.Equal(t, "POINT(1 2)", geo.WriteWKT(g))
	}

	input := vector.NewVec(types.T_geometry.ToType())
	defer input.Free(proc.Mp())
	require.NoError(t, vector.AppendBytes(input, []byte("POINT(1 2)"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte("LINESTRING(0 0)"), false, proc.Mp()))
	require.NoError(t, vector.AppendBytes(input, []byte("POINT(1 2)"), false, proc.Mp()))
	masked := &FunctionSelectList{AnyNull: true, SelectList: []bool{true, false, true}}
	out = vector.NewFunctionResultWrapper(types.T_geometry.ToType(), proc.Mp())
	defer out.Free()
	require.NoError(t, out.PreExtendAndReset(3))
	require.NoError(t, StCentroid([]*vector.Vector{input}, out, proc, 3, masked))
	result := out.GetResultVector()
	require.False(t, result.GetNulls().Contains(0))
	require.True(t, result.GetNulls().Contains(1))
	require.False(t, result.GetNulls().Contains(2))
	for _, row := range []int{0, 2} {
		got := result.GetBytesAt(row)
		g, err := geo.ReadWKB(got)
		require.NoError(t, err)
		require.Equal(t, "POINT(1 2)", geo.WriteWKT(g))
	}
}
