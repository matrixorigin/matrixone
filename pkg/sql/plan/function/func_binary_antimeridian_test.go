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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/geo"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

const (
	antimeridianOuter = "POLYGON((179 -1,-179 -1,-179 1,179 1,179 -1))"
	antimeridianInner = "POLYGON((179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5,179.5 -0.5))"
	ordinaryOuter     = "POLYGON((0 0,2 0,2 2,0 2,0 0))"
	ordinaryInner     = "POLYGON((0.5 0.5,1.5 0.5,1.5 1.5,0.5 1.5,0.5 0.5))"
)

func wgs84GeometryInputs(left, right []string) []FunctionTestInput {
	typ := types.T_geometry.ToType()
	typ.Width = int32(geo.SRIDWGS84 + 1)
	return []FunctionTestInput{
		NewFunctionTestInput(typ, left, []bool{false, false}),
		NewFunctionTestInput(typ, right, []bool{false, false}),
	}
}

func TestWGS84AntimeridianTopologyPredicates(t *testing.T) {
	cases := []struct {
		name string
		fn   fEvalFn
		want []bool
	}{
		{name: "contains", fn: StContains, want: []bool{true, true}},
		{name: "within", fn: StWithin, want: []bool{true, true}},
		{name: "intersects", fn: StIntersects, want: []bool{true, true}},
		{name: "disjoint", fn: StDisjoint, want: []bool{false, false}},
		{name: "touches", fn: StTouches, want: []bool{false, false}},
		{name: "crosses", fn: StCrosses, want: []bool{false, false}},
		{name: "overlaps", fn: StOverlaps, want: []bool{false, false}},
		{name: "equals", fn: StEquals, want: []bool{false, false}},
		{name: "covers", fn: StCovers, want: []bool{true, true}},
		{name: "coveredby", fn: StCoveredBy, want: []bool{true, true}},
	}

	proc := testutil.NewProcess(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			left, right := []string{antimeridianOuter, ordinaryOuter}, []string{antimeridianInner, ordinaryInner}
			if tc.name == "within" || tc.name == "coveredby" {
				left, right = right, left
			}
			fc := NewFunctionTestCase(proc,
				wgs84GeometryInputs(left, right),
				NewFunctionTestResult(types.T_bool.ToType(), false, tc.want, []bool{false, false}),
				tc.fn)
			ok, info := fc.Run()
			require.True(t, ok, info)
		})
	}
}

func TestWGS84AntimeridianOverlay(t *testing.T) {
	proc := testutil.NewProcess(t)

	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want string
	}{
		// These are independent topology oracles for a small local patch:
		// intersection=inner, union=outer, and difference/XOR=outer-with-inner-hole.
		{name: "intersection", fn: StIntersection, want: "POLYGON((179.5 0.5,179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5))"},
		{name: "union", fn: StUnion, want: "POLYGON((179 1,179 -1,-179 -1,-179 1,179 1))"},
		{name: "difference", fn: StDifference, want: "POLYGON((179 1,179 -1,-179 -1,-179 1,179 1),(179.5 -0.5,179.5 0.5,-179.5 0.5,-179.5 -0.5,179.5 -0.5))"},
		{name: "symmetric difference", fn: StSymDifference, want: "POLYGON((179 1,179 -1,-179 -1,-179 1,179 1),(179.5 -0.5,179.5 0.5,-179.5 0.5,-179.5 -0.5,179.5 -0.5))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fc := NewFunctionTestCase(proc,
				wgs84GeometryInputs([]string{antimeridianOuter}, []string{antimeridianInner}),
				NewFunctionTestResult(types.T_geometry.ToType(), false, []string{tc.want}, []bool{false}),
				tc.fn)
			ok, info := fc.Run()
			require.True(t, ok, info)
			got, err := geo.ReadWKB(fc.GetResultVectorDirectly().GetBytesAt(0))
			require.NoError(t, err)
			require.Equal(t, tc.want, geo.WriteWKT(got))
		})
	}
}

func TestWGS84AntimeridianTopologyIsOperandOrderIndependent(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		name string
		fn   fEvalFn
		want bool
	}{
		{name: "intersects", fn: StIntersects, want: true},
		{name: "disjoint", fn: StDisjoint, want: false},
		{name: "overlaps", fn: StOverlaps, want: false},
		{name: "equals", fn: StEquals, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, pair := range [][2]string{{antimeridianOuter, antimeridianInner}, {antimeridianInner, antimeridianOuter}} {
				fc := NewFunctionTestCase(proc,
					wgs84GeometryInputs([]string{pair[0]}, []string{pair[1]}),
					NewFunctionTestResult(types.T_bool.ToType(), false, []bool{tc.want}, []bool{false}), tc.fn)
				ok, info := fc.Run()
				require.True(t, ok, info)
			}
		})
	}
}

func TestWGS84TopologyIsStableUnderUnevenVertexDensification(t *testing.T) {
	point := encodeGeometryPayload("POINT(-80 0)", 0, false)
	for _, line := range []string{
		"LINESTRING(-80 0,82 0)",
		"LINESTRING(-80 0,80 0,81 0,82 0)",
	} {
		t.Run(line, func(t *testing.T) {
			line := encodeGeometryPayload(line, 0, false)
			got, err := geometryPredicateBySRID(geo.SRIDWGS84, line, point, geometryIntersects)
			require.NoError(t, err)
			require.True(t, got)
		})
	}
}

func TestWGS84AntimeridianGeometry32AndMaskedRows(t *testing.T) {
	proc := testutil.NewProcess(t)
	g32 := func(wkt string) string {
		g, err := geo.ParseWKT(wkt)
		require.NoError(t, err)
		payload, err := geo.WriteWKBFloat32(g)
		require.NoError(t, err)
		return string(payload)
	}
	geometry32 := types.T_geometry32.ToType()
	geometry32.Width = int32(geo.SRIDWGS84 + 1)

	contains := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(geometry32, []string{g32(antimeridianOuter)}, []bool{false}),
			NewFunctionTestInput(geometry32, []string{g32(antimeridianInner)}, []bool{false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{true}, []bool{false}), StContains)
	ok, info := contains.Run()
	require.True(t, ok, info)

	intersection := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(geometry32, []string{g32(antimeridianOuter)}, []bool{false}),
			NewFunctionTestInput(geometry32, []string{g32(antimeridianInner)}, []bool{false}),
		},
		NewFunctionTestResult(types.T_geometry32.ToType(), false, []string{"POLYGON((179.5 0.5,179.5 -0.5,-179.5 -0.5,-179.5 0.5,179.5 0.5))"}, []bool{false}), StIntersection)
	ok, info = intersection.Run()
	require.True(t, ok, info)
	_, err := geo.ReadWKBFloat32(intersection.GetResultVectorDirectly().GetBytesAt(0))
	require.NoError(t, err, "intersection should preserve GEOMETRY32 encoding")

	masked := NewFunctionTestCase(proc,
		[]FunctionTestInput{
			NewFunctionTestInput(geometry32, []string{"POINT(181 0)", g32(antimeridianOuter)}, []bool{false, false}),
			NewFunctionTestInput(geometry32, []string{g32(antimeridianInner), g32(antimeridianInner)}, []bool{false, false}),
		},
		NewFunctionTestResult(types.T_bool.ToType(), false, []bool{false, true}, []bool{true, false}), StIntersects).
		WithSelectList(&FunctionSelectList{AnyNull: true, SelectList: []bool{false, true}})
	ok, info = masked.Run()
	require.True(t, ok, info)
}

func TestWGS84AntimeridianRejectsOutOfRangeCoordinates(t *testing.T) {
	proc := testutil.NewProcess(t)
	fc := NewFunctionTestCase(proc,
		wgs84GeometryInputs([]string{"POINT(181 0)", "POINT(0 0)"}, []string{"POINT(0 0)", "POINT(0 0)"}),
		NewFunctionTestResult(types.T_bool.ToType(), true, nil, nil), StIntersects)
	require.NoError(t, fc.result.PreExtendAndReset(2))
	err := StIntersects(fc.parameters, fc.result, proc, 2, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "SRID 4326 longitude")
}

func TestWGS84AntimeridianDoesNotApplyPlanarToleranceToNearbyPoints(t *testing.T) {
	left := encodeGeometryPayload("POINT(0 0)", 0, false)
	right := encodeGeometryPayload("POINT(0.00000001 0)", 0, false)

	for _, tc := range []struct {
		name      string
		predicate func([]byte, []byte) (bool, error)
	}{
		{name: "equals", predicate: geometryEquals},
		{name: "intersects", predicate: geometryIntersects},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := geometryPredicateBySRID(geo.SRIDWGS84, left, right, tc.predicate)
			require.NoError(t, err)
			require.False(t, got)
		})
	}
}

func TestWGS84AntimeridianPredicateToleranceScalesWithGeometry(t *testing.T) {
	for _, tc := range []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{
			name:  "long line keeps an on-line point",
			left:  "LINESTRING(30 -89.99,30 89.99)",
			right: "POINT(30 0)",
			want:  true,
		},
		{
			name:  "short line rejects an off-line point",
			left:  "LINESTRING(0 0,0.00001 0.00001)",
			right: "POINT(0.000005 0.000006)",
			want:  false,
		},
		{
			name:  "short lines preserve a real crossing",
			left:  "LINESTRING(0 0,0.00001 0.00001)",
			right: "LINESTRING(0 0.00001,0.00001 0)",
			want:  true,
		},
		{
			name:  "small polygon rejects a nearby exterior point",
			left:  "POLYGON((0 0,0.00001 0,0.00001 0.00001,0 0.00001,0 0))",
			right: "POINT(0.000005 0.000011)",
			want:  false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			left := encodeGeometryPayload(tc.left, 0, false)
			right := encodeGeometryPayload(tc.right, 0, false)
			got, err := geometryPredicateBySRID(geo.SRIDWGS84, left, right, geometryIntersects)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
