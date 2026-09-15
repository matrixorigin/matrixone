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

	"github.com/stretchr/testify/require"
)

func requireContainsWithin(t *testing.T, containerWKT, candidateWKT string, want bool) {
	t.Helper()
	container := encodeGeometryPayload(containerWKT, 0, false)
	candidate := encodeGeometryPayload(candidateWKT, 0, false)

	contains, err := geometryContains(container, candidate)
	require.NoError(t, err)
	require.Equal(t, want, contains, "ST_CONTAINS(%q,%q)", containerWKT, candidateWKT)

	within, err := geometryWithin(candidate, container)
	require.NoError(t, err)
	require.Equal(t, contains, within, "ST_WITHIN must be the inverse of ST_CONTAINS")
}

func requireCrossesBoth(t *testing.T, leftWKT, rightWKT string, want bool) {
	t.Helper()
	left := encodeGeometryPayload(leftWKT, 0, false)
	right := encodeGeometryPayload(rightWKT, 0, false)

	got, err := geometryCrosses(left, right)
	require.NoError(t, err)
	require.Equal(t, want, got, "ST_CROSSES(%q,%q)", leftWKT, rightWKT)

	got, err = geometryCrosses(right, left)
	require.NoError(t, err)
	require.Equal(t, want, got, "ST_CROSSES must be symmetric for %q and %q", leftWKT, rightWKT)
}

func TestIssue28184PointSetContainsSemantics(t *testing.T) {
	polygon := "POLYGON((0 0,4 0,4 4,0 4,0 0))"
	line := "LINESTRING(0 0,4 0)"
	multiLine := "MULTILINESTRING((0 0,2 0),(4 0,6 0))"
	multiPolygon := "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((4 0,6 0,6 2,4 2,4 0)))"
	polygonWithHole := "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"

	cases := []struct {
		name      string
		container string
		candidate string
		want      bool
	}{
		{name: "issue boundary plus interior", container: polygon, candidate: "MULTIPOINT((0 1),(1 1))", want: true},
		{name: "interior point", container: polygon, candidate: "POINT(1 1)", want: true},
		{name: "boundary-only points", container: polygon, candidate: "MULTIPOINT((0 1),(4 1))", want: false},
		{name: "any exterior point rejects", container: polygon, candidate: "MULTIPOINT((1 1),(5 5))", want: false},
		{name: "two interior points", container: polygon, candidate: "MULTIPOINT((1 1),(3 3))", want: true},
		{name: "shell interior and hole boundary", container: polygonWithHole, candidate: "MULTIPOINT((3 3),(1 1.5))", want: true},
		{name: "shell interior and hole interior", container: polygonWithHole, candidate: "MULTIPOINT((3 3),(1.5 1.5))", want: false},
		{name: "hole boundary and hole interior", container: polygonWithHole, candidate: "MULTIPOINT((1 1.5),(1.5 1.5))", want: false},
		{name: "line endpoint plus interior", container: line, candidate: "MULTIPOINT((0 0),(2 0))", want: true},
		{name: "line boundary only", container: line, candidate: "MULTIPOINT((0 0),(4 0))", want: false},
		{name: "line interior plus exterior", container: line, candidate: "MULTIPOINT((2 0),(5 0))", want: false},
		{name: "multiline points in separate components", container: multiLine, candidate: "MULTIPOINT((1 0),(5 0))", want: true},
		{name: "multiline interior and component gap", container: multiLine, candidate: "MULTIPOINT((1 0),(3 0))", want: false},
		{name: "shared multiline endpoint is interior", container: "MULTILINESTRING((0 0,1 0),(1 0,2 0))", candidate: "POINT(1 0)", want: true},
		{name: "odd multiline endpoint remains boundary", container: "MULTILINESTRING((1 0,0 0),(1 0,0 1),(1 0,2 1))", candidate: "POINT(1 0)", want: false},
		{name: "closed line has no boundary", container: "LINESTRING(0 0,2 0,1 2,0 0)", candidate: "POINT(0 0)", want: true},
		{name: "multipolygon points in separate components", container: multiPolygon, candidate: "MULTIPOINT((1 1),(5 1))", want: true},
		{name: "multipolygon component gap is exterior", container: multiPolygon, candidate: "MULTIPOINT((1 1),(3 1))", want: false},
		{name: "multipolygon boundary plus another component interior", container: multiPolygon, candidate: "MULTIPOINT((0 1),(5 1))", want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireContainsWithin(t, tc.container, tc.candidate, tc.want)
		})
	}
}

func TestIssue28185PointSetCrossesLineSemantics(t *testing.T) {
	line := "LINESTRING(0 0,2 0)"
	multiLine := "MULTILINESTRING((0 0,2 0),(4 0,6 0))"
	cases := []struct {
		name   string
		line   string
		points string
		want   bool
	}{
		{name: "single point in line interior is contained", line: line, points: "POINT(1 0)", want: false},
		{name: "all multipoint members in line interior are contained", line: line, points: "MULTIPOINT((0.5 0),(1.5 0))", want: false},
		{name: "boundary and interior members remain contained", line: line, points: "MULTIPOINT((0 0),(1 0))", want: false},
		{name: "interior and exterior members cross", line: line, points: "MULTIPOINT((1 0),(3 0))", want: true},
		{name: "endpoint and exterior have no interior intersection", line: line, points: "MULTIPOINT((0 0),(3 0))", want: false},
		{name: "all points on disjoint multiline components are contained", line: multiLine, points: "MULTIPOINT((1 0),(5 0))", want: false},
		{name: "multiline interior and component gap cross", line: multiLine, points: "MULTIPOINT((1 0),(3 0))", want: true},
		{name: "shared multiline endpoint is interior", line: "MULTILINESTRING((0 0,1 0),(1 0,2 0))", points: "POINT(1 0)", want: false},
		{name: "shared multiline endpoint and exterior cross", line: "MULTILINESTRING((0 0,1 0),(1 0,2 0))", points: "MULTIPOINT((1 0),(3 0))", want: true},
		{name: "odd multiline endpoint and exterior do not cross", line: "MULTILINESTRING((1 0,0 0),(1 0,0 1),(1 0,2 1))", points: "MULTIPOINT((1 0),(3 0))", want: false},
		{name: "closed line vertex is interior", line: "LINESTRING(0 0,2 0,1 2,0 0)", points: "POINT(0 0)", want: false},
		{name: "closed line interior and exterior cross", line: "LINESTRING(0 0,2 0,1 2,0 0)", points: "MULTIPOINT((0 0),(3 3))", want: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCrossesBoth(t, tc.points, tc.line, tc.want)
		})
	}
}

func TestIssue28185PointSetCrossesPolygonSemantics(t *testing.T) {
	polygon := "POLYGON((0 0,4 0,4 4,0 4,0 0))"
	polygonWithHole := "POLYGON((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"
	multiPolygon := "MULTIPOLYGON(((0 0,2 0,2 2,0 2,0 0)),((4 0,6 0,6 2,4 2,4 0)))"
	cases := []struct {
		name    string
		polygon string
		points  string
		want    bool
	}{
		{name: "one interior point is contained", polygon: polygon, points: "POINT(3 3)", want: false},
		{name: "all members in polygon interior are contained", polygon: polygon, points: "MULTIPOINT((2 2),(3 3))", want: false},
		{name: "interior and polygon exterior cross", polygon: polygon, points: "MULTIPOINT((3 3),(5 5))", want: true},
		{name: "interior and shell boundary are covered", polygon: polygon, points: "MULTIPOINT((3 3),(0 3))", want: false},
		{name: "boundary and exterior have no interior intersection", polygon: polygon, points: "MULTIPOINT((0 3),(5 5))", want: false},
		{name: "interior and hole boundary remain covered", polygon: polygonWithHole, points: "MULTIPOINT((3 3),(1 1.5))", want: false},
		{name: "interior and hole interior cross", polygon: polygonWithHole, points: "MULTIPOINT((3 3),(1.5 1.5))", want: true},
		{name: "hole boundary and hole interior have no polygon interior point", polygon: polygonWithHole, points: "MULTIPOINT((1 1.5),(1.5 1.5))", want: false},
		{name: "multipolygon interiors are all covered", polygon: multiPolygon, points: "MULTIPOINT((1 1),(5 1))", want: false},
		{name: "multipolygon interior and gap cross", polygon: multiPolygon, points: "MULTIPOINT((1 1),(3 1))", want: true},
		{name: "multipolygon interior and other component boundary are covered", polygon: multiPolygon, points: "MULTIPOINT((1 1),(4 1))", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCrossesBoth(t, tc.points, tc.polygon, tc.want)
		})
	}
}

func TestIssue28185CrossesEmptyPointOperands(t *testing.T) {
	polygon := "POLYGON((0 0,4 0,4 4,0 4,0 0))"
	cases := []struct {
		name  string
		left  string
		right string
	}{
		{name: "empty point and polygon", left: "POINT EMPTY", right: polygon},
		{name: "empty multipoint and polygon", left: "MULTIPOINT EMPTY", right: polygon},
		{name: "point and empty polygon", left: "POINT(1 1)", right: "POLYGON EMPTY"},
		{name: "empty point member with polygon", left: "MULTIPOINT(EMPTY,1 1)", right: polygon},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCrossesBoth(t, tc.left, tc.right, false)
		})
	}
}

func TestIssue28185CrossesPointSetsWithEmptyMembers(t *testing.T) {
	polygon := "POLYGON((0 0,4 0,4 4,0 4,0 0))"
	cases := []struct {
		name   string
		points string
		want   bool
	}{
		{name: "empty member does not turn contained set into crossing", points: "MULTIPOINT(EMPTY,1 1)", want: false},
		{name: "empty member does not hide exterior witness", points: "MULTIPOINT(EMPTY,1 1,5 5)", want: true},
		{name: "point-only collection skips empty point", points: "GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 1),POINT(5 5))", want: true},
		{name: "nested point-only collection skips empty members", points: "GEOMETRYCOLLECTION(MULTIPOINT EMPTY,GEOMETRYCOLLECTION(POINT EMPTY,POINT(1 1),POINT(5 5)))", want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCrossesBoth(t, tc.points, polygon, tc.want)
		})
	}
}

func TestIssue28185CrossesRejectsMalformedEmptyLookingPoints(t *testing.T) {
	line := encodeGeometryPayload("LINESTRING(0 0,2 0)", 0, false)
	polygon := encodeGeometryPayload("POLYGON((0 0,4 0,4 4,0 4,0 0))", 0, false)
	malformed := []struct {
		name      string
		payload   []byte
		container []byte
		reverse   bool
	}{
		{name: "POINT() before line", payload: []byte("POINT()"), container: line},
		{name: "POINT() after line", payload: []byte("POINT()"), container: line, reverse: true},
		{name: "empty-looking MultiPoint member before polygon", payload: []byte("MULTIPOINT(())"), container: polygon},
		{name: "empty-looking MultiPoint member after polygon", payload: []byte("MULTIPOINT(())"), container: polygon, reverse: true},
	}
	for _, tc := range malformed {
		t.Run(tc.name, func(t *testing.T) {
			left, right := tc.payload, tc.container
			if tc.reverse {
				left, right = right, left
			}
			_, err := geometryCrosses(left, right)
			require.Error(t, err)
		})
	}
}

func TestIssue28184PointOnlyGeometryCollectionContains(t *testing.T) {
	polygon := "POLYGON((0 0,2 0,2 2,0 2,0 0))"
	cases := []struct {
		name      string
		container string
		candidate string
		want      bool
	}{
		{name: "boundary and interior", container: polygon, candidate: "GEOMETRYCOLLECTION(POINT(0 1),GEOMETRYCOLLECTION(MULTIPOINT((1 1),(1.5 1.5))))", want: true},
		{name: "nested points with exterior", container: polygon, candidate: "GEOMETRYCOLLECTION(POINT(1 1),GEOMETRYCOLLECTION(POINT(3 3)))", want: false},
		{name: "nonpoint leaf prevents point-only fast path", container: polygon, candidate: "GEOMETRYCOLLECTION(POINT(1 1),LINESTRING(3 3,4 4))", want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireContainsWithin(t, tc.container, tc.candidate, tc.want)
		})
	}
}

func TestIssue28185PointOnlyGeometryCollectionCrosses(t *testing.T) {
	polygon := "POLYGON((0 0,2 0,2 2,0 2,0 0))"
	line := "LINESTRING(0 0,2 0)"
	cases := []struct {
		name   string
		points string
		other  string
		want   bool
	}{
		{name: "nested collection has polygon interior and exterior", points: "GEOMETRYCOLLECTION(POINT(1 1),GEOMETRYCOLLECTION(MULTIPOINT((3 3))))", other: polygon, want: true},
		{name: "nested collection has line interior and exterior", points: "GEOMETRYCOLLECTION(POINT(1 0),GEOMETRYCOLLECTION(MULTIPOINT((3 0))))", other: line, want: true},
		{name: "point-only collection wholly inside does not cross", points: "GEOMETRYCOLLECTION(POINT(1 1),MULTIPOINT((1.5 1.5)))", other: polygon, want: false},
		{name: "mixed collection must not discard nonpoint leaf", points: "GEOMETRYCOLLECTION(POINT(1 1),POINT(3 3),LINESTRING(10 10,11 11))", other: polygon, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			requireCrossesBoth(t, tc.points, tc.other, tc.want)
		})
	}
}

func TestIssue28184PointSetPredicateOrderAndDuplicates(t *testing.T) {
	polygon := "POLYGON((0 0,4 0,4 4,0 4,0 0))"
	requireContainsWithin(t, polygon, "MULTIPOINT((0 2),(2 2))", true)
	requireContainsWithin(t, polygon, "MULTIPOINT((2 2),(0 2),(2 2))", true)
	requireCrossesBoth(t, "MULTIPOINT((2 2),(5 5))", polygon, true)
	requireCrossesBoth(t, "MULTIPOINT((5 5),(2 2),(2 2))", polygon, true)
}
