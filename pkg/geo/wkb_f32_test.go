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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

type unsupportedGeometry struct{}

func (unsupportedGeometry) Type() Subtype { return POINT }
func (unsupportedGeometry) Empty() bool   { return false }

func mustWriteWKBFloat32(t *testing.T, g Geometry) []byte {
	t.Helper()
	b, err := WriteWKBFloat32(g)
	require.NoError(t, err)
	return b
}

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
			b := mustWriteWKBFloat32(t, g1)
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
	require.Len(t, mustWriteWKBFloat32(t, g), 13)
	require.Len(t, WriteWKB(g), 21)
}

// A coordinate not exactly representable in float32 loses precision, and the
// round-trip value equals the float32-rounded value (not the original float64).
func TestWKBFloat32PrecisionLoss(t *testing.T) {
	g1 := Point{X: 0.1, Y: 0.2}
	b := mustWriteWKBFloat32(t, g1)
	g2, err := ReadWKBFloat32(b)
	require.NoError(t, err)

	p := g2.(Point)
	require.Equal(t, float64(float32(0.1)), p.X)
	require.Equal(t, float64(float32(0.2)), p.Y)
	require.NotEqual(t, 0.1, p.X)
}

func TestWKBFloat32RejectsNonFiniteOrNarrowingOverflow(t *testing.T) {
	for _, tc := range []struct {
		name string
		geom Geometry
	}{
		{name: "point", geom: Point{X: 3.5e38, Y: 1}},
		{name: "line", geom: LineString{Points: []Coord{{X: 0, Y: 0}, {X: 1, Y: -3.5e38}}}},
		{name: "polygon", geom: Polygon{Rings: [][]Coord{{{X: 0, Y: 0}, {X: 3.5e38, Y: 0}, {X: 0, Y: 0}}}}},
		{name: "multipoint", geom: MultiPoint{Points: []Point{{X: 1, Y: math.Inf(1)}}}},
		{name: "multiline", geom: MultiLineString{Lines: []LineString{{Points: []Coord{{X: math.NaN(), Y: 0}}}}}},
		{name: "multipolygon", geom: MultiPolygon{Polygons: []Polygon{{Rings: [][]Coord{{{X: math.Inf(-1), Y: 0}}}}}}},
		{name: "nested collection", geom: GeometryCollection{Geometries: []Geometry{Point{X: 0, Y: 0}, GeometryCollection{Geometries: []Geometry{MultiPoint{Points: []Point{{X: 1, Y: -3.5e38}}}}}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := WriteWKBFloat32(tc.geom)
			require.Nil(t, out)
			require.EqualError(t, err, "invalid input: geometry coordinate is not finite in GEOMETRY32")
		})
	}
}

func TestWKBFloat32RejectsInvalidGeometryAndRecursion(t *testing.T) {
	var typedNil *Point
	for _, tc := range []struct {
		name string
		geom Geometry
	}{
		{name: "nil", geom: nil},
		{name: "typed nil", geom: typedNil},
		{name: "unsupported", geom: unsupportedGeometry{}},
		{name: "nil collection child", geom: GeometryCollection{Geometries: []Geometry{nil}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, err := WriteWKBFloat32(tc.geom)
			require.Nil(t, out)
			require.Error(t, err)
		})
	}

	atLimit := Geometry(GeometryCollection{})
	for range maxGeometryNestingDepth - 1 {
		atLimit = GeometryCollection{Geometries: []Geometry{atLimit}}
	}
	_, err := WriteWKBFloat32(atLimit)
	require.NoError(t, err)

	deep := GeometryCollection{Geometries: []Geometry{atLimit}}
	out, err := WriteWKBFloat32(deep)
	require.Nil(t, out)
	require.ErrorContains(t, err, "geometry collection nesting depth exceeds")

	children := make([]Geometry, 1)
	cycle := GeometryCollection{Geometries: children}
	children[0] = cycle
	out, err = WriteWKBFloat32(cycle)
	require.Nil(t, out)
	require.ErrorContains(t, err, "geometry collection nesting depth exceeds")
}

func TestWKBFloat32AllowsEmptyFiniteBoundaryAndReuse(t *testing.T) {
	empty := mustWriteWKBFloat32(t, Point{IsEmpty: true, X: math.Inf(1), Y: math.NaN()})
	require.Equal(t, uint32(0x7fc00000), binary.LittleEndian.Uint32(empty[5:9]))
	require.Equal(t, uint32(0x7fc00000), binary.LittleEndian.Uint32(empty[9:13]))

	boundary := float64(math.MaxFloat32)
	justAbove := math.Nextafter(boundary, math.Inf(1))
	b := mustWriteWKBFloat32(t, Point{X: justAbove, Y: -boundary})
	for _, off := range []int{5, 9} {
		bits := binary.LittleEndian.Uint32(b[off : off+4])
		require.NotEqual(t, uint32(0x7f800000), bits&0x7f800000, "finite input emitted non-finite IEEE-754 bits")
	}

	out, err := WriteWKBFloat32(Point{X: 3.5e38, Y: 0})
	require.Nil(t, out)
	require.Error(t, err)
	require.Equal(t, b, mustWriteWKBFloat32(t, Point{X: justAbove, Y: -boundary}))
}

// WKBFloat32ToStandard yields standard float64 WKB equal to encoding the
// float32-rounded geometry directly.
func TestWKBFloat32ToStandard(t *testing.T) {
	g1 := Point{X: 0.1, Y: 0.2}
	std, err := WKBFloat32ToStandard(mustWriteWKBFloat32(t, g1))
	require.NoError(t, err)

	rounded := Point{X: float64(float32(0.1)), Y: float64(float32(0.2))}
	require.Equal(t, WriteWKB(rounded), std)

	// And the result is valid standard WKB.
	back, err := ReadWKB(std)
	require.NoError(t, err)
	require.Equal(t, rounded, back)
}
