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
	"math"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

func TestLineRefRejectsNonFiniteParameters(t *testing.T) {
	lines := []struct {
		name string
		line LineString
	}{
		{name: "empty", line: LineString{}},
		{name: "singleton", line: LineString{Points: []Coord{{X: 2, Y: 3}}}},
		{name: "ordinary", line: LineString{Points: []Coord{{X: 0, Y: 0}, {X: 10, Y: 0}}}},
	}
	values := []struct {
		name  string
		value float64
	}{
		{name: "NaN", value: math.NaN()},
		{name: "+Inf", value: math.Inf(1)},
		{name: "-Inf", value: math.Inf(-1)},
	}

	for _, line := range lines {
		for _, value := range values {
			t.Run("point/"+line.name+"/"+value.name, func(t *testing.T) {
				got, err := InterpolatePoint(line.line, value.value)
				require.Equal(t, Point{}, got)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.EqualError(t, err, "invalid input: ST_LineInterpolatePoint: fraction must be finite")
			})

			// Without the finite-input guard, NaN on a nonempty line makes the
			// generation loop allocate indefinitely. Keep that hazardous case in
			// the externally bounded probe; the empty-line call remains a direct
			// admission oracle, and infinities terminate via the range check.
			if !math.IsNaN(value.value) || len(line.line.Points) == 0 {
				t.Run("points/"+line.name+"/"+value.name, func(t *testing.T) {
					got, err := InterpolatePoints(line.line, value.value)
					require.Nil(t, got)
					require.Error(t, err)
					require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
					require.EqualError(t, err, "invalid input: ST_LineInterpolatePoints: fraction must be finite")
				})
			}

			t.Run("distance/"+line.name+"/"+value.name, func(t *testing.T) {
				got, err := PointAtDistance(line.line, value.value)
				require.Equal(t, Point{}, got)
				require.Error(t, err)
				require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
				require.EqualError(t, err, "invalid input: ST_PointAtDistance: distance must be finite")
			})
		}
	}
}

func TestInterpolatePoint(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	for _, tc := range []struct {
		name string
		f    float64
		want string
	}{
		{name: "clamp below zero", f: -1, want: "POINT(0 0)"},
		{name: "negative zero", f: math.Copysign(0, -1), want: "POINT(0 0)"},
		{name: "zero", f: 0, want: "POINT(0 0)"},
		{name: "middle", f: 0.5, want: "POINT(5 0)"},
		{name: "one", f: 1, want: "POINT(10 0)"},
		{name: "clamp above one", f: 2, want: "POINT(10 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := InterpolatePoint(l, tc.f)
			require.NoError(t, err)
			require.Equal(t, tc.want, WriteWKT(p))
		})
	}
	p, err := InterpolatePoint(l, math.SmallestNonzeroFloat64)
	require.NoError(t, err)
	require.False(t, math.IsNaN(p.X))
	require.False(t, math.IsInf(p.X, 0))
	require.Greater(t, p.X, 0.0)
	require.Equal(t, 0.0, p.Y)

	// Across two segments: total length 20, 75% -> (10, 5).
	l2 := wkt(t, "LINESTRING(0 0, 10 0, 10 10)").(LineString)
	p2, err := InterpolatePoint(l2, 0.75)
	require.NoError(t, err)
	require.Equal(t, "POINT(10 5)", WriteWKT(p2))

	singleton := LineString{Points: []Coord{{X: 2, Y: 3}}}
	p3, err := InterpolatePoint(singleton, 2)
	require.NoError(t, err)
	require.Equal(t, "POINT(2 3)", WriteWKT(p3))

	degenerate := wkt(t, "LINESTRING(1 2, 1 2)").(LineString)
	p4, err := InterpolatePoint(degenerate, 0.5)
	require.NoError(t, err)
	require.Equal(t, "POINT(1 2)", WriteWKT(p4))

	_, err = InterpolatePoint(LineString{}, 0.5)
	require.EqualError(t, err, "invalid input: ST_LineInterpolatePoint: empty line")
}

func TestInterpolatePoints(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	g, err := InterpolatePoints(l, 0.25)
	require.NoError(t, err)
	require.Equal(t, "MULTIPOINT(2.5 0,5 0,7.5 0,10 0)", WriteWKT(g))

	g2, err := InterpolatePoints(l, 1.0)
	require.NoError(t, err)
	require.Equal(t, "POINT(10 0)", WriteWKT(g2))

	g3, err := InterpolatePoints(l, 0.3)
	require.NoError(t, err)
	require.Equal(t, "MULTIPOINT(3 0,6 0,9 0,10 0)", WriteWKT(g3))

	for _, f := range []float64{math.Copysign(0, -1), 0, -0.25, 1.25} {
		got, err := InterpolatePoints(l, f)
		require.Nil(t, got)
		require.EqualError(t, err, "invalid input: ST_LineInterpolatePoints: fraction must be in (0, 1]")
	}

	singleton := LineString{Points: []Coord{{X: 2, Y: 3}}}
	g4, err := InterpolatePoints(singleton, 0.5)
	require.NoError(t, err)
	require.Equal(t, "MULTIPOINT(2 3,2 3)", WriteWKT(g4))
	g5, err := InterpolatePoints(singleton, 1)
	require.NoError(t, err)
	require.Equal(t, "POINT(2 3)", WriteWKT(g5))

	degenerate := wkt(t, "LINESTRING(1 2, 1 2)").(LineString)
	g6, err := InterpolatePoints(degenerate, 0.5)
	require.NoError(t, err)
	require.Equal(t, "MULTIPOINT(1 2,1 2)", WriteWKT(g6))

	got, err := InterpolatePoints(LineString{}, 0.5)
	require.Nil(t, got)
	require.EqualError(t, err, "invalid input: ST_LineInterpolatePoint: empty line")
}

func TestPointAtDistance(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	p, err := PointAtDistance(l, 3)
	require.NoError(t, err)
	require.Equal(t, "POINT(3 0)", WriteWKT(p))

	for _, tc := range []struct {
		name string
		dist float64
		want string
	}{
		{name: "negative zero", dist: math.Copysign(0, -1), want: "POINT(0 0)"},
		{name: "zero", dist: 0, want: "POINT(0 0)"},
		{name: "length", dist: 10, want: "POINT(10 0)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := PointAtDistance(l, tc.dist)
			require.NoError(t, err)
			require.Equal(t, tc.want, WriteWKT(got))
		})
	}

	for _, dist := range []float64{-1, 11} {
		got, err := PointAtDistance(l, dist)
		require.Equal(t, Point{}, got)
		require.EqualError(t, err, "invalid input: ST_PointAtDistance: distance is out of range")
	}

	singleton := LineString{Points: []Coord{{X: 2, Y: 3}}}
	p2, err := PointAtDistance(singleton, math.Copysign(0, -1))
	require.NoError(t, err)
	require.Equal(t, "POINT(2 3)", WriteWKT(p2))
	_, err = PointAtDistance(singleton, math.SmallestNonzeroFloat64)
	require.EqualError(t, err, "invalid input: ST_PointAtDistance: distance is out of range")

	degenerate := wkt(t, "LINESTRING(1 2, 1 2)").(LineString)
	p3, err := PointAtDistance(degenerate, 0)
	require.NoError(t, err)
	require.Equal(t, "POINT(1 2)", WriteWKT(p3))

	_, err = PointAtDistance(LineString{}, 0)
	require.EqualError(t, err, "invalid input: ST_PointAtDistance: empty line")
}
