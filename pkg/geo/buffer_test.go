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

	"github.com/stretchr/testify/require"
)

func TestBufferPoint(t *testing.T) {
	// A point buffer is the disc polygon; area ~ pi r^2 (slightly less).
	g := wkt(t, "POINT(0 0)")
	b, err := Buffer(g, 1.0, 8) // 32 segments
	require.NoError(t, err)
	require.Equal(t, POLYGON, b.Type())
	area := CartesianArea(b)
	// 32-gon area = 0.5*n*sin(2pi/n) = 3.1214; within ~1% of pi.
	require.InDelta(t, math.Pi, area, 0.05)
}

func TestBufferLine(t *testing.T) {
	// A 10-long segment buffered by 1 -> ~ stadium: 2*r*len + pi*r^2.
	g := wkt(t, "LINESTRING(0 0, 10 0)")
	b, err := Buffer(g, 1.0, 16) // 64 segments for the caps
	require.NoError(t, err)
	area := CartesianArea(b)
	want := 2*1.0*10 + math.Pi*1*1
	require.InDelta(t, want, area, 0.1)
}

func TestBufferPolygonGrows(t *testing.T) {
	// Buffering a square by r grows its area beyond the original.
	g := wkt(t, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))")
	b, err := Buffer(g, 1.0, 16)
	require.NoError(t, err)
	area := CartesianArea(b)
	// Exact grown area: 100 + perimeter*r + pi*r^2 = 100 + 40 + pi.
	require.InDelta(t, 100+40+math.Pi, area, 0.2)
}

func TestBufferZeroAndNegative(t *testing.T) {
	g := wkt(t, "POINT(1 2)")
	same, err := Buffer(g, 0, 8)
	require.NoError(t, err)
	require.Equal(t, "POINT(1 2)", WriteWKT(same))

	_, err = Buffer(g, -1, 8)
	require.Error(t, err)
}
