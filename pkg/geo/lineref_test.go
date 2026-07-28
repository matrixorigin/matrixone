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

func TestInterpolatePoint(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	p, err := InterpolatePoint(l, 0.5)
	require.NoError(t, err)
	require.Equal(t, "POINT(5 0)", WriteWKT(p))

	// Across two segments: total length 20, 75% -> (10, 5).
	l2 := wkt(t, "LINESTRING(0 0, 10 0, 10 10)").(LineString)
	p2, err := InterpolatePoint(l2, 0.75)
	require.NoError(t, err)
	require.Equal(t, "POINT(10 5)", WriteWKT(p2))
}

func TestInterpolatePoints(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	g, err := InterpolatePoints(l, 0.25)
	require.NoError(t, err)
	require.Equal(t, "MULTIPOINT(2.5 0,5 0,7.5 0,10 0)", WriteWKT(g))

	g2, err := InterpolatePoints(l, 1.0)
	require.NoError(t, err)
	require.Equal(t, "POINT(10 0)", WriteWKT(g2))

	_, err = InterpolatePoints(l, 0)
	require.Error(t, err)
}

func TestPointAtDistance(t *testing.T) {
	l := wkt(t, "LINESTRING(0 0, 10 0)").(LineString)
	p, err := PointAtDistance(l, 3)
	require.NoError(t, err)
	require.Equal(t, "POINT(3 0)", WriteWKT(p))

	_, err = PointAtDistance(l, 11)
	require.Error(t, err)
}
