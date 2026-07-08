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

func TestHausdorffDistance(t *testing.T) {
	// Two parallel segments 1 unit apart.
	a := wkt(t, "LINESTRING(0 0, 10 0)")
	b := wkt(t, "LINESTRING(0 1, 10 1)")
	d, ok := HausdorffDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, 1.0, d, 1e-9)

	// Identical geometries -> 0.
	d2, ok := HausdorffDistance(a, a)
	require.True(t, ok)
	require.InDelta(t, 0.0, d2, 1e-9)
}

func TestFrechetDistance(t *testing.T) {
	a := wkt(t, "LINESTRING(0 0, 10 0)")
	b := wkt(t, "LINESTRING(0 1, 10 1)")
	d, ok := FrechetDistance(a, b)
	require.True(t, ok)
	require.InDelta(t, 1.0, d, 1e-9)

	// A perpendicular offset at one end raises the Fréchet distance.
	c := wkt(t, "LINESTRING(0 0, 10 0)")
	e := wkt(t, "LINESTRING(0 0, 10 5)")
	d2, ok := FrechetDistance(c, e)
	require.True(t, ok)
	require.InDelta(t, 5.0, d2, 1e-9)
}
