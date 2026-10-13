// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package overfetch

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPostFilterFactor(t *testing.T) {
	require.Equal(t, 5.0, PostFilterFactor(0))
	require.Equal(t, 5.0, PostFilterFactor(9))
	require.Equal(t, 2.0, PostFilterFactor(10))
	require.Equal(t, 2.0, PostFilterFactor(49))
	require.Equal(t, 1.5, PostFilterFactor(50))
	require.Equal(t, 1.3, PostFilterFactor(100))
	require.Equal(t, 1.2, PostFilterFactor(200))
	require.Equal(t, 1.2, PostFilterFactor(math.MaxUint64))
}

func TestFilteredPostModeFactor(t *testing.T) {
	require.Equal(t, 5.0, FilteredPostModeFactor(0))
	require.Equal(t, 5.0, FilteredPostModeFactor(49))
	require.Equal(t, 2.0, FilteredPostModeFactor(50))
	require.Equal(t, 1.5, FilteredPostModeFactor(100))
	require.Equal(t, 1.3, FilteredPostModeFactor(200))
	require.Equal(t, 1.3, FilteredPostModeFactor(math.MaxUint64))
}

func TestLimit(t *testing.T) {
	// zero stays zero (no pushed limit).
	require.Equal(t, uint64(0), Limit(0, 5))
	// factor < 1 clamps to 1, floor still adds 10.
	require.Equal(t, uint64(15), Limit(5, 0.5))
	// small k: max(k*factor, k+10) -> floor dominates.
	require.Equal(t, uint64(15), Limit(5, 1))
	require.Equal(t, uint64(20), Limit(10, 2))
	// large k: multiplier dominates the +10 floor.
	require.Equal(t, uint64(240), Limit(200, 1.2))
	// saturation instead of overflow.
	require.Equal(t, uint64(math.MaxUint64), Limit(math.MaxUint64, 1.2))
	require.Equal(t, uint64(math.MaxUint64), Limit(math.MaxUint64-5, 1))
}

// Relocated from pkg/sql/plan/apply_indices_test.go: exact post-filter fetch
// counts (factor applied, truncated) across every bucket boundary.
func TestPostFilterFactorActualFetch(t *testing.T) {
	for _, tc := range []struct {
		limit uint64
		fetch uint64
	}{
		{3, 15}, {5, 25}, {10, 20}, // 5x then 2x at the <10 boundary
		{20, 40}, {30, 60}, {49, 98}, {50, 75}, // 2x then 1.5x at <50
		{80, 120}, {99, 148}, {100, 130}, // 1.5x then 1.3x at <100
		{150, 195}, {199, 258}, {200, 240}, // 1.3x then 1.2x at <200
		{250, 300}, {500, 600}, {1000, 1200}, // 1.2x tail
	} {
		got := uint64(float64(tc.limit) * PostFilterFactor(tc.limit))
		require.Equalf(t, tc.fetch, got, "limit %d", tc.limit)
	}
}

// The factor is monotonically non-increasing as the limit grows.
func TestPostFilterFactorMonotonic(t *testing.T) {
	prev := 10.0
	for _, k := range []uint64{1, 5, 10, 20, 50, 100, 200, 500, 1000} {
		f := PostFilterFactor(k)
		require.LessOrEqualf(t, f, prev, "limit %d", k)
		require.GreaterOrEqual(t, f, 1.0)
		prev = f
	}
}

func TestConvenienceLimits(t *testing.T) {
	// k=2 -> PostFilter factor 5x -> max(10, 12) = 12.
	require.Equal(t, uint64(12), PostFilterLimit(2))
	// k=2 -> FilteredPostMode factor 5x -> max(10, 12) = 12.
	require.Equal(t, uint64(12), FilteredPostModeLimit(2))
	// k=100 -> PostFilter 1.3x -> 130 ; FilteredPostMode 1.5x -> 150.
	require.Equal(t, uint64(130), PostFilterLimit(100))
	require.Equal(t, uint64(150), FilteredPostModeLimit(100))
	// over-fetch always grows the budget.
	for _, k := range []uint64{1, 3, 7, 25, 64, 128, 512, 4096} {
		require.GreaterOrEqual(t, PostFilterLimit(k), k)
		require.GreaterOrEqual(t, FilteredPostModeLimit(k), k)
	}
}
