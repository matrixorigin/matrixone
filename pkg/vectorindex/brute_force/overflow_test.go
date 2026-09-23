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

package brute_force

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// A candidate distance that leaves the element domain must not leave the winner unassigned.
// minIdx starts at -1 and only moves on `dist < minDist`, which +Inf never satisfies, so every
// candidate overflowing used to return key -1 with no error -- and ProductL2 feeds that key
// straight into vector.UnionOne, which has no row -1.
//
// The negative-index guard is what this asserts. The finite check on the returned SCORES no
// longer lives here: it sat in one entry point but not the other, so the same index validated or
// not depending on which was called. It now sits at each native boundary instead, and no consumer
// of this package reads its distances (ivfflat probes for ids, ProductL2 for the winning key).
func TestSearchRejectsOutOfDomainDistances(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{0}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     1,
	}

	// limit=1 takes the min-scan, where an all-out-of-domain query leaves minIdx at -1: it must
	// fail rather than hand back a key. limit>1 takes the heap, which assigns a real row index
	// whatever its distance, so it returns that row with no error.
	keys2 := make([]int64, 2)
	dists2 := make([]float32, 2)
	require.NoError(t, idx.SearchFloat32(sqlproc, [][]float32{{2e19}},
		vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}, keys2, dists2))
	// The heap fills slots back to front, so a slot with no candidate pads at the FRONT: with one
	// dataset row and limit=2 the result is [-1, 0], not [0, -1].
	require.EqualValues(t, -1, keys2[0], "padding for a slot with no candidate")
	require.EqualValues(t, 0, keys2[1], "the one dataset row")

	// limit=1 is the shape ProductL2 uses, and -1 there would be a winner it feeds to
	// vector.UnionOne. (Above limit=1 a -1 is the documented padding for a slot with no candidate,
	// which is why only this shape asserts the index.)
	keys := make([]int64, 1)
	dists := make([]float32, 1)
	require.Error(t, idx.SearchFloat32(sqlproc, [][]float32{{2e19}},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, keys, dists))
	require.GreaterOrEqual(t, keys[0], int64(0), "-1 is not a row index")

	// float64 centroids take the same path -- this is what ProductL2 builds for an f64 index.
	idx64 := &GoBruteForceIndex[float64, float64]{
		Dataset:   [][]float64{{0}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     1,
	}
	keys64 := make([]int64, 1)
	dists64 := make([]float32, 1)
	require.Error(t, idx64.SearchFloat32(sqlproc, [][]float64{{2e200}},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, keys64, dists64))
	require.GreaterOrEqual(t, keys64[0], int64(0))

	// BF16 carries float32's exponent range, so its kernels reach +Inf where float32 does. An
	// out-of-domain candidate still cannot win a min-comparison, so it changes no ranking.
	bfBig := types.BF16FromFloat32(2e19)
	bfZero := types.BF16FromFloat32(0)
	bf3 := types.BF16FromFloat32(3)
	bf4 := types.BF16FromFloat32(4)
	// Row 0 wins on distance, so the negative-index guard does not fire -- row 1's out-of-domain
	// distance is returned as the second score. That is what the result check catches.
	idxbf := &GoBruteForceIndex[types.BF16, float32]{
		Dataset:   [][]types.BF16{{bf3, bf4}, {bfBig, bfBig}},
		Metric:    metric.Metric_L2sqDistance,
		Dimension: 2,
		Count:     2,
	}
	kbf := make([]int64, 2)
	dbf := make([]float32, 2)
	require.NoError(t, idxbf.SearchFloat32(sqlproc, [][]types.BF16{{bfZero, bfZero}},
		vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}, kbf, dbf))
	require.GreaterOrEqual(t, kbf[0], int64(0), "both slots have a real candidate")
	require.GreaterOrEqual(t, kbf[1], int64(0), "an out-of-domain distance is still a real index")

	// an ordinary bf16 dataset still answers
	idxbf2 := &GoBruteForceIndex[types.BF16, float32]{
		Dataset:   [][]types.BF16{{bf3, bf4}},
		Metric:    metric.Metric_L2sqDistance,
		Dimension: 2,
		Count:     1,
	}
	k1 := make([]int64, 1)
	d1 := make([]float32, 1)
	require.NoError(t, idxbf2.SearchFloat32(sqlproc, [][]types.BF16{{bfZero, bfZero}},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, k1, d1))
	require.EqualValues(t, 25, d1[0])

	// Ordinary magnitudes still resolve to the nearest row.
	idx2 := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{0}, {10}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     2,
	}
	okKeys := make([]int64, 1)
	okDists := make([]float32, 1)
	require.NoError(t, idx2.SearchFloat32(sqlproc, [][]float32{{9}},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, okKeys, okDists))
	require.EqualValues(t, 1, okKeys[0])
	require.EqualValues(t, 1, okDists[0])
}
