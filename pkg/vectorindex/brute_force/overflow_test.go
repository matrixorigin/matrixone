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
func TestSearchRejectsOutOfDomainDistances(t *testing.T) {
	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)

	idx := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{{0}},
		Metric:    metric.Metric_L2Distance,
		Dimension: 1,
		Count:     1,
	}

	for _, limit := range []uint{1, 2} {
		keys := make([]int64, limit)
		dists := make([]float32, limit)
		err := idx.SearchFloat32(sqlproc, [][]float32{{2e19}},
			vectorindex.RuntimeConfig{Limit: limit, NThreads: 1}, keys, dists)
		require.Error(t, err, "limit=%d", limit)
	}

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

	// BF16 carries float32's exponent range, so its kernels reach +Inf where float32 does. They
	// carry no per-distance check -- an out-of-domain candidate cannot win a min-comparison, so
	// the contract is enforced here, once, on the returned scores.
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
	bferr := idxbf.SearchFloat32(sqlproc, [][]types.BF16{{bfZero, bfZero}},
		vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}, kbf, dbf)
	require.Error(t, bferr)
	require.Contains(t, bferr.Error(), "overflows the element domain")

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
