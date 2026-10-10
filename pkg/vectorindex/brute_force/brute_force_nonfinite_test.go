//go:build (amd64 || arm64) && go1.27 && goexperiment.simd

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
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// A candidate whose inner product overflows on the SIMD lanes (products finite, but
// per-lane partial sums reach +Inf and -Inf before the reduction cancels them) used
// to compute as NaN. NaN is unordered: it never compares as the max a top-k heap
// evicts, so it was retained in a slot and dropped a genuinely-nearer finite
// candidate -- a silent wrong result the final-score check could not catch. The metric
// now RECOVERS the in-order reference: cand0's products cancel in source order to a dot of
// 0, so it scores a finite distance 0 -- its true value -- correctly farther than the two
// negative-distance candidates, and never a NaN that corrupts the heap. The two finite
// candidates are selected correctly. This is the wrong-winner-prevention for #29496.
func TestBruteForceExcludesOverflowCandidate29496(t *testing.T) {
	const dim = 32
	mag := float32(1 << 63)
	query := make([]float32, dim)
	cand0 := make([]float32, dim) // alternating +/-mag: dot cancels to 0, but SIMD lanes overflow
	cand1 := make([]float32, dim) // [1,0,...]:   dot = mag,  distance -mag   (finite, near)
	cand2 := make([]float32, dim) // [1,1,0,...]: dot = 2*mag, distance -2*mag (finite, nearest)
	for i := range query {
		query[i] = mag
		if i%2 == 0 {
			cand0[i] = mag
		} else {
			cand0[i] = -mag
		}
	}
	cand1[0] = 1
	cand2[0], cand2[1] = 1, 1

	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	idx := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{cand0, cand1, cand2},
		Metric:    metric.Metric_InnerProduct,
		Dimension: dim,
		Count:     3,
	}

	keys := make([]int64, 2)
	dists := make([]float32, 2)
	require.NoError(t, idx.SearchFloat32(sqlproc, [][]float32{query},
		vectorindex.RuntimeConfig{Limit: 2, NThreads: 1}, keys, dists))

	got := []int64{keys[0], keys[1]}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	require.Equalf(t, []int64{1, 2}, got,
		"top-2 must be the finite candidates {1,2}; the overflowing candidate 0 must be excluded, got keys=%v dists=%v", keys, dists)
}

// TestBruteForceRecoversLostWinner29496 is the public SELECTION oracle for the lost-winner case
// the exclude-test above cannot see. There the other candidates had NEGATIVE distances, so the
// cancellation candidate loses whether it computes as the correct 0 or the wrong +Inf. Here the
// competitor has a POSITIVE distance, so the cancellation candidate's CORRECT distance (0) makes
// it the single nearest. Witness: query is 128 copies of 2^63; candidate 0 alternates +/-2^63
// (every product and source-order partial sum finite, dot cancels to 0 -> distance 0); candidate
// 1 is [-1,0,...] (dot -2^63 -> distance +2^63). If the NEON lane cancellation is not recovered
// (maps to +Inf), key 0 is wrongly dropped and key 1 wins with a finite score 2^63 -- a silent
// wrong result CheckFiniteDists cannot catch. With the metric owner recovering the in-order
// reference, candidate 0 scores 0 and correctly wins (#29496).
func TestBruteForceRecoversLostWinner29496(t *testing.T) {
	const dim = 128
	mag := float32(1 << 63)
	query := make([]float32, dim)
	cand0 := make([]float32, dim) // alternating +/-mag: dot cancels to 0 -> distance 0 (nearest)
	cand1 := make([]float32, dim) // [-1,0,...]: dot = -mag -> distance +mag (farther, but finite)
	for i := range query {
		query[i] = mag
		if i%2 == 0 {
			cand0[i] = mag
		} else {
			cand0[i] = -mag
		}
	}
	cand1[0] = -1

	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	idx := &GoBruteForceIndex[float32, float32]{
		Dataset:   [][]float32{cand0, cand1},
		Metric:    metric.Metric_InnerProduct,
		Dimension: dim,
		Count:     2,
	}

	keys := make([]int64, 1)
	dists := make([]float32, 1)
	require.NoError(t, idx.SearchFloat32(sqlproc, [][]float32{query},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, keys, dists))
	require.Equalf(t, int64(0), keys[0],
		"nearest must be candidate 0 (distance 0); the lane-cancellation candidate must not be dropped (got key=%v dist=%v)", keys[0], dists[0])
}

// TestBruteForceRecoversNarrowCosineWinner29496 is the public SELECTION oracle for narrow (BF16)
// cosine: the SIMD dot lanes overflow before cancellation and map to +Inf, which would drop the true
// nearest. Witness (dim 32, BF16): query 2^60; candidate 0 alternates +/-2^67 (orthogonal to the query
// -> cosine distance 1); candidate 1 is all -1 (antiparallel -> cosine distance 2). Every stored value
// is a finite, admissible BF16 and every product is a finite +/-2^127. Without recovery NEON returns
// +Inf for candidate 0 and wrongly selects candidate 1 (distance ~2); with the metric owner recovering
// the in-order reference, candidate 0 scores 1 and correctly wins (#29496).
func TestBruteForceRecoversNarrowCosineWinner29496(t *testing.T) {
	const dim = 32
	q := make([]types.BF16, dim)
	cand0 := make([]types.BF16, dim) // alternating +/-2^67: orthogonal to q -> cosine distance 1
	cand1 := make([]types.BF16, dim) // all -1: antiparallel -> cosine distance 2
	hi := types.BF16FromFloat32(1 << 67)
	negHi := types.BF16FromFloat32(-(1 << 67))
	one := types.BF16FromFloat32(1 << 60)
	negOne := types.BF16FromFloat32(-1)
	for i := range q {
		q[i] = one
		if i%2 == 0 {
			cand0[i] = hi
		} else {
			cand0[i] = negHi
		}
		cand1[i] = negOne
	}

	proc := testutil.NewProcessWithOwnedMPool(t, "", mpool.MustNewZero())
	sqlproc := sqlexec.NewSqlProcess(proc)
	idx := &GoBruteForceIndex[types.BF16, float64]{
		Dataset:   [][]types.BF16{cand0, cand1},
		Metric:    metric.Metric_CosineDistance,
		Dimension: dim,
		Count:     2,
	}

	keys := make([]int64, 1)
	dists := make([]float32, 1)
	require.NoError(t, idx.SearchFloat32(sqlproc, [][]types.BF16{q},
		vectorindex.RuntimeConfig{Limit: 1, NThreads: 1}, keys, dists))
	require.Equalf(t, int64(0), keys[0],
		"nearest must be candidate 0 (cosine distance 1); narrow-cosine lane cancellation must not drop it (got key=%v dist=%v)", keys[0], dists[0])
}
