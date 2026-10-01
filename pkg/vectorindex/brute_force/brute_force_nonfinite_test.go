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
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
)

// A candidate whose inner product overflows on the SIMD lanes (products finite, but
// per-lane partial sums reach +Inf and -Inf before the reduction cancels them) used
// to compute as NaN. NaN is unordered: it never compares as the max a top-k heap
// evicts, so it was retained in a slot and dropped a genuinely-nearer finite
// candidate -- a silent wrong result the final-score check could not catch (its
// score was a finite 2^63). The metric now maps that NaN to +Inf, which ranks the
// overflowing candidate LAST, so the two finite candidates are selected correctly.
// This is the wrong-winner-prevention for #29496.
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

	proc := testutil.NewProcessWithMPool(t, "", mpool.MustNewZero())
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
