//go:build !((amd64 || arm64) && go1.27 && goexperiment.simd)

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

// Non-SIMD build only: the distance wrappers delegate straight to the 8-wide *Unrolled oracles. A
// block of 8 same-sign extreme-but-finite products overflows to +/-Inf and two opposite-sign blocks
// cancel to NaN. Before the oracle applied the no-NaN finalizer this NaN flowed raw into the top-k
// heap: NaN is unordered, retained in a slot, and dropped a nearer finite candidate. This is the
// build-time scalar hole the #29496 review flagged -- distinct from the runtime CPU opt-out, which
// still runs the SIMD-build code -- so it needs a real non-SIMD-build selection witness.
//
// Witness (f32, dim64): query all M=2^63; candidate 0 holds M at 0..7 and -M at 32..39 (block
// overflow -> NaN, now +Inf, ranks last); candidates [1,0,...] and [2,0,...] are the finite winners.
// Top-2 must be the two finite candidates, never the overflowing one.
func TestBruteForceScalarFallbackNoNaN29496(t *testing.T) {
	const dim = 64
	m := float32(1 << 63)
	query := make([]float32, dim)
	cand0 := make([]float32, dim) // block overflow -> NaN dot without the finalizer
	cand1 := make([]float32, dim) // [1,0,...]: dot = M,   distance -M   (finite, near)
	cand2 := make([]float32, dim) // [2,0,...]: dot = 2*M, distance -2*M (finite, nearest)
	for i := range query {
		query[i] = m
	}
	for i := 0; i < 8; i++ {
		cand0[i] = m
		cand0[32+i] = -m
	}
	cand1[0] = 1
	cand2[0] = 2

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
		"top-2 must be the finite candidates {1,2}; the block-overflow candidate 0 must not win via a NaN distance, got keys=%v dists=%v", keys, dists)
}
