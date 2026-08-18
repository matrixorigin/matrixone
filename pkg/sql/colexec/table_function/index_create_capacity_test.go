// Copyright 2026 Matrix Origin
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

package table_function

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// planCapacity holds all the decision logic for sizing a GPU index build, and it lives in
// a file with no build tag precisely so it can be covered without CUDA. Everything else on
// this path is //go:build gpu, so this is the whole of the off-GPU safety net — the cases
// below are the contract.
func TestPlanCapacity(t *testing.T) {
	const algo, param = "ivfpq", "max_index_capacity"

	t.Run("explicit capacity divides evenly", func(t *testing.T) {
		p, err := planCapacity(20000, 4000, 0, 0, 32, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(4000), p.Capacity)
		require.Equal(t, int64(5), p.NumSubIdx)
		require.Equal(t, int64(20000), p.CdcCutoff, "no short tail, so nothing goes to CDC")
		require.False(t, p.VRAMBound)
	})

	t.Run("short tail below the k-means minimum goes to CDC", func(t *testing.T) {
		// 20010 = 5*4000 + 10; a 10-row tail cannot seed 32 centroids.
		p, err := planCapacity(20010, 4000, 0, 0, 32, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20000), p.CdcCutoff)
	})

	t.Run("short tail at or above the minimum stays on cuVS", func(t *testing.T) {
		p, err := planCapacity(20040, 4000, 0, 0, 32, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20040), p.CdcCutoff, "a 40-row tail can seed 32 centroids")
	})

	// The VRAM bound is the point of the change: it applies whatever the request was.
	t.Run("VRAM caps an explicit request", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 10_000_000, 3_000_000, 0, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity, "an explicit request is a request, not an override")
		require.True(t, p.VRAMBound)
		require.Equal(t, int64(30), p.NumSubIdx)
	})

	t.Run("VRAM caps the auto default", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.True(t, p.VRAMBound)
	})

	t.Run("a request under the VRAM limit is honoured", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 1_000_000, 3_000_000, 0, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(1_000_000), p.Capacity)
		require.False(t, p.VRAMBound)
	})

	// Omitting this clamp is the most dangerous mistake available here: InitEmpty
	// preallocates capacity*dim*sizeof(Q) host bytes before a single row arrives.
	t.Run("capacity never exceeds the source row count", func(t *testing.T) {
		for _, explicit := range []int64{0, 5_000_000} {
			p, err := planCapacity(20, explicit, 3_000_000, 0, 0, false, algo, param)
			require.NoError(t, err)
			require.Equal(t, int64(20), p.Capacity, "a 20-row table must not reserve for millions")
			require.Equal(t, int64(1), p.NumSubIdx)
		}
	})

	t.Run("unmeasured VRAM leaves the request alone", func(t *testing.T) {
		p, err := planCapacity(20000, 4000, 0, 0, 32, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(4000), p.Capacity)
	})

	// A table smaller than the k-means minimum is the ONE legitimate whole-table tail.
	t.Run("small table legitimately routes to the CDC tail", func(t *testing.T) {
		p, err := planCapacity(20, 0, 0, 0, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(0), p.CdcCutoff, "every row served by brute force")
		require.Equal(t, int64(1), p.NumSubIdx)
	})

	t.Run("explicit capacity below the minimum is rejected", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 500, 0, 0, 1024, false, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "max_index_capacity")
		require.Contains(t, err.Error(), "CDC tail",
			"the error must say what would otherwise happen, not just that it is invalid")
	})

	t.Run("VRAM-derived capacity below the minimum names the GPU, not the knob", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 500, 0, 1024, false, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "GPU memory",
			"blaming max_index_capacity would send the operator to the wrong lever")
	})

	t.Run("sharded rejects a split", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 3_000_000, 0, 0, 1024, true, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sharded")
	})

	t.Run("sharded is fine when everything fits one sub-index", func(t *testing.T) {
		p, err := planCapacity(20000, 0, 0, 0, 32, true, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(1), p.NumSubIdx)
	})

	t.Run("sharded is rejected when VRAM forces the split", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, true, algo, param)
		require.Error(t, err, "a VRAM-forced split is still a split")
	})

	t.Run("threshold 0 disables the tail rule", func(t *testing.T) {
		p, err := planCapacity(20010, 4000, 0, 0, 0, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20010), p.CdcCutoff)
	})

	t.Run("empty source is rejected", func(t *testing.T) {
		_, err := planCapacity(0, 0, 0, 0, 32, false, algo, param)
		require.Error(t, err)
	})
}

// TestPlanCapacityHostBound covers the bound that used to come for free.
//
// While the per-row cost included the dataset term, bounding capacity against VRAM
// incidentally bounded the host buffer too — they were the same bytes. Sizing ivfpq
// against the PQ codes instead (correct: the dataset is streamed and never resident)
// decoupled them, and capacity started deriving from a number ~7.7x smaller per row.
// InitEmpty then resizes flattened_host_dataset to capacity*dim up front, so an
// unbounded capacity is an unbounded host allocation.
func TestPlanCapacityHostBound(t *testing.T) {
	const algo, param = "ivfpq", "max_index_capacity"

	t.Run("host bound wins when it is tighter than VRAM", func(t *testing.T) {
		// The regression, in its own units: a 20 GB card fits 63M rows of PQ codes,
		// but the host can only hold 8M rows of dim-768 f16 vectors.
		p, err := planCapacity(88_000_000, 0, 63_000_000, 8_000_000, 6000, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(8_000_000), p.Capacity)
		require.True(t, p.HostBound)
		require.False(t, p.VRAMBound, "only one bound may claim to have decided capacity")
		require.Equal(t, int64(11), p.NumSubIdx)
	})

	t.Run("VRAM bound wins when it is tighter", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 0, 3_000_000, 8_000_000, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.True(t, p.VRAMBound)
		require.False(t, p.HostBound)
	})

	t.Run("host bound clamps an explicit request too", func(t *testing.T) {
		// max_index_capacity is a request, not an override — the same rule the VRAM
		// bound already enforced.
		p, err := planCapacity(88_000_000, 50_000_000, 0, 8_000_000, 6000, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(8_000_000), p.Capacity)
		require.True(t, p.HostBound)
	})

	t.Run("srcRowCount clamp clears both flags", func(t *testing.T) {
		// A small table is not "bounded" by anything; reporting otherwise would have
		// the operator chasing memory that was never the constraint.
		p, err := planCapacity(20, 0, 63_000_000, 8_000_000, 0, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20), p.Capacity)
		require.False(t, p.HostBound)
		require.False(t, p.VRAMBound)
	})

	t.Run("host bound below the k-means minimum names the host lever", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 63_000_000, 500, 1024, false, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "host memory",
			"the message must name the resource that actually bound it")
	})

	t.Run("zero disables the bound", func(t *testing.T) {
		// hostRowsFittingMem returns 0 when the platform cannot report memory; that
		// must not collapse capacity to nothing.
		p, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, false, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.False(t, p.HostBound)
	})
}

// hostRowsFittingMem reads real memory, so assert the contract rather than a figure.
func TestHostRowsFittingMem(t *testing.T) {
	rows, avail, err := hostRowsFittingMem(1536) // dim 768 f16
	require.NoError(t, err)
	require.Positive(t, avail)
	require.Positive(t, rows)
	require.Less(t, uint64(rows)*1536, avail, "must leave headroom, not spend all of it")

	// A wider row must not fit more of itself.
	wide, _, err := hostRowsFittingMem(3072)
	require.NoError(t, err)
	require.LessOrEqual(t, wide, rows)

	// Zero is inert rather than a divide-by-zero.
	z, _, err := hostRowsFittingMem(0)
	require.NoError(t, err)
	require.Zero(t, z)
}
