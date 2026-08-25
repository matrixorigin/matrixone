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
	"github.com/matrixorigin/matrixone/pkg/container/types"
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
		p, err := planCapacity(20000, 4000, 0, 0, 32, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(4000), p.Capacity)
		require.Equal(t, int64(5), p.NumSubIdx)
		require.Equal(t, int64(20000), p.CdcCutoff, "no short tail, so nothing goes to CDC")
		require.False(t, p.VRAMBound)
	})

	t.Run("short tail below the k-means minimum goes to CDC", func(t *testing.T) {
		// 20010 = 5*4000 + 10; a 10-row tail cannot seed 32 centroids.
		p, err := planCapacity(20010, 4000, 0, 0, 32, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20000), p.CdcCutoff)
	})

	t.Run("short tail at or above the minimum stays on cuVS", func(t *testing.T) {
		p, err := planCapacity(20040, 4000, 0, 0, 32, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20040), p.CdcCutoff, "a 40-row tail can seed 32 centroids")
	})

	// The VRAM bound is the point of the change: it applies whatever the request was.
	t.Run("VRAM caps an explicit request", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 10_000_000, 3_000_000, 0, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity, "an explicit request is a request, not an override")
		require.True(t, p.VRAMBound)
		require.Equal(t, int64(30), p.NumSubIdx)
	})

	t.Run("VRAM caps the auto default", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.True(t, p.VRAMBound)
	})

	t.Run("a request under the VRAM limit is honoured", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 1_000_000, 3_000_000, 0, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(1_000_000), p.Capacity)
		require.False(t, p.VRAMBound)
	})

	// Omitting this clamp is the most dangerous mistake available here: InitEmpty
	// preallocates capacity*dim*sizeof(Q) host bytes before a single row arrives.
	t.Run("capacity never exceeds the source row count", func(t *testing.T) {
		for _, explicit := range []int64{0, 5_000_000} {
			p, err := planCapacity(20, explicit, 3_000_000, 0, 0, false, 1, algo, param)
			require.NoError(t, err)
			require.Equal(t, int64(20), p.Capacity, "a 20-row table must not reserve for millions")
			require.Equal(t, int64(1), p.NumSubIdx)
		}
	})

	t.Run("unmeasured VRAM leaves the request alone", func(t *testing.T) {
		p, err := planCapacity(20000, 4000, 0, 0, 32, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(4000), p.Capacity)
	})

	// A table smaller than the k-means minimum is the ONE legitimate whole-table tail.
	t.Run("small table legitimately routes to the CDC tail", func(t *testing.T) {
		p, err := planCapacity(20, 0, 0, 0, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(0), p.CdcCutoff, "every row served by brute force")
		require.Equal(t, int64(1), p.NumSubIdx)
	})

	t.Run("explicit capacity below the minimum is rejected", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 500, 0, 0, 1024, false, 1, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "max_index_capacity")
		require.Contains(t, err.Error(), "CDC tail",
			"the error must say what would otherwise happen, not just that it is invalid")
	})

	t.Run("VRAM-derived capacity below the minimum names the GPU, not the knob", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 500, 0, 1024, false, 1, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "GPU memory",
			"blaming max_index_capacity would send the operator to the wrong lever")
	})

	t.Run("sharded rejects a split", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 3_000_000, 0, 0, 1024, true, 1, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "sharded")
	})

	t.Run("sharded is fine when everything fits one sub-index", func(t *testing.T) {
		p, err := planCapacity(20000, 0, 0, 0, 32, true, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(1), p.NumSubIdx)
	})

	t.Run("sharded is rejected when VRAM forces the split", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, true, 1, algo, param)
		require.Error(t, err, "a VRAM-forced split is still a split")
	})

	t.Run("threshold 0 disables the tail rule", func(t *testing.T) {
		p, err := planCapacity(20010, 4000, 0, 0, 0, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20010), p.CdcCutoff)
	})

	t.Run("empty source is rejected", func(t *testing.T) {
		_, err := planCapacity(0, 0, 0, 0, 32, false, 1, algo, param)
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
		p, err := planCapacity(88_000_000, 0, 63_000_000, 8_000_000, 6000, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(8_000_000), p.Capacity)
		require.True(t, p.HostBound)
		require.False(t, p.VRAMBound, "only one bound may claim to have decided capacity")
		require.Equal(t, int64(11), p.NumSubIdx)
	})

	t.Run("VRAM bound wins when it is tighter", func(t *testing.T) {
		p, err := planCapacity(88_000_000, 0, 3_000_000, 8_000_000, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.True(t, p.VRAMBound)
		require.False(t, p.HostBound)
	})

	t.Run("host bound clamps an explicit request too", func(t *testing.T) {
		// max_index_capacity is a request, not an override — the same rule the VRAM
		// bound already enforced.
		p, err := planCapacity(88_000_000, 50_000_000, 0, 8_000_000, 6000, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(8_000_000), p.Capacity)
		require.True(t, p.HostBound)
	})

	t.Run("srcRowCount clamp clears both flags", func(t *testing.T) {
		// A small table is not "bounded" by anything; reporting otherwise would have
		// the operator chasing memory that was never the constraint.
		p, err := planCapacity(20, 0, 63_000_000, 8_000_000, 0, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(20), p.Capacity)
		require.False(t, p.HostBound)
		require.False(t, p.VRAMBound)
	})

	t.Run("host bound below the k-means minimum names the host lever", func(t *testing.T) {
		_, err := planCapacity(88_000_000, 0, 63_000_000, 500, 1024, false, 1, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "host memory",
			"the message must name the resource that actually bound it")
	})

	t.Run("zero disables the bound", func(t *testing.T) {
		// memory.HostRowsFitting returns 0 when the platform cannot report memory; that
		// must not collapse capacity to nothing.
		p, err := planCapacity(88_000_000, 0, 3_000_000, 0, 1024, false, 1, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(3_000_000), p.Capacity)
		require.False(t, p.HostBound)
	})
}

// SHARDED is validated PER SHARD. Everything else in planCapacity compares the
// whole row count against the k-means minimum, but a sharded build splits first
// and each shard trains its own centroids -- so a table that clears the minimum
// in total can produce shards that every one of them fails. The rows are already
// ingested by the time the native build finds out.
//
// The split is (total / num_shards) & ~31, last shard taking the remainder.
func TestPlanCapacityShardedValidatesPerShard(t *testing.T) {
	const algo, param = "ivfpq", "max_index_capacity"

	t.Run("clears the minimum in total but every shard fails", func(t *testing.T) {
		// 2000 rows over 4 devices -> (2000/4) & ~31 = 480, last shard 560.
		// 2000 >= 1024 so the global check passes; 480 < 1024 so the build cannot.
		_, err := planCapacity(2000, 0, 0, 0, 1024, true, 4, algo, param)
		require.Error(t, err, "planning must refuse before ingest, not after the split")
		require.Contains(t, err.Error(), "shards of 480")
		require.Contains(t, err.Error(), "minimum of 1024")
	})

	t.Run("the CAGRA shape from the same report", func(t *testing.T) {
		// 500 rows over 4 devices -> (500/4) & ~31 = 96, last shard 212.
		_, err := planCapacity(500, 0, 0, 0, 128, true, 4, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards of 96")
	})

	t.Run("shards that clear the minimum are admitted", func(t *testing.T) {
		// 8192 over 4 -> 2048 per shard, comfortably above 1024.
		p, err := planCapacity(8192, 0, 0, 0, 1024, true, 4, algo, param)
		require.NoError(t, err)
		require.Equal(t, int64(8192), p.Capacity, "sharded never splits into sub-indexes")
		require.Equal(t, int64(1), p.NumSubIdx)
	})

	t.Run("the 32-row rounding is what decides it", func(t *testing.T) {
		// 4*1024 = 4096 rows over 4 devices divides EXACTLY, so no rounding loss
		// and 1024 per shard is admitted.
		_, err := planCapacity(4096, 0, 0, 0, 1024, true, 4, algo, param)
		require.NoError(t, err)

		// One row fewer: 4095/4 = 1023, rounded down to 992. Rounding, not the
		// division, is what pushes it under -- which is why the check cannot just
		// divide.
		_, err = planCapacity(4095, 0, 0, 0, 1024, true, 4, algo, param)
		require.Error(t, err)
		require.Contains(t, err.Error(), "shards of 992")
	})

	t.Run("a single shard is not split at all", func(t *testing.T) {
		// numShards 1 means the whole table is the shard, so the global check
		// already covered it and the rounding must not be applied.
		_, err := planCapacity(1024, 0, 0, 0, 1024, true, 1, algo, param)
		require.NoError(t, err, "one shard of 1024 meets a 1024 minimum exactly")
	})

	t.Run("a table below the minimum in TOTAL still takes the CDC tail", func(t *testing.T) {
		// This must be reported as the legitimate small-table case, not as a
		// sharding problem: the per-shard check runs after that path returns.
		p, err := planCapacity(500, 0, 0, 0, 1024, true, 4, algo, param)
		require.NoError(t, err, "a small table routes to brute force regardless of mode")
		require.Zero(t, p.CdcCutoff, "every row goes to the CDC tail")
		require.Equal(t, int64(1), p.NumSubIdx)
	})
}

// quantizerStagingBytes feeds the reservation HostRowsFitting subtracts before
// deriving capacity. It is the term that was missing entirely: hostPerRow
// charges the one-byte STORAGE width for int8/uint8, while the C++ side retains
// RAW BASE rows to sample from, concurrently with the capacity allocation.
func TestQuantizerStagingBytes(t *testing.T) {
	const dim, base = uint64(768), uint64(4)

	// Not charged unless the storage type is one byte: training is gated on
	// sizeof(T)==1, so an f16- or f32-quantized build never stages.
	require.Zero(t, quantizerStagingBytes(false, dim, base, 100000))
	require.Zero(t, quantizerStagingBytes(false, dim, base, 88_000_000),
		"even a huge staged count costs nothing when nothing stages")

	// The row count comes from cuvs.QuantizerStagingRows -- i.e. from
	// matrixone::quantizer_staging_rows, which applies the default, the train
	// limit and the device bound. Nothing here recomputes it; this is the multiply.
	require.Equal(t, uint64(100000*dim*base), quantizerStagingBytes(true, dim, base, 100000))
	require.Equal(t, uint64(100000*dim*2), quantizerStagingBytes(true, dim, 2, 100000),
		"an f16 base stages half as much")

	// A device-capped count charges only what will actually be staged. This is why
	// the count is asked for rather than derived from the limit: charging an
	// uncapped 88M at dim 768 would be ~270 GB and refuse a build that would run.
	require.Equal(t, uint64(1_760_000*dim*base), quantizerStagingBytes(true, dim, base, 1_760_000))

	require.Zero(t, quantizerStagingBytes(true, 0, base, 100000), "no dimension, no arena")
	require.Zero(t, quantizerStagingBytes(true, dim, 0, 100000))
	require.Zero(t, quantizerStagingBytes(true, dim, base, 0), "no staged rows, no arena")
}

func TestBaseElemBytes(t *testing.T) {
	require.Equal(t, uint64(2), baseElemBytes(types.T_array_float16))
	require.Equal(t, uint64(4), baseElemBytes(types.T_array_float32))
}
