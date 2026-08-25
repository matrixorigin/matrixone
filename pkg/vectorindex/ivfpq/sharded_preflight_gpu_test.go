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

//go:build gpu

package ivfpq

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
)

// The load pre-flight reduces a SHARDED artifact with
// MeasureTar -> IsHostResidentComponent -> ShardRank -> PeakDeviceBytes. Every
// link in that chain is covered by unit tests over hand-written component maps,
// and the end-to-end loadIndexes tests all run one SINGLE_GPU sub-index on
// []int{0} -- so nothing has ever fed a REAL sharded artifact through it.
//
// The arithmetic is not the risk; the COUPLING is. The reduction assumes a
// sharded save names its device components "shard_N.bin", that MeasureTar
// classifies them device-resident, and that ShardRank parses those exact names.
// Rename them in C++ (shard-0.bin, or nested under a directory) and ShardRank
// returns -1, which charges each shard to EVERY device -- survivable, since it
// over-refuses. Move one to the host-resident side and the gate under-charges,
// which is the direction that admits an index the GPU cannot hold.
//
// Built through gpu_multi_simulation ([0,0,...]) so it runs on a one-GPU box;
// that is also the aliasing case where every shard lands on the same physical
// card, which is what makes the max-vs-sum assertion below meaningful.
func TestShardedArtifactFeedsThePreflight(t *testing.T) {
	if c, err := cuvs.GetGpuDeviceCount(); err != nil || c < 1 {
		t.Skip("requires >= 1 GPU")
	}

	const (
		dim    = uint32(4)
		count  = uint64(128) // 4 shards of 32; the splitter rounds to a multiple of 32
		shards = 4
	)
	ds := make([]float32, count*uint64(dim))
	ids := make([]int64, count)
	for i := uint64(0); i < count; i++ {
		for j := uint32(0); j < dim; j++ {
			ds[i*uint64(dim)+uint64(j)] = float32((i + 1) * 10)
		}
		ids[i] = int64(i + 100)
	}

	bp := cuvs.DefaultIvfPqBuildParams()
	bp.NLists = 4
	bp.M = 8
	bp.BitsPerCode = 8
	bp.KmeansTrainsetFraction = 1.0

	simDevices := []int{0, 0, 0, 0}
	idx, err := cuvs.NewGpuIvfPq[float32, float32](
		ds, count, dim, cuvs.L2Expanded, bp, simDevices, shards, cuvs.Sharded, ids)
	require.NoError(t, err)
	defer idx.Destroy()
	require.NoError(t, idx.Start())
	require.NoError(t, idx.Build())

	tarPath := filepath.Join(t.TempDir(), "sharded.tar")
	packed, err := idx.Pack(tarPath, "")
	require.NoError(t, err)

	sizes, err := cuvs.MeasureTar(tarPath)
	require.NoError(t, err)

	// This is loadIndexes' own reduction, verbatim.
	device := make(map[string]int64, len(sizes.Files))
	for name, sz := range sizes.Files {
		if !cuvs.IsHostResidentComponent(name) {
			device[name] = sz
		}
	}

	t.Run("device components are shard_N.bin and ShardRank parses every one", func(t *testing.T) {
		require.Len(t, device, shards,
			"a SHARDED save must contribute exactly one device component per shard, got %v", device)
		seen := make(map[int]bool, shards)
		for name := range device {
			rank := memory.ShardRank(name)
			require.GreaterOrEqual(t, rank, 0,
				"ShardRank must parse %q; an unparsed name is charged to every device instead of one", name)
			require.Less(t, rank, shards)
			require.False(t, seen[rank], "duplicate shard rank %d", rank)
			seen[rank] = true
		}
		require.Len(t, seen, shards, "ranks must be 0..%d with no gaps", shards-1)

		// No index.bin: a sharded save must not also emit the whole-index component,
		// which the reduction would charge to EVERY device on top of the shards.
		require.NotContains(t, device, "index.bin")
	})

	t.Run("MeasureTar agrees with Pack about the device bytes", func(t *testing.T) {
		// Pack reports the device total from the component directory; MeasureTar
		// reads the packed archive. The build gate uses the first, this gate the
		// second, so a disagreement means CREATE and load price the same index
		// differently -- the exact defect that made an artifact commit and then be
		// refused at every load.
		require.Equal(t, packed.Device, sizes.Device)
		var summed int64
		for _, sz := range device {
			summed += sz
		}
		require.Equal(t, sizes.Device, summed)
	})

	t.Run("host components stay off the device side", func(t *testing.T) {
		// ids.bin is 8 bytes/row of HOST memory; charging it to VRAM would inflate
		// the gate. It must appear in the tar and be classified host-resident.
		require.Contains(t, sizes.Files, "ids.bin")
		require.NotContains(t, device, "ids.bin")
		require.Positive(t, sizes.Host)
	})

	t.Run("aliased devices sum the shards; distinct devices take the max", func(t *testing.T) {
		comps := []map[string]int64{device}
		var total int64
		biggest := int64(0)
		for _, sz := range device {
			total += sz
			if sz > biggest {
				biggest = sz
			}
		}

		// gpu_multi_simulation: every rank resolves to card 0, so the one card holds
		// ALL the shards. Taking a max here would under-state demand by the shard
		// count -- and under-stating is what admits an index that cannot load.
		require.Equal(t, total, memory.PeakDeviceBytes([]int{0}, comps),
			"aliased device list must accumulate every shard onto the one physical card")

		// Real distinct cards: rank i lands on devices[i], so the busiest card holds
		// exactly one shard and the answer is the largest shard, not the sum.
		require.Equal(t, biggest, memory.PeakDeviceBytes([]int{0, 1, 2, 3}, comps),
			"distinct cards each hold one shard, so the peak is the largest shard")

		// And the reduction is what the gate is actually fed, so the sharded index
		// must be admissible against a budget that holds the biggest shard alone.
		require.NoError(t, memory.DeviceAggregateFitsFree(
			[]int{0, 1, 2, 3}, uint64(biggest), 1, 1,
			func(int, uint64) (int64, uint64, error) { return biggest, uint64(biggest), nil }))
		require.Error(t, memory.DeviceAggregateFitsFree(
			[]int{0, 1, 2, 3}, uint64(biggest), 1, 1,
			func(int, uint64) (int64, uint64, error) { return biggest - 1, uint64(biggest), nil }))
	})
}
