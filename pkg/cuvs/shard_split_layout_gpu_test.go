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

package cuvs

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// planCapacity (pkg/sql/colexec/table_function) refuses a SHARDED build whose
// shards would fall below the k-means minimum, and to do that it has to know the
// split. That file is untagged so CI can exercise the planning without a GPU,
// which means the split arithmetic -- (total / num_shards) & ~31, last shard
// taking the remainder -- is written out in Go as well as in C++.
//
// This is the test that keeps the two honest. Duplicating a bound the native
// side owns is exactly how this subsystem has gone wrong before, so the Go
// formula is pinned against what a REAL sharded build actually writes into its
// manifest. If the native splitter changes, this fails rather than planning
// silently drifting from the layout it is predicting.
func TestShardSplitMatchesNativeLayout(t *testing.T) {
	if c, err := GetGpuDeviceCount(); err != nil || c < 1 {
		t.Skip("requires >= 1 GPU")
	}

	// Chosen so the rounding BITES: 1000/4 = 250, which is not a multiple of 32,
	// so the shards are 224 and the last takes 328. A count that divided evenly
	// would pass even a formula that forgot to round.
	const (
		dim    = uint32(4)
		count  = uint64(1000)
		shards = 4
	)
	ds := make([]float32, count*uint64(dim))
	for i := range ds {
		ds[i] = float32(i%97) / 97.0
	}
	ids := make([]int64, count)
	for i := range ids {
		ids[i] = int64(i)
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0

	// gpu_multi_simulation shape: num_shards is devices_.size(), aliases included.
	idx, err := NewGpuIvfFlat[float32, float32](
		ds, count, dim, L2Expanded, bp, []int{0, 0, 0, 0}, shards, Sharded, ids)
	require.NoError(t, err)
	defer idx.Destroy()
	require.NoError(t, idx.Start())
	require.NoError(t, idx.Build())

	tarPath := filepath.Join(t.TempDir(), "sharded.tar")
	_, err = idx.Pack(tarPath, "")
	require.NoError(t, err)

	dir := filepath.Join(t.TempDir(), "extracted")
	require.NoError(t, os.MkdirAll(dir, 0o755))
	_, err = Unpack(tarPath, dir)
	require.NoError(t, err)

	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var m manifestPeek
	require.NoError(t, json.Unmarshal(raw, &m))
	native := m.BuildParams.ShardSizes
	require.Len(t, native, shards, "a SHARDED save must record one size per shard")

	// The formula planCapacity relies on, written exactly as it is there.
	perShard := (int64(count) / int64(shards)) &^ 31
	require.Equal(t, int64(224), perShard, "the fixture must exercise the rounding")

	for i := 0; i < shards-1; i++ {
		require.Equal(t, uint64(perShard), native[i],
			"shard %d: Go predicts %d, native wrote %d -- the split arithmetic has drifted",
			i, perShard, native[i])
	}
	require.Equal(t, uint64(int64(count)-perShard*(shards-1)), native[shards-1],
		"the last shard must absorb the remainder")

	// And the property planCapacity actually depends on: no shard is smaller than
	// the rounded-down figure, so validating that one figure covers them all.
	var total uint64
	for _, sz := range native {
		require.GreaterOrEqual(t, sz, uint64(perShard),
			"every shard must be at least the rounded-down size")
		total += sz
	}
	require.Equal(t, count, total, "the shards must account for every row")
}
