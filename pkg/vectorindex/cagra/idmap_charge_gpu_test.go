//go:build gpu

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

package cagra

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/cuvs"
	vimemory "github.com/matrixorigin/matrixone/pkg/vectorindex/memory"
	"github.com/stretchr/testify/require"
)

// buildTinyCagra returns a real, built cagra index carrying ids.
func buildTinyCagra(t *testing.T, rows uint64) *cuvs.GpuCagra[float32, float32] {
	t.Helper()
	const dim = uint32(4)
	ds := make([]float32, rows*uint64(dim))
	ids := make([]int64, rows)
	for i := uint64(0); i < rows; i++ {
		for j := uint32(0); j < dim; j++ {
			ds[i*uint64(dim)+uint64(j)] = float32(i + 1)
		}
		ids[i] = int64(i + 1)
	}
	bp := cuvs.DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 16
	bp.GraphDegree = 8

	idx, err := cuvs.NewGpuCagra[float32, float32](
		ds, rows, dim, cuvs.L2Expanded, bp, []int{0}, 1, cuvs.SingleGpu, ids)
	require.NoError(t, err)
	t.Cleanup(func() { _ = idx.Destroy() })
	require.NoError(t, idx.Start())
	require.NoError(t, idx.Build())
	return idx
}

// The FIRST replayed delete materialises id_to_index_ for EVERY row of the index and it stays
// resident for the index's life (cgo/cuvs/index_base.hpp, ensure_id_index). The native side
// reserves that allocation and releases the claim the moment it succeeds, so nothing downstream
// tracks it: uncharged, the governor evicts against a host figure short by 40 bytes per row on
// every generation that ever replayed one delete.
//
// This runs the real native path -- a built cuvs index, a real DeleteIds -- and measures the
// charge the model applies, rather than restating the formula against a stub.
func TestIdMapChargeIsNonZeroAndPerGeneration(t *testing.T) {
	const rows = 64

	gen1 := buildTinyCagra(t, rows)
	require.Equal(t, uint64(rows), gen1.Len())

	// What loadIndexes charges after a replay, on this index.
	idx := &CagraModel[float32, float32]{HostComponentBytes: 4096}
	before := idx.HostComponentBytes

	require.NoError(t, gen1.DeleteIds([]int64{1})) // one delete builds the WHOLE map
	idx.HostComponentBytes += int64(gen1.Len()) * vimemory.HostIDMapBytesPerRow

	delta := idx.HostComponentBytes - before
	require.Positive(t, delta, "a replayed delete must move the host charge off zero")
	require.Equal(t, int64(gen1.Len())*vimemory.HostIDMapBytesPerRow, delta,
		"the map is sized by the INDEX's rows, not by how many ids were deleted")

	// A second generation starts from its own baseline: the charge is per generation and goes
	// away with it, so eviction reclaims it rather than accumulating it across reloads.
	gen2 := buildTinyCagra(t, rows)
	other := &CagraModel[float32, float32]{HostComponentBytes: 4096}
	require.Equal(t, before, other.HostComponentBytes,
		"a fresh generation does not inherit the previous one's id-map charge")
	require.NoError(t, gen2.DeleteIds([]int64{2}))
	other.HostComponentBytes += int64(gen2.Len()) * vimemory.HostIDMapBytesPerRow
	require.Equal(t, idx.HostComponentBytes, other.HostComponentBytes,
		"and an equivalent generation is charged the same, not double")

	// A generation that never replays a delete never builds the map and is charged nothing.
	clean := &CagraModel[float32, float32]{HostComponentBytes: 4096}
	require.Equal(t, before, clean.HostComponentBytes)
}
