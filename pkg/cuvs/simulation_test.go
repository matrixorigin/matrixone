//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
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

package cuvs

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

// Single-GPU "multi-GPU simulation" exercised through the Go bindings. A
// duplicated device list [0,0,0,0] presents 4 logical GPUs on one physical
// device, so REPLICATED / SHARDED build, extend, and search run for real
// without 4-GPU hardware. Per-rank index/dataset state is keyed by logical rank
// (not device id) so the 4 copies coexist instead of colliding on device 0
// (Info() reports "ranks": 4), and the per-device build/extend mutex serializes
// the otherwise-concurrent same-device cuVS calls (which would SIGSEGV).
//
// nthread must be >= len(devices) so every rank's queue has a worker thread.

const simRanks = 4

// Tolerances for the search assertions: IVF-Flat is exact within probed lists;
// IVF-PQ and CAGRA are approximate, so allow an adjacent-vector result on a
// boundary probe (exact recall@1 is covered by the BVT cases).
const simTolExact = int64(0)
const simTolApprox = int64(4)

func simDevices() []int { return []int{0, 0, 0, 0} }

// simData: row i -> a dim-vector filled with (i+1)*10 (well separated so the
// nearest neighbor is unambiguous under PQ/quantization), id = i + 100.
func simData(count uint64, dim uint32) ([]float32, []int64) {
	ds := make([]float32, count*uint64(dim))
	ids := make([]int64, count)
	for i := uint64(0); i < count; i++ {
		v := float32((i + 1) * 10)
		for j := uint32(0); j < dim; j++ {
			ds[i*uint64(dim)+uint64(j)] = v
		}
		ids[i] = int64(i + 100)
	}
	return ds, ids
}

func simRow(ds []float32, dim uint32, row uint64) []float32 {
	return ds[row*uint64(dim) : (row+1)*uint64(dim)]
}

func simRequireRanks(t *testing.T, info string, n int) {
	t.Helper()
	want := fmt.Sprintf(`"ranks": %d`, n)
	if !strings.Contains(info, want) {
		t.Fatalf("expected %q in Info(), got: %s", want, info)
	}
}

// simExpectNeighbor asserts the search returned exactly one neighbor close to
// the expected id. The point is to confirm search runs across all simulated
// ranks and returns a genuinely-near neighbor (well short of an arbitrary id).
func simExpectNeighbor(t *testing.T, neighbors []int64, wantID int64, tol int64) {
	t.Helper()
	if len(neighbors) != 1 {
		t.Fatalf("expected 1 neighbor, got %v", neighbors)
	}
	d := neighbors[0] - wantID
	if d < 0 {
		d = -d
	}
	if d > tol {
		t.Fatalf("expected neighbor within %d of id %d, got %d", tol, wantID, neighbors[0])
	}
}

func simSkipNoGPU(t *testing.T) {
	t.Helper()
	if c, err := GetGpuDeviceCount(); err != nil || c < 1 {
		t.Skip("requires >= 1 GPU")
	}
}

// ---------------------------------------------------------------------------
// IVF-Flat (exact within probed lists)
// ---------------------------------------------------------------------------

func TestSimulatedReplicatedIvfFlat(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(64)
	ds, ids := simData(count, dim)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0
	idx, err := NewGpuIvfFlat[float32, float32](ds, count, dim, L2Expanded, bp, simDevices(), simRanks, Replicated, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks) // 4 replicas coexist on device 0

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 4
	for _, row := range []uint64{0, 30, 63} {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolExact)
	}
}

func TestSimulatedShardedIvfFlat(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(128) // 4 shards of 32 (shard splitter rounds down to a multiple of 32)
	ds, ids := simData(count, dim)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0
	idx, err := NewGpuIvfFlat[float32, float32](ds, count, dim, L2Expanded, bp, simDevices(), simRanks, Sharded, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks) // 4 shards coexist on device 0

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 4
	for _, row := range []uint64{3, 40, 80, 127} { // rows spanning all 4 shards
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolExact)
	}
}

// TestSimulatedShardedDeleteIvfFlat exercises the filtered (soft-delete) SHARDED
// search path under simulation — the case the per-device shard-bitset cache got
// wrong. With the [0,0,0,0] device list every shard maps to physical device 0;
// the cache used to key by dev_id alone, so two shards (different shard_offset,
// same dev_id) collided on one entry and one reused the other's delete-bitset
// slice — a deleted row in one shard could still be returned. The fix keys the
// shard bitset cache by (dev_id, shard_offset). Deletes here span two different
// shards; each must be excluded from its own shard's results.
func TestSimulatedShardedDeleteIvfFlat(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(128) // 4 shards of 32; shard_offsets 0 / 32 / 64 / 96
	ds, ids := simData(count, dim)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0
	idx, err := NewGpuIvfFlat[float32, float32](ds, count, dim, L2Expanded, bp, simDevices(), simRanks, Sharded, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 4

	// Soft-delete rows in two DIFFERENT shards: row 40 (shard 1 @offset 32) and
	// row 80 (shard 2 @offset 64). id = row + 100.
	delRows := []uint64{40, 80}
	for _, row := range delRows {
		if err := idx.DeleteId(int64(row + 100)); err != nil {
			t.Fatalf("delete id %d: %v", row+100, err)
		}
	}

	// Searching a deleted row's own vector must NOT return that id — its shard's
	// delete bitset must apply. With the old dev_id-keyed cache, whichever shard
	// synced first won the single device-0 entry and the other shard's delete was
	// dropped, so the deleted id came back. The nearest *surviving* row (adjacent,
	// within tol 2) is returned instead.
	for _, row := range delRows {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search deleted row %d: %v", row, err)
		}
		if len(res.Neighbors) != 1 {
			t.Fatalf("row %d: expected 1 neighbor, got %v", row, res.Neighbors)
		}
		if res.Neighbors[0] == int64(row+100) {
			t.Fatalf("row %d: deleted id %d was returned — shard delete bitset not applied (cache collision)",
				row, row+100)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), 2) // adjacent surviving row
	}

	// Non-deleted rows in other shards still return themselves — confirms the
	// deletes didn't corrupt the other shards' bitsets.
	for _, row := range []uint64{3, 110} { // shard 0 and shard 3
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolExact)
	}
}

func TestSimulatedReplicatedExtendIvfFlat(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	base := uint64(64)
	ds, ids := simData(base, dim)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 4
	bp.KmeansTrainsetFraction = 1.0
	idx, err := NewGpuIvfFlat[float32, float32](ds, base, dim, L2Expanded, bp, simDevices(), simRanks, Replicated, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Concurrent extend across all 4 replicas on device 0 — would race in cuVS
	// pre-fix; the per-device mutex serializes it.
	nExt := uint64(16)
	ext, extIDs := simExtData(base, nExt, dim)
	if err := idx.Extend(ext, nExt, extIDs); err != nil {
		t.Fatalf("extend: %v", err)
	}

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 4
	// original row still findable, and the extended row is findable (exact index).
	res, err := idx.Search(simRow(ds, dim, 30), 1, dim, 1, sp)
	if err != nil {
		t.Fatalf("search base: %v", err)
	}
	simExpectNeighbor(t, res.Neighbors, 130, simTolExact)
	res, err = idx.Search(ext[5*uint64(dim):6*uint64(dim)], 1, dim, 1, sp)
	if err != nil {
		t.Fatalf("search extended: %v", err)
	}
	simExpectNeighbor(t, res.Neighbors, 905, simTolExact)
}

// ---------------------------------------------------------------------------
// IVF-PQ (approximate)
// ---------------------------------------------------------------------------

func simIvfPqParams() IvfPqBuildParams {
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 4
	bp.M = 8 // one sub-quantizer per dim → near-lossless on well-separated data
	bp.BitsPerCode = 8
	bp.KmeansTrainsetFraction = 1.0
	return bp
}

func TestSimulatedReplicatedIvfPq(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(8)
	count := uint64(64)
	ds, ids := simData(count, dim)

	idx, err := NewGpuIvfPq[float32, float32](ds, count, dim, L2Expanded, simIvfPqParams(), simDevices(), simRanks, Replicated, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 4
	for _, row := range []uint64{0, 30, 63} {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolApprox)
	}
}

func TestSimulatedShardedIvfPq(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(8)
	count := uint64(128) // 4 shards of 32
	ds, ids := simData(count, dim)

	idx, err := NewGpuIvfPq[float32, float32](ds, count, dim, L2Expanded, simIvfPqParams(), simDevices(), simRanks, Sharded, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks)

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 4
	for _, row := range []uint64{3, 40, 80, 127} {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolApprox)
	}
}

func TestSimulatedReplicatedExtendIvfPq(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(8)
	base := uint64(64)
	ds, ids := simData(base, dim)

	idx, err := NewGpuIvfPq[float32, float32](ds, base, dim, L2Expanded, simIvfPqParams(), simDevices(), simRanks, Replicated, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	// Concurrent extend across all 4 replicas on device 0 must not crash — the
	// per-device build/extend mutex serializes the otherwise-racing cuVS calls.
	nExt := uint64(16)
	ext, extIDs := simExtData(base, nExt, dim)
	if err := idx.Extend(ext, nExt, extIDs); err != nil {
		t.Fatalf("extend: %v", err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 4
	// The index stays intact after the concurrent extend: a base vector still
	// resolves to (near) its id. (Exact recall of freshly-extended vectors is
	// approximate-index dependent — covered by the BVT cases, not here; this
	// test's job is that concurrent same-device extend is safe.)
	res, err := idx.Search(simRow(ds, dim, 30), 1, dim, 1, sp)
	if err != nil {
		t.Fatalf("search base after extend: %v", err)
	}
	simExpectNeighbor(t, res.Neighbors, 130, simTolApprox)
	// The extended rows are searchable (search returns a result without error).
	res, err = idx.Search(ext[5*uint64(dim):6*uint64(dim)], 1, dim, 1, sp)
	if err != nil {
		t.Fatalf("search extended: %v", err)
	}
	if len(res.Neighbors) != 1 {
		t.Fatalf("extended search: expected 1 neighbor, got %v", res.Neighbors)
	}
}

// simExtData builds nExt extend rows whose values stay inside the build-time
// value range (so IVF-PQ's pre-trained codebook still quantizes them well) but
// are offset by +5 so they don't collide with base rows. id = i + 900.
func simExtData(base, nExt uint64, dim uint32) ([]float32, []int64) {
	ext := make([]float32, nExt*uint64(dim))
	ids := make([]int64, nExt)
	for i := uint64(0); i < nExt; i++ {
		v := float32((i+1)*10 + 5) // 15, 25, ... — interleaved with base's 10,20,...
		for j := uint32(0); j < dim; j++ {
			ext[i*uint64(dim)+uint64(j)] = v
		}
		ids[i] = int64(i + 900)
	}
	return ext, ids
}

// ---------------------------------------------------------------------------
// CAGRA (approximate)
// ---------------------------------------------------------------------------

func simCagraBuildParams() CagraBuildParams {
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 16 // small graph for the small test set
	bp.GraphDegree = 8
	return bp
}

func TestSimulatedReplicatedCagra(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(64)
	ds, ids := simData(count, dim)

	idx, err := NewGpuCagra[float32, float32](ds, count, dim, L2Expanded, simCagraBuildParams(), simDevices(), simRanks, Replicated, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks)

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 32
	for _, row := range []uint64{0, 30, 63} {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolApprox)
	}
}

func TestSimulatedShardedCagra(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(128) // 4 shards of 32
	ds, ids := simData(count, dim)

	idx, err := NewGpuCagra[float32, float32](ds, count, dim, L2Expanded, simCagraBuildParams(), simDevices(), simRanks, Sharded, ids)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer idx.Destroy()
	if err := idx.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := idx.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}

	info, _ := idx.Info()
	simRequireRanks(t, info, simRanks)

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 32
	for _, row := range []uint64{3, 40, 80, 127} {
		res, err := idx.Search(simRow(ds, dim, row), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search row %d: %v", row, err)
		}
		simExpectNeighbor(t, res.Neighbors, int64(row+100), simTolApprox)
	}
}

// Index files written under 4-GPU simulation must round-trip: a REPLICATED
// (simulated) build saved to a directory reloads as REPLICATED (4 ranks) and as
// SINGLE, and search still returns the match. Exercises SaveToDir +
// NewGpuCagraFromDataDirectory's target-mode handling.
func TestSimulatedCagraSaveLoadAcrossModes(t *testing.T) {
	simSkipNoGPU(t)
	dim := uint32(4)
	count := uint64(64)
	ds, ids := simData(count, dim)

	dir, err := os.MkdirTemp("", "mo_sim_cagra_*")
	if err != nil {
		t.Fatalf("mkdtemp: %v", err)
	}
	defer os.RemoveAll(dir)

	// Build REPLICATED under simulation and save the index files.
	{
		idx, err := NewGpuCagra[float32, float32](ds, count, dim, L2Expanded, simCagraBuildParams(), simDevices(), simRanks, Replicated, ids)
		if err != nil {
			t.Fatalf("new: %v", err)
		}
		if err := idx.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		if err := idx.Build(); err != nil {
			t.Fatalf("build: %v", err)
		}
		info, _ := idx.Info()
		simRequireRanks(t, info, simRanks)
		if err := idx.SaveToDir(dir); err != nil {
			t.Fatalf("save_dir: %v", err)
		}
		idx.Destroy()
	}

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 32

	// Reload as REPLICATED (4 ranks).
	{
		idx, err := NewGpuCagraFromDataDirectory[float32, float32](dir, dim, L2Expanded, simCagraBuildParams(), simDevices(), simRanks, Replicated)
		if err != nil {
			t.Fatalf("load replicated: %v", err)
		}
		defer idx.Destroy()
		if err := idx.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		res, err := idx.Search(simRow(ds, dim, 9), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search after load-replicated: %v", err)
		}
		simExpectNeighbor(t, res.Neighbors, 109, simTolApprox)
	}

	// Reload the same files as SINGLE.
	{
		idx, err := NewGpuCagraFromDataDirectory[float32, float32](dir, dim, L2Expanded, simCagraBuildParams(), []int{0}, 1, SingleGpu)
		if err != nil {
			t.Fatalf("load single: %v", err)
		}
		defer idx.Destroy()
		if err := idx.Start(); err != nil {
			t.Fatalf("start: %v", err)
		}
		res, err := idx.Search(simRow(ds, dim, 9), 1, dim, 1, sp)
		if err != nil {
			t.Fatalf("search after load-single: %v", err)
		}
		simExpectNeighbor(t, res.Neighbors, 109, simTolApprox)
	}
}
