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
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestPeekManifestNShards exercises the manifest reader on synthetic JSON
// — no GPU required. Covers the three cases the load path cares about:
// SHARDED (returns shard_sizes length), non-SHARDED (returns 0), missing
// file (returns error).
func TestPeekManifestNShards(t *testing.T) {
	tmp := t.TempDir()

	// SHARDED manifest with shard_sizes — what save_dir writes.
	sharded := `{
  "schema_version": 1,
  "index_type": "ivf_flat",
  "dist_mode": 2,
  "devices": [0, 1],
  "build_params": {
    "n_lists": 1024,
    "kmeans_trainset_fraction": 0.5,
    "shard_sizes": [512, 488]
  },
  "components": {
    "shards": ["shard_0.bin", "shard_1.bin"]
  }
}`
	dirA := filepath.Join(tmp, "sharded")
	if err := os.MkdirAll(dirA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirA, "manifest.json"), []byte(sharded), 0o644); err != nil {
		t.Fatal(err)
	}
	if n, err := PeekManifestNShards(dirA); err != nil || n != 2 {
		t.Fatalf("SHARDED: want (2, nil), got (%d, %v)", n, err)
	}

	// Non-SHARDED manifest — no shard_sizes, no shards array.
	single := `{
  "schema_version": 1,
  "index_type": "ivf_flat",
  "dist_mode": 0,
  "devices": [0],
  "build_params": { "n_lists": 1024 },
  "components": { "index": "index.bin" }
}`
	dirB := filepath.Join(tmp, "single")
	if err := os.MkdirAll(dirB, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dirB, "manifest.json"), []byte(single), 0o644); err != nil {
		t.Fatal(err)
	}
	if n, err := PeekManifestNShards(dirB); err != nil || n != 0 {
		t.Fatalf("non-SHARDED: want (0, nil), got (%d, %v)", n, err)
	}

	// Missing manifest — error.
	dirC := filepath.Join(tmp, "missing")
	if err := os.MkdirAll(dirC, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := PeekManifestNShards(dirC); err == nil {
		t.Fatalf("missing manifest.json: want error, got nil")
	}
}

// TestDevicesForLoad checks the pure-logic path of the helper that the
// FromDataDirectory wrappers call.
func TestDevicesForLoad(t *testing.T) {
	tmp := t.TempDir()
	manifest := `{"build_params":{"shard_sizes":[1,2]},"components":{"shards":["a","b"]}}`
	if err := os.WriteFile(filepath.Join(tmp, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}

	// SHARDED + 4 devices + 2 saved shards → truncate to first 2.
	got, err := devicesForLoad([]int{0, 1, 2, 3}, Sharded, tmp)
	if err != nil {
		t.Fatalf("Sharded truncate: %v", err)
	}
	if len(got) != 2 || got[0] != 0 || got[1] != 1 {
		t.Fatalf("Sharded truncate: want [0 1], got %v", got)
	}

	// Non-Sharded — devices unchanged regardless of manifest.
	got, err = devicesForLoad([]int{0, 1, 2, 3}, SingleGpu, tmp)
	if err != nil {
		t.Fatalf("SingleGpu unchanged: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("SingleGpu unchanged: want 4 devices, got %d", len(got))
	}

	// SHARDED but caller supplied fewer devices than shards → error.
	if _, err := devicesForLoad([]int{0}, Sharded, tmp); err == nil ||
		!strings.Contains(err.Error(), "saved index has 2 shards but only 1 devices supplied") {
		t.Fatalf("under-supplied devices: want clear error, got %v", err)
	}
}

// TestShardedLoadWithFewerSavedShards is the end-to-end scenario the
// truncation is built for: build a SHARDED index over 2 GPUs (out of the
// N available), save it, then reload while passing all N GPUs. The load
// wrapper should peek the manifest, see 2 shards, and truncate `devices`
// to len 2 before constructing the C++ index. Skips on hosts with < 2 GPUs.
func TestShardedLoadWithFewerSavedShards(t *testing.T) {
	devs, err := GetGpuDeviceList()
	if err != nil || len(devs) < 2 {
		t.Skip("Need at least 2 GPUs for sharded-load-with-truncation test")
	}

	const (
		dimension = uint32(16)
		nVectors  = uint64(1024)
	)
	dataset := make([]float32, nVectors*uint64(dimension))
	for i := uint64(0); i < nVectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i) / float32(nVectors)
		}
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 16

	// --- Save phase: 2 shards over devs[:2] ---
	saveDevs := devs[:2]
	src, err := NewGpuIvfFlat[float32](dataset, nVectors, dimension, L2Expanded,
		bp, saveDevs, uint32(len(saveDevs)), Sharded, nil)
	if err != nil {
		t.Fatalf("save-side build: %v", err)
	}
	if err := src.Start(); err != nil {
		t.Fatalf("save-side start: %v", err)
	}
	if err := src.Build(); err != nil {
		t.Fatalf("save-side build: %v", err)
	}

	tarPath := filepath.Join(t.TempDir(), "idx.tar")
	if err := src.Pack(tarPath); err != nil {
		t.Fatalf("Pack: %v", err)
	}
	src.Destroy()

	// Unpack the tar so we have a dir for NewGpuIvfFlatFromDataDirectory.
	extractDir := filepath.Join(t.TempDir(), "extracted")
	if err := os.MkdirAll(extractDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := Unpack(tarPath, extractDir); err != nil {
		t.Fatalf("Unpack: %v", err)
	}

	// Sanity-check: manifest claims 2 shards.
	savedN, err := PeekManifestNShards(extractDir)
	if err != nil || savedN != 2 {
		t.Fatalf("PeekManifestNShards: want (2, nil), got (%d, %v)", savedN, err)
	}

	// --- Load phase: caller supplies ALL available devs; wrapper should
	// truncate to the saved 2. ---
	dst, err := NewGpuIvfFlatFromDataDirectory[float32](extractDir, dimension, L2Expanded,
		bp, devs, uint32(len(devs)), Sharded)
	if err != nil {
		t.Fatalf("load with extra devices: %v", err)
	}
	defer dst.Destroy()
	// If we got here without error, the C++ side accepted a 2-shard load
	// on a 2-device worker. A quick search confirms it's actually usable.
	queries := make([]float32, dimension)
	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 16
	res, err := dst.Search(queries, 1, dimension, 5, sp)
	if err != nil {
		t.Fatalf("post-load search: %v", err)
	}
	if len(res.Neighbors) != 5 {
		t.Fatalf("post-load search: want 5 neighbors, got %d", len(res.Neighbors))
	}
}
