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
	"strings"
	"testing"
)

// An index is all-ids or all-id-less. A mix leaves host_ids and the row count
// disagreeing, and neither half degrades usefully: rows with no entry resolve to
// -1 through map_neighbor_id, while rows zero-filled to make room for later ids
// report external id 0 -- indistinguishable from a legitimate id 0, which is what
// made delete_id(0) able to hit a row nobody addressed.
//
// Both directions are refused, and this proves both refusals fire rather than
// leaving the contract as a comment. The all-id-less case must keep working: it
// is the implicit-id mode that SHARDED-without-custom-IDs relies on.
func TestMixedIdsRefused(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU")
	}

	const (
		dimension = uint32(16)
		nBase     = uint64(100)
		nExt      = uint64(50)
	)
	newDataset := func(n uint64, val func(uint64) float32) []float32 {
		d := make([]float32, n*uint64(dimension))
		for i := uint64(0); i < n; i++ {
			for j := uint32(0); j < dimension; j++ {
				d[i*uint64(dimension)+uint64(j)] = val(i)
			}
		}
		return d
	}
	base := newDataset(nBase, func(i uint64) float32 { return float32(i) })
	ext := newDataset(nExt, func(uint64) float32 { return 500.5 })
	extIDs := make([]int64, nExt)
	baseIDs := make([]int64, nBase)
	for i := range extIDs {
		extIDs[i] = int64(2000 + i)
	}
	for i := range baseIDs {
		baseIDs[i] = int64(i)
	}

	build := func(t *testing.T, ids []int64) *GpuIvfPq[float32, float32] {
		t.Helper()
		bp := DefaultIvfPqBuildParams()
		bp.NLists = 10
		bp.M = 8
		idx, err := NewGpuIvfPq[float32, float32](
			base, nBase, dimension, L2Expanded, bp, devices, 1, SingleGpu, ids)
		if err != nil {
			t.Fatalf("NewGpuIvfPq: %v", err)
		}
		t.Cleanup(func() { idx.Destroy() })
		if err := idx.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if err := idx.Build(); err != nil {
			t.Fatalf("Build: %v", err)
		}
		return idx
	}

	t.Run("id-less index refuses an id-bearing extend", func(t *testing.T) {
		// Would zero-fill host_ids[0..nBase) to make room for the extended ids,
		// making every base row report external id 0.
		err := build(t, nil).Extend(ext, nExt, extIDs)
		if err == nil {
			t.Fatal("expected the mix to be refused")
		}
		if !strings.Contains(err.Error(), "all-ids or all-id-less") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("id-bearing index refuses an id-less extend", func(t *testing.T) {
		// Would leave the extended rows with no host_ids entry, so search resolves
		// them to -1 and delete_id can never address them.
		err := build(t, baseIDs).Extend(ext, nExt, nil)
		if err == nil {
			t.Fatal("expected the mix to be refused")
		}
		if !strings.Contains(err.Error(), "all-ids or all-id-less") {
			t.Fatalf("refused for the wrong reason: %v", err)
		}
	})

	t.Run("all-id-less stays supported", func(t *testing.T) {
		// The implicit-id mode. Never enters the guarded branches at all.
		if err := build(t, nil).Extend(ext, nExt, nil); err != nil {
			t.Fatalf("an all-id-less extend must still work: %v", err)
		}
	})

	t.Run("all-ids stays supported", func(t *testing.T) {
		if err := build(t, baseIDs).Extend(ext, nExt, extIDs); err != nil {
			t.Fatalf("an all-ids extend must still work: %v", err)
		}
	})
}
