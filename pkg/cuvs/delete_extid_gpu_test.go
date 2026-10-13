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
	"testing"
)

// Deleting by EXTERNAL id is the only thing that reads id_to_index_, and that map
// is now built on demand (index_base.hpp, ensure_id_index) instead of during
// ingest. The pre-existing delete tests all pass nil ids, so they take the
// "host_ids empty -> id IS the internal position" branch and never touch the map;
// nothing covered the lookup path until this.
//
// The invariant under test: the map is either EMPTY (unbuilt) or COMPLETE. A
// delete must therefore resolve an external id correctly whether it is the first
// reader (map unbuilt, must be materialised from host_ids) or a later one (map
// already built, must not be rebuilt or double-counted).
func TestDeleteByExternalIdBuildsIndexOnDemand(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU")
	}

	const (
		dimension = uint32(16)
		nVectors  = uint64(1000)
		// External ids deliberately unequal to internal positions: with id == pos
		// a broken map lookup would still land on the right row and pass.
		idBase = int64(1_000_000)
	)

	dataset := make([]float32, nVectors*uint64(dimension))
	ids := make([]int64, nVectors)
	for i := uint64(0); i < nVectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i * 10)
		}
		ids[i] = idBase + int64(i)*7
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8
	bp.KmeansTrainsetFraction = 1.0
	index, err := NewGpuIvfPq[float32, float32](
		dataset, nVectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, ids)
	if err != nil {
		t.Fatalf("NewGpuIvfPq: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}

	query := func(row uint64) []float32 {
		q := make([]float32, dimension)
		for i := range q {
			q[i] = float32(row * 10)
		}
		return q
	}
	topOf := func(t *testing.T, row uint64) int64 {
		t.Helper()
		r, err := index.Search(query(row), 1, dimension, 1, DefaultIvfPqSearchParams())
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		if len(r.Neighbors) == 0 {
			t.Fatalf("Search returned no neighbours for row %d", row)
		}
		return int64(r.Neighbors[0])
	}

	// Sanity: results come back as EXTERNAL ids, so the map is what a delete has
	// to invert.
	const victimRow = uint64(50)
	victim := idBase + int64(victimRow)*7
	if got := topOf(t, victimRow); got != victim {
		t.Logf("approximate search returned %d, expected %d (continuing)", got, victim)
	}

	// FIRST delete: the map has never been populated -- add_chunk and the
	// constructor skip it now -- so ensure_id_index must materialise it here. A
	// regression shows up as a silent no-op: delete_id returns early on a miss.
	if err := index.DeleteId(victim); err != nil {
		t.Fatalf("DeleteId(%d): %v", victim, err)
	}
	if got := topOf(t, victimRow); got == victim {
		t.Fatalf("external id %d still returned after delete; the id->index lookup missed", victim)
	}

	// SECOND delete: map already built. It must be reused, and must still resolve
	// a different external id.
	const secondRow = uint64(120)
	second := idBase + int64(secondRow)*7
	if err := index.DeleteId(second); err != nil {
		t.Fatalf("DeleteId(%d): %v", second, err)
	}
	if got := topOf(t, secondRow); got == second {
		t.Fatalf("external id %d still returned after delete on an already-built map", second)
	}

	// A never-inserted id must be a no-op, not a panic or a stray deletion.
	if err := index.DeleteId(idBase - 1); err != nil {
		t.Fatalf("DeleteId on an absent id should be a no-op, got %v", err)
	}
	if got := topOf(t, 300); got != idBase+300*7 {
		t.Logf("row 300 resolved to %d (approximate search)", got)
	}
}
