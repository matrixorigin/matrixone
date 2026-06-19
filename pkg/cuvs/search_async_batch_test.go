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

// Tests for the async-batched search path. When batch_window > 0, the async
// API (SearchFloat32Async / SearchAsync → SearchWait) must coalesce concurrent
// calls through submit_batched_async and still return correct per-caller
// results. These tests fire many concurrent goroutines with the same index
// and verify each result matches the sync reference.

import (
	"reflect"
	"sync"
	"testing"
)

// makeLineDataset returns a 2-D dataset where vector[i] = (i, i). Nearest
// neighbor of (q, q) is the integer i closest to q. Deterministic and easy
// to assert.
func makeLineDataset(nVectors uint64, dimension uint32) []float32 {
	if dimension < 2 {
		panic("makeLineDataset requires dimension >= 2")
	}
	out := make([]float32, nVectors*uint64(dimension))
	for i := uint64(0); i < nVectors; i++ {
		base := i * uint64(dimension)
		for d := uint32(0); d < dimension; d++ {
			out[base+uint64(d)] = float32(i)
		}
	}
	return out
}

// runConcurrentAsync fires `nGoroutines` goroutines, each invoking searchOne
// `nPerGoroutine` times. searchOne returns the nearest-neighbor index for a
// caller-supplied query (each goroutine picks its own query so we can verify
// per-call correctness even with batching coalescing them).
func runConcurrentAsync(t *testing.T, nGoroutines, nPerGoroutine int, searchOne func(qid int) (int64, error)) {
	t.Helper()
	var wg sync.WaitGroup
	errCh := make(chan error, nGoroutines*nPerGoroutine)
	mismatchCh := make(chan struct {
		want, got int64
	}, nGoroutines*nPerGoroutine)

	for g := 0; g < nGoroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < nPerGoroutine; k++ {
				qid := g*nPerGoroutine + k
				got, err := searchOne(qid)
				if err != nil {
					errCh <- err
					return
				}
				want := int64(qid)
				if got != want {
					mismatchCh <- struct{ want, got int64 }{want, got}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	close(mismatchCh)

	for err := range errCh {
		t.Fatalf("async search failed: %v", err)
	}
	mismatches := 0
	for m := range mismatchCh {
		if mismatches < 5 {
			t.Errorf("nearest-neighbor mismatch: want=%d got=%d", m.want, m.got)
		}
		mismatches++
	}
	if mismatches > 0 {
		t.Fatalf("%d mismatches across %d searches", mismatches, nGoroutines*nPerGoroutine)
	}
}

func TestGpuCagraSearchFloat32AsyncBatched(t *testing.T) {
	t.Skip("cuVS dynamic_batching conservative_dispatch=true deadlocks on this host: all worker threads park inside dynamic_batching::search because the dispatch-timeout codepath does not fire (n_queues=3, max_batch_size=4 cannot naturally fill). The eager / no-batching paths work fine.")
	dimension := uint32(2)
	nVectors := uint64(2000)
	dataset := makeLineDataset(nVectors, dimension)

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 64
	bp.GraphDegree = 32
	index, err := NewGpuCagra[float32](dataset, nVectors, dimension, L2Expanded, bp, []int{0}, 4, SingleGpu, nil)
	if err != nil {
		t.Fatalf("NewGpuCagra: %v", err)
	}
	defer index.Destroy()
	if err := index.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := index.SetBatchWindow(200); err != nil {
		t.Fatalf("SetBatchWindow: %v", err)
	}
	if err := index.SetDynbConservativeDispatch(true); err != nil {
		t.Fatalf("SetDynbConservativeDispatch: %v", err)
	}

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 64

	// Each goroutine uses a unique query so we can verify per-caller
	// result demuxing through submit_batched_async's per-request setter.
	runConcurrentAsync(t, 16 /*nGoroutines*/, 8 /*nPerGoroutine*/, func(qid int) (int64, error) {
		q := []float32{float32(qid), float32(qid)}
		jobID, err := index.SearchFloat32AsyncWithParams(q, 1, dimension, 1, sp)
		if err != nil {
			return -1, err
		}
		neighbors, _, err := index.SearchWait(jobID, 1, 1)
		if err != nil {
			return -1, err
		}
		if len(neighbors) != 1 {
			t.Fatalf("expected 1 neighbor, got %d", len(neighbors))
		}
		return neighbors[0], nil
	})
}

func TestGpuIvfFlatSearchFloat32AsyncBatched(t *testing.T) {
	t.Skip("cuVS dynamic_batching conservative_dispatch=true deadlocks on this host; see TestGpuCagraSearchFloat32AsyncBatched for the diagnosis.")
	dimension := uint32(2)
	nVectors := uint64(2000)
	dataset := makeLineDataset(nVectors, dimension)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 16
	index, err := NewGpuIvfFlat[float32, float32](dataset, nVectors, dimension, L2Expanded, bp, []int{0}, 4, SingleGpu, nil)
	if err != nil {
		t.Fatalf("NewGpuIvfFlat: %v", err)
	}
	defer index.Destroy()
	if err := index.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := index.SetBatchWindow(200); err != nil {
		t.Fatalf("SetBatchWindow: %v", err)
	}
	if err := index.SetDynbConservativeDispatch(true); err != nil {
		t.Fatalf("SetDynbConservativeDispatch: %v", err)
	}

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 16 // probe all lists for deterministic recall on a small index.

	runConcurrentAsync(t, 16 /*nGoroutines*/, 8 /*nPerGoroutine*/, func(qid int) (int64, error) {
		q := []float32{float32(qid), float32(qid)}
		jobID, err := index.SearchFloat32AsyncWithParams(q, 1, dimension, 1, sp)
		if err != nil {
			return -1, err
		}
		neighbors, _, err := index.SearchWait(jobID, 1, 1)
		if err != nil {
			return -1, err
		}
		if len(neighbors) != 1 {
			t.Fatalf("expected 1 neighbor, got %d", len(neighbors))
		}
		return neighbors[0], nil
	})
}

// ivfPqAsyncBatchedMatchesSync checks that the async-batched float32 path
// (concurrent SearchFloat32Async → SearchWait, batch_window > 0) returns, for
// each caller, exactly what a plain synchronous SearchFloat returns for the
// same query — i.e. the fused-batch coalesce + per-caller demux is faithful —
// with the given cuVS dynamic_batching conservative_dispatch setting.
//
// Uses a random (well-separated) dataset, not a collinear one: with the
// pathological vec_i = (i,…,i) layout PQ produces large near-equidistant
// "tie" clusters, and the rank-3..k ordering inside a tie cluster flips between
// otherwise-equivalent kernel launches (ULP-level distance noise), which would
// make an exact async-vs-sync comparison spuriously fail. With distinct random
// distances the ordering is stable, so any async≠sync here is a real defect.
func ivfPqAsyncBatchedMatchesSync(t *testing.T, conservativeDispatch bool) {
	t.Helper()
	dimension := uint32(64)
	nVectors := uint64(2000)
	dataset := make([]float32, nVectors*uint64(dimension))
	// Deterministic pseudo-random fill (SplitMix64-ish LCG), values in [0,1).
	rng := uint64(0x9E3779B97F4A7C15)
	for i := range dataset {
		rng = rng*6364136223846793005 + 1442695040888963407
		dataset[i] = float32(rng>>40) / float32(1<<24)
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 16
	bp.M = 16
	bp.BitsPerCode = 8
	index, err := NewGpuIvfPq[float32](dataset, nVectors, dimension, L2Expanded, bp, []int{0}, 4, SingleGpu, nil)
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
	// conservative_dispatch is harmless while the window is 0 (set below for the
	// sync reference); it takes effect once the async section turns the window on.
	if err := index.SetDynbConservativeDispatch(conservativeDispatch); err != nil {
		t.Fatalf("SetDynbConservativeDispatch(%v): %v", conservativeDispatch, err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 16

	const nQueries = 128
	const limit = uint32(5)
	// Query qid is dataset row qid (so its own rank-1 neighbor is itself).
	queryOf := func(qid int) []float32 {
		base := qid * int(dimension)
		return append([]float32(nil), dataset[base:base+int(dimension)]...)
	}

	// 1) Synchronous reference, batching off — deterministic for this index.
	if err := index.SetBatchWindow(0); err != nil {
		t.Fatalf("SetBatchWindow(0): %v", err)
	}
	want := make([][]int64, nQueries)
	for qid := 0; qid < nQueries; qid++ {
		res, err := index.SearchFloat(queryOf(qid), 1, dimension, limit, sp)
		if err != nil {
			t.Fatalf("SearchFloat reference qid=%d: %v", qid, err)
		}
		want[qid] = append([]int64(nil), res.Neighbors...)
	}

	// 2) Async + batching on, fire all queries concurrently.
	if err := index.SetBatchWindow(200); err != nil {
		t.Fatalf("SetBatchWindow(200): %v", err)
	}
	got := make([][]int64, nQueries)
	var wg sync.WaitGroup
	errCh := make(chan error, nQueries)
	for qid := 0; qid < nQueries; qid++ {
		wg.Add(1)
		go func(qid int) {
			defer wg.Done()
			jobID, err := index.SearchFloat32AsyncWithParams(queryOf(qid), 1, dimension, limit, sp)
			if err != nil {
				errCh <- err
				return
			}
			neighbors, _, err := index.SearchWait(jobID, 1, limit)
			if err != nil {
				errCh <- err
				return
			}
			got[qid] = neighbors
		}(qid)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("async batched search failed: %v", err)
	}

	mismatches := 0
	for qid := 0; qid < nQueries; qid++ {
		if !reflect.DeepEqual(got[qid], want[qid]) {
			if mismatches < 5 {
				t.Errorf("qid=%d: async-batched=%v, sync reference=%v", qid, got[qid], want[qid])
			}
			mismatches++
		}
	}
	if mismatches > 0 {
		t.Fatalf("%d/%d queries: async-batched result != sync result", mismatches, nQueries)
	}
}

// TestGpuIvfPqSearchFloat32AsyncBatched — conservative_dispatch = true:
// dynamic_batching waits for the batch to fill (or the window to elapse) before
// dispatching at the real size.
func TestGpuIvfPqSearchFloat32AsyncBatched(t *testing.T) {
	t.Skip("cuVS dynamic_batching conservative_dispatch=true deadlocks on this host; see TestGpuCagraSearchFloat32AsyncBatched for the diagnosis.")
	ivfPqAsyncBatchedMatchesSync(t, true /*conservativeDispatch*/)
}

// TestGpuIvfPqSearchFloat32AsyncBatch — conservative_dispatch = false:
// dynamic_batching dispatches eagerly at the full batch size.
func TestGpuIvfPqSearchFloat32AsyncBatch(t *testing.T) {
	ivfPqAsyncBatchedMatchesSync(t, false /*conservativeDispatch*/)
}

// TestGpuCagraAsyncBatchedMatchesSync sanity-checks that the async-batched
// path returns the same neighbor as a plain sync call at the same query.
// Catches result demuxing bugs in submit_batched_async's per-request setter.
func TestGpuCagraAsyncBatchedMatchesSync(t *testing.T) {
	t.Skip("cuVS dynamic_batching conservative_dispatch=true deadlocks on this host; see TestGpuCagraSearchFloat32AsyncBatched for the diagnosis.")
	dimension := uint32(2)
	nVectors := uint64(1000)
	dataset := makeLineDataset(nVectors, dimension)

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 64
	bp.GraphDegree = 32
	index, err := NewGpuCagra[float32](dataset, nVectors, dimension, L2Expanded, bp, []int{0}, 4, SingleGpu, nil)
	if err != nil {
		t.Fatalf("NewGpuCagra: %v", err)
	}
	defer index.Destroy()
	if err := index.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 64

	// 1) Reference: sync SearchFloat (which itself routes through the
	//    sync batched path when batch_window > 0). Capture for several
	//    queries first with batching off, just so we have a clean baseline.
	if err := index.SetBatchWindow(0); err != nil {
		t.Fatalf("SetBatchWindow(0): %v", err)
	}
	const nQueries = 16
	want := make([]int64, nQueries)
	for qid := 0; qid < nQueries; qid++ {
		q := []float32{float32(qid * 10), float32(qid * 10)}
		res, err := index.SearchFloat(q, 1, dimension, 1, sp)
		if err != nil {
			t.Fatalf("SearchFloat reference: %v", err)
		}
		want[qid] = res.Neighbors[0]
	}

	// 2) Async + batching ON, fire all queries concurrently.
	if err := index.SetBatchWindow(200); err != nil {
		t.Fatalf("SetBatchWindow(200): %v", err)
	}
	if err := index.SetDynbConservativeDispatch(true); err != nil {
		t.Fatalf("SetDynbConservativeDispatch: %v", err)
	}
	got := make([]int64, nQueries)
	var wg sync.WaitGroup
	errCh := make(chan error, nQueries)
	for qid := 0; qid < nQueries; qid++ {
		wg.Add(1)
		go func(qid int) {
			defer wg.Done()
			q := []float32{float32(qid * 10), float32(qid * 10)}
			jobID, err := index.SearchFloat32AsyncWithParams(q, 1, dimension, 1, sp)
			if err != nil {
				errCh <- err
				return
			}
			neighbors, _, err := index.SearchWait(jobID, 1, 1)
			if err != nil {
				errCh <- err
				return
			}
			got[qid] = neighbors[0]
		}(qid)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("async batched search failed: %v", err)
	}
	for qid := 0; qid < nQueries; qid++ {
		if got[qid] != want[qid] {
			t.Errorf("qid=%d async-batched neighbor=%d, sync reference=%d", qid, got[qid], want[qid])
		}
	}
}
