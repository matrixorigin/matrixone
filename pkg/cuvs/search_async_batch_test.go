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

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 64

	// Each goroutine uses a unique query so we can verify per-caller
	// result demuxing through submit_batched_async's per-request setter.
	runConcurrentAsync(t, /*nGoroutines=*/ 16, /*nPerGoroutine=*/ 8, func(qid int) (int64, error) {
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
	dimension := uint32(2)
	nVectors := uint64(2000)
	dataset := makeLineDataset(nVectors, dimension)

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 16
	index, err := NewGpuIvfFlat[float32](dataset, nVectors, dimension, L2Expanded, bp, []int{0}, 4, SingleGpu, nil)
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

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 16 // probe all lists for deterministic recall on a small index.

	runConcurrentAsync(t, /*nGoroutines=*/ 16, /*nPerGoroutine=*/ 8, func(qid int) (int64, error) {
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

func TestGpuIvfPqSearchFloat32AsyncBatched(t *testing.T) {
	// IVF-PQ is lossy; for deterministic asserts use a higher dimension and
	// a small enough index that the nearest neighbor stays exact under
	// reasonable search params.
	dimension := uint32(64)
	nVectors := uint64(2000)
	dataset := make([]float32, nVectors*uint64(dimension))
	for i := uint64(0); i < nVectors; i++ {
		base := i * uint64(dimension)
		for d := uint32(0); d < dimension; d++ {
			dataset[base+uint64(d)] = float32(i)
		}
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
	if err := index.SetBatchWindow(200); err != nil {
		t.Fatalf("SetBatchWindow: %v", err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 16

	// Limit=5 (not 1) for IVF-PQ: PQ quantization can shift the rank-1
	// neighbor by ±1 even when the query lies exactly on a dataset point;
	// require the true neighbor to appear within the top 5.
	runConcurrentAsync(t, /*nGoroutines=*/ 16, /*nPerGoroutine=*/ 8, func(qid int) (int64, error) {
		q := make([]float32, dimension)
		for d := range q {
			q[d] = float32(qid)
		}
		jobID, err := index.SearchFloat32AsyncWithParams(q, 1, dimension, 5, sp)
		if err != nil {
			return -1, err
		}
		neighbors, _, err := index.SearchWait(jobID, 1, 5)
		if err != nil {
			return -1, err
		}
		want := int64(qid)
		for _, n := range neighbors {
			if n == want {
				return want, nil
			}
		}
		// Surface the actual top-5 in the test failure for easier diagnosis.
		t.Logf("qid=%d top-5=%v (true neighbor %d missing)", qid, neighbors, want)
		return neighbors[0], nil
	})
}

// TestGpuCagraAsyncBatchedMatchesSync sanity-checks that the async-batched
// path returns the same neighbor as a plain sync call at the same query.
// Catches result demuxing bugs in submit_batched_async's per-request setter.
func TestGpuCagraAsyncBatchedMatchesSync(t *testing.T) {
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
