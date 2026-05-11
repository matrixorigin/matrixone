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
	"math/rand"
	"testing"
)

func TestGpuBruteForce(t *testing.T) {
	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	index, err := NewGpuBruteForce[float32](dataset, n_vectors, dimension, L2Expanded, 1, 0)
	if err != nil {
		t.Fatalf("Failed to create GpuBruteForce: %v", err)
	}
	defer index.Destroy()

	index.Start()
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load GpuBruteForce: %v", err)
	}

	queries := []float32{1.0, 1.0, 100.0, 100.0}
	neighbors, distances, err := index.Search(queries, 2, dimension, 1)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	t.Logf("Neighbors: %v, Distances: %v", neighbors, distances)
	if neighbors[0] != 1 {
		t.Errorf("Expected neighbor 1, got %d", neighbors[0])
	}
	if neighbors[1] != 100 {
		t.Errorf("Expected neighbor 100, got %d", neighbors[1])
	}
}

func TestGpuBruteForceChunked(t *testing.T) {
	dimension := uint32(8)
	totalCount := uint64(100)

	// Create empty index (target type half)
	index, err := NewGpuBruteForceEmpty[Float16](totalCount, dimension, L2Expanded, 1, 0)
	if err != nil {
		t.Fatalf("Failed to create GpuBruteForceEmpty: %v", err)
	}
	defer index.Destroy()

	err = index.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	if index.Cap() != uint64(totalCount) {
		t.Errorf("Expected capacity %d, got %d", totalCount, index.Cap())
	}
	if index.Len() != 0 {
		t.Errorf("Expected length 0, got %d", index.Len())
	}

	// Add data in chunks (from float32, triggers on-the-fly conversion to half)
	chunkSize := uint64(50)
	for i := uint64(0); i < totalCount; i += chunkSize {
		chunk := make([]float32, chunkSize*uint64(dimension))
		val := float32(i/chunkSize*100 + 1)
		for j := range chunk {
			chunk[j] = val
		}
		err = index.AddChunkFloat(chunk, chunkSize, nil)
		if err != nil {
			t.Fatalf("AddChunkFloat failed at offset %d: %v", i, err)
		}

		expectedLen := uint32(i + chunkSize)
		if index.Len() != uint64(expectedLen) {
			t.Fatalf("Expected length %d, got %d", expectedLen, index.Len())
		}
	}

	// Build index
	err = index.Build()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Search
	query := make([]Float16, dimension)
	for i := range query {
		query[i] = Float16(1) // matches first chunk
	}
	neighbors, _, err := index.Search(query, 1, dimension, 1)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	if neighbors[0] < 0 || neighbors[0] >= 50 {
		t.Errorf("Expected neighbor from first chunk (0-49), got %d", neighbors[0])
	}
}

func TestGpuBruteForceFloat16(t *testing.T) {
	dimension := uint32(2)
	count := uint64(2)
	dataset := []float32{1.0, 1.0, 2.0, 2.0}

	// Convert to Float16 on GPU
	hDataset := make([]Float16, len(dataset))
	err := GpuConvertF32ToF16(dataset, hDataset, 0)
	if err != nil {
		t.Fatalf("Failed to convert dataset to F16: %v", err)
	}

	index, err := NewGpuBruteForce(hDataset, count, dimension, L2Expanded, 1, 0)
	if err != nil {
		t.Fatalf("Failed to create F16 GpuBruteForce: %v", err)
	}
	defer index.Destroy()

	index.Start()
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load: %v", err)
	}

	queries := []float32{1.0, 1.0}
	hQueries := make([]Float16, len(queries))
	GpuConvertF32ToF16(queries, hQueries, 0)

	neighbors, distances, err := index.Search(hQueries, 1, dimension, 1)
	if err != nil {
		t.Fatalf("Failed to search F16: %v", err)
	}

	if neighbors[0] != 0 {
		t.Errorf("Expected first neighbor 0, got %d", neighbors[0])
	}
	if distances[0] != 0.0 {
		t.Errorf("Expected distance 0.0, got %f", distances[0])
	}
}

// TestGpuBruteForceFilter exercises the new SetFilterColumns / AddFilterChunk /
// SearchFloatWithFilterAsync path. We build a 100-row index with a single
// int64 INCLUDE column "tier" (values 0..99); ask for the nearest neighbor
// among rows where tier > 50. The expected behavior: the prefilter mask drops
// rows 0..50 inside the brute-force kernel, so even a query closest to row 0
// returns row 51 (the next closest pkid that passes the filter).
func TestGpuBruteForceFilter(t *testing.T) {
	dimension := uint32(2)
	nVectors := uint64(100)

	dataset := make([]float32, nVectors*uint64(dimension))
	pkids := make([]int64, nVectors)
	for i := uint64(0); i < nVectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
		pkids[i] = int64(1000 + i)
	}

	idx, err := NewGpuBruteForceEmpty[float32](nVectors, dimension, L2Expanded, 1, 0)
	if err != nil {
		t.Fatalf("NewGpuBruteForceEmpty: %v", err)
	}
	defer idx.Destroy()
	if err = idx.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	colMetaJSON := `[{"name":"tier","type":1}]` // 1 = int64
	if err = idx.SetFilterColumns(colMetaJSON, nVectors); err != nil {
		t.Fatalf("SetFilterColumns: %v", err)
	}
	if err = idx.AddChunkFloat(dataset, nVectors, pkids); err != nil {
		t.Fatalf("AddChunkFloat: %v", err)
	}
	// One column of int64; row i value = i. No nulls.
	colData := make([]byte, int(nVectors)*8)
	for i := uint64(0); i < nVectors; i++ {
		// little-endian int64
		v := int64(i)
		for b := 0; b < 8; b++ {
			colData[int(i)*8+b] = byte(v >> (8 * b))
		}
	}
	if err = idx.AddFilterChunk(0, colData, nil, nVectors); err != nil {
		t.Fatalf("AddFilterChunk: %v", err)
	}
	if err = idx.Build(); err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Query closest to row 0; without filter NN would be pkid 1000 (row 0).
	queries := []float32{0.0, 0.0}
	predsJSON := `[{"col":0,"op":">","val":50}]`
	jobID, err := idx.SearchFloatWithFilterAsync(queries, 1, dimension, 1, predsJSON)
	if err != nil {
		t.Fatalf("SearchFloatWithFilterAsync: %v", err)
	}
	neighbors, _, err := idx.SearchWait(jobID, 1, 1)
	if err != nil {
		t.Fatalf("SearchWait: %v", err)
	}
	if len(neighbors) != 1 {
		t.Fatalf("expected 1 neighbor, got %d", len(neighbors))
	}
	// pkid 1051 is row 51 — the smallest tier > 50.
	if neighbors[0] != 1051 {
		t.Fatalf("filter prefilter failed: expected pkid 1051, got %d", neighbors[0])
	}

	// Sanity: empty preds JSON falls through to unfiltered NN (pkid 1000).
	jobID2, err := idx.SearchFloatWithFilterAsync(queries, 1, dimension, 1, "")
	if err != nil {
		t.Fatalf("SearchFloatWithFilterAsync (no preds): %v", err)
	}
	neighbors2, _, err := idx.SearchWait(jobID2, 1, 1)
	if err != nil {
		t.Fatalf("SearchWait: %v", err)
	}
	if neighbors2[0] != 1000 {
		t.Fatalf("unfiltered NN expected pkid 1000, got %d", neighbors2[0])
	}
}

func BenchmarkGpuAddChunkAndSearchBruteForceF16(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	// Use Float16 as internal type
	index, err := NewGpuBruteForceEmpty[Float16](uint64(totalCount), dimension, L2Expanded, 8, 0)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}

	// Add data in chunks using AddChunkFloat
	for i := 0; i < totalCount; i += chunkSize {
		chunk := dataset[i*dimension : (i+chunkSize)*dimension]
		if err := index.AddChunkFloat(chunk, uint64(chunkSize), nil); err != nil {
			b.Fatalf("AddChunkFloat failed at %d: %v", i, err)
		}
	}

	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, _, err := index.SearchFloat(queries, 1, dimension, 10)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		neighbors, _, err := index.SearchFloat(queries, numQueries, dimension, limit)
		if err != nil {
			return nil, err
		}
		return neighbors, nil
	})
}

func BenchmarkGpuBruteForceF32(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	index, err := NewGpuBruteForce[float32](dataset, uint64(totalCount), dimension, L2Expanded, 8, 0)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, _, err := index.SearchFloat(queries, 1, dimension, 10)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		neighbors, _, err := index.SearchFloat(queries, numQueries, dimension, limit)
		if err != nil {
			return nil, err
		}
		return neighbors, nil
	})
}
