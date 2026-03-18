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

	if index.Cap() != uint32(totalCount) {
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
		err = index.AddChunkFloat(chunk, chunkSize)
		if err != nil {
			t.Fatalf("AddChunkFloat failed at offset %d: %v", i, err)
		}

		expectedLen := uint32(i + chunkSize)
		if index.Len() != expectedLen {
			t.Errorf("Expected length %d, got %d", expectedLen, index.Len())
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
		if err := index.AddChunkFloat(chunk, uint64(chunkSize)); err != nil {
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
