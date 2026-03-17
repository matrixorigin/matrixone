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
	"os"
	"testing"
)

func TestGpuIvfPq(t *testing.T) {
	dimension := uint32(16)
	n_vectors := uint64(100)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 8 // dimension 16 is divisible by 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	defer index.Destroy()

	err = index.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load/build GpuIvfPq: %v", err)
	}

	centers, err := index.GetCenters()
	if err != nil {
		t.Fatalf("GetCenters failed: %v", err)
	}
	t.Logf("Centers count: %d, dim_ext: %d", len(centers)/int(index.GetDimExt()), index.GetDimExt())

	query := make([]float32, dimension)
	for i := uint32(0); i < dimension; i++ {
		query[i] = 1.0
	}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 5
	result, err := index.Search(query, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	t.Logf("Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
	if result.Neighbors[0] != 1 {
		t.Errorf("Expected neighbor 1, got %d", result.Neighbors[0])
	}
}

func TestGpuIvfPqSaveLoad(t *testing.T) {
	dimension := uint32(4)
	n_vectors := uint64(100)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = float32(i / int(dimension))
	}

	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 2
	bp.M = 2
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq: %v", err)
	}
	index.Start()
	index.Build()

	filename := "test_ivf_pq.idx"
	err = index.Save(filename)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	defer os.Remove(filename)
	index.Destroy()

	index2, err := NewGpuIvfPqFromFile[float32](filename, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPq from file: %v", err)
	}
	defer index2.Destroy()

	err = index2.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	err = index2.Build()
	if err != nil {
		t.Fatalf("Load from file failed: %v", err)
	}

	query := make([]float32, dimension) // all zeros
	sp := DefaultIvfPqSearchParams()
	result, err := index2.Search(query, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 0 {
		t.Errorf("Expected 0, got %d", result.Neighbors[0])
	}
}

func TestGpuIvfPqChunked(t *testing.T) {
	dimension := uint32(8)
	totalCount := uint64(100)
	devices := []int{0}
	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 4

	// Create empty index (target type int8)
	index, err := NewGpuIvfPqEmpty[int8](totalCount, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfPqEmpty: %v", err)
	}
	defer index.Destroy()

	err = index.Start()
	if err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Add data in chunks (from float32, triggers on-the-fly quantization)
	chunkSize := uint64(50)
	for i := uint64(0); i < totalCount; i += chunkSize {
		chunk := make([]float32, chunkSize*uint64(dimension))
		val := float32(i/chunkSize*100 + 1) // 1.0 for first chunk, 101.0 for second
		for j := range chunk {
			chunk[j] = val
		}
		err = index.AddChunkFloat(chunk, chunkSize)
		if err != nil {
			t.Fatalf("AddChunkFloat failed at offset %d: %v", i, err)
		}
	}

	// Debug: check dataset
	ds := index.GetDataset(totalCount * uint64(dimension))
	t.Logf("Dataset[0]: %v, Dataset[50*dim]: %v", ds[0], ds[50*uint64(dimension)])

	// Build index
	err = index.Build()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Search for first chunk
	query1 := make([]int8, dimension)
	for i := range query1 {
		query1[i] = -128 // matches first chunk (1.0)
	}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10
	result1, err := index.Search(query1, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 1 failed: %v", err)
	}
	if result1.Neighbors[0] < 0 || result1.Neighbors[0] >= 50 {
		t.Errorf("Expected neighbor from first chunk (0-49), got %d", result1.Neighbors[0])
	}

	// Search for second chunk
	query2 := make([]int8, dimension)
	for i := range query2 {
		query2[i] = 127 // matches second chunk (101.0)
	}
	result2, err := index.Search(query2, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 2 failed: %v", err)
	}
	if result2.Neighbors[0] < 50 || result2.Neighbors[0] >= 100 {
		t.Errorf("Expected neighbor from second chunk (50-99), got %d", result2.Neighbors[0])
	}
}

func TestGpuShardedIvfPq(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for sharded IVF-PQ test")
	}

	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded)
	if err != nil {
		t.Fatalf("Failed to create sharded IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load sharded failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.1, 0.1, 10.1, 10.1, 10.1, 10.1}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 5
	result, err := index.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search sharded failed: %v", err)
	}
	t.Logf("Sharded Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func TestGpuReplicatedIvfPq(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for replicated IVF-PQ test")
	}

	dimension := uint32(4)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 10
	bp.M = 2
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Replicated)
	if err != nil {
		t.Fatalf("Failed to create replicated IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load replicated failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.1, 0.1, 10.1, 10.1, 10.1, 10.1}
	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 5
	result, err := index.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search replicated failed: %v", err)
	}
	t.Logf("Replicated Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func BenchmarkGpuShardedIvfPq(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for sharded IVF-PQ benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 100
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Sharded)
	if err != nil {
		b.Fatalf("Failed to create sharded IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	for _, useBatching := range []bool{false, true} {
		b.Run(fmt.Sprintf("Batching%v", useBatching), func(b *testing.B) {
			index.SetUseBatching(useBatching)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.Search(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkGpuSingleIvfPq(b *testing.B) {
	devices := []int{0}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 100
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, SingleGpu)
	if err != nil {
		b.Fatalf("Failed to create single IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	for _, useBatching := range []bool{false, true} {
		b.Run(fmt.Sprintf("Batching%v", useBatching), func(b *testing.B) {
			index.SetUseBatching(useBatching)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.Search(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
		})
	}
}

func BenchmarkGpuReplicatedIvfPq(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for replicated IVF-PQ benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfPqBuildParams()
	bp.NLists = 100
	bp.M = 128 // 1024 / 8
	index, err := NewGpuIvfPq[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Replicated)
	if err != nil {
		b.Fatalf("Failed to create replicated IVF-PQ: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}

	sp := DefaultIvfPqSearchParams()
	sp.NProbes = 10

	for _, useBatching := range []bool{false, true} {
		b.Run(fmt.Sprintf("Batching%v", useBatching), func(b *testing.B) {
			index.SetUseBatching(useBatching)
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				queries := make([]float32, dimension)
				for i := range queries {
					queries[i] = rand.Float32()
				}
				for pb.Next() {
					_, err := index.Search(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
		})
	}
}
