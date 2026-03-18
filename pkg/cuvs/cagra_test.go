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

func TestGpuCagra(t *testing.T) {
	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load/build GpuCagra: %v", err)
	}

	queries := []float32{1.0, 1.0, 100.0, 100.0}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}

	t.Logf("Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
	if result.Neighbors[0] != 1 {
		t.Errorf("Expected neighbor 1, got %d", result.Neighbors[0])
	}
	if result.Neighbors[1] != 100 {
		t.Errorf("Expected neighbor 100, got %d", result.Neighbors[1])
	}
}

func TestGpuCagraSaveLoad(t *testing.T) {
	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = float32(i)
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	index.Build()

	filename := "test_cagra.idx"
	err = index.Save(filename)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	defer os.Remove(filename)
	index.Destroy()

	index2, err := NewGpuCagraFromFile[float32](filename, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra from file: %v", err)
	}
	defer index2.Destroy()

	if err := index2.Start(); err != nil {
		t.Fatalf("index2 Start failed: %v", err)
	}
	err = index2.Build()
	if err != nil {
		t.Fatalf("Load from file failed: %v", err)
	}

	queries := []float32{0.0, 0.0}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index2.Search(queries, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 0 {
		t.Errorf("Expected 0, got %d", result.Neighbors[0])
	}
}

func TestGpuShardedCagra(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for sharded CAGRA test")
	}

	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded)
	if err != nil {
		t.Fatalf("Failed to create sharded CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load sharded failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.2, 0.2, 0.3, 0.3, 0.4, 0.4, 0.5, 0.5}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index.Search(queries, 5, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search sharded failed: %v", err)
	}
	t.Logf("Sharded Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func TestGpuCagraChunked(t *testing.T) {
	dimension := uint32(8)
	totalCount := uint64(100)
	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128

	// Create empty index (target type int8)
	index, err := NewGpuCagraEmpty[int8](totalCount, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuCagraEmpty: %v", err)
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
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
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

func TestGpuCagraExtend(t *testing.T) {
	dimension := uint32(16)
	count := uint64(100)
	dataset := make([]float32, count*uint64(dimension))
	for i := range dataset {
		dataset[i] = float32(i)
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	defer index.Destroy()
	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	index.Build()

	extra := make([]float32, 10*dimension)
	for i := range extra {
		extra[i] = 1000.0
	}
	err = index.Extend(extra, 10)
	if err != nil {
		t.Fatalf("Extend failed: %v", err)
	}

	queries := make([]float32, dimension)
	for i := range queries {
		queries[i] = 1000.0
	}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index.Search(queries, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] < 100 {
		t.Errorf("Expected neighbor from extended data, got %d", result.Neighbors[0])
	}
}

func TestGpuCagraMerge(t *testing.T) {
	dimension := uint32(16)
	count := uint64(200)

	// Cluster 1: values around 0
	ds1 := make([]float32, count*uint64(dimension))
	for i := range ds1 {
		ds1[i] = float32(i % 10)
	}
	// Cluster 2: values around 1000
	ds2 := make([]float32, count*uint64(dimension))
	for i := range ds2 {
		ds2[i] = float32(1000 + (i % 10))
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128

	idx1, err := NewGpuCagra[float32](ds1, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create idx1: %v", err)
	}
	idx2, err := NewGpuCagra[float32](ds2, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create idx2: %v", err)
	}
	if err := idx1.Start(); err != nil {
		t.Fatalf("idx1 Start failed: %v", err)
	}
	idx1.Build()
	if err := idx2.Start(); err != nil {
		t.Fatalf("idx2 Start failed: %v", err)
	}
	idx2.Build()
	defer idx1.Destroy()
	defer idx2.Destroy()

	merged, err := MergeGpuCagra([]*GpuCagra[float32]{idx1, idx2}, 1, devices)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer merged.Destroy()

	if err := merged.Start(); err != nil {
		t.Fatalf("merged Start failed: %v", err)
	}

	// Query near Cluster 2
	queries := make([]float32, dimension)
	for i := range queries {
		queries[i] = 1000.0
	}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := merged.Search(queries, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	// Result should be from second index (index >= 200)
	if result.Neighbors[0] < 200 {
		t.Errorf("Expected neighbor from second index (>=200), got %d", result.Neighbors[0])
	}
}

func TestGpuReplicatedCagra(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for replicated CAGRA test")
	}

	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Replicated)
	if err != nil {
		t.Fatalf("Failed to create replicated CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Load replicated failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.2, 0.2, 0.3, 0.3, 0.4, 0.4, 0.5, 0.5}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index.Search(queries, 5, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search replicated failed: %v", err)
	}
	t.Logf("Replicated Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func BenchmarkGpuShardedCagra(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for sharded CAGRA benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Sharded)
	if err != nil {
		b.Fatalf("Failed to create sharded CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	index.Info()

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3

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
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]uint32, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})
		})
	}
}

func BenchmarkGpuSingleCagra(b *testing.B) {
	devices := []int{0}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, SingleGpu)
	if err != nil {
		b.Fatalf("Failed to create single CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	index.Info()

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3

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
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]uint32, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})
		})
	}
}

func BenchmarkGpuReplicatedCagra(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for replicated CAGRA benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Replicated)
	if err != nil {
		b.Fatalf("Failed to create replicated CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	index.Info()

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3

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
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]uint32, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})
		})
	}
}

func BenchmarkGpuAddChunkAndSearchCagraF16(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	// Use Float16 as internal type
	index, err := NewGpuCagraEmpty[Float16](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
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
	index.Info()

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]uint32, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})
}

func BenchmarkGpuAddChunkAndSearchCagraInt8(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultCagraBuildParams()
	bp.IntermediateGraphDegree = 256
	bp.GraphDegree = 128
	// Use int8 as internal type
	index, err := NewGpuCagraEmpty[int8](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
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
	index.Info()

	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		queries := make([]float32, dimension)
		for i := range queries {
			queries[i] = rand.Float32()
		}
		for pb.Next() {
			_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
			if err != nil {
				b.Fatalf("Search failed: %v", err)
			}
		}
	})
	b.StopTimer()
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]uint32, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})
}
