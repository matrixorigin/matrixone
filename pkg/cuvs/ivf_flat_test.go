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

func TestGpuIvfFlat(t *testing.T) {
	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	devices := []int{0}
	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 10
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfFlat: %v", err)
	}
	defer index.Destroy()

	index.Start()
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to load/build GpuIvfFlat: %v", err)
	}

	centers, err := index.GetCenters(10)
	if err != nil {
		t.Fatalf("GetCenters failed: %v", err)
	}
	t.Logf("Centers: %v", centers[:4])

	queries := []float32{1.0, 1.0, 100.0, 100.0}
	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 5
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

func TestGpuIvfFlatSaveLoad(t *testing.T) {
	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = float32(i)
	}

	devices := []int{0}
	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 2
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfFlat: %v", err)
	}
	index.Start()
	index.Build()

	filename := "test_ivf_flat.idx"
	err = index.Save(filename)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	defer os.Remove(filename)
	index.Destroy()

	index2, err := NewGpuIvfFlatFromFile[float32](filename, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfFlat from file: %v", err)
	}
	defer index2.Destroy()

	index2.Start()
	err = index2.Build()
	if err != nil {
		t.Fatalf("Load from file failed: %v", err)
	}

	queries := []float32{0.0, 0.0}
	sp := DefaultIvfFlatSearchParams()
	result, err := index2.Search(queries, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 0 {
		t.Errorf("Expected 0, got %d", result.Neighbors[0])
	}
}

func TestGpuShardedIvfFlat(t *testing.T) {
	t.Skip("Sharded mode is currently disabled due to a suspected bug in cuVS (SIZE_MAX mdspan extents)")
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for sharded IVF-Flat test")
	}

	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 10
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded)
	if err != nil {
		t.Fatalf("Failed to create sharded IVF-Flat: %v", err)
	}
	defer index.Destroy()

	index.Start()
	err = index.Build()
	if err != nil {
		t.Fatalf("Load sharded failed: %v", err)
	}

	queries := []float32{0.1, 0.1, 0.2, 0.2, 0.3, 0.3, 0.4, 0.4, 0.5, 0.5}
	sp := DefaultIvfFlatSearchParams()
	result, err := index.Search(queries, 5, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search sharded failed: %v", err)
	}
	t.Logf("Sharded Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func TestGpuReplicatedIvfFlat(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for replicated IVF-Flat test")
	}

	dimension := uint32(2)
	n_vectors := uint64(1000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		dataset[i*uint64(dimension)] = float32(i)
		dataset[i*uint64(dimension)+1] = float32(i)
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 10
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Replicated)
	if err != nil {
		t.Fatalf("Failed to create replicated IVF-Flat: %v", err)
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
	sp := DefaultIvfFlatSearchParams()
	result, err := index.Search(queries, 5, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search replicated failed: %v", err)
	}
	t.Logf("Replicated Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
}

func BenchmarkGpuShardedIvfFlat(b *testing.B) {
	b.Skip("Sharded mode is currently disabled due to a suspected bug in cuVS (SIZE_MAX mdspan extents)")
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for sharded IVF-Flat benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 1000
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Sharded)
	if err != nil {
		b.Fatalf("Failed to create sharded IVF-Flat: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3

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
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuSingleIvfFlat(b *testing.B) {
	devices := []int{0}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 1000
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, SingleGpu)
	if err != nil {
		b.Fatalf("Failed to create single IVF-Flat: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3

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
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuReplicatedIvfFlat(b *testing.B) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		b.Skip("Need at least 1 GPU for replicated IVF-Flat benchmark")
	}

	dimension := uint32(1024)
	n_vectors := uint64(100000)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 1000
	index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Replicated)
	if err != nil {
		b.Fatalf("Failed to create replicated IVF-Flat: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		b.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		b.Fatalf("Build failed: %v", err)
	}
	// info, _ := index.Info()
	// fmt.Println(info)

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3

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
					_, err := index.SearchFloat(queries, 1, dimension, 10, sp)
					if err != nil {
						b.Fatalf("Search failed: %v", err)
					}
				}
			})
			b.StopTimer()
			ReportRecall(b, dataset, uint64(n_vectors), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
				res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
				if err != nil {
					return nil, err
				}
				return res.Neighbors, nil
			})

		})
	}
}

func BenchmarkGpuAddChunkAndSearchIvfFlatF16(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 1000
	// Use Float16 as internal type
	index, err := NewGpuIvfFlatEmpty[Float16](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
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

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3

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
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})

}

func BenchmarkGpuAddChunkAndSearchIvfFlatInt8(b *testing.B) {
	const dimension = 1024
	const totalCount = 100000
	const chunkSize = 10000

	dataset := make([]float32, totalCount*dimension)
	for i := range dataset {
		dataset[i] = rand.Float32()
	}

	devices := []int{0}
	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 1000
	// Use int8 as internal type
	index, err := NewGpuIvfFlatEmpty[int8](uint64(totalCount), dimension, L2Expanded, bp, devices, 8, SingleGpu)
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

	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3

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
	ReportRecall(b, dataset, uint64(totalCount), uint32(dimension), 10, func(queries []float32, numQueries uint64, limit uint32) ([]int64, error) {
		res, err := index.SearchFloat(queries, numQueries, dimension, limit, sp)
		if err != nil {
			return nil, err
		}
		return res.Neighbors, nil
	})

}

func TestGpuIvfFlatChunked(t *testing.T) {
	dimension := uint32(8)
	totalCount := uint64(100)
	devices := []int{0}
	bp := DefaultIvfFlatBuildParams()
	bp.NLists = 10

	// Create empty index (target type int8)
	index, err := NewGpuIvfFlatEmpty[int8](totalCount, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("Failed to create GpuIvfFlatEmpty: %v", err)
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
	sp := DefaultIvfFlatSearchParams()
	sp.NProbes = 3
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
