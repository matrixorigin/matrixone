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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
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

	if got := index2.Len(); got != uint32(n_vectors) {
		t.Errorf("Expected length %d, got %d", n_vectors, got)
	}
}

func TestGpuCagraPackUnpack(t *testing.T) {
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	for _, filename := range []string{"test_cagra_pack.tar", "test_cagra_pack.tar.gz"} {
		t.Run(filename, func(t *testing.T) {
			if err := index.Pack(filename); err != nil {
				t.Fatalf("Pack failed: %v", err)
			}
			defer os.Remove(filename)

			index2, err := NewGpuCagraEmpty[float32](0, dimension, L2Expanded, bp, devices, 1, SingleGpu)
			if err != nil {
				t.Fatalf("NewGpuCagraEmpty failed: %v", err)
			}
			defer index2.Destroy()
			if err := index2.Start(); err != nil {
				t.Fatalf("index2 Start failed: %v", err)
			}
			if err := index2.Unpack(filename); err != nil {
				t.Fatalf("Unpack failed: %v", err)
			}

			if got := index2.Len(); got != uint32(n_vectors) {
				t.Errorf("Expected length %d, got %d", n_vectors, got)
			}
		})
	}
	index.Destroy()
}

func TestGpuCagraFromDataDirectory(t *testing.T) {
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	if err := index.Build(); err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	// Pack to tar, then extract to a directory, then load via NewGpuCagraFromDataDirectory
	tarFile := "test_cagra_dir.tar"
	if err := index.Pack(tarFile); err != nil {
		t.Fatalf("Pack failed: %v", err)
	}
	defer os.Remove(tarFile)
	index.Destroy()

	tmpDir, err := os.MkdirTemp("", "cagra-dir-test-*")
	if err != nil {
		t.Fatalf("MkdirTemp failed: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	if _, err := Unpack(tarFile, tmpDir); err != nil {
		t.Fatalf("Unpack to dir failed: %v", err)
	}

	index2, err := NewGpuCagraFromDataDirectory[float32](tmpDir, dimension, L2Expanded, bp, devices, 1, SingleGpu)
	if err != nil {
		t.Fatalf("NewGpuCagraFromDataDirectory failed: %v", err)
	}
	defer index2.Destroy()

	queries := []float32{1.0, 1.0, 100.0, 100.0}
	sp := DefaultCagraSearchParams()
	sp.ItopkSize = 128
	sp.SearchWidth = 3
	result, err := index2.Search(queries, 2, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if result.Neighbors[0] != 1 {
		t.Errorf("Expected neighbor 1, got %d", result.Neighbors[0])
	}
	if result.Neighbors[1] != 100 {
		t.Errorf("Expected neighbor 100, got %d", result.Neighbors[1])
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded, nil)
	if err != nil {
		t.Fatalf("Failed to create sharded CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to build sharded index: %v", err)
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
	if result1.Neighbors[0] == 4294967295 || result1.Neighbors[0] >= 50 {
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
	index, err := NewGpuCagra[float32](dataset, count, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
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
	err = index.Extend(extra, 10, nil)
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

	idx1, err := NewGpuCagra[float32](ds1, count, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create idx1: %v", err)
	}
	idx2, err := NewGpuCagra[float32](ds2, count, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
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

func TestGpuCagraMergeWithIds(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for CAGRA merge test")
	}

	dimension := uint32(16)
	count := uint64(100)

	// Index 1: values around 0, IDs [1000..199]
	ds1 := make([]float32, count*uint64(dimension))
	ids1 := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		ids1[i] = uint32(1000 + i)
		for j := uint32(0); j < dimension; j++ {
			ds1[i*uint64(dimension)+uint64(j)] = float32(i % 10)
		}
	}

	// Index 2: values around 5000, IDs [5000..5099]
	ds2 := make([]float32, count*uint64(dimension))
	ids2 := make([]uint32, count)
	for i := uint64(0); i < count; i++ {
		ids2[i] = uint32(5000 + i)
		for j := uint32(0); j < dimension; j++ {
			ds2[i*uint64(dimension)+uint64(j)] = float32(5000 + (i % 10))
		}
	}

	bp := DefaultCagraBuildParams()
	idx1, err := NewGpuCagra[float32](ds1, count, dimension, L2Expanded, bp, devices, 1, SingleGpu, ids1)
	if err != nil {
		t.Fatalf("Failed to create idx1: %v", err)
	}
	idx1.Start()
	idx1.Build()

	idx2, err := NewGpuCagra[float32](ds2, count, dimension, L2Expanded, bp, devices, 1, SingleGpu, ids2)
	if err != nil {
		t.Fatalf("Failed to create idx2: %v", err)
	}
	idx2.Start()
	idx2.Build()

	defer idx1.Destroy()
	defer idx2.Destroy()

	merged, err := MergeGpuCagra([]*GpuCagra[float32]{idx1, idx2}, 1, devices)
	if err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	defer merged.Destroy()
	merged.Start()

	// Query for Cluster 1: expect ID in [1000, 1100)
	q1 := make([]float32, dimension)
	for j := range q1 {
		q1[j] = 0.0
	}
	sp := DefaultCagraSearchParams()
	r1, err := merged.Search(q1, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 1 failed: %v", err)
	}
	if r1.Neighbors[0] < 1000 || r1.Neighbors[0] >= 1100 {
		t.Errorf("Expected neighbor from idx1 [1000, 1100), got %d", r1.Neighbors[0])
	}

	// Query for Cluster 2: expect ID in [5000, 5100)
	q2 := make([]float32, dimension)
	for j := range q2 {
		q2[j] = 5000.0
	}
	r2, err := merged.Search(q2, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search 2 failed: %v", err)
	}
	if r2.Neighbors[0] < 5000 || r2.Neighbors[0] >= 5100 {
		t.Errorf("Expected neighbor from idx2 [5000, 5100), got %d", r2.Neighbors[0])
	}
}

func TestGpuCagraDeleteId(t *testing.T) {
	devices, err := GetGpuDeviceList()
	if err != nil || len(devices) < 1 {
		t.Skip("Need at least 1 GPU for deletion test")
	}

	dimension := uint32(16)
	n_vectors := uint64(100)
	dataset := make([]float32, n_vectors*uint64(dimension))
	for i := uint64(0); i < n_vectors; i++ {
		for j := uint32(0); j < dimension; j++ {
			dataset[i*uint64(dimension)+uint64(j)] = float32(i)
		}
	}

	bp := DefaultCagraBuildParams()
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu, nil)
	if err != nil {
		t.Fatalf("Failed to create GpuCagra: %v", err)
	}
	defer index.Destroy()

	index.Start()
	index.Build()

	// Query exactly at vector 50
	q50 := make([]float32, dimension)
	for i := range q50 {
		q50[i] = 50.0
	}

	sp := DefaultCagraSearchParams()
	r, err := index.Search(q50, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if r.Neighbors[0] != 50 {
		t.Errorf("Expected neighbor 50, got %d", r.Neighbors[0])
	}

	// Delete ID 50
	if err := index.DeleteId(50); err != nil {
		t.Fatalf("DeleteId failed: %v", err)
	}

	// Search again
	r, err = index.Search(q50, 1, dimension, 1, sp)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if r.Neighbors[0] == 50 {
		t.Errorf("Neighbor 50 was deleted but still returned")
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Replicated, nil)
	if err != nil {
		t.Fatalf("Failed to create replicated CAGRA: %v", err)
	}
	defer index.Destroy()

	if err := index.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	err = index.Build()
	if err != nil {
		t.Fatalf("Failed to build replicated index: %v", err)
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 16, Sharded, nil)
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, SingleGpu, nil)
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
	index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 8, Replicated, nil)
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
