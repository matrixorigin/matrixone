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
    err = index.Load()
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
    n_vectors := uint64(100)
    dataset := make([]float32, n_vectors*uint64(dimension))
    for i := range dataset { dataset[i] = float32(i) }

    devices := []int{0}
    bp := DefaultIvfFlatBuildParams()
    bp.NLists = 2
    index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    if err != nil {
        t.Fatalf("Failed to create GpuIvfFlat: %v", err)
    }
    index.Start()
    index.Load()

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
    err = index2.Load()
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
    count, _ := GetGpuDeviceCount()
    if count < 1 {
        t.Skip("Need at least 1 GPU for sharded IVF-Flat test")
    }
    
    devices := []int{0}
    dimension := uint32(2)
    n_vectors := uint64(100)
    dataset := make([]float32, n_vectors*uint64(dimension))
    for i := uint64(0); i < n_vectors; i++ {
        dataset[i*uint64(dimension)] = float32(i)
        dataset[i*uint64(dimension)+1] = float32(i)
    }

    bp := DefaultIvfFlatBuildParams()
    bp.NLists = 5
    index, err := NewGpuIvfFlat[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded)
    if err != nil {
        t.Fatalf("Failed to create sharded IVF-Flat: %v", err)
    }
    defer index.Destroy()

    index.Start()
    err = index.Load()
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
		err = index.AddChunkFloat(chunk, chunkSize, i)
		if err != nil {
			t.Fatalf("AddChunkFloat failed at offset %d: %v", i, err)
		}
	}

	// Build index
	err = index.Load()
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	// Search for first chunk
	query1 := make([]int8, dimension)
	for i := range query1 {
		query1[i] = -128 // matches first chunk (1.0)
	}
	sp := DefaultIvfFlatSearchParams()
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
