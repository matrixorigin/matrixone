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
