package cuvs

import (
    "os"
    "testing"
)

func TestGpuCagra(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = float32(i)
    }

    devices := []int{0}
    bp := DefaultCagraBuildParams()
    index, err := NewGpuCagra[float32](dataset, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    if err != nil {
        t.Fatalf("Failed to create GpuCagra: %v", err)
    }
    defer index.Destroy()

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load/build GpuCagra: %v", err)
    }

    queries := make([]float32, dimension)
    for i := range queries {
        queries[i] = 0.0
    }

    sp := DefaultCagraSearchParams()
    result, err := index.Search(queries, 1, dimension, 5, sp)
    if err != nil {
        t.Fatalf("Search failed: %v", err)
    }

    t.Logf("CAGRA Neighbors: %v, Distances: %v", result.Neighbors, result.Distances)
    if len(result.Neighbors) != 5 {
        t.Errorf("Expected 5 neighbors, got %d", len(result.Neighbors))
    }
    if result.Neighbors[0] != 0 {
        t.Errorf("Expected nearest neighbor to be 0, got %d", result.Neighbors[0])
    }
}

func TestGpuCagraSaveLoad(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = float32(i)
    }

    devices := []int{0}
    bp := DefaultCagraBuildParams()
    index, err := NewGpuCagra[float32](dataset, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    if err != nil {
        t.Fatalf("Failed to create GpuCagra: %v", err)
    }
    err = index.Load()
    if err != nil {
        t.Fatalf("Load failed: %v", err)
    }

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

    err = index2.Load()
    if err != nil {
        t.Fatalf("Load from file failed: %v", err)
    }

    queries := make([]float32, dimension)
    sp := DefaultCagraSearchParams()
    result, err := index2.Search(queries, 1, dimension, 1, sp)
    if err != nil {
        t.Fatalf("Search failed: %v", err)
    }
    if result.Neighbors[0] != 0 {
        t.Errorf("Expected 0, got %d", result.Neighbors[0])
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
    index, err := NewGpuCagra[float32](dataset, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    if err != nil {
        t.Fatalf("Failed to create GpuCagra: %v", err)
    }
    defer index.Destroy()
    index.Load()

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
    for i := range ds1 { ds1[i] = float32(i % 10) }
    // Cluster 2: values around 1000
    ds2 := make([]float32, count*uint64(dimension))
    for i := range ds2 { ds2[i] = float32(1000 + (i % 10)) }

    devices := []int{0}
    bp := DefaultCagraBuildParams()
    bp.IntermediateGraphDegree = 64
    bp.GraphDegree = 32

    idx1, _ := NewGpuCagra[float32](ds1, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    idx2, _ := NewGpuCagra[float32](ds2, count, dimension, L2Expanded, bp, devices, 1, SingleGpu)
    idx1.Load()
    idx2.Load()
    defer idx1.Destroy()
    defer idx2.Destroy()

    merged, err := MergeGpuCagra([]*GpuCagra[float32]{idx1, idx2}, 1, devices)
    if err != nil {
        t.Fatalf("Merge failed: %v", err)
    }
    defer merged.Destroy()

    // Query near Cluster 2
    queries := make([]float32, dimension)
    for i := range queries { queries[i] = 1000.0 }
    sp := DefaultCagraSearchParams()
    result, err := merged.Search(queries, 1, dimension, 1, sp)
    if err != nil {
        t.Fatalf("Search failed: %v", err)
    }
    // Result should be from second index (index >= 200)
    if result.Neighbors[0] < 200 {
        t.Errorf("Expected neighbor from second index (>=200), got %d", result.Neighbors[0])
    }
}

func TestGpuShardedCagra(t *testing.T) {
    count, _ := GetGpuDeviceCount()
    if count < 1 {
        t.Skip("Need at least 1 GPU for sharded CAGRA test")
    }
    
    devices := []int{0} 
    dimension := uint32(16)
    n_vectors := uint64(100)
    dataset := make([]float32, n_vectors*uint64(dimension))
    for i := range dataset { dataset[i] = float32(i) }

    bp := DefaultCagraBuildParams()
    index, err := NewGpuCagra[float32](dataset, n_vectors, dimension, L2Expanded, bp, devices, 1, Sharded)
    if err != nil {
        t.Fatalf("Failed to create sharded CAGRA: %v", err)
    }
    defer index.Destroy()

    err = index.Load()
    if err != nil {
        t.Fatalf("Load sharded failed: %v", err)
    }

    queries := make([]float32, dimension)
    sp := DefaultCagraSearchParams()
    result, err := index.Search(queries, 1, dimension, 5, sp)
    if err != nil {
        t.Fatalf("Search sharded failed: %v", err)
    }
    if len(result.Neighbors) != 5 {
        t.Errorf("Expected 5 neighbors, got %d", len(result.Neighbors))
    }
}
