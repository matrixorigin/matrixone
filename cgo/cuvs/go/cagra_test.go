package mocuvs

import (
    "testing"
    "fmt"
    "os"
    "math/rand"
)

func TestGpuCagraIndex(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    intermediateGraphDegree := uint32(32) // Reduced from 64
    graphDegree := uint32(16)            // Reduced from 32
    nthread := uint32(1)
    deviceID := 0

    index, err := NewGpuCagraIndex(dataset, count, dimension, metric, intermediateGraphDegree, graphDegree, nthread, deviceID)
    if err != nil {
        t.Fatalf("Failed to create GpuCagraIndex: %v", err)
    }

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    queries := dataset[:dimension]
    neighbors, distances, err := index.Search(queries, 1, dimension, 5, 16)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("CAGRA Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor to be 0, got %d", neighbors[0])
    }

    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy: %v", err)
    }
}

func TestGpuCagraIndexSaveLoad(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    intermediateGraphDegree := uint32(32) // Reduced from 64
    graphDegree := uint32(16)            // Reduced from 32
    nthread := uint32(1)
    deviceID := 0
    filename := "test_cagra_go.bin"

    // 1. Build and Save
    {
        index, err := NewGpuCagraIndex(dataset, count, dimension, metric, intermediateGraphDegree, graphDegree, nthread, deviceID)
        if err != nil {
            t.Fatalf("Failed to create: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load: %v", err)
        }
        if err := index.Save(filename); err != nil {
            t.Fatalf("Failed to save: %v", err)
        }
        index.Destroy()
    }

    // 2. Load from file and Search
    {
        index, err := NewGpuCagraIndexFromFile[float32](filename, dimension, metric, nthread, deviceID)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        queries := dataset[:dimension]
        neighbors, _, err := index.Search(queries, 1, dimension, 5, 16)
        if err != nil {
            t.Fatalf("Failed to search: %v", err)
        }
        if neighbors[0] != 0 {
            t.Errorf("Expected first neighbor after load to be 0, got %d", neighbors[0])
        }

        index.Destroy()
    }

    os.Remove(filename)
}

func TestGpuCagraIndexExtend(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    intermediateGraphDegree := uint32(32) // Reduced from 64
    graphDegree := uint32(16)            // Reduced from 32
    nthread := uint32(1)
    deviceID := 0

    index, err := NewGpuCagraIndex(dataset, count, dimension, metric, intermediateGraphDegree, graphDegree, nthread, deviceID)
    if err != nil {
        t.Fatalf("Failed to create: %v", err)
    }
    if err := index.Load(); err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    // Extend with 50 more vectors
    newCount := uint64(50)
    newDataset := make([]float32, newCount*uint64(dimension))
    for i := range newDataset {
        newDataset[i] = rand.Float32()
    }

    if err := index.Extend(newDataset, newCount); err != nil {
        t.Fatalf("Failed to extend: %v", err)
    }

    // Search for one of the new vectors
    queries := newDataset[:dimension]
    neighbors, _, err := index.Search(queries, 1, dimension, 5, 16)
    if err != nil {
        t.Fatalf("Failed to search extended: %v", err)
    }
    
    found := false
    for _, n := range neighbors {
        if n == 100 { // First new vector should have index 100
            found = true
            break
        }
    }
    if !found {
        t.Errorf("Could not find extended vector in search results: %v", neighbors)
    }

    index.Destroy()
}

func TestGpuCagraIndexMerge(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100) // Increased to 100 to accommodate graph degree
    
    dataset1 := make([]float32, count*uint64(dimension))
    for i := range dataset1 { dataset1[i] = rand.Float32() }
    
    dataset2 := make([]float32, count*uint64(dimension))
    for i := range dataset2 { dataset2[i] = rand.Float32() + 10.0 } // Far away

    metric := L2Expanded
    nthread := uint32(1)
    deviceID := 0

    // Using smaller degrees to avoid warnings and speed up build
    idx1, err := NewGpuCagraIndex(dataset1, count, dimension, metric, 32, 16, nthread, deviceID)
    if err != nil { t.Fatalf("NewGpuCagraIndex 1 failed: %v", err) }
    if err := idx1.Load(); err != nil { t.Fatalf("Load 1 failed: %v", err) }
    
    idx2, err := NewGpuCagraIndex(dataset2, count, dimension, metric, 32, 16, nthread, deviceID)
    if err != nil { t.Fatalf("NewGpuCagraIndex 2 failed: %v", err) }
    if err := idx2.Load(); err != nil { t.Fatalf("Load 2 failed: %v", err) }

    mergedIdx, err := MergeCagraIndices([]*GpuCagraIndex[float32]{idx1, idx2}, nthread, deviceID)
    if err != nil {
        t.Fatalf("Failed to merge: %v", err)
    }

    // Search for a vector from the second dataset
    queries := dataset2[:dimension]
    neighbors, _, err := mergedIdx.Search(queries, 1, dimension, 5, 16)
    if err != nil {
        t.Fatalf("Failed to search merged: %v", err)
    }

    found := false
    for _, n := range neighbors {
        if n == 100 { // First vector of second index should be at index 100
            found = true
            break
        }
    }
    if !found {
        t.Errorf("Could not find vector from second index in merged result: %v", neighbors)
    }

    if err := idx1.Destroy(); err != nil { t.Errorf("idx1 Destroy failed: %v", err) }
    if err := idx2.Destroy(); err != nil { t.Errorf("idx2 Destroy failed: %v", err) }
    if err := mergedIdx.Destroy(); err != nil { t.Errorf("mergedIdx Destroy failed: %v", err) }
}
