package mocuvs

import (
    "testing"
    "fmt"
    "os"
    "math/rand"
)

func TestGpuShardedCagraIndex(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    intermediateGraphDegree := uint32(64)
    graphDegree := uint32(32)
    devices := []int{0} // Testing with single GPU in sharded mode
    nthread := uint32(1)

    index, err := NewGpuShardedCagraIndex(dataset, count, dimension, metric, intermediateGraphDegree, graphDegree, devices, nthread)
    if err != nil {
        t.Fatalf("Failed to create GpuShardedCagraIndex: %v", err)
    }

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    // Search for the first vector
    queries := dataset[:dimension]
    neighbors, distances, err := index.Search(queries, 1, dimension, 5, 32)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("Sharded CAGRA Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor to be 0, got %d", neighbors[0])
    }

    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy: %v", err)
    }
}

func TestGpuShardedCagraIndexSaveLoad(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    intermediateGraphDegree := uint32(64)
    graphDegree := uint32(32)
    devices := []int{0}
    nthread := uint32(1)
    filename := "test_sharded_cagra_go.bin"

    // 1. Build and Save
    {
        index, err := NewGpuShardedCagraIndex(dataset, count, dimension, metric, intermediateGraphDegree, graphDegree, devices, nthread)
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
        index, err := NewGpuShardedCagraIndexFromFile[float32](filename, dimension, metric, devices, nthread)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        queries := dataset[:dimension]
        neighbors, _, err := index.Search(queries, 1, dimension, 5, 32)
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
