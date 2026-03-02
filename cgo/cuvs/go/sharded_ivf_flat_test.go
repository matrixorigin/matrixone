package cuvs

import (
    "testing"
    "fmt"
    "os"
    "math/rand"
)

func TestGpuShardedIvfFlatIndex(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    nList := uint32(5)
    devices := []int{0} // Testing with single GPU in sharded mode
    nthread := uint32(1)

    index, err := NewGpuShardedIvfFlatIndex(dataset, count, dimension, metric, nList, devices, nthread)
    if err != nil {
        t.Fatalf("Failed to create GpuShardedIvfFlatIndex: %v", err)
    }

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    centers, err := index.GetCenters()
    if err != nil {
        t.Fatalf("Failed to get centers: %v", err)
    }
    if len(centers) != int(nList * dimension) {
        t.Fatalf("Unexpected centers size: %d", len(centers))
    }
    fmt.Printf("Sharded Centers: %v\n", centers[:min(len(centers), 10)])

    // Search for the first vector
    queries := dataset[:dimension]
    neighbors, distances, err := index.Search(queries, 1, dimension, 5, 2)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("Sharded Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor to be 0, got %d", neighbors[0])
    }

    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy: %v", err)
    }
}

func TestGpuShardedIvfFlatIndexSaveLoad(t *testing.T) {
    dimension := uint32(16)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = rand.Float32()
    }

    metric := L2Expanded
    nList := uint32(5)
    devices := []int{0}
    nthread := uint32(1)
    filename := "test_sharded_ivf_flat_go.bin"

    // 1. Build and Save
    {
        index, err := NewGpuShardedIvfFlatIndex(dataset, count, dimension, metric, nList, devices, nthread)
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
        index, err := NewGpuShardedIvfFlatIndexFromFile(filename, dimension, metric, devices, nthread)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        queries := dataset[:dimension]
        neighbors, _, err := index.Search(queries, 1, dimension, 5, 2)
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

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
