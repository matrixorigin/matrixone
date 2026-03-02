package mocuvs

import (
    "testing"
    "fmt"
    "os"
)

func TestGpuShardedIvfFlatIndex(t *testing.T) {
    dataset := make([]float32, 100*16)
    for i := range dataset {
        dataset[i] = float32(i) / float32(len(dataset))
    }
    countVectors := uint64(100)
    dimension := uint32(16)
    metric := L2Expanded
    nList := uint32(5)
    devices := []int{0}
    nthread := uint32(1)

    index, err := NewGpuShardedIvfFlatIndex(dataset, countVectors, dimension, metric, nList, devices, nthread)
    if err != nil {
        t.Fatalf("Failed to create: %v", err)
    }

    if err := index.Load(); err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    centers, err := index.GetCenters()
    if err != nil {
        t.Fatalf("Failed to get centers: %v", err)
    }
    fmt.Printf("Sharded Centers: %v\n", centers[:10])

    queries := make([]float32, 16)
    copy(queries, dataset[:16])
    neighbors, distances, err := index.Search(queries, 1, dimension, 5, 2)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("Sharded Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Fatalf("Expected neighbor 0, got %d", neighbors[0])
    }

    index.Destroy()
}

func TestGpuShardedIvfFlatIndexSaveLoad(t *testing.T) {
    dataset := make([]float32, 100*16)
    for i := range dataset {
        dataset[i] = float32(i) / float32(len(dataset))
    }
    countVectors := uint64(100)
    dimension := uint32(16)
    metric := L2Expanded
    nList := uint32(5)
    devices := []int{0}
    nthread := uint32(1)
    filename := "test_sharded_ivf_flat_go.bin"

    {
        index, err := NewGpuShardedIvfFlatIndex(dataset, countVectors, dimension, metric, nList, devices, nthread)
        if err != nil {
            t.Fatalf("Failed to create: %v", err)
        }
        index.Load()
        if err := index.Save(filename); err != nil {
            t.Fatalf("Failed to save: %v", err)
        }
        index.Destroy()
    }

    {
        index, err := NewGpuShardedIvfFlatIndexFromFile[float32](filename, dimension, metric, devices, nthread)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        queries := make([]float32, 16)
        copy(queries, dataset[:16])
        neighbors, _, err := index.Search(queries, 1, dimension, 5, 2)
        if err != nil {
            t.Fatalf("Failed to search: %v", err)
        }
        if neighbors[0] != 0 {
            t.Fatalf("Expected neighbor 0, got %d", neighbors[0])
        }
        index.Destroy()
    }

    os.Remove(filename)
}
