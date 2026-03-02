package cuvs

import (
    "testing"
    "fmt"
    "os"
)

func TestGpuIvfFlatIndex(t *testing.T) {
    dataset := []float32{
        1.0, 1.0,
        1.1, 1.1,
        100.0, 100.0,
        101.0, 101.0,
    }
    countVectors := uint64(4)
    dimension := uint32(2)
    metric := L2Expanded
    nList := uint32(2)
    nthread := uint32(1)

    index, err := NewGpuIvfFlatIndex(dataset, countVectors, dimension, metric, nList, nthread)
    if err != nil {
        t.Fatalf("Failed to create GpuIvfFlatIndex: %v", err)
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
    fmt.Printf("Centers: %v\n", centers)

    queries := []float32{1.05, 1.05}
    neighbors, distances, err := index.Search(queries, 1, dimension, 2, 2)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("Neighbors: %v, Distances: %v\n", neighbors, distances)

    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy: %v", err)
    }
}

func TestGpuIvfFlatIndexSaveLoad(t *testing.T) {
    dataset := []float32{
        1.0, 1.0,
        1.1, 1.1,
        100.0, 100.0,
        101.0, 101.0,
    }
    countVectors := uint64(4)
    dimension := uint32(2)
    metric := L2Expanded
    nList := uint32(2)
    nthread := uint32(1)
    filename := "test_ivf_flat_go.bin"

    // 1. Build and Save
    {
        index, err := NewGpuIvfFlatIndex(dataset, countVectors, dimension, metric, nList, nthread)
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
        index, err := NewGpuIvfFlatIndexFromFile(filename, dimension, metric, nthread)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        centers, err := index.GetCenters()
        if err != nil {
            t.Fatalf("Failed to get centers: %v", err)
        }
        if len(centers) != int(nList * dimension) {
            t.Fatalf("Unexpected centers size: %d", len(centers))
        }

        queries := []float32{100.5, 100.5}
        neighbors, _, err := index.Search(queries, 1, dimension, 2, 2)
        if err != nil {
            t.Fatalf("Failed to search: %v", err)
        }
        if neighbors[0] != 2 && neighbors[0] != 3 {
            t.Fatalf("Unexpected neighbor: %d", neighbors[0])
        }

        index.Destroy()
    }

    os.Remove(filename)
}
