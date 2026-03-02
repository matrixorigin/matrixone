package mocuvs

import (
    "testing"
    "fmt"
    "os"
)

func TestGpuIvfFlatIndex(t *testing.T) {
    dimension := uint32(2)
    count := uint64(4)
    dataset := []float32{
        1.0, 1.0,
        1.1, 1.1,
        100.0, 100.0,
        101.0, 101.0,
    }

    metric := L2Expanded
    nList := uint32(2)
    nthread := uint32(1)
    devices := []int{0}

    // 1. Single GPU Mode
    index, err := NewGpuIvfFlatIndex(dataset, count, dimension, metric, nList, devices, nthread, false)
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
    fmt.Printf("Centers: %v\n", centers)

    queries := []float32{1.05, 1.05}
    neighbors, distances, err := index.Search(queries, 1, dimension, 2, 2)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    fmt.Printf("Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 && neighbors[0] != 1 {
        t.Errorf("Expected first neighbor to be 0 or 1, got %d", neighbors[0])
    }

    index.Destroy()
}

func TestGpuIvfFlatIndexSaveLoad(t *testing.T) {
    dimension := uint32(2)
    count := uint64(4)
    dataset := []float32{1.0, 1.0, 1.1, 1.1, 100.0, 100.0, 101.0, 101.0}
    filename := "test_ivf_flat_go.bin"
    devices := []int{0}

    // 1. Build and Save
    {
        index, err := NewGpuIvfFlatIndex(dataset, count, dimension, L2Expanded, 2, devices, 1, false)
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

    // 2. Load and Search
    {
        index, err := NewGpuIvfFlatIndexFromFile[float32](filename, dimension, L2Expanded, devices, 1, false)
        if err != nil {
            t.Fatalf("Failed to create from file: %v", err)
        }
        if err := index.Load(); err != nil {
            t.Fatalf("Failed to load from file: %v", err)
        }

        queries := []float32{100.5, 100.5}
        neighbors, _, err := index.Search(queries, 1, dimension, 2, 2)
        if err != nil {
            t.Fatalf("Failed to search: %v", err)
        }
        if neighbors[0] != 2 && neighbors[0] != 3 {
            t.Errorf("Expected neighbor 2 or 3, got %d", neighbors[0])
        }
        index.Destroy()
    }

    os.Remove(filename)
}

func TestGpuShardedIvfFlatIndex(t *testing.T) {
    dimension := uint32(2)
    count := uint64(100)
    dataset := make([]float32, count*uint64(dimension))
    for i := range dataset {
        dataset[i] = float32(i) / float32(count)
    }

    devices, _ := GetGpuDeviceList()
    if len(devices) < 1 {
        t.Skip("No GPU devices available")
    }
    
    // Test sharding logic on 1 GPU by forcing MG mode.
    index, err := NewGpuIvfFlatIndex(dataset, count, dimension, L2Expanded, 5, devices, 1, true)
    if err != nil {
        t.Fatalf("Failed to create sharded index: %v", err)
    }

    if err := index.Load(); err != nil {
        t.Fatalf("Failed to load sharded index: %v", err)
    }

    centers, err := index.GetCenters()
    if err != nil {
        t.Fatalf("Failed to get sharded centers: %v", err)
    }
    fmt.Printf("Sharded Centers: %v\n", centers[:10])

    queries := dataset[:dimension]
    neighbors, distances, err := index.Search(queries, 1, dimension, 5, 2)
    if err != nil {
        t.Fatalf("Failed to search sharded index: %v", err)
    }
    fmt.Printf("Sharded Neighbors: %v, Distances: %v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor to be 0, got %d", neighbors[0])
    }

    index.Destroy()
}
