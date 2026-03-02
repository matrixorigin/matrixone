package mocuvs

import (
    "testing"
    "fmt"
)

func TestNewGpuBruteForce(t *testing.T) {
    dimension := uint32(3)
    count := uint64(2)
    dataset := []float32{1.0, 2.0, 3.0, 4.0, 5.0, 6.0}
    
    // Test with float32
    index, err := NewGpuBruteForce(dataset, count, dimension, L2Expanded, 1, 0)
    if err != nil {
        t.Fatalf("Failed to create GpuBruteForce: %v", err)
    }

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    queries := []float32{1.0, 2.0, 3.0}
    neighbors, distances, err := index.Search(queries, 1, dimension, 1)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }

    fmt.Printf("Search Result: Neighbors=%v, Distances=%v\n", neighbors, distances)

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor to be 0, got %d", neighbors[0])
    }
    if distances[0] != 0.0 {
        t.Errorf("Expected first distance to be 0.0, got %f", distances[0])
    }

    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy: %v", err)
    }
}

func TestGpuBruteForceFloat16(t *testing.T) {
    dimension := uint32(2)
    count := uint64(2)
    dataset := []float32{1.0, 1.0, 2.0, 2.0}
    
    // Convert to Float16 on GPU
    hDataset := make([]Float16, len(dataset))
    err := GpuConvertF32ToF16(dataset, hDataset, 0)
    if err != nil {
        t.Fatalf("Failed to convert dataset to F16: %v", err)
    }

    index, err := NewGpuBruteForce(hDataset, count, dimension, L2Expanded, 1, 0)
    if err != nil {
        t.Fatalf("Failed to create F16 GpuBruteForce: %v", err)
    }

    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load: %v", err)
    }

    queries := []float32{1.0, 1.0}
    hQueries := make([]Float16, len(queries))
    GpuConvertF32ToF16(queries, hQueries, 0)

    neighbors, distances, err := index.Search(hQueries, 1, dimension, 1)
    if err != nil {
        t.Fatalf("Failed to search F16: %v", err)
    }

    if neighbors[0] != 0 {
        t.Errorf("Expected first neighbor 0, got %d", neighbors[0])
    }
    if distances[0] != 0.0 {
        t.Errorf("Expected distance 0.0, got %f", distances[0])
    }

    index.Destroy()
}
