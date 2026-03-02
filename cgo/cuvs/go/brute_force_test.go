package cuvs

import (
    "testing"
    "fmt"
)

func TestNewGpuBruteForceIndex(t *testing.T) {
    // Example dataset: 2 vectors, each with 3 dimensions
    dataset := []float32{
        1.0, 2.0, 3.0,
        4.0, 5.0, 6.0,
    }
    countVectors := uint64(2)
    dimension := uint32(3)
    metric := L2Expanded
    nthread := uint32(1)
    deviceID := 0

    // Create the index
    index, err := NewGpuBruteForceIndex(dataset, countVectors, dimension, metric, nthread, deviceID)
    if err != nil {
        t.Fatalf("Failed to create GpuBruteForceIndex: %v", err)
    }
    if index == nil {
        t.Fatalf("NewGpuBruteForceIndex returned nil index")
    }

    // Load the index
    err = index.Load()
    if err != nil {
        t.Fatalf("Failed to load GpuBruteForceIndex: %v", err)
    }

    // Simple search (queries match dataset for simplicity)
    queries := []float32{
        1.0, 2.0, 3.0, // Query 1
    }
    numQueries := uint64(1)
    queryDimension := uint32(3)
    limit := uint32(1)

    neighbors, distances, err := index.Search(queries, numQueries, queryDimension, limit)
    if err != nil {
        t.Fatalf("Failed to search: %v", err)
    }
    if neighbors == nil || len(neighbors) == 0 {
        t.Fatalf("Search returned empty neighbors")
    }
    fmt.Printf("Search Result: Neighbors=%v, Distances=%v\n", neighbors, distances)

    // Destroy the index
    err = index.Destroy()
    if err != nil {
        t.Fatalf("Failed to destroy GpuBruteForceIndex: %v", err)
    }
}
