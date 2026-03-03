package mocuvs

import (
    "testing"
    "fmt"
)

func TestGpuKMeans_Float32(t *testing.T) {
    nClusters := uint32(3)
    dimension := uint32(2)
    nSamples := uint64(9)

    // Create 3 clusters
    dataset := []float32{
        0.1, 0.1,   0.0, 0.2,   0.2, 0.0,  // Cluster 0
        10.1, 10.1, 10.0, 10.2, 10.2, 10.0, // Cluster 1
        20.1, 20.1, 20.0, 20.2, 20.2, 20.0, // Cluster 2
    }

    deviceID := 0
    kmeans, err := NewGpuKMeans[float32](nClusters, dimension, L2Expanded, 100, 1e-4, 1, deviceID, 1)
    if err != nil {
        t.Fatalf("Failed to create GpuKMeans: %v", err)
    }
    defer kmeans.Destroy()

    inertia, nIter, err := kmeans.Fit(dataset, nSamples)
    if err != nil {
        t.Fatalf("Fit failed: %v", err)
    }
    fmt.Printf("Fit: inertia=%f, nIter=%d\n", inertia, nIter)

    labels, pInertia, err := kmeans.Predict(dataset, nSamples)
    if err != nil {
        t.Fatalf("Predict failed: %v", err)
    }
    fmt.Printf("Predict labels: %v, inertia=%f\n", labels, pInertia)

    if len(labels) != int(nSamples) {
        t.Errorf("Expected %d labels, got %d", nSamples, len(labels))
    }

    // Basic clustering check
    if labels[0] != labels[1] || labels[1] != labels[2] {
        t.Errorf("Cluster 0 points should have same label")
    }
    if labels[3] != labels[4] || labels[4] != labels[5] {
        t.Errorf("Cluster 1 points should have same label")
    }
    if labels[6] != labels[7] || labels[7] != labels[8] {
        t.Errorf("Cluster 2 points should have same label")
    }

    centroids, err := kmeans.GetCentroids()
    if err != nil {
        t.Fatalf("GetCentroids failed: %v", err)
    }
    if len(centroids) != int(nClusters*dimension) {
        t.Errorf("Expected %d centroid elements, got %d", nClusters*dimension, len(centroids))
    }
}

func TestGpuKMeans_FitPredict_Float16(t *testing.T) {
    nClusters := uint32(2)
    dimension := uint32(4)
    nSamples := uint64(10)

    dataset := make([]float32, nSamples*uint64(dimension))
    for i := range dataset {
        dataset[i] = 0.5
    }
    
    // Convert to F16
    datasetF16 := make([]Float16, len(dataset))
    err := GpuConvertF32ToF16(dataset, datasetF16, 0)
    if err != nil {
        t.Fatalf("F32 to F16 conversion failed: %v", err)
    }

    deviceID := 0
    kmeans, err := NewGpuKMeans[Float16](nClusters, dimension, L2Expanded, 100, 1e-4, 1, deviceID, 1)
    if err != nil {
        t.Fatalf("Failed to create GpuKMeans: %v", err)
    }
    defer kmeans.Destroy()

    labels, inertia, nIter, err := kmeans.FitPredict(datasetF16, nSamples)
    if err != nil {
        t.Fatalf("FitPredict failed: %v", err)
    }
    fmt.Printf("FitPredict: inertia=%f, nIter=%d\n", inertia, nIter)
    if len(labels) != int(nSamples) {
        t.Errorf("Expected %d labels, got %d", nSamples, len(labels))
    }
}
