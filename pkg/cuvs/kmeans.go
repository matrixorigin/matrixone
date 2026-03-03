package cuvs

/*
#include "../../cgo/cuvs/kmeans_c.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"
import (
    "fmt"
    "runtime"
    "unsafe"
)

// GpuKMeans represents the C++ gpu_kmeans_t object.
type GpuKMeans[T VectorType] struct {
    cKMeans C.gpu_kmeans_c
    nClusters uint32
    dimension uint32
}

// NewGpuKMeans creates a new GpuKMeans instance.
func NewGpuKMeans[T VectorType](nClusters uint32, dimension uint32, metric DistanceType, maxIter int, deviceID int, nthread uint32) (*GpuKMeans[T], error) {
    qtype := GetQuantization[T]()

    var errmsg *C.char
    cKMeans := C.gpu_kmeans_new(
        C.uint32_t(nClusters),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        C.int(maxIter),
        C.int(deviceID),
        C.uint32_t(nthread),
        C.quantization_t(qtype),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cKMeans == nil {
        return nil, fmt.Errorf("failed to create GpuKMeans")
    }
    return &GpuKMeans[T]{cKMeans: cKMeans, nClusters: nClusters, dimension: dimension}, nil
}

// Destroy frees the C++ gpu_kmeans_t instance
func (gk *GpuKMeans[T]) Destroy() error {
    if gk.cKMeans == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_kmeans_destroy(gk.cKMeans, unsafe.Pointer(&errmsg))
    gk.cKMeans = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Fit computes the cluster centroids.
func (gk *GpuKMeans[T]) Fit(dataset []T, nSamples uint64) (float32, int64, error) {
    if gk.cKMeans == nil {
        return 0, 0, fmt.Errorf("GpuKMeans is not initialized")
    }
    if len(dataset) == 0 || nSamples == 0 {
        return 0, 0, nil
    }

    var errmsg *C.char
    res := C.gpu_kmeans_fit(
        gk.cKMeans,
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(nSamples),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return 0, 0, fmt.Errorf("%s", errStr)
    }

    return float32(res.inertia), int64(res.n_iter), nil
}

// Predict assigns labels to new data based on existing centroids.
func (gk *GpuKMeans[T]) Predict(dataset []T, nSamples uint64) ([]int64, float32, error) {
    if gk.cKMeans == nil {
        return nil, 0, fmt.Errorf("GpuKMeans is not initialized")
    }
    if len(dataset) == 0 || nSamples == 0 {
        return nil, 0, nil
    }

    var errmsg *C.char
    res := C.gpu_kmeans_predict(
        gk.cKMeans,
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(nSamples),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, 0, fmt.Errorf("%s", errStr)
    }

    if res.result_ptr == nil {
        return nil, 0, fmt.Errorf("predict returned nil result")
    }

    labels := make([]int64, nSamples)
    C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
    runtime.KeepAlive(labels)

    C.gpu_kmeans_free_result(res.result_ptr)

    return labels, float32(res.inertia), nil
}

// FitPredict performs both fitting and labeling in one step.
func (gk *GpuKMeans[T]) FitPredict(dataset []T, nSamples uint64) ([]int64, float32, int64, error) {
    if gk.cKMeans == nil {
        return nil, 0, 0, fmt.Errorf("GpuKMeans is not initialized")
    }
    if len(dataset) == 0 || nSamples == 0 {
        return nil, 0, 0, nil
    }

    var errmsg *C.char
    res := C.gpu_kmeans_fit_predict(
        gk.cKMeans,
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(nSamples),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, 0, 0, fmt.Errorf("%s", errStr)
    }

    if res.result_ptr == nil {
        return nil, 0, 0, fmt.Errorf("fit_predict returned nil result")
    }

    labels := make([]int64, nSamples)
    C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
    runtime.KeepAlive(labels)

    C.gpu_kmeans_free_result(res.result_ptr)

    return labels, float32(res.inertia), int64(res.n_iter), nil
}

// GetCentroids retrieves the trained centroids.
func (gk *GpuKMeans[T]) GetCentroids() ([]T, error) {
    if gk.cKMeans == nil {
        return nil, fmt.Errorf("GpuKMeans is not initialized")
    }
    centroids := make([]T, gk.nClusters*gk.dimension)
    var errmsg *C.char
    C.gpu_kmeans_get_centroids(gk.cKMeans, unsafe.Pointer(&centroids[0]), unsafe.Pointer(&errmsg))
    runtime.KeepAlive(centroids)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }
    return centroids, nil
}
