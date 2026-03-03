package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "ivf_flat_c.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"
import (
    "fmt"
    "runtime"
    "unsafe"
)

// GpuIvfFlat represents the C++ gpu_ivf_flat_t object.
type GpuIvfFlat[T VectorType] struct {
    cIvfFlat  C.gpu_ivf_flat_c
    dimension uint32
}

// NewGpuIvfFlat creates a new GpuIvfFlat instance from a dataset.
func NewGpuIvfFlat[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType, 
                                 bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfFlat[T], error) {
    if len(devices) == 0 {
        return nil, fmt.Errorf("at least one device must be specified")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cDevices := make([]C.int, len(devices))
    for i, d := range devices {
        cDevices[i] = C.int(d)
    }

    cBP := C.ivf_flat_build_params_t{
        n_lists:                  C.uint32_t(bp.NLists),
        add_data_on_build:        C.bool(bp.AddDataOnBuild),
        kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
    }

    cIvfFlat := C.gpu_ivf_flat_new(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(count),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        cBP,
        &cDevices[0],
        C.int(len(devices)),
        C.uint32_t(nthread),
        C.distribution_mode_t(mode),
        C.quantization_t(qtype),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)
    runtime.KeepAlive(cDevices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIvfFlat == nil {
        return nil, fmt.Errorf("failed to create GpuIvfFlat")
    }

    return &GpuIvfFlat[T]{cIvfFlat: cIvfFlat, dimension: dimension}, nil
}

// NewGpuIvfFlatFromFile creates a new GpuIvfFlat instance by loading from a file.
func NewGpuIvfFlatFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, 
                                         bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfFlat[T], error) {
    if len(devices) == 0 {
        return nil, fmt.Errorf("at least one device must be specified")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    cDevices := make([]C.int, len(devices))
    for i, d := range devices {
        cDevices[i] = C.int(d)
    }

    cBP := C.ivf_flat_build_params_t{
        n_lists:                  C.uint32_t(bp.NLists),
        add_data_on_build:        C.bool(bp.AddDataOnBuild),
        kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
    }

    cIvfFlat := C.gpu_ivf_flat_load_file(
        cFilename,
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        cBP,
        &cDevices[0],
        C.int(len(devices)),
        C.uint32_t(nthread),
        C.distribution_mode_t(mode),
        C.quantization_t(qtype),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(cDevices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIvfFlat == nil {
        return nil, fmt.Errorf("failed to load GpuIvfFlat from file")
    }

    return &GpuIvfFlat[T]{cIvfFlat: cIvfFlat, dimension: dimension}, nil
}

// Destroy frees the C++ gpu_ivf_flat_t instance
func (gi *GpuIvfFlat[T]) Destroy() error {
    if gi.cIvfFlat == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_ivf_flat_destroy(gi.cIvfFlat, unsafe.Pointer(&errmsg))
    gi.cIvfFlat = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Load triggers the build or file loading process
func (gi *GpuIvfFlat[T]) Load() error {
    if gi.cIvfFlat == nil {
        return fmt.Errorf("GpuIvfFlat is not initialized")
    }
    var errmsg *C.char
    C.gpu_ivf_flat_load(gi.cIvfFlat, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Save serializes the index to a file
func (gi *GpuIvfFlat[T]) Save(filename string) error {
    if gi.cIvfFlat == nil {
        return fmt.Errorf("GpuIvfFlat is not initialized")
    }
    var errmsg *C.char
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    C.gpu_ivf_flat_save(gi.cIvfFlat, cFilename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a K-Nearest Neighbor search
func (gi *GpuIvfFlat[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) (SearchResultIvfFlat, error) {
    if gi.cIvfFlat == nil {
        return SearchResultIvfFlat{}, fmt.Errorf("GpuIvfFlat is not initialized")
    }
    if len(queries) == 0 || numQueries == 0 {
        return SearchResultIvfFlat{}, nil
    }

    var errmsg *C.char
    cSP := C.ivf_flat_search_params_t{
        n_probes: C.uint32_t(sp.NProbes),
    }

    res := C.gpu_ivf_flat_search(
        gi.cIvfFlat,
        unsafe.Pointer(&queries[0]),
        C.uint64_t(numQueries),
        C.uint32_t(dimension),
        C.uint32_t(limit),
        cSP,
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(queries)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return SearchResultIvfFlat{}, fmt.Errorf("%s", errStr)
    }

    if res.result_ptr == nil {
        return SearchResultIvfFlat{}, fmt.Errorf("search returned nil result")
    }

    totalElements := uint64(numQueries) * uint64(limit)
    neighbors := make([]int64, totalElements)
    distances := make([]float32, totalElements)

    C.gpu_ivf_flat_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
    C.gpu_ivf_flat_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
    runtime.KeepAlive(neighbors)
    runtime.KeepAlive(distances)

    C.gpu_ivf_flat_free_result(res.result_ptr)

    return SearchResultIvfFlat{
        Neighbors: neighbors,
        Distances: distances,
    }, nil
}

// GetCenters retrieves the trained centroids.
func (gi *GpuIvfFlat[T]) GetCenters(nLists uint32) ([]float32, error) {
    if gi.cIvfFlat == nil {
        return nil, fmt.Errorf("GpuIvfFlat is not initialized")
    }
    centers := make([]float32, nLists*gi.dimension)
    var errmsg *C.char
    C.gpu_ivf_flat_get_centers(gi.cIvfFlat, (*C.float)(&centers[0]), unsafe.Pointer(&errmsg))
    runtime.KeepAlive(centers)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }
    return centers, nil
}

// GetNList retrieves the number of lists (centroids) in the index.
func (gi *GpuIvfFlat[T]) GetNList() uint32 {
    if gi.cIvfFlat == nil {
        return 0
    }
    return uint32(C.gpu_ivf_flat_get_n_list(gi.cIvfFlat))
}

// SearchResultIvfFlat contains the neighbors and distances from an IVF-Flat search.
type SearchResultIvfFlat struct {
    Neighbors []int64
    Distances []float32
}
