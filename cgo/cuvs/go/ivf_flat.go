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

// GpuIvfFlatIndex represents the C++ gpu_ivf_flat_index_t object.
// It supports both single-GPU and sharded multi-GPU modes.
type GpuIvfFlatIndex[T VectorType] struct {
    cIndex C.gpu_ivf_flat_index_c
    n_list uint32
    dimension uint32
}

// NewGpuIvfFlatIndex creates a new GpuIvfFlatIndex instance for building from dataset.
// devices: List of GPU device IDs. If len(devices) == 1, it runs in single-GPU mode.
// If len(devices) > 1, it shards the index across those GPUs.
// force_mg: If true, forces the use of the sharded API even for a single device (useful for testing).
func NewGpuIvfFlatIndex[T VectorType](dataset []T, count_vectors uint64, dimension uint32, metric DistanceType, n_list uint32, devices []int, nthread uint32, force_mg bool) (*GpuIvfFlatIndex[T], error) {
    if len(dataset) == 0 || count_vectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, count_vectors, and dimension cannot be zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty")
    }

    qtype := GetQuantization[T]()
    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.gpu_ivf_flat_index_new(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(count_vectors),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        C.uint32_t(n_list),
        &cDevices[0],
        C.uint32_t(len(devices)),
        C.uint32_t(nthread),
        C.quantization_t(qtype),
        C.bool(force_mg),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)
    runtime.KeepAlive(cDevices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuIvfFlatIndex")
    }
    return &GpuIvfFlatIndex[T]{cIndex: cIndex, n_list: n_list, dimension: dimension}, nil
}

// NewGpuIvfFlatIndexFromFile creates a new GpuIvfFlatIndex instance for loading from file.
func NewGpuIvfFlatIndexFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, devices []int, nthread uint32, force_mg bool) (*GpuIvfFlatIndex[T], error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty")
    }

    qtype := GetQuantization[T]()
    c_filename := C.CString(filename)
    defer C.free(unsafe.Pointer(c_filename))

    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.gpu_ivf_flat_index_new_from_file(
        c_filename,
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        &cDevices[0],
        C.uint32_t(len(devices)),
        C.uint32_t(nthread),
        C.quantization_t(qtype),
        C.bool(force_mg),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(cDevices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuIvfFlatIndex from file")
    }
    return &GpuIvfFlatIndex[T]{cIndex: cIndex, n_list: 0, dimension: dimension}, nil
}

// Load loads the index to the GPU
func (gbi *GpuIvfFlatIndex[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuIvfFlatIndex is not initialized")
    }
    var errmsg *C.char
    C.gpu_ivf_flat_index_load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    gbi.n_list = uint32(C.gpu_ivf_flat_index_get_n_list(gbi.cIndex))
    return nil
}

// Save saves the index to file
func (gbi *GpuIvfFlatIndex[T]) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuIvfFlatIndex is not initialized")
    }
    c_filename := C.CString(filename)
    defer C.free(unsafe.Pointer(c_filename))

    var errmsg *C.char
    C.gpu_ivf_flat_index_save(gbi.cIndex, c_filename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuIvfFlatIndex[T]) Search(queries []T, num_queries uint64, query_dimension uint32, limit uint32, n_probes uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuIvfFlatIndex is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, fmt.Errorf("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_ivf_flat_index_search(
		gbi.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(num_queries),
		C.uint32_t(query_dimension),
		C.uint32_t(limit),
        C.uint32_t(n_probes),
		unsafe.Pointer(&errmsg),
	)
    runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, fmt.Errorf("%s", errStr)
	}
	if cResult == nil {
		return nil, nil, fmt.Errorf("search returned nil result")
	}

	// Allocate slices for results
	neighbors := make([]int64, num_queries*uint64(limit))
	distances := make([]float32, num_queries*uint64(limit))

	C.gpu_ivf_flat_index_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
    runtime.KeepAlive(neighbors)
    runtime.KeepAlive(distances)

	C.gpu_ivf_flat_index_free_search_result(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ gpu_ivf_flat_index_t instance
func (gbi *GpuIvfFlatIndex[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_ivf_flat_index_destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// GetCenters retrieves the centroids
func (gbi *GpuIvfFlatIndex[T]) GetCenters() ([]float32, error) {
	if gbi.cIndex == nil {
		return nil, fmt.Errorf("GpuIvfFlatIndex is not initialized")
	}
	if gbi.n_list == 0 {
		return nil, fmt.Errorf("n_list is zero, ensure index is loaded")
	}
	centers := make([]float32, gbi.n_list*gbi.dimension)
	var errmsg *C.char
	C.gpu_ivf_flat_index_get_centers(gbi.cIndex, (*C.float)(&centers[0]), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(centers)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, fmt.Errorf("%s", errStr)
	}
	return centers, nil
}
