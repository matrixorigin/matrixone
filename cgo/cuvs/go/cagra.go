package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "cagra_c.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"
import (
    "fmt"
    "runtime"
    "unsafe"
)

// GpuCagra represents the C++ gpu_cagra_t object.
// It supports both single-GPU and sharded multi-GPU modes.
type GpuCagra[T VectorType] struct {
    cIndex C.gpu_cagra_c
}

// NewGpuCagra creates a new GpuCagra instance for building from dataset.
// devices: List of GPU device IDs. If len(devices) == 1, it runs in single-GPU mode.
// If len(devices) > 1, it shards the index across those GPUs.
// force_mg: If true, forces the use of the sharded API even for a single device (useful for testing).
func NewGpuCagra[T VectorType](dataset []T, count_vectors uint64, dimension uint32, metric DistanceType, intermediate_graph_degree uint32, graph_degree uint32, devices []int, nthread uint32, force_mg bool) (*GpuCagra[T], error) {
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
    cIndex := C.gpu_cagra_new(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(count_vectors),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        C.size_t(intermediate_graph_degree),
        C.size_t(graph_degree),
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
        return nil, fmt.Errorf("failed to create GpuCagra")
    }
    return &GpuCagra[T]{cIndex: cIndex}, nil
}

// NewGpuCagraFromFile creates a new GpuCagra instance for loading from file.
func NewGpuCagraFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, devices []int, nthread uint32, force_mg bool) (*GpuCagra[T], error) {
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
    cIndex := C.gpu_cagra_new_from_file(
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
        return nil, fmt.Errorf("failed to create GpuCagra from file")
    }
    return &GpuCagra[T]{cIndex: cIndex}, nil
}

// Load loads the index to the GPU
func (gbi *GpuCagra[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagra is not initialized")
    }
    var errmsg *C.char
    C.gpu_cagra_load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Save saves the index to file
func (gbi *GpuCagra[T]) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagra is not initialized")
    }
    c_filename := C.CString(filename)
    defer C.free(unsafe.Pointer(c_filename))

    var errmsg *C.char
    C.gpu_cagra_save(gbi.cIndex, c_filename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuCagra[T]) Search(queries []T, num_queries uint64, query_dimension uint32, limit uint32, itopk_size uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuCagra is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, fmt.Errorf("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_cagra_search(
		gbi.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(num_queries),
		C.uint32_t(query_dimension),
		C.uint32_t(limit),
        C.size_t(itopk_size),
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

	C.gpu_cagra_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
    runtime.KeepAlive(neighbors)
    runtime.KeepAlive(distances)

	C.gpu_cagra_free_search_result(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuCagra instance
func (gbi *GpuCagra[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_cagra_destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Extend adds new vectors to the existing index (single-GPU only)
func (gbi *GpuCagra[T]) Extend(additional_data []T, num_vectors uint64) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagra is not initialized")
    }
    if len(additional_data) == 0 || num_vectors == 0 {
        return nil
    }

    var errmsg *C.char
    C.gpu_cagra_extend(
        gbi.cIndex,
        unsafe.Pointer(&additional_data[0]),
        C.uint64_t(num_vectors),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(additional_data)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// MergeCagra merges multiple single-GPU CAGRA indices into a single one.
func MergeCagra[T VectorType](indices []*GpuCagra[T], devices []int, nthread uint32) (*GpuCagra[T], error) {
    if len(indices) == 0 {
        return nil, fmt.Errorf("indices list cannot be empty")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty")
    }

    cIndices := make([]C.gpu_cagra_c, len(indices))
    for i, idx := range indices {
        if idx.cIndex == nil {
            return nil, fmt.Errorf("index at position %d is nil or destroyed", i)
        }
        cIndices[i] = idx.cIndex
    }

    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cMergedIndex := C.gpu_cagra_merge(
        &cIndices[0],
        C.uint32_t(len(indices)),
        C.uint32_t(nthread),
        &cDevices[0],
        C.uint32_t(len(devices)),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(cIndices)
    runtime.KeepAlive(indices)
    runtime.KeepAlive(cDevices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cMergedIndex == nil {
        return nil, fmt.Errorf("failed to merge CAGRA indices")
    }

    return &GpuCagra[T]{cIndex: cMergedIndex}, nil
}
