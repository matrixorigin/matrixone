package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "cagra_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "runtime"
    "unsafe"
)

// GpuCagraIndex represents the C++ gpu_cagra_index_t object
type GpuCagraIndex[T VectorType] struct {
    cIndex C.gpu_cagra_index_c
}

// NewGpuCagraIndex creates a new GpuCagraIndex instance for building from dataset
func NewGpuCagraIndex[T VectorType](dataset []T, count_vectors uint64, dimension uint32, metric DistanceType, intermediate_graph_degree uint32, graph_degree uint32, nthread uint32, device_id int) (*GpuCagraIndex[T], error) {
    if len(dataset) == 0 || count_vectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, count_vectors, and dimension cannot be zero")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cIndex := C.gpu_cagra_index_new(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(count_vectors),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        C.size_t(intermediate_graph_degree),
        C.size_t(graph_degree),
        C.uint32_t(nthread),
        C.int(device_id),
        C.quantization_t(qtype),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(dataset)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuCagraIndex")
    }
    return &GpuCagraIndex[T]{cIndex: cIndex}, nil
}

// NewGpuCagraIndexFromFile creates a new GpuCagraIndex instance for loading from file
func NewGpuCagraIndexFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, nthread uint32, device_id int) (*GpuCagraIndex[T], error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }

    qtype := GetQuantization[T]()
    c_filename := C.CString(filename)
    defer C.free(unsafe.Pointer(c_filename))

    var errmsg *C.char
    cIndex := C.gpu_cagra_index_new_from_file(
        c_filename,
        C.uint32_t(dimension),
        C.distance_type_t(metric),
        C.uint32_t(nthread),
        C.int(device_id),
        C.quantization_t(qtype),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuCagraIndex from file")
    }
    return &GpuCagraIndex[T]{cIndex: cIndex}, nil
}

// Load loads the index to the GPU
func (gbi *GpuCagraIndex[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagraIndex is not initialized")
    }
    var errmsg *C.char
    C.gpu_cagra_index_load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Save saves the index to file
func (gbi *GpuCagraIndex[T]) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagraIndex is not initialized")
    }
    c_filename := C.CString(filename)
    defer C.free(unsafe.Pointer(c_filename))

    var errmsg *C.char
    C.gpu_cagra_index_save(gbi.cIndex, c_filename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuCagraIndex[T]) Search(queries []T, num_queries uint64, query_dimension uint32, limit uint32, itopk_size uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuCagraIndex is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, fmt.Errorf("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_cagra_index_search(
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

	C.gpu_cagra_index_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
    runtime.KeepAlive(neighbors)
    runtime.KeepAlive(distances)

	C.gpu_cagra_index_free_search_result(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuCagraIndex instance
func (gbi *GpuCagraIndex[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_cagra_index_destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Extend adds new vectors to the existing index
func (gbi *GpuCagraIndex[T]) Extend(additional_data []T, num_vectors uint64) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagraIndex is not initialized")
    }
    if len(additional_data) == 0 || num_vectors == 0 {
        return nil
    }

    var errmsg *C.char
    C.gpu_cagra_index_extend(
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

// MergeCagraIndices merges multiple CAGRA indices into a single one
func MergeCagraIndices[T VectorType](indices []*GpuCagraIndex[T], nthread uint32, device_id int) (*GpuCagraIndex[T], error) {
    if len(indices) == 0 {
        return nil, fmt.Errorf("indices list cannot be empty")
    }

    cIndices := make([]C.gpu_cagra_index_c, len(indices))
    for i, idx := range indices {
        if idx.cIndex == nil {
            return nil, fmt.Errorf("index at position %d is nil or destroyed", i)
        }
        cIndices[i] = idx.cIndex
    }

    var errmsg *C.char
    cMergedIndex := C.gpu_cagra_index_merge(
        &cIndices[0],
        C.uint32_t(len(indices)),
        C.uint32_t(nthread),
        C.int(device_id),
        unsafe.Pointer(&errmsg),
    )
    runtime.KeepAlive(cIndices)
    runtime.KeepAlive(indices)

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cMergedIndex == nil {
        return nil, fmt.Errorf("failed to merge CAGRA indices")
    }

    return &GpuCagraIndex[T]{cIndex: cMergedIndex}, nil
}
