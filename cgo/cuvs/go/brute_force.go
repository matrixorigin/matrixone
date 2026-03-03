package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c -I../cpp

#include "brute_force_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "runtime"
    "unsafe"
)

// GpuBruteForce represents the C++ gpu_brute_force_t object
type GpuBruteForce[T VectorType] struct {
    cIndex C.gpu_brute_force_c
}

// NewGpuBruteForce creates a new GpuBruteForce instance
func NewGpuBruteForce[T VectorType](dataset []T, count_vectors uint64, dimension uint32, metric DistanceType, nthread uint32, device_id int) (*GpuBruteForce[T], error) {
    if len(dataset) == 0 || count_vectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, count_vectors, and dimension cannot be zero")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cIndex := C.gpu_brute_force_new(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(count_vectors),
        C.uint32_t(dimension),
        C.distance_type_t(metric),
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
        return nil, fmt.Errorf("failed to create GpuBruteForce")
    }
    return &GpuBruteForce[T]{cIndex: cIndex}, nil
}

// Load loads the index to the GPU
func (gbi *GpuBruteForce[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuBruteForce is not initialized")
    }
    var errmsg *C.char
    C.gpu_brute_force_load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuBruteForce[T]) Search(queries []T, num_queries uint64, query_dimension uint32, limit uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, fmt.Errorf("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search(
		gbi.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(num_queries),
		C.uint32_t(query_dimension),
		C.uint32_t(limit),
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

	C.gpu_brute_force_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
    runtime.KeepAlive(neighbors)
    runtime.KeepAlive(distances)

	C.gpu_brute_force_free_search_result(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuBruteForce instance
func (gbi *GpuBruteForce[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.gpu_brute_force_destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil // Mark as destroyed
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}
