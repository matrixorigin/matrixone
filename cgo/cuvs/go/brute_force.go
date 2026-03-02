package cuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "brute_force_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

// GpuBruteForceIndex represents the C++ GpuBruteForceIndex object
type GpuBruteForceIndex struct {
    cIndex C.GpuBruteForceIndexC
}

// NewGpuBruteForceIndex creates a new GpuBruteForceIndex instance
func NewGpuBruteForceIndex(dataset []float32, countVectors uint64, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuBruteForceIndex, error) {
    return NewGpuBruteForceIndexUnsafe(unsafe.Pointer(&dataset[0]), countVectors, dimension, metric, nthread, deviceID, F32)
}

// NewGpuBruteForceIndexUnsafe creates a new GpuBruteForceIndex instance with generic pointer and quantization type
func NewGpuBruteForceIndexUnsafe(dataset unsafe.Pointer, countVectors uint64, dimension uint32, metric DistanceType, nthread uint32, deviceID int, qtype Quantization) (*GpuBruteForceIndex, error) {
    if dataset == nil || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }

    var errmsg *C.char
    cIndex := C.GpuBruteForceIndex_NewUnsafe(
        dataset,
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.uint32_t(nthread),
        C.int(deviceID),
        C.CuvsQuantizationC(qtype),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuBruteForceIndex")
    }
    return &GpuBruteForceIndex{cIndex: cIndex}, nil
}

// Load loads the index to the GPU
func (gbi *GpuBruteForceIndex) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuBruteForceIndex is not initialized")
    }
    var errmsg *C.char
    C.GpuBruteForceIndex_Load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuBruteForceIndex) Search(queries []float32, numQueries uint64, queryDimension uint32, limit uint32) ([]int64, []float32, error) {
	return gbi.SearchUnsafe(unsafe.Pointer(&queries[0]), numQueries, queryDimension, limit)
}

// SearchUnsafe performs a search operation with generic pointer
func (gbi *GpuBruteForceIndex) SearchUnsafe(queries unsafe.Pointer, numQueries uint64, queryDimension uint32, limit uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuBruteForceIndex is not initialized")
	}
	if queries == nil || numQueries == 0 || queryDimension == 0 {
		return nil, nil, fmt.Errorf("queries, numQueries, and queryDimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.GpuBruteForceIndex_SearchUnsafe(
		gbi.cIndex,
		queries,
		C.uint64_t(numQueries),
		C.uint32_t(queryDimension),
		C.uint32_t(limit),
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, fmt.Errorf("%s", errStr)
	}
	if cResult == nil {
		return nil, nil, fmt.Errorf("search returned nil result")
	}

	// Allocate slices for results
	neighbors := make([]int64, numQueries*uint64(limit))
	distances := make([]float32, numQueries*uint64(limit))

	C.GpuBruteForceIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

	C.GpuBruteForceIndex_FreeSearchResult(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuBruteForceIndex instance
func (gbi *GpuBruteForceIndex) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.GpuBruteForceIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil // Mark as destroyed
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}
