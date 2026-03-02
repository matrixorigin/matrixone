package cuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "cagra_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

// GpuCagraIndex represents the C++ GpuCagraIndex object
type GpuCagraIndex[T VectorType] struct {
    cIndex C.GpuCagraIndexC
}

// NewGpuCagraIndex creates a new GpuCagraIndex instance for building from dataset
func NewGpuCagraIndex[T VectorType](dataset []T, countVectors uint64, dimension uint32, metric DistanceType, intermediateGraphDegree uint32, graphDegree uint32, nthread uint32, deviceID int) (*GpuCagraIndex[T], error) {
    if len(dataset) == 0 || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cIndex := C.GpuCagraIndex_New(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.size_t(intermediateGraphDegree),
        C.size_t(graphDegree),
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
        return nil, fmt.Errorf("failed to create GpuCagraIndex")
    }
    return &GpuCagraIndex[T]{cIndex: cIndex}, nil
}

// NewGpuCagraIndexFromFile creates a new GpuCagraIndex instance for loading from file
func NewGpuCagraIndexFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuCagraIndex[T], error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }

    qtype := GetQuantization[T]()
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    cIndex := C.GpuCagraIndex_NewFromFile(
        cFilename,
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
    C.GpuCagraIndex_Load(gbi.cIndex, unsafe.Pointer(&errmsg))
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
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    C.GpuCagraIndex_Save(gbi.cIndex, cFilename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuCagraIndex[T]) Search(queries []T, numQueries uint64, queryDimension uint32, limit uint32, itopk_size uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuCagraIndex is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
		return nil, nil, fmt.Errorf("queries, numQueries, and queryDimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.GpuCagraIndex_Search(
		gbi.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(queryDimension),
		C.uint32_t(limit),
        C.size_t(itopk_size),
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

	C.GpuCagraIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

	C.GpuCagraIndex_FreeSearchResult(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuCagraIndex instance
func (gbi *GpuCagraIndex[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.GpuCagraIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Extend adds new vectors to the existing index
func (gbi *GpuCagraIndex[T]) Extend(additionalData []T, numVectors uint64) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuCagraIndex is not initialized")
    }
    if len(additionalData) == 0 || numVectors == 0 {
        return nil
    }

    var errmsg *C.char
    C.GpuCagraIndex_Extend(
        gbi.cIndex,
        unsafe.Pointer(&additionalData[0]),
        C.uint64_t(numVectors),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// MergeCagraIndices merges multiple CAGRA indices into a single one
func MergeCagraIndices[T VectorType](indices []*GpuCagraIndex[T], nthread uint32, deviceID int) (*GpuCagraIndex[T], error) {
    if len(indices) == 0 {
        return nil, fmt.Errorf("indices list cannot be empty")
    }

    cIndices := make([]C.GpuCagraIndexC, len(indices))
    for i, idx := range indices {
        if idx.cIndex == nil {
            return nil, fmt.Errorf("index at position %d is nil or destroyed", i)
        }
        cIndices[i] = idx.cIndex
    }

    var errmsg *C.char
    cMergedIndex := C.GpuCagraIndex_Merge(
        &cIndices[0],
        C.uint32_t(len(indices)),
        C.uint32_t(nthread),
        C.int(deviceID),
        unsafe.Pointer(&errmsg),
    )

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
