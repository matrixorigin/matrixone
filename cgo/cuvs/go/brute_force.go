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

// DistanceType maps to C.CuvsDistanceTypeC
type DistanceType C.CuvsDistanceTypeC

const (
    L2Expanded      DistanceType = C.DistanceType_L2Expanded
    L1              DistanceType = C.DistanceType_L1
    InnerProduct    DistanceType = C.DistanceType_InnerProduct
    CosineSimilarity DistanceType = C.DistanceType_CosineSimilarity
    Jaccard         DistanceType = C.DistanceType_Jaccard
    Hamming         DistanceType = C.DistanceType_Hamming
    Unknown         DistanceType = C.DistanceType_Unknown
)

// GpuBruteForceIndex represents the C++ GpuBruteForceIndex object
type GpuBruteForceIndex struct {
    cIndex C.GpuBruteForceIndexC
}

// NewGpuBruteForceIndex creates a new GpuBruteForceIndex instance
func NewGpuBruteForceIndex(dataset []float32, countVectors uint64, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuBruteForceIndex, error) {
    if len(dataset) == 0 || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }
    if uint64(len(dataset)) != countVectors * uint64(dimension) {
        return nil, fmt.Errorf("dataset size (%d) does not match countVectors (%d) * dimension (%d)", len(dataset), countVectors, dimension)
    }

    var errmsg *C.char
    cIndex := C.GpuBruteForceIndex_New(
        (*C.float)(&dataset[0]),
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.uint32_t(nthread),
        C.int(deviceID),
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

// SearchResult wraps the C-side search result object
type SearchResult struct {
	cResult C.GpuBruteForceSearchResultC
}

// Search performs a search operation
func (gbi *GpuBruteForceIndex) Search(queries []float32, numQueries uint64, queryDimension uint32, limit uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuBruteForceIndex is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
		return nil, nil, fmt.Errorf("queries, numQueries, and queryDimension cannot be zero")
	}
	if uint64(len(queries)) != numQueries*uint64(queryDimension) {
		return nil, nil, fmt.Errorf("queries size (%d) does not match numQueries (%d) * queryDimension (%d)", len(queries), numQueries, queryDimension)
	}

	var cQueries *C.float
	if len(queries) > 0 {
		cQueries = (*C.float)(&queries[0])
	}

	var errmsg *C.char
	cResult := C.GpuBruteForceIndex_Search(
		gbi.cIndex,
		cQueries,
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

	var cNeighbors *C.int64_t
	if len(neighbors) > 0 {
		cNeighbors = (*C.int64_t)(unsafe.Pointer(&neighbors[0]))
	}

	var cDistances *C.float
	if len(distances) > 0 {
		cDistances = (*C.float)(unsafe.Pointer(&distances[0]))
	}

	C.GpuBruteForceIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), cNeighbors, cDistances)

	// Free the C++ search result object now that we have copied the data
	C.GpuBruteForceIndex_FreeSearchResult(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuBruteForceIndex instance
func (gbi *GpuBruteForceIndex) Destroy() error {
    if gbi.cIndex == nil {
        return nil // Already destroyed or not initialized
    }
    var errmsg *C.char
    C.GpuBruteForceIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil // Mark as destroyed anyway

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}
