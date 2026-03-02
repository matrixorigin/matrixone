package cuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "ivf_flat_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

// GpuIvfFlatIndex represents the C++ GpuIvfFlatIndex object
type GpuIvfFlatIndex[T VectorType] struct {
    cIndex C.GpuIvfFlatIndexC
    nList uint32
    dimension uint32
}

// NewGpuIvfFlatIndex creates a new GpuIvfFlatIndex instance for building from dataset
func NewGpuIvfFlatIndex[T VectorType](dataset []T, countVectors uint64, dimension uint32, metric DistanceType, nList uint32, nthread uint32, deviceID int) (*GpuIvfFlatIndex[T], error) {
    if len(dataset) == 0 || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }

    qtype := GetQuantization[T]()
    var errmsg *C.char
    cIndex := C.GpuIvfFlatIndex_New(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.uint32_t(nList),
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
        return nil, fmt.Errorf("failed to create GpuIvfFlatIndex")
    }
    return &GpuIvfFlatIndex[T]{cIndex: cIndex, nList: nList, dimension: dimension}, nil
}

// NewGpuIvfFlatIndexFromFile creates a new GpuIvfFlatIndex instance for loading from file
func NewGpuIvfFlatIndexFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuIvfFlatIndex[T], error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }

    qtype := GetQuantization[T]()
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    cIndex := C.GpuIvfFlatIndex_NewFromFile(
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
        return nil, fmt.Errorf("failed to create GpuIvfFlatIndex from file")
    }
    return &GpuIvfFlatIndex[T]{cIndex: cIndex, nList: 0, dimension: dimension}, nil
}

// Load loads the index to the GPU
func (gbi *GpuIvfFlatIndex[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuIvfFlatIndex is not initialized")
    }
    var errmsg *C.char
    C.GpuIvfFlatIndex_Load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    gbi.nList = uint32(C.GpuIvfFlatIndex_GetNList(gbi.cIndex))
    return nil
}

// Save saves the index to file
func (gbi *GpuIvfFlatIndex[T]) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("GpuIvfFlatIndex is not initialized")
    }
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    C.GpuIvfFlatIndex_Save(gbi.cIndex, cFilename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

// Search performs a search operation
func (gbi *GpuIvfFlatIndex[T]) Search(queries []T, numQueries uint64, queryDimension uint32, limit uint32, n_probes uint32) ([]int64, []float32, error) {
	if gbi.cIndex == nil {
		return nil, nil, fmt.Errorf("GpuIvfFlatIndex is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
		return nil, nil, fmt.Errorf("queries, numQueries, and queryDimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.GpuIvfFlatIndex_Search(
		gbi.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(queryDimension),
		C.uint32_t(limit),
        C.uint32_t(n_probes),
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

	C.GpuIvfFlatIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

	C.GpuIvfFlatIndex_FreeSearchResult(cResult);

	return neighbors, distances, nil
}

// Destroy frees the C++ GpuIvfFlatIndex instance
func (gbi *GpuIvfFlatIndex[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.GpuIvfFlatIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
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
    if gbi.nList == 0 {
        return nil, fmt.Errorf("nList is zero, ensure index is loaded")
    }
    centers := make([]float32, gbi.nList * gbi.dimension)
    var errmsg *C.char
    C.GpuIvfFlatIndex_GetCenters(gbi.cIndex, (*C.float)(&centers[0]), unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }
    return centers, nil
}
