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

type GpuCagraIndex struct {
    cIndex C.GpuCagraIndexC
    dimension uint32
}

func NewGpuCagraIndex(dataset []float32, countVectors uint64, dimension uint32, metric DistanceType, intermediateGraphDegree uint32, graphDegree uint32, nthread uint32, deviceID int) (*GpuCagraIndex, error) {
    return NewGpuCagraIndexUnsafe(unsafe.Pointer(&dataset[0]), countVectors, dimension, metric, intermediateGraphDegree, graphDegree, nthread, deviceID, F32)
}

func NewGpuCagraIndexUnsafe(dataset unsafe.Pointer, countVectors uint64, dimension uint32, metric DistanceType, intermediateGraphDegree uint32, graphDegree uint32, nthread uint32, deviceID int, qtype Quantization) (*GpuCagraIndex, error) {
    if dataset == nil || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }

    var errmsg *C.char
    cIndex := C.GpuCagraIndex_NewUnsafe(
        dataset,
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
    return &GpuCagraIndex{cIndex: cIndex, dimension: dimension}, nil
}

func NewGpuCagraIndexFromFile(filename string, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuCagraIndex, error) {
    return NewGpuCagraIndexFromFileUnsafe(filename, dimension, metric, nthread, deviceID, F32)
}

func NewGpuCagraIndexFromFileUnsafe(filename string, dimension uint32, metric DistanceType, nthread uint32, deviceID int, qtype Quantization) (*GpuCagraIndex, error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }

    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    cIndex := C.GpuCagraIndex_NewFromFileUnsafe(
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
    return &GpuCagraIndex{cIndex: cIndex, dimension: dimension}, nil
}

func (gbi *GpuCagraIndex) Load() error {
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

func (gbi *GpuCagraIndex) Save(filename string) error {
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

func (gbi *GpuCagraIndex) Search(queries []float32, numQueries uint64, queryDimension uint32, limit uint32, itopkSize uint32) ([]int64, []float32, error) {
    return gbi.SearchUnsafe(unsafe.Pointer(&queries[0]), numQueries, queryDimension, limit, itopkSize)
}

func (gbi *GpuCagraIndex) SearchUnsafe(queries unsafe.Pointer, numQueries uint64, queryDimension uint32, limit uint32, itopkSize uint32) ([]int64, []float32, error) {
    if gbi.cIndex == nil {
        return nil, nil, fmt.Errorf("GpuCagraIndex is not initialized")
    }
    if queries == nil || numQueries == 0 || queryDimension == 0 {
        return nil, nil, fmt.Errorf("invalid query input")
    }

    var errmsg *C.char
    cResult := C.GpuCagraIndex_SearchUnsafe(
        gbi.cIndex,
        queries,
        C.uint64_t(numQueries),
        C.uint32_t(queryDimension),
        C.uint32_t(limit),
        C.size_t(itopkSize),
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

    neighbors := make([]int64, numQueries*uint64(limit))
    distances := make([]float32, numQueries*uint64(limit))

    C.GpuCagraIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

    C.GpuCagraIndex_FreeSearchResult(cResult)

    return neighbors, distances, nil
}

func (gbi *GpuCagraIndex) Destroy() error {
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
