package cuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "sharded_ivf_flat_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

// GpuShardedIvfFlatIndex represents the C++ GpuShardedIvfFlatIndex object
type GpuShardedIvfFlatIndex struct {
    cIndex C.GpuShardedIvfFlatIndexC
    nList uint32
    dimension uint32
}

// NewGpuShardedIvfFlatIndex creates a new GpuShardedIvfFlatIndex instance for building from dataset across multiple GPUs
func NewGpuShardedIvfFlatIndex(dataset []float32, countVectors uint64, dimension uint32, metric DistanceType, nList uint32, devices []int, nthread uint32) (*GpuShardedIvfFlatIndex, error) {
    return NewGpuShardedIvfFlatIndexUnsafe(unsafe.Pointer(&dataset[0]), countVectors, dimension, metric, nList, devices, nthread, F32)
}

// NewGpuShardedIvfFlatIndexUnsafe creates a new GpuShardedIvfFlatIndex instance with generic pointer and quantization type
func NewGpuShardedIvfFlatIndexUnsafe(dataset unsafe.Pointer, countVectors uint64, dimension uint32, metric DistanceType, nList uint32, devices []int, nthread uint32, qtype Quantization) (*GpuShardedIvfFlatIndex, error) {
    if dataset == nil || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty for sharded index")
    }

    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.GpuShardedIvfFlatIndex_NewUnsafe(
        dataset,
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.uint32_t(nList),
        &cDevices[0],
        C.uint32_t(len(devices)),
        C.uint32_t(nthread),
        C.CuvsQuantizationC(qtype),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuShardedIvfFlatIndex")
    }
    return &GpuShardedIvfFlatIndex{cIndex: cIndex, nList: nList, dimension: dimension}, nil
}

// NewGpuShardedIvfFlatIndexFromFile creates a new GpuShardedIvfFlatIndex instance for loading from file (multi-GPU)
func NewGpuShardedIvfFlatIndexFromFile(filename string, dimension uint32, metric DistanceType, devices []int, nthread uint32) (*GpuShardedIvfFlatIndex, error) {
    return NewGpuShardedIvfFlatIndexFromFileUnsafe(filename, dimension, metric, devices, nthread, F32)
}

// NewGpuShardedIvfFlatIndexFromFileUnsafe creates a new GpuShardedIvfFlatIndex instance for loading from file with quantization type
func NewGpuShardedIvfFlatIndexFromFileUnsafe(filename string, dimension uint32, metric DistanceType, devices []int, nthread uint32, qtype Quantization) (*GpuShardedIvfFlatIndex, error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty for sharded index")
    }

    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.GpuShardedIvfFlatIndex_NewFromFileUnsafe(
        cFilename,
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        &cDevices[0],
        C.uint32_t(len(devices)),
        C.uint32_t(nthread),
        C.CuvsQuantizationC(qtype),
        unsafe.Pointer(&errmsg),
    )

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }

    if cIndex == nil {
        return nil, fmt.Errorf("failed to create GpuShardedIvfFlatIndex from file")
    }
    return &GpuShardedIvfFlatIndex{cIndex: cIndex, nList: 0, dimension: dimension}, nil
}

func (gbi *GpuShardedIvfFlatIndex) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("index is not initialized")
    }
    var errmsg *C.char
    C.GpuShardedIvfFlatIndex_Load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    gbi.nList = uint32(C.GpuShardedIvfFlatIndex_GetNList(gbi.cIndex))
    return nil
}

func (gbi *GpuShardedIvfFlatIndex) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("index is not initialized")
    }
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    C.GpuShardedIvfFlatIndex_Save(gbi.cIndex, cFilename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

func (gbi *GpuShardedIvfFlatIndex) Search(queries []float32, numQueries uint64, queryDimension uint32, limit uint32, nProbes uint32) ([]int64, []float32, error) {
    return gbi.SearchUnsafe(unsafe.Pointer(&queries[0]), numQueries, queryDimension, limit, nProbes)
}

func (gbi *GpuShardedIvfFlatIndex) SearchUnsafe(queries unsafe.Pointer, numQueries uint64, queryDimension uint32, limit uint32, nProbes uint32) ([]int64, []float32, error) {
    if gbi.cIndex == nil {
        return nil, nil, fmt.Errorf("index is not initialized")
    }
    if queries == nil || numQueries == 0 || queryDimension == 0 {
        return nil, nil, fmt.Errorf("invalid query input")
    }

    var errmsg *C.char
    cResult := C.GpuShardedIvfFlatIndex_SearchUnsafe(
        gbi.cIndex,
        queries,
        C.uint64_t(numQueries),
        C.uint32_t(queryDimension),
        C.uint32_t(limit),
        C.uint32_t(nProbes),
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

    C.GpuShardedIvfFlatIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

    C.GpuShardedIvfFlatIndex_FreeSearchResult(cResult)

    return neighbors, distances, nil
}

func (gbi *GpuShardedIvfFlatIndex) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.GpuShardedIvfFlatIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

func (gbi *GpuShardedIvfFlatIndex) GetCenters() ([]float32, error) {
    if gbi.cIndex == nil {
        return nil, fmt.Errorf("index is not initialized")
    }
    if gbi.nList == 0 {
        return nil, fmt.Errorf("nList is zero, ensure index is loaded")
    }
    centers := make([]float32, gbi.nList * gbi.dimension)
    var errmsg *C.char
    C.GpuShardedIvfFlatIndex_GetCenters(gbi.cIndex, (*C.float)(&centers[0]), unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return nil, fmt.Errorf("%s", errStr)
    }
    return centers, nil
}
