package mocuvs

/*
#cgo LDFLAGS: /home/eric/github/matrixone/cgo/cuvs/c/libmocuvs.so -Wl,-rpath=/home/eric/github/matrixone/cgo/cuvs/c
#cgo CFLAGS: -I../c

#include "sharded_cagra_c.h"
#include <stdlib.h>
*/
import "C"
import (
    "fmt"
    "unsafe"
)

// GpuShardedCagraIndex represents the C++ GpuShardedCagraIndex object
type GpuShardedCagraIndex[T VectorType] struct {
    cIndex C.GpuShardedCagraIndexC
}

// NewGpuShardedCagraIndex creates a new GpuShardedCagraIndex instance for building from dataset across multiple GPUs
func NewGpuShardedCagraIndex[T VectorType](dataset []T, countVectors uint64, dimension uint32, metric DistanceType, intermediateGraphDegree uint32, graphDegree uint32, devices []int, nthread uint32) (*GpuShardedCagraIndex[T], error) {
    if len(dataset) == 0 || countVectors == 0 || dimension == 0 {
        return nil, fmt.Errorf("dataset, countVectors, and dimension cannot be zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty for sharded index")
    }

    qtype := GetQuantization[T]()
    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.GpuShardedCagraIndex_New(
        unsafe.Pointer(&dataset[0]),
        C.uint64_t(countVectors),
        C.uint32_t(dimension),
        C.CuvsDistanceTypeC(metric),
        C.size_t(intermediateGraphDegree),
        C.size_t(graphDegree),
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
        return nil, fmt.Errorf("failed to create GpuShardedCagraIndex")
    }
    return &GpuShardedCagraIndex[T]{cIndex: cIndex}, nil
}

// NewGpuShardedCagraIndexFromFile creates a new GpuShardedCagraIndex instance for loading from file (multi-GPU)
func NewGpuShardedCagraIndexFromFile[T VectorType](filename string, dimension uint32, metric DistanceType, devices []int, nthread uint32) (*GpuShardedCagraIndex[T], error) {
    if filename == "" || dimension == 0 {
        return nil, fmt.Errorf("filename and dimension cannot be empty or zero")
    }
    if len(devices) == 0 {
        return nil, fmt.Errorf("devices list cannot be empty for sharded index")
    }

    qtype := GetQuantization[T]()
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    cDevices := make([]C.int, len(devices))
    for i, dev := range devices {
        cDevices[i] = C.int(dev)
    }

    var errmsg *C.char
    cIndex := C.GpuShardedCagraIndex_NewFromFile(
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
        return nil, fmt.Errorf("failed to create GpuShardedCagraIndex from file")
    }
    return &GpuShardedCagraIndex[T]{cIndex: cIndex}, nil
}

func (gbi *GpuShardedCagraIndex[T]) Load() error {
    if gbi.cIndex == nil {
        return fmt.Errorf("index is not initialized")
    }
    var errmsg *C.char
    C.GpuShardedCagraIndex_Load(gbi.cIndex, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

func (gbi *GpuShardedCagraIndex[T]) Save(filename string) error {
    if gbi.cIndex == nil {
        return fmt.Errorf("index is not initialized")
    }
    cFilename := C.CString(filename)
    defer C.free(unsafe.Pointer(cFilename))

    var errmsg *C.char
    C.GpuShardedCagraIndex_Save(gbi.cIndex, cFilename, unsafe.Pointer(&errmsg))
    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}

func (gbi *GpuShardedCagraIndex[T]) Search(queries []T, numQueries uint64, queryDimension uint32, limit uint32, itopk_size uint32) ([]int64, []float32, error) {
    if gbi.cIndex == nil {
        return nil, nil, fmt.Errorf("index is not initialized")
    }
    if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
        return nil, nil, fmt.Errorf("invalid query input")
    }

    var errmsg *C.char
    cResult := C.GpuShardedCagraIndex_Search(
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

    neighbors := make([]int64, numQueries*uint64(limit))
    distances := make([]float32, numQueries*uint64(limit))

    C.GpuShardedCagraIndex_GetResults(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))

    C.GpuShardedCagraIndex_FreeSearchResult(cResult)

    return neighbors, distances, nil
}

func (gbi *GpuShardedCagraIndex[T]) Destroy() error {
    if gbi.cIndex == nil {
        return nil
    }
    var errmsg *C.char
    C.GpuShardedCagraIndex_Destroy(gbi.cIndex, unsafe.Pointer(&errmsg))
    gbi.cIndex = nil

    if errmsg != nil {
        errStr := C.GoString(errmsg)
        C.free(unsafe.Pointer(errmsg))
        return fmt.Errorf("%s", errStr)
    }
    return nil
}
