//go:build gpu

// Copyright 2021 - 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cuvs

/*
#include "../../cgo/cuvs/cagra_c.h"
#include <stdlib.h>
#include <stdbool.h>
*/
import "C"
import (
	"fmt"
	"os"
	"runtime"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// GpuCagra represents the C++ gpu_cagra_t object.
type GpuCagra[T VectorType] struct {
	cCagra      C.gpu_cagra_c
	dimension   uint32
	nthread     uint32
	distMode    DistributionMode
	batchWindowUs int64
}

// SetBatchWindow sets the batching window in microseconds for search operations.
// A window of 0 disables batching; any positive value enables batching with that delay.
func (gi *GpuCagra[T]) SetBatchWindow(windowUs int64) error {
	gi.batchWindowUs = windowUs
	if gi.cCagra != nil {
		var errmsg *C.char
		C.gpu_cagra_set_batch_window(gi.cCagra, C.int64_t(windowUs), unsafe.Pointer(&errmsg))
		if errmsg != nil {
			errStr := C.GoString(errmsg)
			C.free(unsafe.Pointer(errmsg))
			return moerr.NewInternalErrorNoCtx(errStr)
		}
	}
	return nil
}

// NewGpuCagra creates a new GpuCagra instance from a dataset.
// ids may be nil to use internal sequential IDs (0..count-1).
func NewGpuCagra[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType,
	bp CagraBuildParams, devices []int, nthread uint32, mode DistributionMode, ids []int64) (*GpuCagra[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	var cIds *C.int64_t
	if len(ids) > 0 {
		cIds = (*C.int64_t)(unsafe.Pointer(&ids[0]))
	}

	cBP := C.cagra_build_params_t{
		intermediate_graph_degree: C.size_t(bp.IntermediateGraphDegree),
		graph_degree:              C.size_t(bp.GraphDegree),
		attach_dataset_on_build:   C.bool(bp.AttachDatasetOnBuild),
	}

	cCagra := C.gpu_cagra_new(
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(count),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		cBP,
		&cDevices[0],
		C.int(len(devices)),
		C.uint32_t(nthread),
		C.distribution_mode_t(mode),
		C.quantization_t(qtype),
		cIds,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)
	runtime.KeepAlive(cDevices)
	runtime.KeepAlive(ids)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cCagra == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuCagra")
	}

	return &GpuCagra[T]{
		cCagra:    cCagra,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuCagraFromFile creates a new GpuCagra instance by loading from a file.
func NewGpuCagraFromFile[T VectorType](filename string, dimension uint32, metric DistanceType,
	bp CagraBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuCagra[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.cagra_build_params_t{
		intermediate_graph_degree: C.size_t(bp.IntermediateGraphDegree),
		graph_degree:              C.size_t(bp.GraphDegree),
		attach_dataset_on_build:   C.bool(bp.AttachDatasetOnBuild),
	}

	cCagra := C.gpu_cagra_load_file(
		cFilename,
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		cBP,
		&cDevices[0],
		C.int(len(devices)),
		C.uint32_t(nthread),
		C.distribution_mode_t(mode),
		C.quantization_t(qtype),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cCagra == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to load GpuCagra from file")
	}

	return &GpuCagra[T]{
		cCagra:    cCagra,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuCagraFromDataDirectory loads a GpuCagra index from a directory written by save_dir.
func NewGpuCagraFromDataDirectory[T VectorType](dir string, dimension uint32, metric DistanceType,
	bp CagraBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuCagra[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.cagra_build_params_t{
		intermediate_graph_degree: C.size_t(bp.IntermediateGraphDegree),
		graph_degree:              C.size_t(bp.GraphDegree),
		attach_dataset_on_build:   C.bool(bp.AttachDatasetOnBuild),
	}

	var errmsg *C.char
	cCagra := C.gpu_cagra_new_empty(
		0,
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		cBP,
		&cDevices[0],
		C.int(len(devices)),
		C.uint32_t(nthread),
		C.distribution_mode_t(mode),
		C.quantization_t(qtype),
		nil,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	if cCagra == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuCagra for loading")
	}

	C.gpu_cagra_start(cCagra, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_cagra_destroy(cCagra, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_cagra_load_dir(cCagra, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_cagra_destroy(cCagra, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return &GpuCagra[T]{
		cCagra:    cCagra,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// Destroy frees the C++ gpu_cagra_t instance
func (gi *GpuCagra[T]) Destroy() error {
	if gi.cCagra == nil {
		return nil
	}
	var errmsg *C.char
	C.gpu_cagra_destroy(gi.cCagra, unsafe.Pointer(&errmsg))
	gi.cCagra = nil
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Start initializes the worker and resources
func (gi *GpuCagra[T]) Start() error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	if gi.distMode == Replicated && gi.nthread > 1 {
		var errmsg *C.char
		C.gpu_cagra_set_per_thread_device(gi.cCagra, C.bool(true), unsafe.Pointer(&errmsg))
		if errmsg != nil {
			errStr := C.GoString(errmsg)
			C.free(unsafe.Pointer(errmsg))
			return moerr.NewInternalErrorNoCtx(errStr)
		}
	}

	if gi.batchWindowUs > 0 {
		if err := gi.SetBatchWindow(gi.batchWindowUs); err != nil {
			return err
		}
	}

	var errmsg *C.char
	C.gpu_cagra_start(gi.cCagra, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Build triggers the build or file loading process
func (gi *GpuCagra[T]) Build() error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	C.gpu_cagra_build(gi.cCagra, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// NewGpuCagraEmpty creates a new GpuCagra instance with pre-allocated buffer but no data yet.
func NewGpuCagraEmpty[T VectorType](totalCount uint64, dimension uint32, metric DistanceType,
	bp CagraBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuCagra[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.cagra_build_params_t{
		intermediate_graph_degree: C.size_t(bp.IntermediateGraphDegree),
		graph_degree:              C.size_t(bp.GraphDegree),
		attach_dataset_on_build:   C.bool(bp.AttachDatasetOnBuild),
	}

	cCagra := C.gpu_cagra_new_empty(
		C.uint64_t(totalCount),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		cBP,
		&cDevices[0],
		C.int(len(devices)),
		C.uint32_t(nthread),
		C.distribution_mode_t(mode),
		C.quantization_t(qtype),
		nil,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cCagra == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuCagra")
	}

	return &GpuCagra[T]{
		cCagra:    cCagra,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gi *GpuCagra[T]) AddChunk(chunk []T, chunkCount uint64, ids []int64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	var cIds *C.int64_t
	if len(ids) > 0 {
		cIds = (*C.int64_t)(unsafe.Pointer(&ids[0]))
	}
	C.gpu_cagra_add_chunk(
		gi.cCagra,
		unsafe.Pointer(&chunk[0]),
		C.uint64_t(chunkCount),
		cIds,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(chunk)
	runtime.KeepAlive(ids)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddChunkFloat adds a chunk of float32 data, performing on-the-fly quantization if needed.
func (gi *GpuCagra[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []int64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	var cIds *C.int64_t
	if len(ids) > 0 {
		cIds = (*C.int64_t)(unsafe.Pointer(&ids[0]))
	}
	C.gpu_cagra_add_chunk_float(
		gi.cCagra,
		(*C.float)(&chunk[0]),
		C.uint64_t(chunkCount),
		cIds,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(chunk)
	runtime.KeepAlive(ids)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// TrainQuantizer trains the scalar quantizer (if T is 1-byte)
func (gi *GpuCagra[T]) TrainQuantizer(trainData []float32, nSamples uint64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(trainData) == 0 || nSamples == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_cagra_train_quantizer(
		gi.cCagra,
		(*C.float)(&trainData[0]),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(trainData)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// SetQuantizer sets the scalar quantizer parameters (if T is 1-byte)
func (gi *GpuCagra[T]) SetQuantizer(min, max float32) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	var errmsg *C.char
	C.gpu_cagra_set_quantizer(
		gi.cCagra,
		C.float(min),
		C.float(max),
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// GetQuantizer gets the scalar quantizer parameters (if T is 1-byte)
func (gi *GpuCagra[T]) GetQuantizer() (float32, float32, error) {
	if gi.cCagra == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	var errmsg *C.char
	var cMin, cMax C.float
	C.gpu_cagra_get_quantizer(
		gi.cCagra,
		&cMin,
		&cMax,
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, 0, moerr.NewInternalErrorNoCtx(errStr)
	}
	return float32(cMin), float32(cMax), nil
}

// Save serializes the index to a file
func (gi *GpuCagra[T]) Save(filename string) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	C.gpu_cagra_save(gi.cCagra, cFilename, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Pack saves the index to a .tar or .tar.gz file using save_dir.
func (gi *GpuCagra[T]) Pack(filename string) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "cagra-pack-*")
	if err != nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tmpDir)

	var errmsg *C.char
	cDir := C.CString(tmpDir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_cagra_save_dir(gi.cCagra, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}

	return Pack(tmpDir, filename)
}

// Unpack extracts a .tar or .tar.gz file and loads index components via load_dir.
// The index must already be initialized and started before calling Unpack.
func (gi *GpuCagra[T]) Unpack(filename string) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "cagra-unpack-*")
	if err != nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tmpDir)

	if _, err := Unpack(filename, tmpDir); err != nil {
		return err
	}

	var errmsg *C.char
	cDir := C.CString(tmpDir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_cagra_load_dir(gi.cCagra, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// DeleteId removes an ID from the index (soft delete).
func (gi *GpuCagra[T]) DeleteId(id int64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	C.gpu_cagra_delete_id(gi.cCagra, C.int64_t(id), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Search performs a K-Nearest Neighbor search
func (gi *GpuCagra[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (SearchResult, error) {
	if gi.cCagra == nil {
		return SearchResult{}, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResult{}, nil
	}

	var errmsg *C.char
	cSP := C.cagra_search_params_t{
		itopk_size:   C.size_t(sp.ItopkSize),
		search_width: C.size_t(sp.SearchWidth),
	}

	res := C.gpu_cagra_search(
		gi.cCagra,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return SearchResult{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResult{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_cagra_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_cagra_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_cagra_free_result(res.result_ptr)

	return SearchResult{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// SearchFloat performs a K-Nearest Neighbor search with float32 queries
func (gi *GpuCagra[T]) SearchFloat(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (SearchResult, error) {
	if gi.cCagra == nil {
		return SearchResult{}, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResult{}, nil
	}

	var errmsg *C.char
	cSP := C.cagra_search_params_t{
		itopk_size:   C.size_t(sp.ItopkSize),
		search_width: C.size_t(sp.SearchWidth),
	}

	res := C.gpu_cagra_search_float(
		gi.cCagra,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return SearchResult{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResult{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_cagra_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_cagra_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_cagra_free_result(res.result_ptr)

	return SearchResult{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// SearchAsync performs a K-Nearest Neighbor search asynchronously.
func (gi *GpuCagra[T]) SearchAsync(queries []T, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchAsyncWithParams(queries, numQueries, dimension, limit, DefaultCagraSearchParams())
}

// SearchAsyncWithParams performs a K-Nearest Neighbor search asynchronously with custom parameters.
func (gi *GpuCagra[T]) SearchAsyncWithParams(queries []T, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (uint64, error) {
	if gi.cCagra == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cSP := C.cagra_search_params_t{
		itopk_size:   C.size_t(sp.ItopkSize),
		search_width: C.size_t(sp.SearchWidth),
	}

	jobID := C.gpu_cagra_search_async(
		gi.cCagra,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	return uint64(jobID), nil
}

// SearchFloat32Async performs a K-Nearest Neighbor search with float32 queries asynchronously.
func (gi *GpuCagra[T]) SearchFloat32Async(queries []float32, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchFloat32AsyncWithParams(queries, numQueries, dimension, limit, DefaultCagraSearchParams())
}

// SearchFloat32AsyncWithParams performs a K-Nearest Neighbor search with float32 queries asynchronously with custom parameters.
func (gi *GpuCagra[T]) SearchFloat32AsyncWithParams(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (uint64, error) {
	if gi.cCagra == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cSP := C.cagra_search_params_t{
		itopk_size:   C.size_t(sp.ItopkSize),
		search_width: C.size_t(sp.SearchWidth),
	}

	jobID := C.gpu_cagra_search_float_async(
		gi.cCagra,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	return uint64(jobID), nil
}

// SearchWait waits for an asynchronous search to complete and returns the results.
func (gi *GpuCagra[T]) SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error) {
	if gi.cCagra == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}

	var errmsg *C.char
	res := C.gpu_cagra_search_wait(gi.cCagra, C.uint64_t(jobID), unsafe.Pointer(&errmsg))

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("search_wait returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_cagra_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_cagra_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_cagra_free_result(res.result_ptr)

	return neighbors, distances, nil
}

// Cap returns the capacity of the index buffer
func (gi *GpuCagra[T]) Cap() uint64 {
	if gi.cCagra == nil {
		return 0
	}
	return uint64(C.gpu_cagra_cap(gi.cCagra))
}

// Len returns current number of vectors in index
func (gi *GpuCagra[T]) Len() uint64 {
	if gi.cCagra == nil {
		return 0
	}
	return uint64(C.gpu_cagra_len(gi.cCagra))
}

// Info returns detailed information about the index as a JSON string.
func (gi *GpuCagra[T]) Info() (string, error) {
	if gi.cCagra == nil {
		return "", moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	infoPtr := C.gpu_cagra_info(gi.cCagra, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		if infoPtr != nil {
			C.free(unsafe.Pointer(infoPtr))
		}
		return "", moerr.NewInternalErrorNoCtx(errStr)
	}
	if infoPtr == nil {
		return "{}", nil
	}
	info := C.GoString(infoPtr)
	C.free(unsafe.Pointer(infoPtr))
	return info, nil
}

// Extend adds more vectors to the index (single-GPU only).
// newIDs may be nil to auto-assign sequential IDs starting from the current index size.
func (gi *GpuCagra[T]) Extend(additionalData []T, numVectors uint64, newIDs []int64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(additionalData) == 0 || numVectors == 0 {
		return nil
	}

	var idsPtr *C.int64_t
	if len(newIDs) > 0 {
		idsPtr = (*C.int64_t)(unsafe.Pointer(&newIDs[0]))
	}

	var errmsg *C.char
	C.gpu_cagra_extend(
		gi.cCagra,
		unsafe.Pointer(&additionalData[0]),
		C.uint64_t(numVectors),
		idsPtr,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(additionalData)
	runtime.KeepAlive(newIDs)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// MergeGpuCagra combines multiple single-GPU GpuCagra indices into a new one.
func MergeGpuCagra[T VectorType](indices []*GpuCagra[T], nthread uint32, devices []int) (*GpuCagra[T], error) {
	if len(indices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("no indices to merge")
	}
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	cIndices := make([]C.gpu_cagra_c, len(indices))
	for i, idx := range indices {
		cIndices[i] = idx.cCagra
	}

	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	var errmsg *C.char
	cCagra := C.gpu_cagra_merge(
		&cIndices[0],
		C.int(len(indices)),
		C.uint32_t(nthread),
		&cDevices[0],
		C.int(len(devices)),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(cIndices)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cCagra == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to merge GpuCagra indices")
	}

	return &GpuCagra[T]{
		cCagra:    cCagra,
		dimension: indices[0].dimension,
		nthread:   nthread,
		distMode:  indices[0].distMode,
	}, nil
}

// SearchResult contains the neighbors and distances from a CAGRA search.
type SearchResult struct {
	Neighbors []int64
	Distances []float32
}

// SaveToDir saves the index files to a directory using gpu_cagra_save_dir.
// This is used by CagraModel to save to a directory before packing to tar.
func (gi *GpuCagra[T]) SaveToDir(dirPath string) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	cDir := C.CString(dirPath)
	defer C.free(unsafe.Pointer(cDir))
	C.gpu_cagra_save_dir(gi.cCagra, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// LoadFromDir loads index components from a directory using gpu_cagra_load_dir.
// The index must already be initialized and started before calling LoadFromDir.
func (gi *GpuCagra[T]) LoadFromDir(dirPath string) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	cDir := C.CString(dirPath)
	defer C.free(unsafe.Pointer(cDir))
	C.gpu_cagra_load_dir(gi.cCagra, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}
