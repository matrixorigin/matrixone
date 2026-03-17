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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"runtime"
	"unsafe"
)

// GpuCagra represents the C++ gpu_cagra_t object.
type GpuCagra[T VectorType] struct {
	cCagra    C.gpu_cagra_c
	dimension uint32
	nthread   uint32
	distMode  DistributionMode
}

// NewGpuCagra creates a new GpuCagra instance from a dataset.
func NewGpuCagra[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType,
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
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)
	runtime.KeepAlive(cDevices)

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
func (gi *GpuCagra[T]) AddChunk(chunk []T, chunkCount uint64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_cagra_add_chunk(
		gi.cCagra,
		unsafe.Pointer(&chunk[0]),
		C.uint64_t(chunkCount),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(chunk)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddChunkFloat adds a chunk of float32 data, performing on-the-fly quantization if needed.
func (gi *GpuCagra[T]) AddChunkFloat(chunk []float32, chunkCount uint64) error {
	if gi.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_cagra_add_chunk_float(
		gi.cCagra,
		(*C.float)(&chunk[0]),
		C.uint64_t(chunkCount),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(chunk)

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
func (gc *GpuCagra[T]) Save(filename string) error {
	if gc.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	var errmsg *C.char
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	C.gpu_cagra_save(gc.cCagra, cFilename, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Search performs a K-Nearest Neighbor search
func (gc *GpuCagra[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (SearchResult, error) {
	if gc.cCagra == nil {
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
		gc.cCagra,
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
	neighbors := make([]uint32, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_cagra_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.uint32_t)(unsafe.Pointer(&neighbors[0])))
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
func (gc *GpuCagra[T]) SearchFloat(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp CagraSearchParams) (SearchResult, error) {
	if gc.cCagra == nil {
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
		gc.cCagra,
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
	neighbors := make([]uint32, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_cagra_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.uint32_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_cagra_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_cagra_free_result(res.result_ptr)

	return SearchResult{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// Cap returns the capacity of the index buffer
func (gc *GpuCagra[T]) Cap() uint32 {
	if gc.cCagra == nil {
		return 0
	}
	return uint32(C.gpu_cagra_cap(gc.cCagra))
}

// Len returns current number of vectors in index
func (gc *GpuCagra[T]) Len() uint32 {
	if gc.cCagra == nil {
		return 0
	}
	return uint32(C.gpu_cagra_len(gc.cCagra))
}

// Extend adds more vectors to the index (single-GPU only)
func (gc *GpuCagra[T]) Extend(additionalData []T, numVectors uint64) error {
	if gc.cCagra == nil {
		return moerr.NewInternalErrorNoCtx("GpuCagra is not initialized")
	}
	if len(additionalData) == 0 || numVectors == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_cagra_extend(
		gc.cCagra,
		unsafe.Pointer(&additionalData[0]),
		C.uint64_t(numVectors),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(additionalData)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Merge combines multiple single-GPU GpuCagra indices into a new one.
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

	return &GpuCagra[T]{cCagra: cCagra, dimension: indices[0].dimension}, nil
}

// SearchResult contains the neighbors and distances from a search.
type SearchResult struct {
	Neighbors []uint32
	Distances []float32
}
