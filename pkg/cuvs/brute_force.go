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
#include "../../cgo/cuvs/brute_force_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"runtime"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// GpuBruteForce represents the C++ gpu_brute_force_t object
type GpuBruteForce[T VectorType] struct {
	cIndex C.gpu_brute_force_c
}

// NewGpuBruteForce creates a new GpuBruteForce instance
func NewGpuBruteForce[T VectorType](dataset []T, countVectors uint64, dimension uint32, metric DistanceType, nthread uint32, deviceID int) (*GpuBruteForce[T], error) {
	if len(dataset) == 0 || countVectors == 0 || dimension == 0 {
		return nil, moerr.NewInternalErrorNoCtx("dataset, count_vectors, and dimension cannot be zero")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cIndex := C.gpu_brute_force_new(
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(countVectors),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		C.uint32_t(nthread),
		C.int(deviceID),
		C.quantization_t(qtype),
		nil,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cIndex == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuBruteForce")
	}
	return &GpuBruteForce[T]{cIndex: cIndex}, nil
}

// NewGpuBruteForceEmpty creates a new GpuBruteForce instance with pre-allocated buffer but no data yet.
func NewGpuBruteForceEmpty[T VectorType](totalCount uint64, dimension uint32, metric DistanceType,
	nthread uint32, deviceID int) (*GpuBruteForce[T], error) {

	qtype := GetQuantization[T]()
	var errmsg *C.char

	cBruteForce := C.gpu_brute_force_new_empty(
		C.uint64_t(totalCount),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		C.uint32_t(nthread),
		C.int(deviceID),
		C.quantization_t(qtype),
		nil,
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cBruteForce == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuBruteForce")
	}

	return &GpuBruteForce[T]{cIndex: cBruteForce}, nil
}

// Start initializes the worker and resources
func (gb *GpuBruteForce[T]) Start() error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	var errmsg *C.char
	C.gpu_brute_force_start(gb.cIndex, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Build triggers the dataset loading to GPU
func (gb *GpuBruteForce[T]) Build() error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	var errmsg *C.char
	C.gpu_brute_force_build(gb.cIndex, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
// If ids is non-nil it must have length chunkCount and supplies external int64
// ids (e.g. pkids) that the brute-force search will return in `neighbors`
// instead of the internal 0..N-1 row index.
func (gb *GpuBruteForce[T]) AddChunk(chunk []T, chunkCount uint64, ids []int64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}
	if ids != nil && uint64(len(ids)) != chunkCount {
		return moerr.NewInternalErrorNoCtx("ids length does not match chunkCount")
	}

	var errmsg *C.char
	var idsPtr *C.int64_t
	if ids != nil {
		idsPtr = (*C.int64_t)(&ids[0])
	}
	C.gpu_brute_force_add_chunk(
		gb.cIndex,
		unsafe.Pointer(&chunk[0]),
		C.uint64_t(chunkCount),
		idsPtr,
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

// AddChunkFloat adds a chunk of float32 data, performing on-the-fly conversion if needed.
// See AddChunk for the meaning of ids.
func (gb *GpuBruteForce[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []int64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}
	if ids != nil && uint64(len(ids)) != chunkCount {
		return moerr.NewInternalErrorNoCtx("ids length does not match chunkCount")
	}

	var errmsg *C.char
	var idsPtr *C.int64_t
	if ids != nil {
		idsPtr = (*C.int64_t)(&ids[0])
	}
	C.gpu_brute_force_add_chunk_float(
		gb.cIndex,
		(*C.float)(&chunk[0]),
		C.uint64_t(chunkCount),
		idsPtr,
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

// SearchInto performs a search and writes results into caller-provided slices (no internal allocation).
// neighbors and distances must be pre-allocated to at least numQueries*limit elements.
func (gb *GpuBruteForce[T]) SearchInto(queries []T, numQueries uint64, queryDimension uint32, limit uint32, neighbors []int64, distances []float32) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
		return moerr.NewInternalErrorNoCtx("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search(
		gb.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(queryDimension),
		C.uint32_t(limit),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	if cResult == nil {
		return moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	C.gpu_brute_force_get_results(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)
	C.gpu_brute_force_free_search_result(cResult)
	return nil
}

// Search performs a search operation
func (gb *GpuBruteForce[T]) Search(queries []T, numQueries uint64, queryDimension uint32, limit uint32) ([]int64, []float32, error) {
	neighbors := make([]int64, numQueries*uint64(limit))
	distances := make([]float32, numQueries*uint64(limit))
	if err := gb.SearchInto(queries, numQueries, queryDimension, limit, neighbors, distances); err != nil {
		return nil, nil, err
	}
	return neighbors, distances, nil
}

// SearchFloatInto performs a search with float32 queries and writes results into caller-provided slices.
// neighbors and distances must be pre-allocated to at least numQueries*limit elements.
func (gb *GpuBruteForce[T]) SearchFloatInto(queries []float32, numQueries uint64, queryDimension uint32, limit uint32, neighbors []int64, distances []float32) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 || queryDimension == 0 {
		return moerr.NewInternalErrorNoCtx("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search_float(
		gb.cIndex,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(queryDimension),
		C.uint32_t(limit),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	if cResult == nil {
		return moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	C.gpu_brute_force_get_results(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)
	C.gpu_brute_force_free_search_result(cResult)
	return nil
}

// SearchFloat performs a search operation with float32 queries
func (gb *GpuBruteForce[T]) SearchFloat(queries []float32, numQueries uint64, queryDimension uint32, limit uint32) ([]int64, []float32, error) {
	neighbors := make([]int64, numQueries*uint64(limit))
	distances := make([]float32, numQueries*uint64(limit))
	if err := gb.SearchFloatInto(queries, numQueries, queryDimension, limit, neighbors, distances); err != nil {
		return nil, nil, err
	}
	return neighbors, distances, nil
}

// SearchAsync performs a K-Nearest Neighbor search asynchronously.
func (gb *GpuBruteForce[T]) SearchAsync(queries []T, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	if gb.cIndex == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	jobID := C.gpu_brute_force_search_async(
		gb.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
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
func (gb *GpuBruteForce[T]) SearchFloat32Async(queries []float32, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	if gb.cIndex == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	jobID := C.gpu_brute_force_search_float_async(
		gb.cIndex,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
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
func (gb *GpuBruteForce[T]) SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error) {
	if gb.cIndex == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search_wait(gb.cIndex, C.uint64_t(jobID), unsafe.Pointer(&errmsg))

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cResult == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("search_wait returned nil result")
	}

	// Allocate slices for results
	neighbors := make([]int64, numQueries*uint64(limit))
	distances := make([]float32, numQueries*uint64(limit))

	C.gpu_brute_force_get_results(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_brute_force_free_search_result(cResult)

	return neighbors, distances, nil
}

// Cap returns the capacity of the index buffer
func (gb *GpuBruteForce[T]) Cap() uint64 {
	if gb.cIndex == nil {
		return 0
	}
	return uint64(C.gpu_brute_force_cap(gb.cIndex))
}

// Len returns current number of vectors in index
func (gb *GpuBruteForce[T]) Len() uint64 {
	if gb.cIndex == nil {
		return 0
	}
	return uint64(C.gpu_brute_force_len(gb.cIndex))
}

// Info returns detailed information about the index as a JSON string.
func (gb *GpuBruteForce[T]) Info() (string, error) {
	if gb.cIndex == nil {
		return "", moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	var errmsg *C.char
	infoPtr := C.gpu_brute_force_info(gb.cIndex, unsafe.Pointer(&errmsg))
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

// Destroy frees the C++ GpuBruteForce instance
func (gb *GpuBruteForce[T]) Destroy() error {
	if gb.cIndex == nil {
		return nil
	}
	var errmsg *C.char
	C.gpu_brute_force_destroy(gb.cIndex, unsafe.Pointer(&errmsg))
	gb.cIndex = nil // Mark as destroyed
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// SetFilterColumns registers filter-column metadata. See GpuCagra.SetFilterColumns.
func (gb *GpuBruteForce[T]) SetFilterColumns(colMetaJSON string, totalCount uint64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	var errmsg *C.char
	cMeta := C.CString(colMetaJSON)
	defer C.free(unsafe.Pointer(cMeta))
	C.gpu_brute_force_set_filter_columns(gb.cIndex, cMeta, C.uint64_t(totalCount), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddFilterChunk appends raw filter-column bytes. See GpuCagra.AddFilterChunk.
func (gb *GpuBruteForce[T]) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(data) == 0 || nrows == 0 {
		return nil
	}
	var errmsg *C.char
	var cNullBitmap *C.uint32_t
	if len(nullBitmap) > 0 {
		cNullBitmap = (*C.uint32_t)(unsafe.Pointer(&nullBitmap[0]))
	}
	C.gpu_brute_force_add_filter_chunk(
		gb.cIndex,
		C.uint32_t(colIdx),
		unsafe.Pointer(&data[0]),
		cNullBitmap,
		C.uint64_t(nrows),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(data)
	runtime.KeepAlive(nullBitmap)
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// SearchWithFilter runs a filtered K-NN search. predsJSON="" = unfiltered.
func (gb *GpuBruteForce[T]) SearchWithFilter(queries []T, numQueries uint64, dimension uint32, limit uint32, predsJSON string) ([]int64, []float32, error) {
	if gb.cIndex == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return nil, nil, nil
	}

	var errmsg *C.char
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	cResult := C.gpu_brute_force_search_with_filter(
		gb.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cPreds,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	if cResult == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)
	C.gpu_brute_force_get_results(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)
	C.gpu_brute_force_free_search_result(cResult)
	return neighbors, distances, nil
}

// SearchFloatWithFilter runs a filtered K-NN search with float32 queries.
func (gb *GpuBruteForce[T]) SearchFloatWithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, predsJSON string) ([]int64, []float32, error) {
	if gb.cIndex == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return nil, nil, nil
	}

	var errmsg *C.char
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	cResult := C.gpu_brute_force_search_float_with_filter(
		gb.cIndex,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cPreds,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	if cResult == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)
	C.gpu_brute_force_get_results(cResult, C.uint64_t(numQueries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)
	C.gpu_brute_force_free_search_result(cResult)
	return neighbors, distances, nil
}

// SearchFloatWithFilterAsync submits a filtered float32 K-NN search and
// returns a job_id; collect the result with SearchWait. Mirrors
// SearchFloat32Async + the predicate-eval semantics of SearchFloatWithFilter.
// Used by the multi-index brute-force fallback so it runs in parallel with
// the primary IVF/CAGRA shards.
func (gb *GpuBruteForce[T]) SearchFloatWithFilterAsync(queries []float32, numQueries uint64, dimension uint32, limit uint32, predsJSON string) (uint64, error) {
	if gb.cIndex == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	jobID := C.gpu_brute_force_search_float_with_filter_async(
		gb.cIndex,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cPreds,
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
