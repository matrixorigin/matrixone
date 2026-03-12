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
func NewGpuBruteForce[T VectorType](dataset []T, count_vectors uint64, dimension uint32, metric DistanceType, nthread uint32, device_id int) (*GpuBruteForce[T], error) {
	if len(dataset) == 0 || count_vectors == 0 || dimension == 0 {
		return nil, moerr.NewInternalErrorNoCtx("dataset, count_vectors, and dimension cannot be zero")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cIndex := C.gpu_brute_force_new(
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(count_vectors),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		C.uint32_t(nthread),
		C.int(device_id),
		C.quantization_t(qtype),
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

// Load triggers the dataset loading to GPU
func (gb *GpuBruteForce[T]) Load() error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	var errmsg *C.char
	C.gpu_brute_force_load(gb.cIndex, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gb *GpuBruteForce[T]) AddChunk(chunk []T, chunkCount uint64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_brute_force_add_chunk(
		gb.cIndex,
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

// AddChunkFloat adds a chunk of float32 data, performing on-the-fly conversion if needed.
func (gb *GpuBruteForce[T]) AddChunkFloat(chunk []float32, chunkCount uint64) error {
	if gb.cIndex == nil {
		return moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_brute_force_add_chunk_float(
		gb.cIndex,
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

// Search performs a search operation
func (gb *GpuBruteForce[T]) Search(queries []T, num_queries uint64, query_dimension uint32, limit uint32) ([]int64, []float32, error) {
	if gb.cIndex == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search(
		gb.cIndex,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(num_queries),
		C.uint32_t(query_dimension),
		C.uint32_t(limit),
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

	// Allocate slices for results
	neighbors := make([]int64, num_queries*uint64(limit))
	distances := make([]float32, num_queries*uint64(limit))

	C.gpu_brute_force_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_brute_force_free_search_result(cResult)

	return neighbors, distances, nil
}

// SearchFloat performs a search operation with float32 queries
func (gb *GpuBruteForce[T]) SearchFloat(queries []float32, num_queries uint64, query_dimension uint32, limit uint32) ([]int64, []float32, error) {
	if gb.cIndex == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuBruteForce is not initialized")
	}
	if len(queries) == 0 || num_queries == 0 || query_dimension == 0 {
		return nil, nil, moerr.NewInternalErrorNoCtx("queries, num_queries, and query_dimension cannot be zero")
	}

	var errmsg *C.char
	cResult := C.gpu_brute_force_search_float(
		gb.cIndex,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(num_queries),
		C.uint32_t(query_dimension),
		C.uint32_t(limit),
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

	// Allocate slices for results
	neighbors := make([]int64, num_queries*uint64(limit))
	distances := make([]float32, num_queries*uint64(limit))

	C.gpu_brute_force_get_results(cResult, C.uint64_t(num_queries), C.uint32_t(limit), (*C.int64_t)(unsafe.Pointer(&neighbors[0])), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_brute_force_free_search_result(cResult)

	return neighbors, distances, nil
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
