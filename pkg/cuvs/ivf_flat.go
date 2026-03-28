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
#include "../../cgo/cuvs/ivf_flat_c.h"
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

// GpuIvfFlat represents the C++ gpu_ivf_flat_t object.
type GpuIvfFlat[T VectorType] struct {
	cIvfFlat    C.gpu_ivf_flat_c
	dimension   uint32
	nthread     uint32
	distMode    DistributionMode
	useBatching bool
}

// SetUseBatching enables or disables dynamic batching for search operations.
func (gi *GpuIvfFlat[T]) SetUseBatching(enable bool) error {
	gi.useBatching = enable
	if gi.cIvfFlat != nil {
		var errmsg *C.char
		C.gpu_ivf_flat_set_use_batching(gi.cIvfFlat, C.bool(enable), unsafe.Pointer(&errmsg))
		if errmsg != nil {
			errStr := C.GoString(errmsg)
			C.free(unsafe.Pointer(errmsg))
			return moerr.NewInternalErrorNoCtx(errStr)
		}
	}
	return nil
}

// NewGpuIvfFlat creates a new GpuIvfFlat instance from a dataset.
func NewGpuIvfFlat[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType,
	bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode, ids []int64) (*GpuIvfFlat[T], error) {
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

	cBP := C.ivf_flat_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfFlat := C.gpu_ivf_flat_new(
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

	if cIvfFlat == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuIvfFlat")
	}

	return &GpuIvfFlat[T]{
		cIvfFlat:  cIvfFlat,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfFlatFromFile creates a new GpuIvfFlat instance by loading from a file.
func NewGpuIvfFlatFromFile[T VectorType](filename string, dimension uint32, metric DistanceType,
	bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfFlat[T], error) {
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

	cBP := C.ivf_flat_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfFlat := C.gpu_ivf_flat_load_file(
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

	if cIvfFlat == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to load GpuIvfFlat from file")
	}

	return &GpuIvfFlat[T]{
		cIvfFlat:  cIvfFlat,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfFlatFromDataDirectory loads a GpuIvfFlat index from a directory written by save_dir.
func NewGpuIvfFlatFromDataDirectory[T VectorType](dir string, dimension uint32, metric DistanceType,
	bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfFlat[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.ivf_flat_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	var errmsg *C.char
	cIvfFlat := C.gpu_ivf_flat_new_empty(
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
	if cIvfFlat == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuIvfFlat for loading")
	}

	C.gpu_ivf_flat_start(cIvfFlat, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_ivf_flat_destroy(cIvfFlat, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_ivf_flat_load_dir(cIvfFlat, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_ivf_flat_destroy(cIvfFlat, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return &GpuIvfFlat[T]{
		cIvfFlat:  cIvfFlat,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// Destroy frees the C++ gpu_ivf_flat_t instance
func (gi *GpuIvfFlat[T]) Destroy() error {
	if gi.cIvfFlat == nil {
		return nil
	}
	var errmsg *C.char
	C.gpu_ivf_flat_destroy(gi.cIvfFlat, unsafe.Pointer(&errmsg))
	gi.cIvfFlat = nil
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Start initializes the worker and resources
func (gi *GpuIvfFlat[T]) Start() error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}

	if gi.distMode == Replicated && gi.nthread > 1 {
		var errmsg *C.char
		C.gpu_ivf_flat_set_per_thread_device(gi.cIvfFlat, C.bool(true), unsafe.Pointer(&errmsg))
		if errmsg != nil {
			errStr := C.GoString(errmsg)
			C.free(unsafe.Pointer(errmsg))
			return moerr.NewInternalErrorNoCtx(errStr)
		}
	}

	if gi.useBatching {
		if err := gi.SetUseBatching(true); err != nil {
			return err
		}
	}

	var errmsg *C.char
	C.gpu_ivf_flat_start(gi.cIvfFlat, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Build triggers the build or file loading process
func (gi *GpuIvfFlat[T]) Build() error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	var errmsg *C.char
	C.gpu_ivf_flat_build(gi.cIvfFlat, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// NewGpuIvfFlatEmpty creates a new GpuIvfFlat instance with pre-allocated buffer but no data yet.
func NewGpuIvfFlatEmpty[T VectorType](totalCount uint64, dimension uint32, metric DistanceType,
	bp IvfFlatBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfFlat[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.ivf_flat_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfFlat := C.gpu_ivf_flat_new_empty(
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

	if cIvfFlat == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuIvfFlat")
	}

	return &GpuIvfFlat[T]{
		cIvfFlat:  cIvfFlat,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gi *GpuIvfFlat[T]) AddChunk(chunk []T, chunkCount uint64) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_flat_add_chunk(
		gi.cIvfFlat,
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
func (gi *GpuIvfFlat[T]) AddChunkFloat(chunk []float32, chunkCount uint64) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_flat_add_chunk_float(
		gi.cIvfFlat,
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
func (gi *GpuIvfFlat[T]) TrainQuantizer(trainData []float32, nSamples uint64) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(trainData) == 0 || nSamples == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_flat_train_quantizer(
		gi.cIvfFlat,
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
func (gi *GpuIvfFlat[T]) SetQuantizer(min, max float32) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}

	var errmsg *C.char
	C.gpu_ivf_flat_set_quantizer(
		gi.cIvfFlat,
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
func (gi *GpuIvfFlat[T]) GetQuantizer() (float32, float32, error) {
	if gi.cIvfFlat == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}

	var errmsg *C.char
	var cMin, cMax C.float
	C.gpu_ivf_flat_get_quantizer(
		gi.cIvfFlat,
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
func (gi *GpuIvfFlat[T]) Save(filename string) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	var errmsg *C.char
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	C.gpu_ivf_flat_save(gi.cIvfFlat, cFilename, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Pack saves the index to a .tar or .tar.gz file using save_dir.
func (gi *GpuIvfFlat[T]) Pack(filename string) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "ivf-flat-pack-*")
	if err != nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tmpDir)

	var errmsg *C.char
	cDir := C.CString(tmpDir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_ivf_flat_save_dir(gi.cIvfFlat, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}

	return Pack(tmpDir, filename)
}

// Unpack extracts a .tar or .tar.gz file and loads index components via load_dir.
// The index must already be initialized and started before calling Unpack.
func (gi *GpuIvfFlat[T]) Unpack(filename string) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "ivf-flat-unpack-*")
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

	C.gpu_ivf_flat_load_dir(gi.cIvfFlat, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Search performs a K-Nearest Neighbor search
func (gi *GpuIvfFlat[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) (SearchResultIvfFlat, error) {
	if gi.cIvfFlat == nil {
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfFlat{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_flat_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	res := C.gpu_ivf_flat_search(
		gi.cIvfFlat,
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
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_ivf_flat_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_ivf_flat_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_ivf_flat_free_result(res.result_ptr)

	return SearchResultIvfFlat{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// SearchFloat performs a K-Nearest Neighbor search with float32 queries
func (gi *GpuIvfFlat[T]) SearchFloat(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfFlatSearchParams) (SearchResultIvfFlat, error) {
	if gi.cIvfFlat == nil {
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfFlat{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_flat_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	res := C.gpu_ivf_flat_search_float(
		gi.cIvfFlat,
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
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResultIvfFlat{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_ivf_flat_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_ivf_flat_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_ivf_flat_free_result(res.result_ptr)

	return SearchResultIvfFlat{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// Cap returns the capacity of the index buffer
func (gi *GpuIvfFlat[T]) Cap() uint32 {
	if gi.cIvfFlat == nil {
		return 0
	}
	return uint32(C.gpu_ivf_flat_cap(gi.cIvfFlat))
}

// Len returns current number of vectors in index
func (gi *GpuIvfFlat[T]) Len() uint32 {
	if gi.cIvfFlat == nil {
		return 0
	}
	return uint32(C.gpu_ivf_flat_len(gi.cIvfFlat))
}

// Info returns detailed information about the index as a JSON string.
func (gi *GpuIvfFlat[T]) Info() (string, error) {
	if gi.cIvfFlat == nil {
		return "", moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	var errmsg *C.char
	infoPtr := C.gpu_ivf_flat_info(gi.cIvfFlat, unsafe.Pointer(&errmsg))
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

// GetCenters retrieves the trained centroids.
func (gi *GpuIvfFlat[T]) GetCenters(nLists uint32) ([]T, error) {
	if gi.cIvfFlat == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	centers := make([]T, nLists*gi.dimension)
	var errmsg *C.char
	C.gpu_ivf_flat_get_centers(gi.cIvfFlat, unsafe.Pointer(&centers[0]), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(centers)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	return centers, nil
}

// GetNList retrieves the number of lists (centroids) in the index.
func (gi *GpuIvfFlat[T]) GetNList() uint32 {
	if gi.cIvfFlat == nil {
		return 0
	}
	return uint32(C.gpu_ivf_flat_get_n_list(gi.cIvfFlat))
}

// Extend adds new vectors to an already-built index without rebuilding.
// newIDs may be nil to auto-assign sequential IDs starting from the current index size.
func (gi *GpuIvfFlat[T]) Extend(newData []T, nRows uint64, newIDs []int64) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(newData) == 0 || nRows == 0 {
		return nil
	}

	var idsPtr *C.int64_t
	if len(newIDs) > 0 {
		idsPtr = (*C.int64_t)(unsafe.Pointer(&newIDs[0]))
	}

	var errmsg *C.char
	C.gpu_ivf_flat_extend(
		gi.cIvfFlat,
		unsafe.Pointer(&newData[0]),
		C.uint64_t(nRows),
		idsPtr,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(newData)
	runtime.KeepAlive(newIDs)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// ExtendFloat adds new float32 vectors to an already-built index, quantizing on-the-fly if needed.
// newIDs may be nil to auto-assign sequential IDs starting from the current index size.
func (gi *GpuIvfFlat[T]) ExtendFloat(newData []float32, nRows uint64, newIDs []int64) error {
	if gi.cIvfFlat == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfFlat is not initialized")
	}
	if len(newData) == 0 || nRows == 0 {
		return nil
	}

	var idsPtr *C.int64_t
	if len(newIDs) > 0 {
		idsPtr = (*C.int64_t)(unsafe.Pointer(&newIDs[0]))
	}

	var errmsg *C.char
	C.gpu_ivf_flat_extend_float(
		gi.cIvfFlat,
		(*C.float)(unsafe.Pointer(&newData[0])),
		C.uint64_t(nRows),
		idsPtr,
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(newData)
	runtime.KeepAlive(newIDs)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// SearchResultIvfFlat contains the neighbors and distances from an IVF-Flat search.
type SearchResultIvfFlat struct {
	Neighbors []int64
	Distances []float32
}
