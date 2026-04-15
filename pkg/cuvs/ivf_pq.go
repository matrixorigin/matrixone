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
#include "../../cgo/cuvs/ivf_pq_c.h"
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

// GpuIvfPq represents the C++ gpu_ivf_pq_t object.
type GpuIvfPq[T VectorType] struct {
	cIvfPq      C.gpu_ivf_pq_c
	dimension   uint32
	nthread     uint32
	distMode    DistributionMode
	batchWindowUs int64
}

// SetBatchWindow sets the batching window in microseconds for search operations.
// A window of 0 disables batching; any positive value enables batching with that delay.
func (gi *GpuIvfPq[T]) SetBatchWindow(windowUs int64) error {
	gi.batchWindowUs = windowUs
	if gi.cIvfPq != nil {
		var errmsg *C.char
		C.gpu_ivf_pq_set_batch_window(gi.cIvfPq, C.int64_t(windowUs), unsafe.Pointer(&errmsg))
		if errmsg != nil {
			errStr := C.GoString(errmsg)
			C.free(unsafe.Pointer(errmsg))
			return moerr.NewInternalErrorNoCtx(errStr)
		}
	}
	return nil
}

// NewGpuIvfPq creates a new GpuIvfPq instance from a dataset.
// ids may be nil to use internal sequential IDs (0..count-1).
func NewGpuIvfPq[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode, ids []int64) (*GpuIvfPq[T], error) {
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

	cBP := C.ivf_pq_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		m:                        C.uint32_t(bp.M),
		bits_per_code:            C.uint32_t(bp.BitsPerCode),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfPq := C.gpu_ivf_pq_new(
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

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuIvfPq")
	}

	return &GpuIvfPq[T]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqFromDataFile creates a new GpuIvfPq instance from a MODF datafile.
func NewGpuIvfPqFromDataFile[T VectorType](datafilename string, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cFilename := C.CString(datafilename)
	defer C.free(unsafe.Pointer(cFilename))

	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.ivf_pq_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		m:                        C.uint32_t(bp.M),
		bits_per_code:            C.uint32_t(bp.BitsPerCode),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfPq := C.gpu_ivf_pq_new_from_data_file(
		cFilename,
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

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuIvfPq from data file")
	}

	// dimension will be updated when GetDim() is called, but we can set it to 0 for now
	// or ideally GetDim() should be used.
	return &GpuIvfPq[T]{
		cIvfPq:    cIvfPq,
		dimension: 0,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqEmpty creates a new GpuIvfPq instance with pre-allocated buffer but no data yet.
func NewGpuIvfPqEmpty[T VectorType](totalCount uint64, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	var errmsg *C.char
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.ivf_pq_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		m:                        C.uint32_t(bp.M),
		bits_per_code:            C.uint32_t(bp.BitsPerCode),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfPq := C.gpu_ivf_pq_new_empty(
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

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuIvfPq")
	}

	return &GpuIvfPq[T]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gi *GpuIvfPq[T]) AddChunk(chunk []T, chunkCount uint64, ids []int64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	var cIds *C.int64_t
	if len(ids) > 0 {
		cIds = (*C.int64_t)(unsafe.Pointer(&ids[0]))
	}
	C.gpu_ivf_pq_add_chunk(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []int64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	var cIds *C.int64_t
	if len(ids) > 0 {
		cIds = (*C.int64_t)(unsafe.Pointer(&ids[0]))
	}
	C.gpu_ivf_pq_add_chunk_float(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) TrainQuantizer(trainData []float32, nSamples uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(trainData) == 0 || nSamples == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_pq_train_quantizer(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) SetQuantizer(min, max float32) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	var errmsg *C.char
	C.gpu_ivf_pq_set_quantizer(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) GetQuantizer() (float32, float32, error) {
	if gi.cIvfPq == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	var errmsg *C.char
	var cMin, cMax C.float
	C.gpu_ivf_pq_get_quantizer(
		gi.cIvfPq,
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

// NewGpuIvfPqFromFile creates a new GpuIvfPq instance by loading from a file.
func NewGpuIvfPqFromFile[T VectorType](filename string, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[T], error) {
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

	cBP := C.ivf_pq_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		m:                        C.uint32_t(bp.M),
		bits_per_code:            C.uint32_t(bp.BitsPerCode),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	cIvfPq := C.gpu_ivf_pq_load_file(
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

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to load GpuIvfPq from file")
	}

	return &GpuIvfPq[T]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqFromDataDirectory loads a GpuIvfPq index from a directory written by save_dir.
func NewGpuIvfPqFromDataDirectory[T VectorType](dir string, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[T], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	qtype := GetQuantization[T]()
	cDevices := make([]C.int, len(devices))
	for i, d := range devices {
		cDevices[i] = C.int(d)
	}

	cBP := C.ivf_pq_build_params_t{
		n_lists:                  C.uint32_t(bp.NLists),
		m:                        C.uint32_t(bp.M),
		bits_per_code:            C.uint32_t(bp.BitsPerCode),
		add_data_on_build:        C.bool(bp.AddDataOnBuild),
		kmeans_trainset_fraction: C.double(bp.KmeansTrainsetFraction),
	}

	var errmsg *C.char
	cIvfPq := C.gpu_ivf_pq_new_empty(
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
	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create empty GpuIvfPq for loading")
	}

	C.gpu_ivf_pq_start(cIvfPq, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_ivf_pq_destroy(cIvfPq, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	cDir := C.CString(dir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_ivf_pq_load_dir(cIvfPq, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_ivf_pq_destroy(cIvfPq, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return &GpuIvfPq[T]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// Destroy frees the C++ gpu_ivf_pq_t instance
func (gi *GpuIvfPq[T]) Destroy() error {
	if gi.cIvfPq == nil {
		return nil
	}
	var errmsg *C.char
	C.gpu_ivf_pq_destroy(gi.cIvfPq, unsafe.Pointer(&errmsg))
	gi.cIvfPq = nil
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Start initializes the worker and resources
func (gi *GpuIvfPq[T]) Start() error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	if gi.distMode == Replicated && gi.nthread > 1 {
		var errmsg *C.char
		C.gpu_ivf_pq_set_per_thread_device(gi.cIvfPq, C.bool(true), unsafe.Pointer(&errmsg))
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
	C.gpu_ivf_pq_start(gi.cIvfPq, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Build triggers the build or file loading process
func (gi *GpuIvfPq[T]) Build() error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	C.gpu_ivf_pq_build(gi.cIvfPq, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Save serializes the index to a file
func (gi *GpuIvfPq[T]) Save(filename string) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	cFilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cFilename))

	C.gpu_ivf_pq_save(gi.cIvfPq, cFilename, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Pack saves the index to a .tar or .tar.gz file using save_dir.
func (gi *GpuIvfPq[T]) Pack(filename string) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "ivf-pq-pack-*")
	if err != nil {
		return moerr.NewInternalErrorNoCtx(fmt.Sprintf("failed to create temp dir: %v", err))
	}
	defer os.RemoveAll(tmpDir)

	var errmsg *C.char
	cDir := C.CString(tmpDir)
	defer C.free(unsafe.Pointer(cDir))

	C.gpu_ivf_pq_save_dir(gi.cIvfPq, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}

	return Pack(tmpDir, filename)
}

// Unpack extracts a .tar or .tar.gz file and loads index components via load_dir.
// The index must already be initialized and started before calling Unpack.
func (gi *GpuIvfPq[T]) Unpack(filename string) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	tmpDir, err := os.MkdirTemp("", "ivf-pq-unpack-*")
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

	C.gpu_ivf_pq_load_dir(gi.cIvfPq, cDir, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// DeleteId removes an ID from the index (soft delete).
func (gi *GpuIvfPq[T]) DeleteId(id int64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	C.gpu_ivf_pq_delete_id(gi.cIvfPq, C.int64_t(id), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Search performs a K-Nearest Neighbor search
func (gi *GpuIvfPq[T]) Search(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (SearchResultIvfPq, error) {
	if gi.cIvfPq == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfPq{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	res := C.gpu_ivf_pq_search(
		gi.cIvfPq,
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
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_ivf_pq_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_ivf_pq_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_ivf_pq_free_result(res.result_ptr)

	return SearchResultIvfPq{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// SearchFloat performs an IVF-PQ search operation with float32 queries
func (gi *GpuIvfPq[T]) SearchFloat(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (SearchResultIvfPq, error) {
	if gi.cIvfPq == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfPq{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	res := C.gpu_ivf_pq_search_float(
		gi.cIvfPq,
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
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("search returned nil result")
	}

	totalElements := uint64(numQueries) * uint64(limit)
	neighbors := make([]int64, totalElements)
	distances := make([]float32, totalElements)

	C.gpu_ivf_pq_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_ivf_pq_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_ivf_pq_free_result(res.result_ptr)

	return SearchResultIvfPq{
		Neighbors: neighbors,
		Distances: distances,
	}, nil
}

// SearchAsync performs a K-Nearest Neighbor search asynchronously.
func (gi *GpuIvfPq[T]) SearchAsync(queries []T, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchAsyncWithParams(queries, numQueries, dimension, limit, DefaultIvfPqSearchParams())
}

// SearchAsyncWithParams performs a K-Nearest Neighbor search asynchronously with custom parameters.
func (gi *GpuIvfPq[T]) SearchAsyncWithParams(queries []T, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (uint64, error) {
	if gi.cIvfPq == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	jobID := C.gpu_ivf_pq_search_async(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) SearchFloat32Async(queries []float32, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchFloat32AsyncWithParams(queries, numQueries, dimension, limit, DefaultIvfPqSearchParams())
}

// SearchFloat32AsyncWithParams performs a K-Nearest Neighbor search with float32 queries asynchronously with custom parameters.
func (gi *GpuIvfPq[T]) SearchFloat32AsyncWithParams(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (uint64, error) {
	if gi.cIvfPq == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{
		n_probes: C.uint32_t(sp.NProbes),
	}

	jobID := C.gpu_ivf_pq_search_float_async(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error) {
	if gi.cIvfPq == nil {
		return nil, nil, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	var errmsg *C.char
	res := C.gpu_ivf_pq_search_wait(gi.cIvfPq, C.uint64_t(jobID), unsafe.Pointer(&errmsg))

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

	C.gpu_ivf_pq_get_neighbors(res.result_ptr, C.uint64_t(totalElements), (*C.int64_t)(unsafe.Pointer(&neighbors[0])))
	C.gpu_ivf_pq_get_distances(res.result_ptr, C.uint64_t(totalElements), (*C.float)(unsafe.Pointer(&distances[0])))
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	C.gpu_ivf_pq_free_result(res.result_ptr)

	return neighbors, distances, nil
}

// Cap returns the capacity of the index buffer
func (gi *GpuIvfPq[T]) Cap() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_cap(gi.cIvfPq))
}

// Len returns current number of vectors in index
func (gi *GpuIvfPq[T]) Len() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_len(gi.cIvfPq))
}

// Info returns detailed information about the index as a JSON string.
func (gi *GpuIvfPq[T]) Info() (string, error) {
	if gi.cIvfPq == nil {
		return "", moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	infoPtr := C.gpu_ivf_pq_info(gi.cIvfPq, unsafe.Pointer(&errmsg))
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
func (gi *GpuIvfPq[T]) GetCenters() ([]T, error) {
	if gi.cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	nList := gi.GetNList()
	dim := gi.GetRotDim()
	centers := make([]T, nList*dim)
	var errmsg *C.char
	C.gpu_ivf_pq_get_centers(gi.cIvfPq, unsafe.Pointer(&centers[0]), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(centers)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	return centers, nil
}

// GetNList retrieves the number of lists (centroids) in the index.
func (gi *GpuIvfPq[T]) GetNList() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_n_list(gi.cIvfPq))
}

// GetDim retrieves the dimension of the index.
func (gi *GpuIvfPq[T]) GetDim() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_dim(gi.cIvfPq))
}

// GetRotDim retrieves the rotated dimension of the index.
func (gi *GpuIvfPq[T]) GetRotDim() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_rot_dim(gi.cIvfPq))
}

// GetDimExt retrieves the extended dimension of the index (including norms and padding).
func (gi *GpuIvfPq[T]) GetDimExt() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_dim_ext(gi.cIvfPq))
}

// GetDataset retrieves the flattened host dataset (for debugging).
func (gi *GpuIvfPq[T]) GetDataset(totalElements uint64) []T {
	if gi.cIvfPq == nil {
		return nil
	}
	data := make([]T, totalElements)
	C.gpu_ivf_pq_get_dataset(gi.cIvfPq, unsafe.Pointer(&data[0]))
	return data
}

// Extend adds new vectors to an already-built index without rebuilding.
// newIDs may be nil to auto-assign sequential IDs starting from the current index size.
func (gi *GpuIvfPq[T]) Extend(newData []T, nRows uint64, newIDs []int64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(newData) == 0 || nRows == 0 {
		return nil
	}

	var idsPtr *C.int64_t
	if len(newIDs) > 0 {
		idsPtr = (*C.int64_t)(unsafe.Pointer(&newIDs[0]))
	}

	var errmsg *C.char
	C.gpu_ivf_pq_extend(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) ExtendFloat(newData []float32, nRows uint64, newIDs []int64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(newData) == 0 || nRows == 0 {
		return nil
	}

	var idsPtr *C.int64_t
	if len(newIDs) > 0 {
		idsPtr = (*C.int64_t)(unsafe.Pointer(&newIDs[0]))
	}

	var errmsg *C.char
	C.gpu_ivf_pq_extend_float(
		gi.cIvfPq,
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

// SearchResultIvfPq contains the neighbors and distances from an IVF-PQ search.
type SearchResultIvfPq struct {
	Neighbors []int64
	Distances []float32
}
