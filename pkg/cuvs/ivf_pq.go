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
type GpuIvfPq[B, Q VectorType] struct {
	cIvfPq                   C.gpu_ivf_pq_c
	dimension                uint32
	nthread                  uint32
	distMode                 DistributionMode
	batchWindowUs            int64
	dynbConservativeDispatch bool
}

// SetBatchWindow sets the batching window in microseconds for search operations.
// A window of 0 disables batching; any positive value enables batching with that delay.
func (gi *GpuIvfPq[B, Q]) SetBatchWindow(windowUs int64) error {
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

// SetDynbConservativeDispatch sets the cuVS dynamic_batching conservative_dispatch
// flag. false (default): dispatch eagerly at the full batch size. true: wait for
// the batch to fill or the window to elapse, then dispatch at the real size.
// Has no effect unless the batch window is > 0.
func (gi *GpuIvfPq[B, Q]) SetDynbConservativeDispatch(enable bool) error {
	gi.dynbConservativeDispatch = enable
	if gi.cIvfPq != nil {
		var errmsg *C.char
		C.gpu_ivf_pq_set_dynb_conservative_dispatch(gi.cIvfPq, C.bool(enable), unsafe.Pointer(&errmsg))
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
func NewGpuIvfPq[B, Q VectorType](dataset []Q, count uint64, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode, ids []int64) (*GpuIvfPq[B, Q], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	btype := GetQuantization[B]()
	qtype := GetQuantization[Q]()
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
		C.quantization_t(btype),
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

	return &GpuIvfPq[B, Q]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqFromDataFile creates a new GpuIvfPq instance from a MODF datafile.
func NewGpuIvfPqFromDataFile[B, Q VectorType](datafilename string, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[B, Q], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	btype := GetQuantization[B]()
	qtype := GetQuantization[Q]()
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
		C.quantization_t(btype),
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
	return &GpuIvfPq[B, Q]{
		cIvfPq:    cIvfPq,
		dimension: 0,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqEmpty creates a new GpuIvfPq instance with pre-allocated buffer but no data yet.
func NewGpuIvfPqEmpty[B, Q VectorType](totalCount uint64, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[B, Q], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	btype := GetQuantization[B]()
	qtype := GetQuantization[Q]()
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
		C.quantization_t(btype),
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

	return &GpuIvfPq[B, Q]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gi *GpuIvfPq[B, Q]) AddChunk(chunk []Q, chunkCount uint64, ids []int64) error {
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
func (gi *GpuIvfPq[B, Q]) AddChunkFloat(chunk []float32, chunkCount uint64, ids []int64) error {
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

// AddChunkQuantize adds a chunk of base-typed (B) data, quantizing natively to
// the storage type Q (int8/uint8) via the B-source quantizer. base_data is the
// raw bytes of chunkCount*dim B-typed elements. No f32 detour.
func (gi *GpuIvfPq[B, Q]) AddChunkQuantize(chunk []B, chunkCount uint64, ids []int64) error {
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
	C.gpu_ivf_pq_add_chunk_quantize(
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

// QuantizeQuery quantizes a base-typed (B) query to the storage type Q
// (int8/uint8) via the B-source quantizer, writing numQueries*dimension values
// into out. The caller then runs the normal native Search([]Q).
func (gi *GpuIvfPq[B, Q]) QuantizeQuery(queries []B, numQueries uint64, out []Q) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_pq_quantize_query(
		gi.cIvfPq,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		unsafe.Pointer(&out[0]),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(queries)
	runtime.KeepAlive(out)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// TrainQuantizer trains the scalar quantizer (if Q is 1-byte) from base-typed
// (B) training data.
func (gi *GpuIvfPq[B, Q]) TrainQuantizer(trainData []B, nSamples uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(trainData) == 0 || nSamples == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_pq_train_quantizer(
		gi.cIvfPq,
		unsafe.Pointer(&trainData[0]),
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
func (gi *GpuIvfPq[B, Q]) SetQuantizer(min, max float32) error {
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
func (gi *GpuIvfPq[B, Q]) GetQuantizer() (float32, float32, error) {
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
func NewGpuIvfPqFromFile[B, Q VectorType](filename string, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[B, Q], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	btype := GetQuantization[B]()
	qtype := GetQuantization[Q]()
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
		C.quantization_t(btype),
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

	return &GpuIvfPq[B, Q]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// NewGpuIvfPqFromDataDirectory loads a GpuIvfPq index from a directory written by save_dir.
// For Sharded loads we peek manifest.json to learn the saved shard count and
// truncate `devices` to that count, so the C++ worker only spawns threads /
// RMM pools on devices that will actually host a shard.
func NewGpuIvfPqFromDataDirectory[B, Q VectorType](dir string, dimension uint32, metric DistanceType,
	bp IvfPqBuildParams, devices []int, nthread uint32, mode DistributionMode) (*GpuIvfPq[B, Q], error) {
	if len(devices) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("at least one device must be specified")
	}

	var err error
	devices, err = devicesForLoad(devices, mode, dir)
	if err != nil {
		return nil, err
	}

	btype := GetQuantization[B]()
	qtype := GetQuantization[Q]()
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
		C.quantization_t(btype),
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

	C.gpu_ivf_pq_load_dir(cIvfPq, cDir, C.distribution_mode_t(mode), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		C.gpu_ivf_pq_destroy(cIvfPq, nil)
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return &GpuIvfPq[B, Q]{
		cIvfPq:    cIvfPq,
		dimension: dimension,
		nthread:   nthread,
		distMode:  mode,
	}, nil
}

// Destroy frees the C++ gpu_ivf_pq_t instance
func (gi *GpuIvfPq[B, Q]) Destroy() error {
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
func (gi *GpuIvfPq[B, Q]) Start() error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}

	if gi.batchWindowUs > 0 {
		if err := gi.SetBatchWindow(gi.batchWindowUs); err != nil {
			return err
		}
	}

	if gi.dynbConservativeDispatch {
		if err := gi.SetDynbConservativeDispatch(true); err != nil {
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
func (gi *GpuIvfPq[B, Q]) Build() error {
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
func (gi *GpuIvfPq[B, Q]) Save(filename string) error {
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
func (gi *GpuIvfPq[B, Q]) Pack(filename string) error {
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
// mode overrides the distribution mode at load time — pass Replicated to broadcast
// a SINGLE_GPU .tar to all GPUs without rebuilding.
// The index must already be initialized and started before calling Unpack.
func (gi *GpuIvfPq[B, Q]) Unpack(filename string, mode DistributionMode) error {
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

	C.gpu_ivf_pq_load_dir(gi.cIvfPq, cDir, C.distribution_mode_t(mode), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// DeleteId removes an ID from the index (soft delete).
func (gi *GpuIvfPq[B, Q]) DeleteId(id int64) error {
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

// DeleteIds applies DeleteId in a loop. See cagra.GpuCagra.DeleteIds for
// the rationale.
func (gi *GpuIvfPq[B, Q]) DeleteIds(ids []int64) error {
	for _, id := range ids {
		if err := gi.DeleteId(id); err != nil {
			return err
		}
	}
	return nil
}

// Search performs a K-Nearest Neighbor search
func (gi *GpuIvfPq[B, Q]) Search(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (SearchResultIvfPq, error) {
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
func (gi *GpuIvfPq[B, Q]) SearchFloat(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (SearchResultIvfPq, error) {
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
func (gi *GpuIvfPq[B, Q]) SearchAsync(queries []Q, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchAsyncWithParams(queries, numQueries, dimension, limit, DefaultIvfPqSearchParams())
}

// SearchAsyncWithParams performs a K-Nearest Neighbor search asynchronously with custom parameters.
func (gi *GpuIvfPq[B, Q]) SearchAsyncWithParams(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (uint64, error) {
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
func (gi *GpuIvfPq[B, Q]) SearchFloat32Async(queries []float32, numQueries uint64, dimension uint32, limit uint32) (uint64, error) {
	return gi.SearchFloat32AsyncWithParams(queries, numQueries, dimension, limit, DefaultIvfPqSearchParams())
}

// SearchFloat32AsyncWithParams performs a K-Nearest Neighbor search with float32 queries asynchronously with custom parameters.
func (gi *GpuIvfPq[B, Q]) SearchFloat32AsyncWithParams(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams) (uint64, error) {
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
func (gi *GpuIvfPq[B, Q]) SearchWait(jobID uint64, numQueries uint64, limit uint32) ([]int64, []float32, error) {
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
func (gi *GpuIvfPq[B, Q]) Cap() uint64 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint64(C.gpu_ivf_pq_cap(gi.cIvfPq))
}

// Len returns current number of vectors in index
func (gi *GpuIvfPq[B, Q]) Len() uint64 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint64(C.gpu_ivf_pq_len(gi.cIvfPq))
}

// GetFilterColMetaJSON returns the INCLUDE-column metadata of the loaded
// index as a JSON string ready to be re-fed into SetFilterColumns. Returns
// "" for indexes that were built without INCLUDE columns.
func (gi *GpuIvfPq[B, Q]) GetFilterColMetaJSON() string {
	if gi.cIvfPq == nil {
		return ""
	}
	var errmsg *C.char
	jsonPtr := C.gpu_ivf_pq_get_filter_col_meta_json(gi.cIvfPq, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		C.free(unsafe.Pointer(errmsg))
	}
	if jsonPtr == nil {
		return ""
	}
	out := C.GoString(jsonPtr)
	C.free(unsafe.Pointer(jsonPtr))
	return out
}

// Info returns detailed information about the index as a JSON string.
func (gi *GpuIvfPq[B, Q]) Info() (string, error) {
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
func (gi *GpuIvfPq[B, Q]) GetCenters() ([]Q, error) {
	if gi.cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	nList := gi.GetNList()
	dim := gi.GetRotDim()
	centers := make([]Q, nList*dim)
	var errmsg *C.char
	C.gpu_ivf_pq_get_centers(gi.cIvfPq, unsafe.Pointer(&centers[0]), C.uint64_t(len(centers)), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(centers)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	return centers, nil
}

// GetNList retrieves the number of lists (centroids) in the index.
func (gi *GpuIvfPq[B, Q]) GetNList() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_n_list(gi.cIvfPq))
}

// GetDim retrieves the dimension of the index.
func (gi *GpuIvfPq[B, Q]) GetDim() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_dim(gi.cIvfPq))
}

// GetRotDim retrieves the rotated dimension of the index.
func (gi *GpuIvfPq[B, Q]) GetRotDim() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_rot_dim(gi.cIvfPq))
}

// GetDimExt retrieves the extended dimension of the index (including norms and padding).
func (gi *GpuIvfPq[B, Q]) GetDimExt() uint32 {
	if gi.cIvfPq == nil {
		return 0
	}
	return uint32(C.gpu_ivf_pq_get_dim_ext(gi.cIvfPq))
}

// GetDataset retrieves the flattened host dataset (for debugging).
func (gi *GpuIvfPq[B, Q]) GetDataset(totalElements uint64) []Q {
	if gi.cIvfPq == nil {
		return nil
	}
	data := make([]Q, totalElements)
	C.gpu_ivf_pq_get_dataset(gi.cIvfPq, unsafe.Pointer(&data[0]))
	return data
}

// Extend adds new vectors to an already-built index without rebuilding.
// newIDs may be nil to auto-assign sequential IDs starting from the current index size.
func (gi *GpuIvfPq[B, Q]) Extend(newData []Q, nRows uint64, newIDs []int64) error {
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
func (gi *GpuIvfPq[B, Q]) ExtendFloat(newData []float32, nRows uint64, newIDs []int64) error {
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

// SetFilterColumns registers filter-column metadata. See GpuCagra.SetFilterColumns.
func (gi *GpuIvfPq[B, Q]) SetFilterColumns(colMetaJSON string, totalCount uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	cMeta := C.CString(colMetaJSON)
	defer C.free(unsafe.Pointer(cMeta))
	C.gpu_ivf_pq_set_filter_columns(gi.cIvfPq, cMeta, C.uint64_t(totalCount), unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// AddFilterChunk appends raw filter-column bytes. See GpuCagra.AddFilterChunk.
func (gi *GpuIvfPq[B, Q]) AddFilterChunk(colIdx uint32, data []byte, nullBitmap []uint32, nrows uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(data) == 0 || nrows == 0 {
		return nil
	}
	var errmsg *C.char
	var cNullBitmap *C.uint32_t
	if len(nullBitmap) > 0 {
		cNullBitmap = (*C.uint32_t)(unsafe.Pointer(&nullBitmap[0]))
	}
	C.gpu_ivf_pq_add_filter_chunk(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[B, Q]) SearchWithFilter(queries []Q, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams, predsJSON string) (SearchResultIvfPq, error) {
	if gi.cIvfPq == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfPq{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{n_probes: C.uint32_t(sp.NProbes)}
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	res := C.gpu_ivf_pq_search_with_filter(
		gi.cIvfPq,
		unsafe.Pointer(&queries[0]),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		cPreds,
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

	return SearchResultIvfPq{Neighbors: neighbors, Distances: distances}, nil
}

// SearchFloatWithFilter runs a filtered K-NN search with float32 queries.
func (gi *GpuIvfPq[B, Q]) SearchFloatWithFilter(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams, predsJSON string) (SearchResultIvfPq, error) {
	if gi.cIvfPq == nil {
		return SearchResultIvfPq{}, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return SearchResultIvfPq{}, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{n_probes: C.uint32_t(sp.NProbes)}
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	res := C.gpu_ivf_pq_search_float_with_filter(
		gi.cIvfPq,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
		cPreds,
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

	return SearchResultIvfPq{Neighbors: neighbors, Distances: distances}, nil
}

// SearchFloatWithFilterAsync submits a filtered float32 K-NN search and
// returns a job_id; collect the result with SearchWait. Mirrors
// SearchFloat32AsyncWithParams + the predicate-eval semantics of
// SearchFloatWithFilter. Used by MultiGpuIvfPq to dispatch per-shard
// filtered searches in parallel.
func (gi *GpuIvfPq[B, Q]) SearchFloatWithFilterAsync(queries []float32, numQueries uint64, dimension uint32, limit uint32, sp IvfPqSearchParams, predsJSON string) (uint64, error) {
	if gi.cIvfPq == nil {
		return 0, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(queries) == 0 || numQueries == 0 {
		return 0, nil
	}

	var errmsg *C.char
	cSP := C.ivf_pq_search_params_t{n_probes: C.uint32_t(sp.NProbes)}
	cPreds := C.CString(predsJSON)
	defer C.free(unsafe.Pointer(cPreds))

	jobID := C.gpu_ivf_pq_search_float_with_filter_async(
		gi.cIvfPq,
		(*C.float)(unsafe.Pointer(&queries[0])),
		C.uint64_t(numQueries),
		C.uint32_t(dimension),
		C.uint32_t(limit),
		cSP,
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
