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
	"runtime"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// GpuIvfPq represents the C++ gpu_ivf_pq_t object.
type GpuIvfPq[T VectorType] struct {
	cIvfPq    C.gpu_ivf_pq_c
	dimension uint32
}

// NewGpuIvfPq creates a new GpuIvfPq instance from a dataset.
func NewGpuIvfPq[T VectorType](dataset []T, count uint64, dimension uint32, metric DistanceType,
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
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuIvfPq")
	}

	return &GpuIvfPq[T]{cIvfPq: cIvfPq, dimension: dimension}, nil
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
	return &GpuIvfPq[T]{cIvfPq: cIvfPq, dimension: 0}, nil
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
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(cDevices)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuIvfPq")
	}

	return &GpuIvfPq[T]{cIvfPq: cIvfPq, dimension: dimension}, nil
}

// AddChunk adds a chunk of data to the pre-allocated buffer.
func (gi *GpuIvfPq[T]) AddChunk(chunk []T, chunkCount uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_pq_add_chunk(
		gi.cIvfPq,
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
func (gi *GpuIvfPq[T]) AddChunkFloat(chunk []float32, chunkCount uint64) error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	if len(chunk) == 0 || chunkCount == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_ivf_pq_add_chunk_float(
		gi.cIvfPq,
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

	return &GpuIvfPq[T]{cIvfPq: cIvfPq, dimension: dimension}, nil
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
	var errmsg *C.char
	C.gpu_ivf_pq_start(gi.cIvfPq, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Load triggers the build or file loading process
func (gi *GpuIvfPq[T]) Load() error {
	if gi.cIvfPq == nil {
		return moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	var errmsg *C.char
	C.gpu_ivf_pq_load(gi.cIvfPq, unsafe.Pointer(&errmsg))
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

// GetCenters retrieves the trained centroids.
func (gi *GpuIvfPq[T]) GetCenters() ([]float32, error) {
	if gi.cIvfPq == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuIvfPq is not initialized")
	}
	nLists := gi.GetNList()
	dimExt := gi.GetDimExt()
	centers := make([]float32, nLists*dimExt)
	var errmsg *C.char
	C.gpu_ivf_pq_get_centers(gi.cIvfPq, (*C.float)(&centers[0]), unsafe.Pointer(&errmsg))
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

// SearchResultIvfPq contains the neighbors and distances from an IVF-PQ search.
type SearchResultIvfPq struct {
	Neighbors []int64
	Distances []float32
}
