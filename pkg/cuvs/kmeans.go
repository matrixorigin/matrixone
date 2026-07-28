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
#include "../../cgo/cuvs/kmeans_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"runtime"
	"unsafe"
)

// GpuKMeans represents the C++ gpu_kmeans_t object
type GpuKMeans[T VectorType] struct {
	cKMeans   C.gpu_kmeans_c
	nClusters uint32
	dimension uint32
}

// NewGpuKMeans creates a new GpuKMeans instance
func NewGpuKMeans[T VectorType](nClusters uint32, dimension uint32, metric DistanceType, maxIter int, deviceID int, nthread uint32) (*GpuKMeans[T], error) {
	qtype := GetQuantization[T]()
	var errmsg *C.char
	cKMeans := C.gpu_kmeans_new(
		C.uint32_t(nClusters),
		C.uint32_t(dimension),
		C.distance_type_t(metric),
		C.int(maxIter),
		C.int(deviceID),
		C.uint32_t(nthread),
		C.quantization_t(qtype),
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	if cKMeans == nil {
		return nil, moerr.NewInternalErrorNoCtx("failed to create GpuKMeans")
	}

	return &GpuKMeans[T]{cKMeans: cKMeans, nClusters: nClusters, dimension: dimension}, nil
}

// Destroy frees the C++ gpu_kmeans_t instance
func (gk *GpuKMeans[T]) Destroy() error {
	if gk.cKMeans == nil {
		return nil
	}
	var errmsg *C.char
	C.gpu_kmeans_destroy(gk.cKMeans, unsafe.Pointer(&errmsg))
	gk.cKMeans = nil
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// Start initializes the worker and resources
func (gk *GpuKMeans[T]) Start() error {
	if gk.cKMeans == nil {
		return moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	var errmsg *C.char
	C.gpu_kmeans_start(gk.cKMeans, unsafe.Pointer(&errmsg))
	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

// TrainQuantizer trains the scalar quantizer (if T is 1-byte)
func (gk *GpuKMeans[T]) TrainQuantizer(trainData []float32, nSamples uint64) error {
	if gk.cKMeans == nil {
		return moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(trainData) == 0 || nSamples == 0 {
		return nil
	}

	var errmsg *C.char
	C.gpu_kmeans_train_quantizer(
		gk.cKMeans,
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
func (gk *GpuKMeans[T]) SetQuantizer(min, max float32) error {
	if gk.cKMeans == nil {
		return moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}

	var errmsg *C.char
	C.gpu_kmeans_set_quantizer(
		gk.cKMeans,
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
func (gk *GpuKMeans[T]) GetQuantizer() (float32, float32, error) {
	if gk.cKMeans == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}

	var errmsg *C.char
	var cMin, cMax C.float
	C.gpu_kmeans_get_quantizer(
		gk.cKMeans,
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

// Fit computes the cluster centroids
func (gk *GpuKMeans[T]) Fit(dataset []T, nSamples uint64) (float32, int64, error) {
	if gk.cKMeans == nil {
		return 0, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(dataset) == 0 || nSamples == 0 {
		return 0, 0, nil
	}

	var errmsg *C.char
	res := C.gpu_kmeans_fit(
		gk.cKMeans,
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	return float32(res.inertia), int64(res.n_iter), nil
}

// Predict assigns labels to new data based on existing centroids.
func (gk *GpuKMeans[T]) Predict(dataset []T, nSamples uint64) ([]int64, float32, error) {
	if gk.cKMeans == nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(dataset) == 0 || nSamples == 0 {
		return nil, 0, nil
	}

	var errmsg *C.char
	res := C.gpu_kmeans_predict(
		gk.cKMeans,
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("predict returned nil result")
	}

	labels := make([]int64, nSamples)
	C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
	runtime.KeepAlive(labels)

	C.gpu_kmeans_free_result(res.result_ptr)

	return labels, float32(res.inertia), nil
}

// PredictFloat assigns labels to new float32 data based on existing centroids.
func (gk *GpuKMeans[T]) PredictFloat(dataset []float32, nSamples uint64) ([]int64, float32, error) {
	if gk.cKMeans == nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(dataset) == 0 || nSamples == 0 {
		return nil, 0, nil
	}

	var errmsg *C.char
	res := C.gpu_kmeans_predict_float(
		gk.cKMeans,
		(*C.float)(unsafe.Pointer(&dataset[0])),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return nil, 0, moerr.NewInternalErrorNoCtx("predict returned nil result")
	}

	labels := make([]int64, nSamples)
	C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
	runtime.KeepAlive(labels)

	C.gpu_kmeans_free_result(res.result_ptr)

	return labels, float32(res.inertia), nil
}

// FitPredict performs both fitting and labeling in one step.
func (gk *GpuKMeans[T]) FitPredict(dataset []T, nSamples uint64) ([]int64, float32, int64, error) {
	if gk.cKMeans == nil {
		return nil, 0, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(dataset) == 0 || nSamples == 0 {
		return nil, 0, 0, nil
	}

	var errmsg *C.char
	res := C.gpu_kmeans_fit_predict(
		gk.cKMeans,
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, 0, 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return nil, 0, 0, moerr.NewInternalErrorNoCtx("fit_predict returned nil result")
	}

	labels := make([]int64, nSamples)
	C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
	runtime.KeepAlive(labels)

	C.gpu_kmeans_free_result(res.result_ptr)

	return labels, float32(res.inertia), int64(res.n_iter), nil
}

// FitPredictFloat performs both fitting and labeling in one step for float32 data.
func (gk *GpuKMeans[T]) FitPredictFloat(dataset []float32, nSamples uint64) ([]int64, float32, int64, error) {
	if gk.cKMeans == nil {
		return nil, 0, 0, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	if len(dataset) == 0 || nSamples == 0 {
		return nil, 0, 0, nil
	}

	var errmsg *C.char
	res := C.gpu_kmeans_fit_predict_float(
		gk.cKMeans,
		(*C.float)(unsafe.Pointer(&dataset[0])),
		C.uint64_t(nSamples),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(dataset)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, 0, 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	if res.result_ptr == nil {
		return nil, 0, 0, moerr.NewInternalErrorNoCtx("fit_predict returned nil result")
	}

	labels := make([]int64, nSamples)
	C.gpu_kmeans_get_labels(res.result_ptr, C.uint64_t(nSamples), (*C.int64_t)(unsafe.Pointer(&labels[0])))
	runtime.KeepAlive(labels)

	C.gpu_kmeans_free_result(res.result_ptr)

	return labels, float32(res.inertia), int64(res.n_iter), nil
}

// GetCentroids retrieves the trained centroids.
func (gk *GpuKMeans[T]) GetCentroids() ([]T, error) {
	if gk.cKMeans == nil {
		return nil, moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	centroids := make([]T, gk.nClusters*gk.dimension)
	var errmsg *C.char
	C.gpu_kmeans_get_centroids(gk.cKMeans, unsafe.Pointer(&centroids[0]), unsafe.Pointer(&errmsg))
	runtime.KeepAlive(centroids)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}
	return centroids, nil
}

// Info returns detailed information about the index as a JSON string.
func (gk *GpuKMeans[T]) Info() (string, error) {
	if gk.cKMeans == nil {
		return "", moerr.NewInternalErrorNoCtx("GpuKMeans is not initialized")
	}
	var errmsg *C.char
	infoPtr := C.gpu_kmeans_info(gk.cKMeans, unsafe.Pointer(&errmsg))
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
