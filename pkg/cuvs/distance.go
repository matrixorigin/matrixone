//go:build gpu

/*
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cuvs

/*
#include "../../cgo/cuvs/distance_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"runtime"
	"unsafe"
)

// PairwiseDistance performs a pairwise distance calculation on GPU.
// The GPU device is selected automatically using round-robin across all available devices.
func PairwiseDistance[T VectorType](
	x []T,
	nX uint64,
	y []T,
	nY uint64,
	dim uint32,
	metric DistanceType,
) ([]float32, error) {
	if len(x) == 0 || len(y) == 0 {
		return nil, nil
	}

	qtype := GetQuantization[T]()
	dist := make([]float32, nX*nY)

	var errmsg *C.char
	C.gpu_pairwise_distance(
		unsafe.Pointer(&x[0]),
		C.uint64_t(nX),
		unsafe.Pointer(&y[0]),
		C.uint64_t(nY),
		C.uint32_t(dim),
		C.distance_type_t(metric),
		C.quantization_t(qtype),
		(*C.float)(unsafe.Pointer(&dist[0])),
		unsafe.Pointer(&errmsg),
	)
	runtime.KeepAlive(x)
	runtime.KeepAlive(y)
	runtime.KeepAlive(dist)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return dist, nil
}

// PairwiseDistanceLaunch launches a pairwise distance calculation on GPU asynchronously.
// The GPU device is selected automatically using round-robin across all available devices.
func PairwiseDistanceLaunch[T VectorType](
	x []T,
	nX uint64,
	y []T,
	nY uint64,
	dim uint32,
	metric DistanceType,
	dist []float32,
) (uint64, error) {
	if len(x) == 0 || len(y) == 0 || len(dist) < int(nX*nY) {
		return 0, moerr.NewInternalErrorNoCtx("invalid arguments for PairwiseDistanceLaunch")
	}

	qtype := GetQuantization[T]()

	var errmsg *C.char
	jobID := C.gpu_pairwise_distance_launch(
		unsafe.Pointer(&x[0]),
		C.uint64_t(nX),
		unsafe.Pointer(&y[0]),
		C.uint64_t(nY),
		C.uint32_t(dim),
		C.distance_type_t(metric),
		C.quantization_t(qtype),
		(*C.float)(unsafe.Pointer(&dist[0])),
		unsafe.Pointer(&errmsg),
	)

	// x and y must remain live until PairwiseDistanceWait returns, because the
	// GPU kernel may still be reading host memory after this call returns.
	// The caller is responsible for keeping x and y alive until then.
	// These KeepAlives only protect against GC within this function frame.
	runtime.KeepAlive(x)
	runtime.KeepAlive(y)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return 0, moerr.NewInternalErrorNoCtx(errStr)
	}

	return uint64(jobID), nil
}

// PairwiseDistanceWait waits for a pairwise distance calculation to complete.
func PairwiseDistanceWait(jobID uint64) error {
	var errmsg *C.char
	C.gpu_pairwise_distance_wait(C.uint64_t(jobID), unsafe.Pointer(&errmsg))

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}

	return nil
}
