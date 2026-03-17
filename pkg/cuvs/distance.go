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
func PairwiseDistance[T VectorType](
	x []T,
	nX uint64,
	y []T,
	nY uint64,
	dim uint32,
	metric DistanceType,
	deviceID int,
) ([]float32, error) {
	if len(x) == 0 || len(y) == 0 {
		return nil, moerr.NewInternalErrorNoCtx("empty x or y")
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
		C.int(deviceID),
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
