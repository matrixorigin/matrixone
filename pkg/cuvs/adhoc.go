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
#include "../../cgo/cuvs/adhoc_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"unsafe"
)

// AdhocBruteForceSearch performs an ad-hoc brute-force search on GPU without using a worker thread.
// The GPU device is selected automatically using round-robin across all available devices.
func AdhocBruteForceSearch[T VectorType](
	dataset []T,
	nRows uint64,
	dim uint32,
	queries []T,
	nQueries uint64,
	limit uint32,
	metric DistanceType,
) ([]int64, []float32, error) {
	if len(dataset) == 0 || len(queries) == 0 {
		return nil, nil, nil
	}

	qtype := GetQuantization[T]()

	neighbors := make([]int64, nQueries*uint64(limit))
	distances := make([]float32, nQueries*uint64(limit))

	var errmsg *C.char
	C.gpu_adhoc_brute_force_search(
		unsafe.Pointer(&dataset[0]),
		C.uint64_t(nRows),
		C.uint32_t(dim),
		unsafe.Pointer(&queries[0]),
		C.uint64_t(nQueries),
		C.uint32_t(limit),
		C.distance_type_t(metric),
		C.quantization_t(qtype),
		(*C.int64_t)(unsafe.Pointer(&neighbors[0])),
		(*C.float)(unsafe.Pointer(&distances[0])),
		unsafe.Pointer(&errmsg),
	)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(unsafe.Pointer(errmsg))
		return nil, nil, moerr.NewInternalErrorNoCtx(errStr)
	}

	return neighbors, distances, nil
}
