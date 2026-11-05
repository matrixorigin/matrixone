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
	"runtime"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/util"
)

// AdhocBruteForceSearchInto performs an ad-hoc brute-force search and writes results into
// caller-provided slices (no internal allocation).
// neighbors and distances must be pre-allocated to at least nQueries*limit elements.
func AdhocBruteForceSearchInto[T VectorType](
	dataset []T,
	nRows uint64,
	dim uint32,
	queries []T,
	nQueries uint64,
	limit uint32,
	metric DistanceType,
	neighbors []int64,
	distances []float32,
) error {
	qtype := GetQuantization[T]()

	var errmsg *C.char
	C.gpu_adhoc_brute_force_search(
		util.UnsafePointer(&dataset[0]),
		C.uint64_t(nRows),
		C.uint32_t(dim),
		util.UnsafePointer(&queries[0]),
		C.uint64_t(nQueries),
		C.uint32_t(limit),
		C.distance_type_t(metric),
		C.quantization_t(qtype),
		(*C.int64_t)(util.UnsafePointer(&neighbors[0])),
		(*C.float)(util.UnsafePointer(&distances[0])),
		util.UnsafePointer(&errmsg),
	)
	runtime.KeepAlive(dataset)
	runtime.KeepAlive(queries)
	runtime.KeepAlive(neighbors)
	runtime.KeepAlive(distances)

	if errmsg != nil {
		errStr := C.GoString(errmsg)
		C.free(util.UnsafePointer(errmsg))
		return moerr.NewInternalErrorNoCtx(errStr)
	}
	return nil
}

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

	neighbors := make([]int64, nQueries*uint64(limit))
	distances := make([]float32, nQueries*uint64(limit))
	if err := AdhocBruteForceSearchInto(dataset, nRows, dim, queries, nQueries, limit, metric, neighbors, distances); err != nil {
		return nil, nil, err
	}
	return neighbors, distances, nil
}
