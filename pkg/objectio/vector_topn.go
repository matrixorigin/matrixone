// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package objectio

import (
	"container/heap"
	"context"
	"fmt"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const maxVectorTopLimit = uint64(^uint(0) >> 1)

func vectorTopLimit(ctx context.Context, limit uint64) (int, error) {
	if limit == 0 {
		return 0, moerr.NewInternalError(ctx, "vector index top limit must be positive")
	}
	if limit > maxVectorTopLimit {
		return 0, moerr.NewInternalError(ctx, fmt.Sprintf("vector index top limit %d overflows int", limit))
	}
	return int(limit), nil
}

func vectorTopDistOf[T types.ArrayElement](numVec []byte, m metric.MetricType) (func([]byte) (float64, error), error) {
	distFunc, err := metric.ResolveDistanceFn[T, float64](m)
	if err != nil {
		return nil, err
	}
	rhs := types.BytesToArray[T](numVec)
	return func(b []byte) (float64, error) {
		return distFunc(types.BytesToArray[T](b), rhs)
	}, nil
}

// TopNVector computes the vector-index TopN result without taking ownership of
// vecCol. Callers must keep vecCol's backing storage alive for this call.
func TopNVector(
	ctx context.Context,
	selectRows []int64,
	vecCol *vector.Vector,
	orderByLimit *IndexReaderTopOp,
) ([]int64, []float64, error) {
	if vecCol == nil || orderByLimit == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx("nil vector topn input")
	}
	if selectRows == nil {
		selectRows = make([]int64, vecCol.Length())
		for i := range selectRows {
			selectRows[i] = int64(i)
		}
	}

	nullsBm := vecCol.GetNulls()
	selectRows = slices.DeleteFunc(selectRows, func(row int64) bool {
		return row < 0 || row >= int64(vecCol.Length()) || nullsBm.Contains(uint64(row))
	})

	searchResults := make([]vectorindex.SearchResult, 0, len(selectRows))
	topLimit, err := vectorTopLimit(ctx, orderByLimit.Limit)
	if err != nil {
		return nil, nil, err
	}

	var distOf func(colBytes []byte) (float64, error)
	switch orderByLimit.Typ {
	case types.T_array_float32:
		distOf, err = vectorTopDistOf[float32](orderByLimit.NumVec, orderByLimit.MetricType)
	case types.T_array_float64:
		distOf, err = vectorTopDistOf[float64](orderByLimit.NumVec, orderByLimit.MetricType)
	case types.T_array_bf16:
		distOf, err = vectorTopDistOf[types.BF16](orderByLimit.NumVec, orderByLimit.MetricType)
	case types.T_array_float16:
		distOf, err = vectorTopDistOf[types.Float16](orderByLimit.NumVec, orderByLimit.MetricType)
	case types.T_array_int8:
		distOf, err = vectorTopDistOf[int8](orderByLimit.NumVec, orderByLimit.MetricType)
	case types.T_array_uint8:
		distOf, err = vectorTopDistOf[uint8](orderByLimit.NumVec, orderByLimit.MetricType)
	default:
		return nil, nil, moerr.NewInternalError(ctx, fmt.Sprintf(
			"only support float32/float64/bf16/float16/int8/uint8 type for topn: %s",
			orderByLimit.Typ,
		))
	}
	if err != nil {
		return nil, nil, err
	}

	for _, row := range selectRows {
		dist64, err := distOf(vecCol.GetBytesAt(int(row)))
		if err != nil {
			return nil, nil, err
		}

		if orderByLimit.LowerBoundType == plan.BoundType_INCLUSIVE {
			if dist64 < orderByLimit.LowerBound {
				continue
			}
		} else if orderByLimit.LowerBoundType == plan.BoundType_EXCLUSIVE {
			if dist64 <= orderByLimit.LowerBound {
				continue
			}
		}
		if orderByLimit.UpperBoundType == plan.BoundType_INCLUSIVE {
			if dist64 > orderByLimit.UpperBound {
				continue
			}
		} else if orderByLimit.UpperBoundType == plan.BoundType_EXCLUSIVE {
			if dist64 >= orderByLimit.UpperBound {
				continue
			}
		}

		if len(orderByLimit.DistHeap) >= topLimit {
			if dist64 < orderByLimit.DistHeap[0] {
				orderByLimit.DistHeap[0] = dist64
				heap.Fix(&orderByLimit.DistHeap, 0)
			} else {
				continue
			}
		} else {
			heap.Push(&orderByLimit.DistHeap, dist64)
		}

		searchResults = append(searchResults, vectorindex.SearchResult{
			Id:       row,
			Distance: dist64,
		})
	}

	if len(orderByLimit.DistHeap) == 0 {
		return []int64{}, []float64{}, nil
	}
	searchResults = slices.DeleteFunc(searchResults, func(res vectorindex.SearchResult) bool {
		return res.Distance > orderByLimit.DistHeap[0]
	})

	sels := make([]int64, len(searchResults))
	dists := make([]float64, len(searchResults))
	for i, res := range searchResults {
		sels[i] = res.Id
		dists[i] = res.Distance
	}
	return sels, dists, nil
}

// SearchCachedVectorTopN computes TopN while the caller-held IOEntry cache
// lease is pinned. The borrowed Vector and sealed cache backing never escape.
func SearchCachedVectorTopN(
	ctx context.Context,
	entry fileservice.IOEntry,
	selectRows []int64,
	orderByLimit *IndexReaderTopOp,
) ([]int64, []float64, error) {
	var source vector.Vector
	if err := bindCachedVectorForScope(&source, entry.CachedData); err != nil {
		return nil, nil, err
	}
	defer source.Free(nil)
	return TopNVector(ctx, selectRows, &source, orderByLimit)
}
