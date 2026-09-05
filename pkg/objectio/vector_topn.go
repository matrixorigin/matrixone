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
	"math"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

const maxVectorTopLimit = uint64(^uint(0) >> 1)

// vectorTopResult keeps the row paired with its distance. ordinal preserves
// the input-row order expected by the block materializer after heap operations.
type vectorTopResult struct {
	row      int64
	distance float64
	ordinal  int
}

// vectorTopResultHeap is a max heap. Keeping only its smallest K entries makes
// the per-block candidate state bounded even when every successive distance
// displaces the previous global heap maximum.
type vectorTopResultHeap []vectorTopResult

func (h vectorTopResultHeap) Len() int { return len(h) }

func (h vectorTopResultHeap) Less(i, j int) bool {
	if h[i].distance == h[j].distance {
		return h[i].ordinal > h[j].ordinal
	}
	return h[i].distance > h[j].distance
}

func (h vectorTopResultHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *vectorTopResultHeap) Push(x any) {
	*h = append(*h, x.(vectorTopResult))
}

func (h *vectorTopResultHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func retainVectorTopResult(results *vectorTopResultHeap, limit int, result vectorTopResult) {
	if results.Len() < limit {
		heap.Push(results, result)
		return
	}
	if result.distance < (*results)[0].distance {
		(*results)[0] = result
		heap.Fix(results, 0)
	}
}

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

	topLimit, err := vectorTopLimit(ctx, orderByLimit.Limit)
	if err != nil {
		return nil, nil, err
	}
	// Keep row/distance pairs in a second bounded heap. DistHeap contains only
	// distances because it is shared by block reads; recording every accepted
	// candidate here would grow to O(N) for descending input.
	searchResults := make(vectorTopResultHeap, 0, min(len(selectRows), topLimit))
	nullsBm := vecCol.GetNulls()
	rangeActive := orderByLimit.LowerBoundType != plan.BoundType_UNBOUNDED ||
		orderByLimit.UpperBoundType != plan.BoundType_UNBOUNDED
	if orderByLimit.LowerBoundType != plan.BoundType_UNBOUNDED && math.IsNaN(orderByLimit.LowerBound) ||
		orderByLimit.UpperBoundType != plan.BoundType_UNBOUNDED && math.IsNaN(orderByLimit.UpperBound) {
		orderByLimit.DistHeap = orderByLimit.DistHeap[:0]
		return []int64{}, []float64{}, nil
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

	for ordinal, row := range selectRows {
		// Do not compact selectRows in place: callers may still need the physical
		// row mapping after TopN returns.
		if row < 0 || row >= int64(vecCol.Length()) || nullsBm.Contains(uint64(row)) {
			continue
		}
		dist64, err := distOf(vecCol.GetBytesAt(int(row)))
		if err != nil {
			return nil, nil, err
		}
		if rangeActive && math.IsNaN(dist64) {
			continue
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

		retainVectorTopResult(&searchResults, topLimit, vectorTopResult{
			row: row, distance: dist64, ordinal: ordinal,
		})
	}

	if len(orderByLimit.DistHeap) == 0 {
		return []int64{}, []float64{}, nil
	}
	cutoff := orderByLimit.DistHeap[0]
	searchResults = slices.DeleteFunc(searchResults, func(res vectorTopResult) bool {
		return res.distance > cutoff
	})
	slices.SortFunc(searchResults, func(left, right vectorTopResult) int {
		return left.ordinal - right.ordinal
	})

	sels := make([]int64, len(searchResults))
	dists := make([]float64, len(searchResults))
	for i, res := range searchResults {
		sels[i] = res.row
		dists[i] = res.distance
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
