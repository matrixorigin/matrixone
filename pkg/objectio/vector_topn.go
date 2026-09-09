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

// vectorTopAccumulator owns one column's bounded candidate state. Its distance
// heap remains reader-wide, while result rows/ordinals are block-global.
type vectorTopAccumulator struct {
	order       *IndexReaderTopOp
	limit       int
	distOf      func([]byte) (float64, error)
	rangeActive bool
	emptyRange  bool
	results     vectorTopResultHeap
}

func newVectorTopAccumulator(ctx context.Context, order *IndexReaderTopOp, candidates int) (vectorTopAccumulator, error) {
	a := vectorTopAccumulator{order: order}
	if order == nil {
		return a, moerr.NewInvalidInputNoCtx("nil vector topn input")
	}
	var err error
	a.limit, err = vectorTopLimit(ctx, order.Limit)
	if err != nil {
		return a, err
	}
	a.rangeActive = order.LowerBoundType != plan.BoundType_UNBOUNDED ||
		order.UpperBoundType != plan.BoundType_UNBOUNDED
	if order.LowerBoundType != plan.BoundType_UNBOUNDED && math.IsNaN(order.LowerBound) ||
		order.UpperBoundType != plan.BoundType_UNBOUNDED && math.IsNaN(order.UpperBound) {
		order.DistHeap = order.DistHeap[:0]
		a.emptyRange = true
		return a, nil
	}
	switch order.Typ {
	case types.T_array_float32:
		a.distOf, err = vectorTopDistOf[float32](order.NumVec, order.MetricType)
	case types.T_array_float64:
		a.distOf, err = vectorTopDistOf[float64](order.NumVec, order.MetricType)
	case types.T_array_bf16:
		a.distOf, err = vectorTopDistOf[types.BF16](order.NumVec, order.MetricType)
	case types.T_array_float16:
		a.distOf, err = vectorTopDistOf[types.Float16](order.NumVec, order.MetricType)
	case types.T_array_int8:
		a.distOf, err = vectorTopDistOf[int8](order.NumVec, order.MetricType)
	case types.T_array_uint8:
		a.distOf, err = vectorTopDistOf[uint8](order.NumVec, order.MetricType)
	default:
		err = moerr.NewInternalErrorf(ctx, "only support float32/float64/bf16/float16/int8/uint8 type for topn: %s", order.Typ)
	}
	if err != nil {
		return a, err
	}
	a.results = make(vectorTopResultHeap, 0, min(candidates, a.limit))
	return a, nil
}

// consume borrows one vector. Non-nil selections contain block-global rows;
// ordinalBase is their starting position in the original selection. A nil
// selection visits all local rows without allocating an all-row index slice.
func (a *vectorTopAccumulator) consume(ctx context.Context, vec *vector.Vector, rowBase int64, selected []int64, ordinalBase int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if a.emptyRange {
		return nil
	}
	order := a.order
	count := vec.Length()
	if selected != nil {
		count = len(selected)
	}
	nulls := vec.GetNulls()
	for i := 0; i < count; i++ {
		localRow := int64(i)
		row := rowBase + localRow
		if selected != nil {
			row = selected[i]
			localRow = row - rowBase
		}
		if localRow < 0 || localRow >= int64(vec.Length()) || nulls.Contains(uint64(localRow)) {
			continue
		}
		dist, err := a.distOf(vec.GetBytesAt(int(localRow)))
		if err != nil {
			return err
		}
		if a.rangeActive && math.IsNaN(dist) {
			continue
		}
		if order.LowerBoundType == plan.BoundType_INCLUSIVE {
			if dist < order.LowerBound {
				continue
			}
		} else if order.LowerBoundType == plan.BoundType_EXCLUSIVE {
			if dist <= order.LowerBound {
				continue
			}
		}
		if order.UpperBoundType == plan.BoundType_INCLUSIVE {
			if dist > order.UpperBound {
				continue
			}
		} else if order.UpperBoundType == plan.BoundType_EXCLUSIVE {
			if dist >= order.UpperBound {
				continue
			}
		}
		if len(order.DistHeap) >= a.limit {
			if dist < order.DistHeap[0] {
				order.DistHeap[0] = dist
				heap.Fix(&order.DistHeap, 0)
			} else {
				continue
			}
		} else {
			heap.Push(&order.DistHeap, dist)
		}
		retainVectorTopResult(&a.results, a.limit, vectorTopResult{
			row: row, distance: dist, ordinal: ordinalBase + i,
		})
	}
	return nil
}

func (a *vectorTopAccumulator) finish() ([]int64, []float64, error) {
	if len(a.order.DistHeap) == 0 {
		return []int64{}, []float64{}, nil
	}
	cutoff := a.order.DistHeap[0]
	a.results = slices.DeleteFunc(a.results, func(res vectorTopResult) bool { return res.distance > cutoff })
	slices.SortFunc(a.results, func(left, right vectorTopResult) int { return left.ordinal - right.ordinal })
	rows := make([]int64, len(a.results))
	distances := make([]float64, len(a.results))
	for i, res := range a.results {
		rows[i], distances[i] = res.row, res.distance
	}
	return rows, distances, nil
}

// TopNVector computes the vector-index TopN result without taking ownership of
// vecCol. Callers must keep vecCol's backing storage alive for this call.
func TopNVector(ctx context.Context, selectRows []int64, vecCol *vector.Vector, orderByLimit *IndexReaderTopOp) ([]int64, []float64, error) {
	if vecCol == nil || orderByLimit == nil {
		return nil, nil, moerr.NewInvalidInputNoCtx("nil vector topn input")
	}
	count := vecCol.Length()
	if selectRows != nil {
		count = len(selectRows)
	}
	acc, err := newVectorTopAccumulator(ctx, orderByLimit, count)
	if err != nil {
		return nil, nil, err
	}
	if err = acc.consume(ctx, vecCol, 0, selectRows, 0); err != nil {
		return nil, nil, err
	}
	return acc.finish()
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
