// Copyright 2024 Matrix Origin
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

package mergesort

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type AObjMerger interface {
	Merge(context.Context) ([]*batch.Batch, func(), []int, error)
}

type aObjMerger[T any] struct {
	heap *heapSlice[T]

	cols  [][]T
	nulls []*nulls.Nulls

	bats      []*containers.Batch
	rowIdx    []int64
	accRowCnt []int64

	mapping  []int
	toLayout []uint32
	vpool    DisposableVecPool
}

func MergeAObj(
	ctx context.Context,
	vpool DisposableVecPool,
	batches []*containers.Batch,
	sortKeyPos int,
	toLayout []uint32) ([]*batch.Batch, func(), []int, error) {
	var merger AObjMerger
	typ := batches[0].Vecs[sortKeyPos].GetType()
	if typ.IsVarlen() {
		merger = newAObjMerger(vpool, batches, sort.GenericLess[string], sortKeyPos, vector.InefficientMustStrCol, toLayout)
	} else {
		switch typ.Oid {
		case types.T_bool:
			merger = newAObjMerger(vpool, batches, sort.BoolLess, sortKeyPos, vector.MustFixedCol[bool], toLayout)
		case types.T_bit:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], toLayout)
		case types.T_int8:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int8], sortKeyPos, vector.MustFixedCol[int8], toLayout)
		case types.T_int16:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int16], sortKeyPos, vector.MustFixedCol[int16], toLayout)
		case types.T_int32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int32], sortKeyPos, vector.MustFixedCol[int32], toLayout)
		case types.T_int64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int64], sortKeyPos, vector.MustFixedCol[int64], toLayout)
		case types.T_float32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float32], sortKeyPos, vector.MustFixedCol[float32], toLayout)
		case types.T_float64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float64], sortKeyPos, vector.MustFixedCol[float64], toLayout)
		case types.T_uint8:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint8], sortKeyPos, vector.MustFixedCol[uint8], toLayout)
		case types.T_uint16:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint16], sortKeyPos, vector.MustFixedCol[uint16], toLayout)
		case types.T_uint32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint32], sortKeyPos, vector.MustFixedCol[uint32], toLayout)
		case types.T_uint64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], toLayout)
		case types.T_date:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Date], sortKeyPos, vector.MustFixedCol[types.Date], toLayout)
		case types.T_timestamp:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Timestamp], sortKeyPos, vector.MustFixedCol[types.Timestamp], toLayout)
		case types.T_datetime:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Datetime], sortKeyPos, vector.MustFixedCol[types.Datetime], toLayout)
		case types.T_time:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Time], sortKeyPos, vector.MustFixedCol[types.Time], toLayout)
		case types.T_enum:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Enum], sortKeyPos, vector.MustFixedCol[types.Enum], toLayout)
		case types.T_decimal64:
			merger = newAObjMerger(vpool, batches, sort.Decimal64Less, sortKeyPos, vector.MustFixedCol[types.Decimal64], toLayout)
		case types.T_decimal128:
			merger = newAObjMerger(vpool, batches, sort.Decimal128Less, sortKeyPos, vector.MustFixedCol[types.Decimal128], toLayout)
		case types.T_uuid:
			merger = newAObjMerger(vpool, batches, sort.UuidLess, sortKeyPos, vector.MustFixedCol[types.Uuid], toLayout)
		case types.T_TS:
			merger = newAObjMerger(vpool, batches, sort.TsLess, sortKeyPos, vector.MustFixedCol[types.TS], toLayout)
		case types.T_Rowid:
			merger = newAObjMerger(vpool, batches, sort.RowidLess, sortKeyPos, vector.MustFixedCol[types.Rowid], toLayout)
		case types.T_Blockid:
			merger = newAObjMerger(vpool, batches, sort.BlockidLess, sortKeyPos, vector.MustFixedCol[types.Blockid], toLayout)
		default:
			return nil, nil, nil, moerr.NewErrUnsupportedDataType(ctx, typ)
		}
	}
	return merger.Merge(ctx)
}

func newAObjMerger[T any](
	vpool DisposableVecPool,
	batches []*containers.Batch,
	lessFunc sort.LessFunc[T],
	sortKeyPos int,
	mustColFunc func(*vector.Vector) []T,
	toLayout []uint32) AObjMerger {
	size := len(batches)
	m := &aObjMerger[T]{
		vpool:     vpool,
		heap:      newHeapSlice[T](size, lessFunc),
		cols:      make([][]T, size),
		nulls:     make([]*nulls.Nulls, size),
		rowIdx:    make([]int64, size),
		accRowCnt: make([]int64, size),
		bats:      batches,
		toLayout:  toLayout,
	}

	totalRowCnt := 0
	for i, blk := range batches {
		sortKeyCol := blk.Vecs[sortKeyPos].GetDownstreamVector()
		m.cols[i] = mustColFunc(sortKeyCol)
		m.nulls[i] = sortKeyCol.GetNulls()
		m.rowIdx[i] = 0
		m.accRowCnt[i] = int64(totalRowCnt)
		totalRowCnt += len(m.cols[i])
	}
	m.mapping = make([]int, totalRowCnt)
	for i := range m.mapping {
		m.mapping[i] = -1
	}

	return m
}

func (am *aObjMerger[T]) Merge(ctx context.Context) ([]*batch.Batch, func(), []int, error) {
	for i := 0; i < len(am.bats); i++ {
		heapPush(am.heap, heapElem[T]{
			data:   am.cols[i][0],
			isNull: am.nulls[i].Contains(0),
			src:    uint32(i),
		})
	}

	cnBat := containers.ToCNBatch(am.bats[0])
	batches := make([]*batch.Batch, len(am.toLayout))
	releaseFs := make([]func(), len(am.toLayout))

	blkCnt := 0
	bufferRowCnt := 0
	k := 0
	for am.heap.Len() != 0 {
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		default:
		}
		blkIdx := am.nextPos()
		if am.bats[blkIdx].Deletes.Contains(uint64(am.rowIdx[blkIdx])) {
			// row is deleted
			am.pushNewElem(blkIdx)
			continue
		}
		if batches[blkCnt] == nil {
			batches[blkCnt], releaseFs[blkCnt] = getSimilarBatch(cnBat, int(am.toLayout[blkCnt]), am.vpool)
		}
		rowIdx := am.rowIdx[blkIdx]
		for i := range batches[blkCnt].Vecs {
			err := batches[blkCnt].Vecs[i].UnionOne(am.bats[blkIdx].Vecs[i].GetDownstreamVector(), rowIdx, am.vpool.GetMPool())
			if err != nil {
				return nil, nil, nil, err
			}
		}

		am.mapping[am.accRowCnt[blkIdx]+rowIdx] = k
		k++
		bufferRowCnt++
		// write new block
		if bufferRowCnt == int(am.toLayout[blkCnt]) {
			bufferRowCnt = 0
			blkCnt++
		}

		am.pushNewElem(blkIdx)
	}
	return batches, func() {
		for _, f := range releaseFs {
			if f != nil {
				f()
			}
		}
	}, am.mapping, nil
}

func (am *aObjMerger[T]) nextPos() uint32 {
	return heapPop[T](am.heap).src
}

func (am *aObjMerger[T]) pushNewElem(blkIdx uint32) bool {
	am.rowIdx[blkIdx]++
	if am.rowIdx[blkIdx] >= int64(len(am.cols[blkIdx])) {
		return false
	}
	nextRow := am.rowIdx[blkIdx]
	heapPush(am.heap, heapElem[T]{
		data:   am.cols[blkIdx][nextRow],
		isNull: am.nulls[blkIdx].Contains(uint64(nextRow)),
		src:    blkIdx,
	})
	return true
}
