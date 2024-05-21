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
	Merge(context.Context) ([]*batch.Batch, func(), []uint32, error)
}

type aObjMerger[T any] struct {
	heap *heapSlice[T]

	cols  []*vector.Vector
	nulls []*nulls.Nulls

	// get i-th data from vector.
	// this function wraps vector.GetFixedAt or vector.UnsafeGetStringAt
	// this enables Merge struct handling fixed and varlen vectors.
	getData func(*vector.Vector, int) T

	resultBlkCnt int

	bats      []*containers.Batch
	rowIdx    []int64
	accRowCnt []int64

	mapping   []uint32
	rowPerBlk uint32
	vpool     DisposableVecPool
}

func MergeAObj(
	ctx context.Context,
	vpool DisposableVecPool,
	batches []*containers.Batch,
	sortKeyPos int,
	rowPerBlk uint32,
	resultBlkCnt int) ([]*batch.Batch, func(), []uint32, error) {
	var merger AObjMerger
	typ := batches[0].Vecs[sortKeyPos].GetType()
	if typ.IsVarlen() {
		merger = newAObjMerger(vpool, batches, sort.GenericLess[string], sortKeyPos, vector.UnsafeGetStringAt, rowPerBlk, resultBlkCnt)
	} else {
		switch typ.Oid {
		case types.T_bool:
			merger = newAObjMerger(vpool, batches, sort.BoolLess, sortKeyPos, vector.GetFixedAt[bool], rowPerBlk, resultBlkCnt)
		case types.T_bit:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, vector.GetFixedAt[uint64], rowPerBlk, resultBlkCnt)
		case types.T_int8:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int8], sortKeyPos, vector.GetFixedAt[int8], rowPerBlk, resultBlkCnt)
		case types.T_int16:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int16], sortKeyPos, vector.GetFixedAt[int16], rowPerBlk, resultBlkCnt)
		case types.T_int32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int32], sortKeyPos, vector.GetFixedAt[int32], rowPerBlk, resultBlkCnt)
		case types.T_int64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int64], sortKeyPos, vector.GetFixedAt[int64], rowPerBlk, resultBlkCnt)
		case types.T_float32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float32], sortKeyPos, vector.GetFixedAt[float32], rowPerBlk, resultBlkCnt)
		case types.T_float64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float64], sortKeyPos, vector.GetFixedAt[float64], rowPerBlk, resultBlkCnt)
		case types.T_uint8:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint8], sortKeyPos, vector.GetFixedAt[uint8], rowPerBlk, resultBlkCnt)
		case types.T_uint16:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint16], sortKeyPos, vector.GetFixedAt[uint16], rowPerBlk, resultBlkCnt)
		case types.T_uint32:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint32], sortKeyPos, vector.GetFixedAt[uint32], rowPerBlk, resultBlkCnt)
		case types.T_uint64:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, vector.GetFixedAt[uint64], rowPerBlk, resultBlkCnt)
		case types.T_date:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Date], sortKeyPos, vector.GetFixedAt[types.Date], rowPerBlk, resultBlkCnt)
		case types.T_timestamp:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Timestamp], sortKeyPos, vector.GetFixedAt[types.Timestamp], rowPerBlk, resultBlkCnt)
		case types.T_datetime:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Datetime], sortKeyPos, vector.GetFixedAt[types.Datetime], rowPerBlk, resultBlkCnt)
		case types.T_time:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Time], sortKeyPos, vector.GetFixedAt[types.Time], rowPerBlk, resultBlkCnt)
		case types.T_enum:
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Enum], sortKeyPos, vector.GetFixedAt[types.Enum], rowPerBlk, resultBlkCnt)
		case types.T_decimal64:
			merger = newAObjMerger(vpool, batches, sort.Decimal64Less, sortKeyPos, vector.GetFixedAt[types.Decimal64], rowPerBlk, resultBlkCnt)
		case types.T_decimal128:
			merger = newAObjMerger(vpool, batches, sort.Decimal128Less, sortKeyPos, vector.GetFixedAt[types.Decimal128], rowPerBlk, resultBlkCnt)
		case types.T_uuid:
			merger = newAObjMerger(vpool, batches, sort.UuidLess, sortKeyPos, vector.GetFixedAt[types.Uuid], rowPerBlk, resultBlkCnt)
		case types.T_TS:
			merger = newAObjMerger(vpool, batches, sort.TsLess, sortKeyPos, vector.GetFixedAt[types.TS], rowPerBlk, resultBlkCnt)
		case types.T_Rowid:
			merger = newAObjMerger(vpool, batches, sort.RowidLess, sortKeyPos, vector.GetFixedAt[types.Rowid], rowPerBlk, resultBlkCnt)
		case types.T_Blockid:
			merger = newAObjMerger(vpool, batches, sort.BlockidLess, sortKeyPos, vector.GetFixedAt[types.Blockid], rowPerBlk, resultBlkCnt)
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
	getFunc func(*vector.Vector, int) T,
	rowPerBlk uint32,
	resultBlkCnt int) AObjMerger {
	size := len(batches)
	m := &aObjMerger[T]{
		vpool:        vpool,
		heap:         newHeapSlice[T](size, lessFunc),
		cols:         make([]*vector.Vector, size),
		nulls:        make([]*nulls.Nulls, size),
		rowIdx:       make([]int64, size),
		accRowCnt:    make([]int64, size),
		bats:         batches,
		resultBlkCnt: resultBlkCnt,
		rowPerBlk:    rowPerBlk,
		getData:      getFunc,
	}

	totalRowCnt := 0
	for i, blk := range batches {
		sortKeyCol := blk.Vecs[sortKeyPos].GetDownstreamVector()
		m.cols[i] = sortKeyCol
		m.nulls[i] = sortKeyCol.GetNulls()
		m.rowIdx[i] = 0
		m.accRowCnt[i] = int64(totalRowCnt)
		totalRowCnt += m.cols[i].Length()
	}
	m.mapping = make([]uint32, totalRowCnt)

	return m
}

func (am *aObjMerger[T]) Merge(ctx context.Context) ([]*batch.Batch, func(), []uint32, error) {
	for i := 0; i < len(am.bats); i++ {
		heapPush(am.heap, heapElem[T]{
			data:   am.getData(am.cols[i], 0),
			isNull: am.nulls[i].Contains(0),
			src:    uint32(i),
		})
	}

	cnBat := containers.ToCNBatch(am.bats[0])
	batches := make([]*batch.Batch, am.resultBlkCnt)
	releaseFs := make([]func(), am.resultBlkCnt)

	blkCnt := 0
	bufferRowCnt := 0
	k := uint32(0)
	for am.heap.Len() != 0 {
		select {
		case <-ctx.Done():
			return nil, nil, nil, ctx.Err()
		default:
		}
		blkIdx := am.nextPos()
		rowIdx := am.rowIdx[blkIdx]
		if batches[blkCnt] == nil {
			batches[blkCnt], releaseFs[blkCnt] = getSimilarBatch(cnBat, int(am.rowPerBlk), am.vpool)
		}
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
		if bufferRowCnt == int(am.rowPerBlk) {
			bufferRowCnt = 0
			blkCnt++
		}

		am.pushNewElem(blkIdx)
	}
	return batches, func() {
		for _, f := range releaseFs {
			f()
		}
	}, am.mapping, nil
}

func (am *aObjMerger[T]) nextPos() uint32 {
	return heapPop[T](am.heap).src
}

func (am *aObjMerger[T]) pushNewElem(blkIdx uint32) bool {
	am.rowIdx[blkIdx]++
	if am.rowIdx[blkIdx] >= int64(am.cols[blkIdx].Length()) {
		return false
	}
	nextRow := am.rowIdx[blkIdx]
	heapPush(am.heap, heapElem[T]{
		data:   am.getData(am.cols[blkIdx], int(nextRow)),
		isNull: am.nulls[blkIdx].Contains(uint64(nextRow)),
		src:    blkIdx,
	})
	return true
}
