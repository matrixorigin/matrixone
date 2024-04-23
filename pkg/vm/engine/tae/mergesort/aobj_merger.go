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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type AObjMerger interface {
	Merge(context.Context) ([]*batch.Batch, func(), []uint32)
}

type aObjMerger[T any] struct {
	heap *heapSlice[T]

	cols        [][]T
	nulls       []*nulls.Nulls
	sortKeyType *types.Type

	resultBlkCnt int

	bats      []*containers.Batch
	rowIdx    []int64
	accRowCnt []int64

	mapping   []uint32
	rowPerBlk uint32
	vpool     DisposableVecPool
}

func MergeAObj(vpool DisposableVecPool,
	batches []*containers.Batch,
	sortKeyPos int,
	rowPerBlk uint32,
	resultBlkCnt int) ([]*batch.Batch, func(), []uint32) {
	var merger AObjMerger
	typ := batches[0].Vecs[sortKeyPos].GetType()
	if typ.IsVarlen() {
		merger = newAObjMerger(vpool, batches, NumericLess[string], sortKeyPos, vector.MustStrCol, rowPerBlk, resultBlkCnt)
	} else {
		switch typ.Oid {
		case types.T_bool:
			merger = newAObjMerger(vpool, batches, BoolLess, sortKeyPos, vector.MustFixedCol[bool], rowPerBlk, resultBlkCnt)
		case types.T_bit:
			merger = newAObjMerger(vpool, batches, NumericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], rowPerBlk, resultBlkCnt)
		case types.T_int8:
			merger = newAObjMerger(vpool, batches, NumericLess[int8], sortKeyPos, vector.MustFixedCol[int8], rowPerBlk, resultBlkCnt)
		case types.T_int16:
			merger = newAObjMerger(vpool, batches, NumericLess[int16], sortKeyPos, vector.MustFixedCol[int16], rowPerBlk, resultBlkCnt)
		case types.T_int32:
			merger = newAObjMerger(vpool, batches, NumericLess[int32], sortKeyPos, vector.MustFixedCol[int32], rowPerBlk, resultBlkCnt)
		case types.T_int64:
			merger = newAObjMerger(vpool, batches, NumericLess[int64], sortKeyPos, vector.MustFixedCol[int64], rowPerBlk, resultBlkCnt)
		case types.T_float32:
			merger = newAObjMerger(vpool, batches, NumericLess[float32], sortKeyPos, vector.MustFixedCol[float32], rowPerBlk, resultBlkCnt)
		case types.T_float64:
			merger = newAObjMerger(vpool, batches, NumericLess[float64], sortKeyPos, vector.MustFixedCol[float64], rowPerBlk, resultBlkCnt)
		case types.T_uint8:
			merger = newAObjMerger(vpool, batches, NumericLess[uint8], sortKeyPos, vector.MustFixedCol[uint8], rowPerBlk, resultBlkCnt)
		case types.T_uint16:
			merger = newAObjMerger(vpool, batches, NumericLess[uint16], sortKeyPos, vector.MustFixedCol[uint16], rowPerBlk, resultBlkCnt)
		case types.T_uint32:
			merger = newAObjMerger(vpool, batches, NumericLess[uint32], sortKeyPos, vector.MustFixedCol[uint32], rowPerBlk, resultBlkCnt)
		case types.T_uint64:
			merger = newAObjMerger(vpool, batches, NumericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64], rowPerBlk, resultBlkCnt)
		case types.T_date:
			merger = newAObjMerger(vpool, batches, NumericLess[types.Date], sortKeyPos, vector.MustFixedCol[types.Date], rowPerBlk, resultBlkCnt)
		case types.T_timestamp:
			merger = newAObjMerger(vpool, batches, NumericLess[types.Timestamp], sortKeyPos, vector.MustFixedCol[types.Timestamp], rowPerBlk, resultBlkCnt)
		case types.T_datetime:
			merger = newAObjMerger(vpool, batches, NumericLess[types.Datetime], sortKeyPos, vector.MustFixedCol[types.Datetime], rowPerBlk, resultBlkCnt)
		case types.T_time:
			merger = newAObjMerger(vpool, batches, NumericLess[types.Time], sortKeyPos, vector.MustFixedCol[types.Time], rowPerBlk, resultBlkCnt)
		case types.T_enum:
			merger = newAObjMerger(vpool, batches, NumericLess[types.Enum], sortKeyPos, vector.MustFixedCol[types.Enum], rowPerBlk, resultBlkCnt)
		case types.T_decimal64:
			merger = newAObjMerger(vpool, batches, LtTypeLess[types.Decimal64], sortKeyPos, vector.MustFixedCol[types.Decimal64], rowPerBlk, resultBlkCnt)
		case types.T_decimal128:
			merger = newAObjMerger(vpool, batches, LtTypeLess[types.Decimal128], sortKeyPos, vector.MustFixedCol[types.Decimal128], rowPerBlk, resultBlkCnt)
		case types.T_uuid:
			merger = newAObjMerger(vpool, batches, LtTypeLess[types.Uuid], sortKeyPos, vector.MustFixedCol[types.Uuid], rowPerBlk, resultBlkCnt)
		case types.T_TS:
			merger = newAObjMerger(vpool, batches, TsLess, sortKeyPos, vector.MustFixedCol[types.TS], rowPerBlk, resultBlkCnt)
		case types.T_Rowid:
			merger = newAObjMerger(vpool, batches, RowidLess, sortKeyPos, vector.MustFixedCol[types.Rowid], rowPerBlk, resultBlkCnt)
		case types.T_Blockid:
			merger = newAObjMerger(vpool, batches, BlockidLess, sortKeyPos, vector.MustFixedCol[types.Blockid], rowPerBlk, resultBlkCnt)
		default:
			panic(fmt.Sprintf("unsupported type %s", typ.String()))
		}
	}
	return merger.Merge(context.Background())
}

func newAObjMerger[T any](
	vpool DisposableVecPool,
	batches []*containers.Batch,
	lessFunc lessFunc[T],
	sortKeyPos int,
	mustColFunc func(*vector.Vector) []T,
	rowPerBlk uint32,
	resultBlkCnt int) AObjMerger {
	size := len(batches)
	m := &aObjMerger[T]{
		vpool:        vpool,
		heap:         newHeapSlice[T](size, lessFunc),
		cols:         make([][]T, size),
		nulls:        make([]*nulls.Nulls, size),
		rowIdx:       make([]int64, size),
		accRowCnt:    make([]int64, size),
		bats:         batches,
		resultBlkCnt: resultBlkCnt,
		rowPerBlk:    rowPerBlk,
	}

	totalRowCnt := 0
	for i, blk := range batches {
		sortKeyCol := blk.Vecs[sortKeyPos].GetDownstreamVector()
		m.sortKeyType = sortKeyCol.GetType()
		m.cols[i] = mustColFunc(sortKeyCol)
		m.nulls[i] = sortKeyCol.GetNulls()
		m.rowIdx[i] = 0
		m.accRowCnt[i] = int64(totalRowCnt)
		totalRowCnt += len(m.cols[i])
	}
	m.mapping = make([]uint32, totalRowCnt)

	return m
}

func (am *aObjMerger[T]) Merge(ctx context.Context) ([]*batch.Batch, func(), []uint32) {
	for i := 0; i < len(am.bats); i++ {
		heapPush(am.heap, heapElem[T]{
			data:   am.cols[i][0],
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
			logutil.Errorf("merge task canceled")
			return nil, nil, nil
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
				panic(err)
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
	}, am.mapping
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
