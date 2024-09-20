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

type aObjMerger[T comparable] struct {
	heap *heapSlice[T]

	df    dataFetcher[T]
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
	size := len(batches)
	if typ.IsVarlen() {
		df := &varlenaDataFetcher{
			cols: make([]struct {
				data []types.Varlena
				area []byte
			}, size),
		}
		merger = newAObjMerger(vpool, batches, sort.GenericLess[string], sortKeyPos, df, toLayout)
	} else {
		switch typ.Oid {
		case types.T_bool:
			df := &fixedDataFetcher[bool]{
				mustColFunc: vector.MustFixedColNoTypeCheck[bool],
				cols:        make([][]bool, size),
			}
			merger = newAObjMerger(vpool, batches, sort.BoolLess, sortKeyPos, df, toLayout)
		case types.T_bit:
			df := &fixedDataFetcher[uint64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint64],
				cols:        make([][]uint64, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, df, toLayout)
		case types.T_int8:
			df := &fixedDataFetcher[int8]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int8],
				cols:        make([][]int8, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int8], sortKeyPos, df, toLayout)
		case types.T_int16:
			df := &fixedDataFetcher[int16]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int16],
				cols:        make([][]int16, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int16], sortKeyPos, df, toLayout)
		case types.T_int32:
			df := &fixedDataFetcher[int32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int32],
				cols:        make([][]int32, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int32], sortKeyPos, df, toLayout)
		case types.T_int64:
			df := &fixedDataFetcher[int64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int64],
				cols:        make([][]int64, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[int64], sortKeyPos, df, toLayout)
		case types.T_float32:
			df := &fixedDataFetcher[float32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[float32],
				cols:        make([][]float32, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float32], sortKeyPos, df, toLayout)
		case types.T_float64:
			df := &fixedDataFetcher[float64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[float64],
				cols:        make([][]float64, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[float64], sortKeyPos, df, toLayout)
		case types.T_uint8:
			df := &fixedDataFetcher[uint8]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint8],
				cols:        make([][]uint8, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint8], sortKeyPos, df, toLayout)
		case types.T_uint16:
			df := &fixedDataFetcher[uint16]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint16],
				cols:        make([][]uint16, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint16], sortKeyPos, df, toLayout)
		case types.T_uint32:
			df := &fixedDataFetcher[uint32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint32],
				cols:        make([][]uint32, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint32], sortKeyPos, df, toLayout)
		case types.T_uint64:
			df := &fixedDataFetcher[uint64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint64],
				cols:        make([][]uint64, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[uint64], sortKeyPos, df, toLayout)
		case types.T_date:
			df := &fixedDataFetcher[types.Date]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Date],
				cols:        make([][]types.Date, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Date], sortKeyPos, df, toLayout)
		case types.T_timestamp:
			df := &fixedDataFetcher[types.Timestamp]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Timestamp],
				cols:        make([][]types.Timestamp, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Timestamp], sortKeyPos, df, toLayout)
		case types.T_datetime:
			df := &fixedDataFetcher[types.Datetime]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Datetime],
				cols:        make([][]types.Datetime, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Datetime], sortKeyPos, df, toLayout)
		case types.T_time:
			df := &fixedDataFetcher[types.Time]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Time],
				cols:        make([][]types.Time, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Time], sortKeyPos, df, toLayout)
		case types.T_enum:
			df := &fixedDataFetcher[types.Enum]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Enum],
				cols:        make([][]types.Enum, size),
			}
			merger = newAObjMerger(vpool, batches, sort.GenericLess[types.Enum], sortKeyPos, df, toLayout)
		case types.T_decimal64:
			df := &fixedDataFetcher[types.Decimal64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Decimal64],
				cols:        make([][]types.Decimal64, size),
			}
			merger = newAObjMerger(vpool, batches, sort.Decimal64Less, sortKeyPos, df, toLayout)
		case types.T_decimal128:
			df := &fixedDataFetcher[types.Decimal128]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Decimal128],
				cols:        make([][]types.Decimal128, size),
			}
			merger = newAObjMerger(vpool, batches, sort.Decimal128Less, sortKeyPos, df, toLayout)
		case types.T_uuid:
			df := &fixedDataFetcher[types.Uuid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Uuid],
				cols:        make([][]types.Uuid, size),
			}
			merger = newAObjMerger(vpool, batches, sort.UuidLess, sortKeyPos, df, toLayout)
		case types.T_TS:
			df := &fixedDataFetcher[types.TS]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.TS],
				cols:        make([][]types.TS, size),
			}
			merger = newAObjMerger(vpool, batches, sort.TsLess, sortKeyPos, df, toLayout)
		case types.T_Rowid:
			df := &fixedDataFetcher[types.Rowid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Rowid],
				cols:        make([][]types.Rowid, size),
			}
			merger = newAObjMerger(vpool, batches, sort.RowidLess, sortKeyPos, df, toLayout)
		case types.T_Blockid:
			df := &fixedDataFetcher[types.Blockid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Blockid],
				cols:        make([][]types.Blockid, size),
			}
			merger = newAObjMerger(vpool, batches, sort.BlockidLess, sortKeyPos, df, toLayout)
		default:
			return nil, nil, nil, moerr.NewErrUnsupportedDataType(ctx, typ)
		}
	}
	return merger.Merge(ctx)
}

func newAObjMerger[T comparable](
	vpool DisposableVecPool,
	batches []*containers.Batch,
	lessFunc sort.LessFunc[T],
	sortKeyPos int,
	df dataFetcher[T],
	toLayout []uint32) AObjMerger {
	size := len(batches)
	m := &aObjMerger[T]{
		vpool:     vpool,
		heap:      newHeapSlice[T](size, lessFunc),
		df:        df,
		nulls:     make([]*nulls.Nulls, size),
		rowIdx:    make([]int64, size),
		accRowCnt: make([]int64, size),
		bats:      batches,
		toLayout:  toLayout,
	}

	totalRowCnt := 0
	for i, blk := range batches {
		sortKeyCol := blk.Vecs[sortKeyPos].GetDownstreamVector()
		m.df.mustToCol(sortKeyCol, uint32(i))
		m.nulls[i] = sortKeyCol.GetNulls()
		m.rowIdx[i] = 0
		m.accRowCnt[i] = int64(totalRowCnt)
		totalRowCnt += m.df.length(uint32(i))
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
			data:   am.df.at(uint32(i), 0),
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
	if am.rowIdx[blkIdx] >= int64(am.df.length(blkIdx)) {
		return false
	}
	nextRow := am.rowIdx[blkIdx]
	heapPush(am.heap, heapElem[T]{
		data:   am.df.at(blkIdx, uint32(nextRow)),
		isNull: am.nulls[blkIdx].Contains(uint64(nextRow)),
		src:    blkIdx,
	})
	return true
}
