package mergesort

import (
	"context"
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

func NewAObjMerger[T any](
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
