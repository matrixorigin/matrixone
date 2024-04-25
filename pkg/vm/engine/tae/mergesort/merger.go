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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type Merger interface {
	Merge(context.Context)
}

type releasableBatch struct {
	bat      *batch.Batch
	releaseF func()
}

type merger[T any] struct {
	heap *heapSlice[T]

	cols    [][]T
	deletes []*nulls.Nulls
	nulls   []*nulls.Nulls

	buffer *batch.Batch

	bats   []releasableBatch
	rowIdx []int64

	objCnt           int
	objBlkCnts       []int
	accObjBlkCnts    []int
	loadedObjBlkCnts []int

	host MergeTaskHost

	writer *blockio.BlockWriter

	sortKeyIdx int

	mustColFunc func(*vector.Vector) []T

	totalRowCnt   uint32
	totalSize     uint32
	rowPerBlk     uint32
	rowSize       uint32
	targetObjSize uint32
}

func newMerger[T any](host MergeTaskHost, lessFunc lessFunc[T], sortKeyPos int, mustColFunc func(*vector.Vector) []T) Merger {
	size := host.GetObjectCnt()
	m := &merger[T]{
		host:       host,
		objCnt:     size,
		bats:       make([]releasableBatch, size),
		rowIdx:     make([]int64, size),
		cols:       make([][]T, size),
		deletes:    make([]*nulls.Nulls, size),
		nulls:      make([]*nulls.Nulls, size),
		heap:       newHeapSlice[T](size, lessFunc),
		sortKeyIdx: sortKeyPos,

		accObjBlkCnts:    host.GetAccBlkCnts(),
		objBlkCnts:       host.GetBlkCnts(),
		rowPerBlk:        host.GetBlockMaxRows(),
		targetObjSize:    host.GetTargetObjSize(),
		totalSize:        host.GetTotalSize(),
		totalRowCnt:      host.GetTotalRowCnt(),
		loadedObjBlkCnts: make([]int, size),
		mustColFunc:      mustColFunc,
	}
	m.rowSize = m.totalSize / m.totalRowCnt
	totalBlkCnt := 0
	for _, cnt := range m.objBlkCnts {
		totalBlkCnt += cnt
	}
	initTransferMapping(host.GetCommitEntry(), totalBlkCnt)

	return m
}

func (m *merger[T]) Merge(ctx context.Context) {
	for i := 0; i < m.objCnt; i++ {
		if ok := m.loadBlk(uint32(i)); !ok {
			return
		}

		heapPush(m.heap, heapElem[T]{
			data:   m.cols[i][m.rowIdx[i]],
			isNull: m.nulls[i].Contains(uint64(m.rowIdx[i])),
			src:    uint32(i),
		})
	}

	var releaseF func()
	m.buffer, releaseF = getSimilarBatch(m.bats[0].bat, int(m.rowPerBlk), m.host)
	defer releaseF()

	objCnt := 0
	blkCnt := 0
	bufferRowCnt := 0
	objRowCnt := uint32(0)
	mergedRowCnt := uint32(0)
	commitEntry := m.host.GetCommitEntry()
	for m.heap.Len() != 0 {
		select {
		case <-ctx.Done():
			logutil.Errorf("merge task canceled")
			return
		default:
		}
		objIdx := m.nextPos()
		if m.deletes[objIdx].Contains(uint64(m.rowIdx[objIdx])) {
			// row is deleted
			m.pushNewElem(objIdx)
			continue
		}
		rowIdx := m.rowIdx[objIdx]
		for i := range m.buffer.Vecs {
			err := m.buffer.Vecs[i].UnionOne(m.bats[objIdx].bat.Vecs[i], rowIdx, m.host.GetMPool())
			if err != nil {
				panic(err)
			}
		}

		commitEntry.Booking.Mappings[m.accObjBlkCnts[objIdx]+m.loadedObjBlkCnts[objIdx]-1].M[int32(rowIdx)] = api.TransDestPos{
			ObjIdx: int32(objCnt),
			BlkIdx: int32(uint32(blkCnt)),
			RowIdx: int32(bufferRowCnt),
		}

		bufferRowCnt++
		objRowCnt++
		mergedRowCnt++
		// write new block
		if bufferRowCnt == int(m.rowPerBlk) {
			bufferRowCnt = 0
			blkCnt++

			if m.writer == nil {
				m.writer = m.host.PrepareNewWriter()
			}

			if _, err := m.writer.WriteBatch(m.buffer); err != nil {
				panic(err)
			}
			// force clean
			m.buffer.CleanOnlyData()

			// write new object
			if m.needNewObject(blkCnt, objRowCnt, mergedRowCnt) {
				// write object and reset writer
				m.syncObject(ctx)
				// reset writer after sync
				blkCnt = 0
				objRowCnt = 0
				objCnt++
			}
		}

		m.pushNewElem(objIdx)
	}

	// write remain data
	if bufferRowCnt > 0 {
		blkCnt++

		if m.writer == nil {
			m.writer = m.host.PrepareNewWriter()
		}
		if _, err := m.writer.WriteBatch(m.buffer); err != nil {
			panic(err)
		}
		m.buffer.CleanOnlyData()
	}
	if blkCnt > 0 {
		m.syncObject(ctx)
	}
}

func (m *merger[T]) needNewObject(blkCnt int, objRowCnt, mergedRowCnt uint32) bool {
	if m.targetObjSize == 0 {
		if blkCnt == int(options.DefaultBlocksPerObject) {
			return true
		}
		return false
	}

	if objRowCnt*m.rowSize > m.targetObjSize {
		if (m.totalRowCnt-mergedRowCnt)*m.rowSize > m.targetObjSize {
			return true
		}
	}
	return false
}

func (m *merger[T]) nextPos() uint32 {
	return heapPop[T](m.heap).src
}

func (m *merger[T]) loadBlk(objIdx uint32) bool {
	nextBatch, del, releaseF, err := m.host.LoadNextBatch(objIdx)
	if m.bats[objIdx].bat != nil {
		m.bats[objIdx].releaseF()
	}
	if err != nil {
		if errors.Is(err, ErrNoMoreBlocks) {
			return false
		}
		if m.loadedObjBlkCnts[objIdx] != m.objBlkCnts[objIdx] {
			panic("channel closed unexpectedly")
		}
		return false
	}

	m.bats[objIdx] = releasableBatch{bat: nextBatch, releaseF: releaseF}
	m.loadedObjBlkCnts[objIdx]++

	vec := nextBatch.GetVector(int32(m.sortKeyIdx))
	m.cols[objIdx] = m.mustColFunc(vec)
	m.nulls[objIdx] = vec.GetNulls()
	m.deletes[objIdx] = del
	m.rowIdx[objIdx] = 0
	return true
}

func (m *merger[T]) pushNewElem(objIdx uint32) bool {
	m.rowIdx[objIdx]++
	if m.rowIdx[objIdx] >= int64(len(m.cols[objIdx])) {
		if ok := m.loadBlk(objIdx); !ok {
			return false
		}
	}
	nextRow := m.rowIdx[objIdx]
	heapPush(m.heap, heapElem[T]{
		data:   m.cols[objIdx][nextRow],
		isNull: m.nulls[objIdx].Contains(uint64(nextRow)),
		src:    objIdx,
	})
	return true
}

func (m *merger[T]) syncObject(ctx context.Context) {
	if _, _, err := m.writer.Sync(ctx); err != nil {
		panic(err)
	}
	cobjstats := m.writer.GetObjectStats()[:objectio.SchemaTombstone]
	commitEntry := m.host.GetCommitEntry()
	for _, cobj := range cobjstats {
		commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobj.Clone().Marshal())
	}
	m.writer = nil
}
