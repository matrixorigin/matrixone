package mergesort

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
)

type Merger interface {
	Merge()
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

	objBlkCnts       []int
	totalBlkCnt      int
	accObjBlkCnts    []int
	loadedObjBlkCnts []int

	host MergeTaskHost

	writer *blockio.BlockWriter

	sortKeyIdx int

	mustColFunc func(*vector.Vector) []T

	rowPerBlk uint32
	blkPerObj uint16
}

func newMerger[T any](host MergeTaskHost, lessFunc lessFunc[T], sortKeyPos int, mustColFunc func(*vector.Vector) []T) Merger {
	size := host.GetObjectCnt()
	m := &merger[T]{
		host:       host,
		bats:       make([]releasableBatch, size),
		rowIdx:     make([]int64, size),
		cols:       make([][]T, size),
		deletes:    make([]*nulls.Nulls, size),
		nulls:      make([]*nulls.Nulls, size),
		heap:       newHeapSlice[T](size, lessFunc),
		sortKeyIdx: sortKeyPos,

		accObjBlkCnts:    host.GetAccBlkCnts(),
		objBlkCnts:       host.GetBlkCnts(),
		loadedObjBlkCnts: make([]int, size),
		mustColFunc:      mustColFunc,
	}
	m.rowPerBlk, m.blkPerObj = host.GetObjLayout()
	for _, cnt := range m.objBlkCnts {
		m.totalBlkCnt += cnt
	}

	initTransferMapping(host.GetCommitEntry(), m.totalBlkCnt)

	return m
}

func (m *merger[T]) Merge() {
	for i := 0; i < m.host.GetObjectCnt(); i++ {
		if ok := m.loadBlk(uint32(i)); !ok {
			return
		}

		heapPush(m.heap, heapElem[T]{
			data:   m.cols[i][m.rowIdx[i]],
			isNull: m.nulls[i].Contains(uint64(m.rowIdx[i])),
			src:    uint32(i),
		})
	}

	m.buffer = batch.New(false, m.bats[0].bat.Attrs)
	rfs := make([]func(), 0, len(m.bats[0].bat.Vecs))
	for i := range m.bats[0].bat.Vecs {
		var fs func()
		m.buffer.Vecs[i], fs = m.host.GetVector(m.bats[0].bat.Vecs[i].GetType())
		rfs = append(rfs, fs)
	}
	releaseF := func() {
		for _, f := range rfs {
			f()
		}
	}
	defer releaseF()

	objCnt := 0
	blkCnt := 0
	bufferRowCnt := 0
	commitEntry := m.host.GetCommitEntry()
	for m.heap.Len() != 0 {
		objIdx := m.nextPos()
		if m.deletes[objIdx].Contains(uint64(m.rowIdx[objIdx])) {
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
			if blkCnt == int(m.blkPerObj) {
				m.syncObject(context.TODO())
				// reset writer after sync
				blkCnt = 0
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
		m.syncObject(context.TODO())
	}

	return
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
