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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/sort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
)

type Merger interface {
	merge(context.Context) error
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
	blkPerObj     uint16
	rowSize       uint32
	targetObjSize uint32
}

func newMerger[T any](host MergeTaskHost, lessFunc sort.LessFunc[T], sortKeyPos int, mustColFunc func(*vector.Vector) []T) Merger {
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
		blkPerObj:        host.GetObjectMaxBlocks(),
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
	if host.DoTransfer() {
		initTransferMapping(host.GetCommitEntry(), totalBlkCnt)
	}

	return m
}

func (m *merger[T]) merge(ctx context.Context) error {
	for i := 0; i < m.objCnt; i++ {
		if ok, err := m.loadBlk(ctx, uint32(i)); !ok {
			return errors.Join(moerr.NewInternalError(ctx, "failed to load first blk"), err)
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
	objBlkCnt := 0
	bufferRowCnt := 0
	objRowCnt := uint32(0)
	mergedRowCnt := uint32(0)
	commitEntry := m.host.GetCommitEntry()
	for m.heap.Len() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		objIdx := m.nextPos()
		if m.deletes[objIdx].Contains(uint64(m.rowIdx[objIdx])) {
			// row is deleted
			if err := m.pushNewElem(ctx, objIdx); err != nil {
				return err
			}
			continue
		}
		rowIdx := m.rowIdx[objIdx]
		for i := range m.buffer.Vecs {
			err := m.buffer.Vecs[i].UnionOne(m.bats[objIdx].bat.Vecs[i], rowIdx, m.host.GetMPool())
			if err != nil {
				return err
			}
		}

		if m.host.DoTransfer() {
			commitEntry.Booking.Mappings[m.accObjBlkCnts[objIdx]+m.loadedObjBlkCnts[objIdx]-1].M[int32(rowIdx)] = api.TransDestPos{
				ObjIdx: int32(objCnt),
				BlkIdx: int32(uint32(objBlkCnt)),
				RowIdx: int32(bufferRowCnt),
			}
		}

		bufferRowCnt++
		objRowCnt++
		mergedRowCnt++
		// write new block
		if bufferRowCnt == int(m.rowPerBlk) {
			bufferRowCnt = 0
			objBlkCnt++

			if m.writer == nil {
				m.writer = m.host.PrepareNewWriter()
			}

			if _, err := m.writer.WriteBatch(m.buffer); err != nil {
				return err
			}
			// force clean
			m.buffer.CleanOnlyData()

			// write new object
			if m.needNewObject(objBlkCnt, objRowCnt, mergedRowCnt) {
				// write object and reset writer
				if err := m.syncObject(ctx); err != nil {
					return err
				}
				// reset writer after sync
				objBlkCnt = 0
				objRowCnt = 0
				objCnt++
			}
		}

		if err := m.pushNewElem(ctx, objIdx); err != nil {
			return err
		}
	}

	// write remain data
	if bufferRowCnt > 0 {
		objBlkCnt++

		if m.writer == nil {
			m.writer = m.host.PrepareNewWriter()
		}
		if _, err := m.writer.WriteBatch(m.buffer); err != nil {
			return err
		}
		m.buffer.CleanOnlyData()
	}
	if objBlkCnt > 0 {
		if err := m.syncObject(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *merger[T]) needNewObject(objBlkCnt int, objRowCnt, mergedRowCnt uint32) bool {
	if m.targetObjSize == 0 {
		if m.blkPerObj == 0 {
			return objBlkCnt == int(options.DefaultBlocksPerObject)
		}
		return objBlkCnt == int(m.blkPerObj)
	}

	if objRowCnt*m.rowSize > m.targetObjSize {
		return (m.totalRowCnt-mergedRowCnt)*m.rowSize > m.targetObjSize
	}
	return false
}

func (m *merger[T]) nextPos() uint32 {
	return heapPop[T](m.heap).src
}

func (m *merger[T]) loadBlk(ctx context.Context, objIdx uint32) (bool, error) {
	nextBatch, del, releaseF, err := m.host.LoadNextBatch(ctx, objIdx)
	if m.bats[objIdx].bat != nil {
		m.bats[objIdx].releaseF()
	}
	if err != nil {
		if errors.Is(err, ErrNoMoreBlocks) {
			return false, nil
		}
		return false, err
	}

	m.bats[objIdx] = releasableBatch{bat: nextBatch, releaseF: releaseF}
	m.loadedObjBlkCnts[objIdx]++

	vec := nextBatch.GetVector(int32(m.sortKeyIdx))
	m.cols[objIdx] = m.mustColFunc(vec)
	m.nulls[objIdx] = vec.GetNulls()
	m.deletes[objIdx] = del
	m.rowIdx[objIdx] = 0
	return true, nil
}

func (m *merger[T]) pushNewElem(ctx context.Context, objIdx uint32) error {
	m.rowIdx[objIdx]++
	if m.rowIdx[objIdx] >= int64(len(m.cols[objIdx])) {
		if ok, err := m.loadBlk(ctx, objIdx); !ok {
			return err
		}
	}
	nextRow := m.rowIdx[objIdx]
	heapPush(m.heap, heapElem[T]{
		data:   m.cols[objIdx][nextRow],
		isNull: m.nulls[objIdx].Contains(uint64(nextRow)),
		src:    objIdx,
	})
	return nil
}

func (m *merger[T]) syncObject(ctx context.Context) error {
	if _, _, err := m.writer.Sync(ctx); err != nil {
		return err
	}
	cobjstats := m.writer.GetObjectStats()[:objectio.SchemaTombstone]
	commitEntry := m.host.GetCommitEntry()
	for _, cobj := range cobjstats {
		commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobj.Clone().Marshal())
	}
	m.writer = nil
	return nil
}

func mergeObjs(ctx context.Context, mergeHost MergeTaskHost, sortKeyPos int) error {
	var merger Merger
	typ := mergeHost.GetSortKeyType()
	if typ.IsVarlen() {
		merger = newMerger(mergeHost, sort.GenericLess[string], sortKeyPos, vector.MustStrCol)
	} else {
		switch typ.Oid {
		case types.T_bool:
			merger = newMerger(mergeHost, sort.BoolLess, sortKeyPos, vector.MustFixedCol[bool])
		case types.T_bit:
			merger = newMerger(mergeHost, sort.GenericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64])
		case types.T_int8:
			merger = newMerger(mergeHost, sort.GenericLess[int8], sortKeyPos, vector.MustFixedCol[int8])
		case types.T_int16:
			merger = newMerger(mergeHost, sort.GenericLess[int16], sortKeyPos, vector.MustFixedCol[int16])
		case types.T_int32:
			merger = newMerger(mergeHost, sort.GenericLess[int32], sortKeyPos, vector.MustFixedCol[int32])
		case types.T_int64:
			merger = newMerger(mergeHost, sort.GenericLess[int64], sortKeyPos, vector.MustFixedCol[int64])
		case types.T_float32:
			merger = newMerger(mergeHost, sort.GenericLess[float32], sortKeyPos, vector.MustFixedCol[float32])
		case types.T_float64:
			merger = newMerger(mergeHost, sort.GenericLess[float64], sortKeyPos, vector.MustFixedCol[float64])
		case types.T_uint8:
			merger = newMerger(mergeHost, sort.GenericLess[uint8], sortKeyPos, vector.MustFixedCol[uint8])
		case types.T_uint16:
			merger = newMerger(mergeHost, sort.GenericLess[uint16], sortKeyPos, vector.MustFixedCol[uint16])
		case types.T_uint32:
			merger = newMerger(mergeHost, sort.GenericLess[uint32], sortKeyPos, vector.MustFixedCol[uint32])
		case types.T_uint64:
			merger = newMerger(mergeHost, sort.GenericLess[uint64], sortKeyPos, vector.MustFixedCol[uint64])
		case types.T_date:
			merger = newMerger(mergeHost, sort.GenericLess[types.Date], sortKeyPos, vector.MustFixedCol[types.Date])
		case types.T_timestamp:
			merger = newMerger(mergeHost, sort.GenericLess[types.Timestamp], sortKeyPos, vector.MustFixedCol[types.Timestamp])
		case types.T_datetime:
			merger = newMerger(mergeHost, sort.GenericLess[types.Datetime], sortKeyPos, vector.MustFixedCol[types.Datetime])
		case types.T_time:
			merger = newMerger(mergeHost, sort.GenericLess[types.Time], sortKeyPos, vector.MustFixedCol[types.Time])
		case types.T_enum:
			merger = newMerger(mergeHost, sort.GenericLess[types.Enum], sortKeyPos, vector.MustFixedCol[types.Enum])
		case types.T_decimal64:
			merger = newMerger(mergeHost, sort.Decimal64Less, sortKeyPos, vector.MustFixedCol[types.Decimal64])
		case types.T_decimal128:
			merger = newMerger(mergeHost, sort.Decimal128Less, sortKeyPos, vector.MustFixedCol[types.Decimal128])
		case types.T_uuid:
			merger = newMerger(mergeHost, sort.UuidLess, sortKeyPos, vector.MustFixedCol[types.Uuid])
		case types.T_TS:
			merger = newMerger(mergeHost, sort.TsLess, sortKeyPos, vector.MustFixedCol[types.TS])
		case types.T_Rowid:
			merger = newMerger(mergeHost, sort.RowidLess, sortKeyPos, vector.MustFixedCol[types.Rowid])
		case types.T_Blockid:
			merger = newMerger(mergeHost, sort.BlockidLess, sortKeyPos, vector.MustFixedCol[types.Blockid])
		default:
			return moerr.NewErrUnsupportedDataType(ctx, typ)
		}
	}
	return merger.merge(ctx)
}
