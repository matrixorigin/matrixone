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
)

type Merger interface {
	merge(context.Context) error
}

type releasableBatch struct {
	bat      *batch.Batch
	releaseF func()
}

type fixedDataFetcher[T any] struct {
	mustColFunc func(*vector.Vector) []T
	cols        [][]T
}

func (f *fixedDataFetcher[T]) mustToCol(v *vector.Vector, i uint32) {
	f.cols[i] = f.mustColFunc(v)
}

func (f *fixedDataFetcher[T]) length(i uint32) int {
	return len(f.cols[i])
}

func (f *fixedDataFetcher[T]) at(i, j uint32) T {
	return f.cols[i][j]
}

type varlenaDataFetcher struct {
	cols []struct {
		data []types.Varlena
		area []byte
	}
}

func (f *varlenaDataFetcher) mustToCol(v *vector.Vector, i uint32) {
	data, area := vector.MustVarlenaRawData(v)
	f.cols[i] = struct {
		data []types.Varlena
		area []byte
	}{data: data, area: area}
}

func (f *varlenaDataFetcher) at(i, j uint32) string {
	return f.cols[i].data[j].UnsafeGetString(f.cols[i].area)
}

func (f *varlenaDataFetcher) length(i uint32) int {
	return len(f.cols[i].data)
}

type merger[T comparable] struct {
	heap *heapSlice[T]

	df      dataFetcher[T]
	deletes []*nulls.Nulls
	nulls   []*nulls.Nulls

	buffer *batch.Batch

	bats   []releasableBatch
	rowIdx []uint32

	objCnt           int
	objBlkCnts       []int
	accObjBlkCnts    []int
	loadedObjBlkCnts []int

	host MergeTaskHost

	writer *blockio.BlockWriter

	sortKeyIdx int

	isTombstone bool
	rowPerBlk   uint32
	stats       mergeStats
}

func newMerger[T comparable](host MergeTaskHost, lessFunc sort.LessFunc[T], sortKeyPos int, isTombstone bool, df dataFetcher[T]) Merger {
	size := host.GetObjectCnt()
	rowSizeU64 := host.GetTotalSize() / uint64(host.GetTotalRowCnt())
	m := &merger[T]{
		host:   host,
		objCnt: size,

		df:         df,
		bats:       make([]releasableBatch, size),
		rowIdx:     make([]uint32, size),
		deletes:    make([]*nulls.Nulls, size),
		nulls:      make([]*nulls.Nulls, size),
		heap:       newHeapSlice(size, lessFunc),
		sortKeyIdx: sortKeyPos,

		accObjBlkCnts: host.GetAccBlkCnts(),
		objBlkCnts:    host.GetBlkCnts(),
		rowPerBlk:     host.GetBlockMaxRows(),
		stats: mergeStats{
			totalRowCnt:   host.GetTotalRowCnt(),
			rowSize:       uint32(rowSizeU64),
			targetObjSize: host.GetTargetObjSize(),
			blkPerObj:     host.GetObjectMaxBlocks(),
		},
		loadedObjBlkCnts: make([]int, size),
		isTombstone:      isTombstone,
	}
	totalBlkCnt := 0
	for _, cnt := range m.objBlkCnts {
		totalBlkCnt += cnt
	}
	if host.DoTransfer() {
		host.InitTransferMaps(totalBlkCnt)
	}

	return m
}

func (m *merger[T]) merge(ctx context.Context) error {
	for i := 0; i < m.objCnt; i++ {
		if ok, err := m.loadBlk(ctx, uint32(i)); !ok {
			if err == nil {
				continue
			}
			return errors.Join(moerr.NewInternalError(ctx, "failed to load first blk"), err)
		}

		heapPush(m.heap, heapElem[T]{
			data:   m.df.at(uint32(i), 0),
			isNull: m.nulls[i].Contains(0),
			src:    uint32(i),
		})
	}
	defer m.release()

	var releaseF func()
	for i := 0; i < m.objCnt; i++ {
		if m.bats[i].bat != nil {
			m.buffer, releaseF = getSimilarBatch(m.bats[i].bat, int(m.rowPerBlk), m.host)
			break
		}
	}
	// all batches are empty.
	if m.buffer == nil {
		return nil
	}
	defer releaseF()

	transferMaps := m.host.GetTransferMaps()
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
			err := m.buffer.Vecs[i].UnionOne(m.bats[objIdx].bat.Vecs[i], int64(rowIdx), m.host.GetMPool())
			if err != nil {
				return err
			}
		}

		if m.host.DoTransfer() {
			transferMaps[m.accObjBlkCnts[objIdx]+m.loadedObjBlkCnts[objIdx]-1][rowIdx] = api.TransferDestPos{
				ObjIdx: uint8(m.stats.objCnt),
				BlkIdx: uint16(m.stats.objBlkCnt),
				RowIdx: uint32(m.stats.blkRowCnt),
			}
		}

		m.stats.blkRowCnt++
		m.stats.objRowCnt++
		m.stats.mergedRowCnt++
		// write new block
		if m.stats.blkRowCnt == int(m.rowPerBlk) {
			m.stats.blkRowCnt = 0
			m.stats.objBlkCnt++

			if m.writer == nil {
				m.writer = m.host.PrepareNewWriter()
			}
			if m.isTombstone {
				m.writer.SetDataType(objectio.SchemaTombstone)
				if _, err := m.writer.WriteBatch(m.buffer); err != nil {
					return err
				}
			} else {
				m.writer.SetDataType(objectio.SchemaData)
				if _, err := m.writer.WriteBatch(m.buffer); err != nil {
					return err
				}
			}
			// force clean
			m.buffer.CleanOnlyData()

			// write new object
			if m.stats.needNewObject() {
				// write object and reset writer
				if err := m.syncObject(ctx); err != nil {
					return err
				}
				// reset writer after sync
				m.stats.objBlkCnt = 0
				m.stats.objRowCnt = 0
				m.stats.objCnt++
			}
		}

		if err := m.pushNewElem(ctx, objIdx); err != nil {
			return err
		}
	}

	// write remain data
	if m.stats.blkRowCnt > 0 {
		m.stats.objBlkCnt++

		if m.writer == nil {
			m.writer = m.host.PrepareNewWriter()
		}
		if m.isTombstone {
			m.writer.SetDataType(objectio.SchemaTombstone)
			if _, err := m.writer.WriteBatch(m.buffer); err != nil {
				return err
			}
		} else {

			if _, err := m.writer.WriteBatch(m.buffer); err != nil {
				return err
			}
		}
		m.buffer.CleanOnlyData()
	}
	if m.stats.objBlkCnt > 0 {
		if err := m.syncObject(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (m *merger[T]) nextPos() uint32 {
	return heapPop[T](m.heap).src
}

func (m *merger[T]) loadBlk(ctx context.Context, objIdx uint32) (bool, error) {
	nextBatch, del, releaseF, err := m.host.LoadNextBatch(ctx, objIdx)
	if m.bats[objIdx].bat != nil {
		m.bats[objIdx].releaseF()
		m.bats[objIdx].releaseF = nil
	}
	if err != nil {
		if errors.Is(err, ErrNoMoreBlocks) {
			return false, nil
		}
		return false, err
	}
	for nextBatch.RowCount() == 0 {
		releaseF()
		nextBatch, del, releaseF, err = m.host.LoadNextBatch(ctx, objIdx)
		if err != nil {
			if errors.Is(err, ErrNoMoreBlocks) {
				return false, nil
			}
			return false, err
		}
	}

	m.bats[objIdx] = releasableBatch{bat: nextBatch, releaseF: releaseF}
	m.loadedObjBlkCnts[objIdx]++

	vec := nextBatch.GetVector(int32(m.sortKeyIdx))
	m.df.mustToCol(vec, objIdx)
	m.nulls[objIdx] = vec.GetNulls()
	m.deletes[objIdx] = del
	m.rowIdx[objIdx] = 0
	return true, nil
}

func (m *merger[T]) pushNewElem(ctx context.Context, objIdx uint32) error {
	m.rowIdx[objIdx]++
	if m.rowIdx[objIdx] >= uint32(m.df.length(objIdx)) {
		if ok, err := m.loadBlk(ctx, objIdx); !ok {
			return err
		}
	}
	nextRow := m.rowIdx[objIdx]
	heapPush(m.heap, heapElem[T]{
		data:   m.df.at(objIdx, nextRow),
		isNull: m.nulls[objIdx].Contains(uint64(nextRow)),
		src:    objIdx,
	})
	return nil
}

func (m *merger[T]) syncObject(ctx context.Context) error {
	if _, _, err := m.writer.Sync(ctx); err != nil {
		return err
	}
	cobjstats := m.writer.GetObjectStats()
	commitEntry := m.host.GetCommitEntry()
	commitEntry.CreatedObjs = append(commitEntry.CreatedObjs, cobjstats.Clone().Marshal())
	m.writer = nil
	return nil
}

func (m *merger[T]) release() {
	for _, bat := range m.bats {
		if bat.releaseF != nil {
			bat.releaseF()
		}
	}
}

func mergeObjs(ctx context.Context, mergeHost MergeTaskHost, sortKeyPos int, isTombstone bool) error {
	var merger Merger
	typ := mergeHost.GetSortKeyType()
	size := mergeHost.GetObjectCnt()
	if typ.IsVarlen() {
		df := &varlenaDataFetcher{
			cols: make([]struct {
				data []types.Varlena
				area []byte
			}, size),
		}
		merger = newMerger(mergeHost, sort.GenericLess[string], sortKeyPos, isTombstone, df)
	} else {
		switch typ.Oid {
		case types.T_bool:
			df := &fixedDataFetcher[bool]{
				mustColFunc: vector.MustFixedColNoTypeCheck[bool],
				cols:        make([][]bool, size),
			}
			merger = newMerger(mergeHost, sort.BoolLess, sortKeyPos, isTombstone, df)
		case types.T_bit:
			df := &fixedDataFetcher[uint64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint64],
				cols:        make([][]uint64, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[uint64], sortKeyPos, isTombstone, df)
		case types.T_int8:
			df := &fixedDataFetcher[int8]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int8],
				cols:        make([][]int8, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[int8], sortKeyPos, isTombstone, df)
		case types.T_int16:
			df := &fixedDataFetcher[int16]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int16],
				cols:        make([][]int16, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[int16], sortKeyPos, isTombstone, df)
		case types.T_int32:
			df := &fixedDataFetcher[int32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int32],
				cols:        make([][]int32, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[int32], sortKeyPos, isTombstone, df)
		case types.T_int64:
			df := &fixedDataFetcher[int64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[int64],
				cols:        make([][]int64, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[int64], sortKeyPos, isTombstone, df)
		case types.T_float32:
			df := &fixedDataFetcher[float32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[float32],
				cols:        make([][]float32, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[float32], sortKeyPos, isTombstone, df)
		case types.T_float64:
			df := &fixedDataFetcher[float64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[float64],
				cols:        make([][]float64, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[float64], sortKeyPos, isTombstone, df)
		case types.T_uint8:
			df := &fixedDataFetcher[uint8]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint8],
				cols:        make([][]uint8, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[uint8], sortKeyPos, isTombstone, df)
		case types.T_uint16:
			df := &fixedDataFetcher[uint16]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint16],
				cols:        make([][]uint16, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[uint16], sortKeyPos, isTombstone, df)
		case types.T_uint32:
			df := &fixedDataFetcher[uint32]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint32],
				cols:        make([][]uint32, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[uint32], sortKeyPos, isTombstone, df)
		case types.T_uint64:
			df := &fixedDataFetcher[uint64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[uint64],
				cols:        make([][]uint64, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[uint64], sortKeyPos, isTombstone, df)
		case types.T_date:
			df := &fixedDataFetcher[types.Date]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Date],
				cols:        make([][]types.Date, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[types.Date], sortKeyPos, isTombstone, df)
		case types.T_timestamp:
			df := &fixedDataFetcher[types.Timestamp]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Timestamp],
				cols:        make([][]types.Timestamp, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[types.Timestamp], sortKeyPos, isTombstone, df)
		case types.T_datetime:
			df := &fixedDataFetcher[types.Datetime]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Datetime],
				cols:        make([][]types.Datetime, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[types.Datetime], sortKeyPos, isTombstone, df)
		case types.T_time:
			df := &fixedDataFetcher[types.Time]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Time],
				cols:        make([][]types.Time, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[types.Time], sortKeyPos, isTombstone, df)
		case types.T_enum:
			df := &fixedDataFetcher[types.Enum]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Enum],
				cols:        make([][]types.Enum, size),
			}
			merger = newMerger(mergeHost, sort.GenericLess[types.Enum], sortKeyPos, isTombstone, df)
		case types.T_decimal64:
			df := &fixedDataFetcher[types.Decimal64]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Decimal64],
				cols:        make([][]types.Decimal64, size),
			}
			merger = newMerger(mergeHost, sort.Decimal64Less, sortKeyPos, isTombstone, df)
		case types.T_decimal128:
			df := &fixedDataFetcher[types.Decimal128]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Decimal128],
				cols:        make([][]types.Decimal128, size),
			}
			merger = newMerger(mergeHost, sort.Decimal128Less, sortKeyPos, isTombstone, df)
		case types.T_uuid:
			df := &fixedDataFetcher[types.Uuid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Uuid],
				cols:        make([][]types.Uuid, size),
			}
			merger = newMerger(mergeHost, sort.UuidLess, sortKeyPos, isTombstone, df)
		case types.T_TS:
			df := &fixedDataFetcher[types.TS]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.TS],
				cols:        make([][]types.TS, size),
			}
			merger = newMerger(mergeHost, sort.TsLess, sortKeyPos, isTombstone, df)
		case types.T_Rowid:
			df := &fixedDataFetcher[types.Rowid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Rowid],
				cols:        make([][]types.Rowid, size),
			}
			merger = newMerger(mergeHost, sort.RowidLess, sortKeyPos, isTombstone, df)
		case types.T_Blockid:
			df := &fixedDataFetcher[types.Blockid]{
				mustColFunc: vector.MustFixedColNoTypeCheck[types.Blockid],
				cols:        make([][]types.Blockid, size),
			}
			merger = newMerger(mergeHost, sort.BlockidLess, sortKeyPos, isTombstone, df)
		default:
			return moerr.NewErrUnsupportedDataType(ctx, typ)
		}
	}
	return merger.merge(ctx)
}
