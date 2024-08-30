// Copyright 2023 Matrix Origin
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

package logtailreplay

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sort"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taeCatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/tidwall/btree"
)

const (
	ChangesHandle_Object uint8 = iota
	ChangesHandle_Row
)

const (
	RowHandle_DataBatchIDX uint8 = iota
	RowHandle_TombstoneBatchIDX
)

type BatchHandle struct {
	maxRow      uint32
	mp          *mpool.MPool
	isTombstone bool

	batches     *batch.Batch
	batchLength int
	dataOffsets int
}

func NewRowHandle(data *batch.Batch, maxRow uint32, mp *mpool.MPool) (handle *BatchHandle, err error) {
	err = sortBatch(data, 1, mp)
	if err != nil {
		return
	}
	handle = &BatchHandle{
		maxRow:  maxRow,
		mp:      mp,
		batches: data,
	}
	if data != nil {
		handle.batchLength = data.Vecs[0].Length()
	}
	return
}

func closeBatch(bat *batch.Batch, mp *mpool.MPool) {
	if bat == nil {
		return
	}
	for _, vec := range bat.Vecs {
		if !vec.NeedDup() {
			vec.Free(mp)
		}
	}
}
func (r *BatchHandle) Close() {
	// for _, bat := range r.batches {
	// 	closeBatch(bat, r.mp)
	// }
}
func (r *BatchHandle) Next() (data *batch.Batch, err error) {
	data, err = r.next()
	if err != nil {
		return
	}
	if data == nil {
		return nil, moerr.GetOkExpectedEOF()
	}
	return
}
func (r *BatchHandle) next() (bat *batch.Batch, err error) {
	src := r.batches
	if src == nil {
		return nil, nil
	}
	start := r.dataOffsets
	batchLength := src.Vecs[0].Length()
	if start == batchLength {
		return nil, nil
	}
	var end int
	if start+int(r.maxRow) > batchLength {
		end = batchLength
	} else {
		end = start + int(r.maxRow)
	}
	r.dataOffsets = end
	bat = batch.NewWithSize(0)
	bat.Attrs = append(bat.Attrs, src.Attrs...)
	for _, vec := range src.Vecs {
		newVec, err := vec.Window(start, end)
		if err != nil {
			return nil, err
		}
		bat.Vecs = append(bat.Vecs, newVec)
	}
	return
}

func readObjects(stats objectio.ObjectStats, blockID uint32, fs fileservice.FileService, isTombstone bool) (bat *batch.Batch, err error) {
	metaType := objectio.SchemaData
	if isTombstone {
		metaType = objectio.SchemaTombstone
	}
	loc := catalog.BuildLocation(stats, uint16(blockID), 8192) //TODO
	bat, _, err = blockio.LoadOneBlock(context.TODO(), fs, loc, metaType)
	return
}

type ObjectHandle struct {
	entry       *ObjectEntry
	blockOffset uint32
	fs          fileservice.FileService
	isTombstone bool
	mp          *mpool.MPool
}

func newObjectHandle(entry *ObjectEntry, fs fileservice.FileService, tombstone bool, mp *mpool.MPool) (h *ObjectHandle) {
	return &ObjectHandle{
		entry:       entry,
		fs:          fs,
		isTombstone: tombstone,
		mp:          mp,
	}
}

func (r *ObjectHandle) Close() {
}
func (r *ObjectHandle) Next() (batch *batch.Batch, err error) {
	if r.blockOffset == r.entry.BlkCnt() {
		return nil, moerr.GetOkExpectedEOF()
	}
	batch, err = readObjects(r.entry.ObjectStats, r.blockOffset, r.fs, r.isTombstone)
	if err != nil {
		return
	}
	r.blockOffset++
	if r.entry.ObjectStats.GetCNCreated() || !r.isTombstone {
		return
	}
	sortBatch(batch, 3, r.mp)
	return
}

type baseHandle struct {
	objectIter  btree.IterG[ObjectEntry]
	rowIter     btree.IterG[RowEntry]
	isTombstone bool

	rowHandle    *BatchHandle
	ObjectHandle *ObjectHandle

	start, end  types.TS
	currentType uint8
	tid         uint64
	maxRow      uint32
	mp          *mpool.MPool
	fs          fileservice.FileService
}

func NewBaseHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32, tombstone bool, fs fileservice.FileService) *baseHandle {
	var iter btree.IterG[ObjectEntry]
	if tombstone {
		iter = state.tombstoneObjects.Copy().Iter()
	} else {
		iter = state.dataObjects.Copy().Iter()
	}
	return &baseHandle{
		objectIter:  iter,
		rowIter:     state.rows.Copy().Iter(),
		start:       start,
		end:         end,
		tid:         state.tid,
		maxRow:      maxRow,
		mp:          mp,
		fs:          fs,
		isTombstone: tombstone,
	}
}
func (p *baseHandle) Close() {
	p.objectIter.Release()
	p.rowIter.Release()
}
func (p *baseHandle) Next() (bat *batch.Batch, err error) {
	for {
		switch p.currentType {
		case ChangesHandle_Object:
			if p.ObjectHandle == nil {
				p.ObjectHandle, err = p.newObjectHandle()
				if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
					p.currentType = ChangesHandle_Row
					continue
				}
			}
			for {
				bat, err = p.ObjectHandle.Next()
				if err == nil {
					return
				}
				if !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
					return
				}
				p.ObjectHandle, err = p.newObjectHandle()
				if err == nil {
					continue
				} else if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
					p.currentType = ChangesHandle_Row
					break
				} else {
					return
				}
			}
		case ChangesHandle_Row:
			if p.rowHandle == nil {
				p.rowHandle, err = p.newBatchHandleWithRowIterator()
				if err != nil {
					return
				}
			}
			bat, err = p.rowHandle.Next()
			return
		default:
			panic(fmt.Sprintf("invalid type %d", p.currentType))
		}
	}
}
func (p *baseHandle) newBatchHandleWithRowIterator() (h *BatchHandle, err error) {
	bat := p.getBatchesFromRowIterator()
	h, err = NewRowHandle(bat, p.maxRow, p.mp)
	if err != nil {
		closeBatch(bat, p.mp)
	}
	return
}
func (r *baseHandle) getBatchesFromRowIterator() (bat *batch.Batch) {
	for r.rowIter.Next() {
		entry := r.rowIter.Item()
		if checkTS(r.start, r.end, entry.Time) {
			if !entry.Deleted && !r.isTombstone {
				fillInInsertBatch(&bat, &entry, r.mp)
			}
			if entry.Deleted && r.isTombstone {
				fillInDeleteBatch(&bat, &entry, r.mp)
			}
		}
	}
	return
}
func (r *baseHandle) newObjectHandle() (h *ObjectHandle, err error) {
	for r.objectIter.Next() {
		entry := r.objectIter.Item()
		if checkObjectEntry(&entry, r.start, r.end) {
			return newObjectHandle(&entry, r.fs, r.isTombstone, r.mp), nil
		}
	}
	return nil, moerr.GetOkExpectedEOF()
}

type ChangeHandler struct {
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
}

func NewChangesHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32, fs fileservice.FileService) *ChangeHandler {
	return &ChangeHandler{
		tombstoneHandle: NewBaseHandler(state, start, end, mp, maxRow, true, fs),
		dataHandle:      NewBaseHandler(state, start, end, mp, maxRow, false, fs),
	}
}

func (p *ChangeHandler) Close() error {
	p.dataHandle.Close()
	p.tombstoneHandle.Close()
	return nil
}
func (p *ChangeHandler) Next() (data, tombstone *batch.Batch, hint engine.Hint, err error) {
	data, err = p.dataHandle.Next()
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		return
	}
	tombstone, err = p.tombstoneHandle.Next()
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		return
	}
	if tombstone == nil && data == nil {
		err = moerr.GetOkExpectedEOF()
	} else {
		err = nil
	}
	hint = engine.Tail_wip
	return
}
func sortBatch(bat *batch.Batch, sortIdx int, mp *mpool.MPool) error {
	if bat == nil {
		return nil
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("logic error, batch attrs %v, sort idx %d", bat.Attrs, sortIdx))
	}
	sortedIdx := make([]int64, bat.Vecs[0].Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, bat.Vecs[sortIdx])
	for i := 0; i < len(bat.Vecs); i++ {
		err := bat.Vecs[i].Shuffle(sortedIdx, mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func checkObjectEntry(entry *ObjectEntry, start, end types.TS) bool {
	if entry.Appendable {
		if entry.CreateTime.Greater(&end) {
			return false
		}
		if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.Less(&start) {
			return false
		}
		return true
	} else {
		if !entry.ObjectStats.GetCNCreated() {
			return false
		}
		return entry.CreateTime.GreaterEq(&start) && entry.DeleteTime.LessEq(&end)
	}
}

func newDataBatchWithBatch(src *batch.Batch) (data *batch.Batch) {
	data = batch.NewWithSize(0)
	data.Attrs = append(data.Attrs, src.Attrs...)
	for _, vec := range src.Vecs {
		newVec := vector.NewVec(*vec.GetType())
		data.Vecs = append(data.Vecs, newVec)
	}
	return
}

func fillInInsertBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = newDataBatchWithBatch(entry.Batch)
	}
	for i, vec := range entry.Batch.Vecs {
		if vec.IsNull(uint64(entry.Offset)) {
			vector.AppendAny((*bat).Vecs[i], nil, true, mp)
		} else {
			var val any
			switch vec.GetType().Oid {
			case types.T_bool:
				val = vector.GetFixedAt[bool](vec, int(entry.Offset))
			case types.T_bit:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_int8:
				val = vector.GetFixedAt[int8](vec, int(entry.Offset))
			case types.T_int16:
				val = vector.GetFixedAt[int16](vec, int(entry.Offset))
			case types.T_int32:
				val = vector.GetFixedAt[int32](vec, int(entry.Offset))
			case types.T_int64:
				val = vector.GetFixedAt[int64](vec, int(entry.Offset))
			case types.T_uint8:
				val = vector.GetFixedAt[uint8](vec, int(entry.Offset))
			case types.T_uint16:
				val = vector.GetFixedAt[uint16](vec, int(entry.Offset))
			case types.T_uint32:
				val = vector.GetFixedAt[uint32](vec, int(entry.Offset))
			case types.T_uint64:
				val = vector.GetFixedAt[uint64](vec, int(entry.Offset))
			case types.T_decimal64:
				val = vector.GetFixedAt[types.Decimal64](vec, int(entry.Offset))
			case types.T_decimal128:
				val = vector.GetFixedAt[types.Decimal128](vec, int(entry.Offset))
			case types.T_uuid:
				val = vector.GetFixedAt[types.Uuid](vec, int(entry.Offset))
			case types.T_float32:
				val = vector.GetFixedAt[float32](vec, int(entry.Offset))
			case types.T_float64:
				val = vector.GetFixedAt[float64](vec, int(entry.Offset))
			case types.T_date:
				val = vector.GetFixedAt[types.Date](vec, int(entry.Offset))
			case types.T_time:
				val = vector.GetFixedAt[types.Time](vec, int(entry.Offset))
			case types.T_datetime:
				val = vector.GetFixedAt[types.Datetime](vec, int(entry.Offset))
			case types.T_timestamp:
				val = vector.GetFixedAt[types.Timestamp](vec, int(entry.Offset))
			case types.T_enum:
				val = vector.GetFixedAt[types.Enum](vec, int(entry.Offset))
			case types.T_TS:
				val = vector.GetFixedAt[types.TS](vec, int(entry.Offset))
			case types.T_Rowid:
				val = vector.GetFixedAt[types.Rowid](vec, int(entry.Offset))
			case types.T_Blockid:
				val = vector.GetFixedAt[types.Blockid](vec, int(entry.Offset))
			case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
				types.T_array_float32, types.T_array_float64, types.T_datalink:
				val = vec.GetBytesAt(int(entry.Offset))
			default:
				//return vector.ErrVecTypeNotSupport
				panic(any("No Support"))
			}
			vector.AppendAny((*bat).Vecs[i], val, false, mp)
		}
	}

}
func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = batch.NewWithSize(3)
		(*bat).SetAttributes([]string{
			taeCatalog.AttrRowID,
			taeCatalog.AttrCommitTs,
			taeCatalog.AttrPKVal,
		})
		(*bat).Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
		(*bat).Vecs[2] = vector.NewVec(types.T_varchar.ToType())
	}
	vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp)
	vector.AppendFixed((*bat).Vecs[1], entry.Time, false, mp)
	vector.AppendBytes((*bat).Vecs[2], entry.PrimaryIndexBytes, false, mp)
}

func isCreatedByCN(entry *ObjectEntry) bool {
	return entry.ObjectStats.GetCNCreated()
}

func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LessEq(&end) && ts.GreaterEq(&start)
}
