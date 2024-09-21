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

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
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
	maxRow uint32
	mp     *mpool.MPool

	batches     *batch.Batch
	batchLength int
	dataOffsets int
	ctx         context.Context
}

func NewRowHandle(data *batch.Batch, maxRow uint32, mp *mpool.MPool, ctx context.Context) (handle *BatchHandle, err error) {
	err = sortBatch(data, len(data.Vecs)-1, mp)
	if err != nil {
		return
	}
	handle = &BatchHandle{
		maxRow:  maxRow,
		mp:      mp,
		batches: data,
		ctx:     ctx,
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
func (r *BatchHandle) Close() {}
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

func readObjects(stats objectio.ObjectStats, blockID uint32, fs fileservice.FileService, isTombstone bool, ctx context.Context) (bat *batch.Batch, err error) {
	metaType := objectio.SchemaData
	if isTombstone {
		metaType = objectio.SchemaTombstone
	}
	loc := stats.BlockLocation(uint16(blockID), 8192)
	bat, _, err = blockio.LoadOneBlock(
		ctx,
		fs,
		loc,
		metaType)
	return
}

func updateTombstoneBatch(bat *batch.Batch, start, end types.TS, mp *mpool.MPool) {
	bat.Vecs[0].Free(mp) // rowid
	//bat.Vecs[2].Free(mp) // phyaddr
	bat.Vecs = []*vector.Vector{bat.Vecs[1], bat.Vecs[2]}
	bat.Attrs = []string{
		catalog.AttrPKVal,
		catalog.AttrCommitTs}
	applyTSFilterForBatch(bat, 1, start, end, mp)
	sortBatch(bat, 1, mp)
}
func updateDataBatch(bat *batch.Batch, start, end types.TS, mp *mpool.MPool) {
	bat.Vecs[len(bat.Vecs)-2].Free(mp) // rowid
	bat.Vecs = append(bat.Vecs[:len(bat.Vecs)-2], bat.Vecs[len(bat.Vecs)-1])
	applyTSFilterForBatch(bat, len(bat.Vecs)-1, start, end, mp)
}
func updateCNTombstoneBatch(bat *batch.Batch, committs types.TS, mp *mpool.MPool) {
	var pk *vector.Vector
	for _, vec := range bat.Vecs {
		if vec.GetType().Oid != types.T_Rowid {
			pk = vec
		} else {
			vec.Free(mp)
		}
	}
	commitTS, err := vector.NewConstFixed(types.T_TS.ToType(), committs, pk.Length(), mp)
	if err != nil {
		return
	}
	bat.Vecs = []*vector.Vector{pk, commitTS}
	bat.Attrs = []string{catalog.AttrPKVal, catalog.AttrCommitTs}
}
func updateCNDataBatch(bat *batch.Batch, commitTS types.TS, mp *mpool.MPool) {
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, bat.Vecs[0].Length(), mp)
	if err != nil {
		return
	}
	bat.Vecs = append(bat.Vecs, commitTSVec)
}

type ObjectHandle struct {
	start, end  types.TS
	entry       *ObjectEntry
	blockOffset uint32
	fs          fileservice.FileService
	isTombstone bool
	mp          *mpool.MPool
	ctx         context.Context
}

func newObjectHandle(ctx context.Context, entry *ObjectEntry, fs fileservice.FileService, tombstone bool, mp *mpool.MPool, start, end types.TS) (h *ObjectHandle) {
	return &ObjectHandle{
		entry:       entry,
		fs:          fs,
		isTombstone: tombstone,
		mp:          mp,
		ctx:         ctx,
		start:       start,
		end:         end,
	}
}

func (r *ObjectHandle) Close() {
}
func (r *ObjectHandle) Next() (bat *batch.Batch, err error) {
	if r.blockOffset == r.entry.BlkCnt() {
		return nil, moerr.GetOkExpectedEOF()
	}
	bat, err = readObjects(r.entry.ObjectStats, r.blockOffset, r.fs, r.isTombstone, r.ctx)
	if err != nil {
		return
	}
	if r.entry.ObjectStats.GetCNCreated() {
		if r.isTombstone {
			updateCNTombstoneBatch(
				bat,
				r.entry.CreateTime,
				r.mp,
			)
		} else {
			updateCNDataBatch(
				bat,
				r.entry.CreateTime,
				r.mp,
			)
		}
	} else {
		if r.isTombstone {
			updateTombstoneBatch(bat, r.start, r.end, r.mp)
		} else {
			updateDataBatch(bat, r.start, r.end, r.mp)
		}
	}
	r.blockOffset++
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
	fs          fileservice.FileService
}

func NewBaseHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32, tombstone bool, fs fileservice.FileService, ctx context.Context) *baseHandle {
	var iter btree.IterG[ObjectEntry]
	if tombstone {
		iter = state.tombstoneObjectsNameIndex.Copy().Iter()
	} else {
		iter = state.dataObjectsNameIndex.Copy().Iter()
	}
	return &baseHandle{
		objectIter:  iter,
		rowIter:     state.rows.Copy().Iter(),
		start:       start,
		end:         end,
		tid:         state.tid,
		maxRow:      maxRow,
		fs:          fs,
		isTombstone: tombstone,
	}
}
func (p *baseHandle) Close() {
	p.objectIter.Release()
	p.rowIter.Release()
}
func (p *baseHandle) Next(mp *mpool.MPool, ctx context.Context) (bat *batch.Batch, err error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		switch p.currentType {
		case ChangesHandle_Object:
			if p.ObjectHandle == nil {
				p.ObjectHandle, err = p.newObjectHandle(mp, ctx)
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
				p.ObjectHandle, err = p.newObjectHandle(mp, ctx)
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
				p.rowHandle, err = p.newBatchHandleWithRowIterator(mp, ctx)
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
func (p *baseHandle) newBatchHandleWithRowIterator(mp *mpool.MPool, ctx context.Context) (h *BatchHandle, err error) {
	bat := p.getBatchesFromRowIterator(mp, ctx)
	if bat == nil {
		return nil, moerr.GetOkExpectedEOF()
	}
	h, err = NewRowHandle(bat, p.maxRow, mp, ctx)
	if err != nil {
		closeBatch(bat, mp)
	}
	return
}
func (p *baseHandle) getBatchesFromRowIterator(mp *mpool.MPool, ctx context.Context) (bat *batch.Batch) {
	for p.rowIter.Next() {
		entry := p.rowIter.Item()
		if checkTS(p.start, p.end, entry.Time) {
			if !entry.Deleted && !p.isTombstone {
				fillInInsertBatch(&bat, &entry, mp)
			}
			if entry.Deleted && p.isTombstone {
				fillInDeleteBatch(&bat, &entry, mp)
			}
		}
	}
	return
}
func (p *baseHandle) newObjectHandle(mp *mpool.MPool, ctx context.Context) (h *ObjectHandle, err error) {
	for p.objectIter.Next() {
		entry := p.objectIter.Item()
		if checkObjectEntry(&entry, p.start, p.end) {
			return newObjectHandle(ctx, &entry, p.fs, p.isTombstone, mp, p.start, p.end), nil
		}
	}
	return nil, moerr.GetOkExpectedEOF()
}

type ChangeHandler struct {
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
}

func NewChangesHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32, fs fileservice.FileService, ctx context.Context) (*ChangeHandler, error) {
	if state.minTS.Greater(&start) {
		return nil, moerr.NewErrStaleReadNoCtx(state.minTS.ToString(), start.ToString())
	}
	return &ChangeHandler{
		tombstoneHandle: NewBaseHandler(state, start, end, mp, maxRow, true, fs, ctx),
		dataHandle:      NewBaseHandler(state, start, end, mp, maxRow, false, fs, ctx),
	}, nil
}

func (p *ChangeHandler) Close() error {
	p.dataHandle.Close()
	p.tombstoneHandle.Close()
	return nil
}
func (p *ChangeHandler) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	data, err = p.dataHandle.Next(mp, ctx)
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		return
	}
	tombstone, err = p.tombstoneHandle.Next(mp, ctx)
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		return
	}
	err = nil
	hint = engine.ChangesHandle_Tail_wip
	return
}
func applyTSFilterForBatch(bat *batch.Batch, sortIdx int, start, end types.TS, mp *mpool.MPool) error {
	if bat == nil {
		return nil
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("logic error, batch attrs %v, sort idx %d", bat.Attrs, sortIdx))
	}
	commitTSs := vector.MustFixedColWithTypeCheck[types.TS](bat.Vecs[sortIdx])
	deletes := make([]int64, 0)
	for i, ts := range commitTSs {
		if ts.Less(&start) || ts.Greater(&end) {
			deletes = append(deletes, int64(i))
		}
	}
	for _, vec := range bat.Vecs {
		vec.Shrink(deletes, true)
	}
	return nil
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
	if entry.GetAppendable() {
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
	data.Attrs = append(data.Attrs, src.Attrs[2:]...)
	for _, vec := range src.Vecs {
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		newVec := vector.NewVec(*vec.GetType())
		data.Vecs = append(data.Vecs, newVec)
	}
	data.Attrs = append(data.Attrs, catalog.AttrCommitTs)
	newVec := vector.NewVec(types.T_TS.ToType())
	data.Vecs = append(data.Vecs, newVec)
	return
}

func appendFromEntry(src, vec *vector.Vector, offset int, mp *mpool.MPool) {
	if src.IsNull(uint64(offset)) {
		vector.AppendAny(vec, nil, true, mp)
	} else {
		var val any
		switch vec.GetType().Oid {
		case types.T_bool:
			val = vector.GetFixedAtNoTypeCheck[bool](src, offset)
		case types.T_bit:
			val = vector.GetFixedAtNoTypeCheck[uint64](src, offset)
		case types.T_int8:
			val = vector.GetFixedAtNoTypeCheck[int8](src, offset)
		case types.T_int16:
			val = vector.GetFixedAtNoTypeCheck[int16](src, offset)
		case types.T_int32:
			val = vector.GetFixedAtNoTypeCheck[int32](src, offset)
		case types.T_int64:
			val = vector.GetFixedAtNoTypeCheck[int64](src, offset)
		case types.T_uint8:
			val = vector.GetFixedAtNoTypeCheck[uint8](src, offset)
		case types.T_uint16:
			val = vector.GetFixedAtNoTypeCheck[uint16](src, offset)
		case types.T_uint32:
			val = vector.GetFixedAtNoTypeCheck[uint32](src, offset)
		case types.T_uint64:
			val = vector.GetFixedAtNoTypeCheck[uint64](src, offset)
		case types.T_decimal64:
			val = vector.GetFixedAtNoTypeCheck[types.Decimal64](src, offset)
		case types.T_decimal128:
			val = vector.GetFixedAtNoTypeCheck[types.Decimal128](src, offset)
		case types.T_uuid:
			val = vector.GetFixedAtNoTypeCheck[types.Uuid](src, offset)
		case types.T_float32:
			val = vector.GetFixedAtNoTypeCheck[float32](src, offset)
		case types.T_float64:
			val = vector.GetFixedAtNoTypeCheck[float64](src, offset)
		case types.T_date:
			val = vector.GetFixedAtNoTypeCheck[types.Date](src, offset)
		case types.T_time:
			val = vector.GetFixedAtNoTypeCheck[types.Time](src, offset)
		case types.T_datetime:
			val = vector.GetFixedAtNoTypeCheck[types.Datetime](src, offset)
		case types.T_timestamp:
			val = vector.GetFixedAtNoTypeCheck[types.Timestamp](src, offset)
		case types.T_enum:
			val = vector.GetFixedAtNoTypeCheck[types.Enum](src, offset)
		case types.T_TS:
			val = vector.GetFixedAtNoTypeCheck[types.TS](src, offset)
		case types.T_Rowid:
			val = vector.GetFixedAtNoTypeCheck[types.Rowid](src, offset)
		case types.T_Blockid:
			val = vector.GetFixedAtNoTypeCheck[types.Blockid](src, offset)
		case types.T_char, types.T_varchar, types.T_binary, types.T_varbinary, types.T_json, types.T_blob, types.T_text,
			types.T_array_float32, types.T_array_float64, types.T_datalink:
			val = src.GetBytesAt(offset)
		default:
			//return vector.ErrVecTypeNotSupport
			panic(any("No Support"))
		}
		vector.AppendAny(vec, val, false, mp)
	}

}

func fillInInsertBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = newDataBatchWithBatch(entry.Batch)
	}
	for i, vec := range entry.Batch.Vecs {
		if vec.GetType().Oid == types.T_Rowid || vec.GetType().Oid == types.T_TS {
			continue
		}
		appendFromEntry(vec, (*bat).Vecs[i-2], int(entry.Offset), mp)
	}
	appendFromEntry(entry.Batch.Vecs[1], (*bat).Vecs[len((*bat).Vecs)-1], int(entry.Offset), mp)

}
func fillInDeleteBatch(bat **batch.Batch, entry *RowEntry, mp *mpool.MPool) {
	pkVec := entry.Batch.Vecs[2]
	if *bat == nil {
		(*bat) = batch.NewWithSize(2)
		(*bat).SetAttributes([]string{
			catalog.AttrPKVal,
			catalog.AttrCommitTs,
		})
		(*bat).Vecs[0] = vector.NewVec(*pkVec.GetType())
		(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
	}
	appendFromEntry(pkVec, (*bat).Vecs[0], int(entry.Offset), mp)
	vector.AppendFixed((*bat).Vecs[1], entry.Time, false, mp)
}

func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LessEq(&end) && ts.GreaterEq(&start)
}
