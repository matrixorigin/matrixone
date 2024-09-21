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
	rowOffsetCursor int
	mp              *mpool.MPool

	batches     *batch.Batch
	batchLength int
	ctx         context.Context
}

func NewRowHandle(data *batch.Batch, mp *mpool.MPool, ctx context.Context) (handle *BatchHandle, err error) {
	err = sortBatch(data, len(data.Vecs)-1, mp)
	if err != nil {
		return
	}
	handle = &BatchHandle{
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
func (r *BatchHandle) isEnd() bool {
	return r.batches == nil || r.rowOffsetCursor >= r.batchLength
}
func (r *BatchHandle) NextTS() types.TS {
	if r.isEnd() {
		return types.TS{}
	}
	commitTSVec := r.batches.Vecs[len(r.batches.Vecs)-1]
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTSVec, r.rowOffsetCursor)
}
func (r *BatchHandle) Close() {
	r.batches.Clean(r.mp)
}
func (r *BatchHandle) Next(data **batch.Batch, mp *mpool.MPool) (err error) {
	if r.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = r.next(data, mp)
	if err != nil {
		return
	}
	r.rowOffsetCursor++
	return
}
func (r *BatchHandle) next(bat **batch.Batch, mp *mpool.MPool) (err error) {
	start := r.rowOffsetCursor
	end := start + 1
	if *bat == nil {
		*bat = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, r.batches.Attrs...)
		for _, vec := range r.batches.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				return err
			}
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	} else {
		for i, vec := range (*bat).Vecs {
			appendFromEntry(r.batches.Vecs[i], vec, r.rowOffsetCursor, mp)
		}
	}
	return
}

type CNObjectHandle struct {
	isTombstone        bool
	objectOffsetCursor int
	blkOffsetCursor    int
	objects            []*ObjectEntry
	fs                 fileservice.FileService
	mp                 *mpool.MPool
}

func NewCNObjectHandle(isTombstone bool, objects []*ObjectEntry, fs fileservice.FileService, mp *mpool.MPool) *CNObjectHandle {
	return &CNObjectHandle{
		isTombstone: isTombstone,
		objects:     objects,
		fs:          fs,
		mp:          mp,
	}
}
func (h *CNObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects)
}
func (h *CNObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	currentObject := h.objects[h.objectOffsetCursor].ObjectStats
	data, err := readObjects(currentObject, uint32(h.blkOffsetCursor), h.fs, h.isTombstone, ctx)
	if h.isTombstone {
		updateCNTombstoneBatch(
			data,
			h.NextTS(),
			h.mp,
		)
	} else {
		updateCNDataBatch(
			data,
			h.NextTS(),
			h.mp,
		)
	}
	if *bat == nil {
		*bat = data
	} else {
		srcLen := data.Vecs[0].Length()
		sels := make([]int64, srcLen)
		for j := 0; j < srcLen; j++ {
			sels[j] = int64(j)
		}
		for i, vec := range (*bat).Vecs {
			src := data.Vecs[i]
			vec.Union(src, sels, mp)
		}
	}
	h.blkOffsetCursor++
	if h.blkOffsetCursor >= int(currentObject.BlkCnt()) {
		h.blkOffsetCursor = 0
		h.objectOffsetCursor++
	}
	return
}
func (h *CNObjectHandle) NextTS() types.TS {
	if h.isEnd() {
		return types.TS{}
	}
	currentObject := h.objects[h.objectOffsetCursor]
	return currentObject.CreateTime
}

type AObjectHandle struct {
	isTombstone        bool
	start, end         types.TS
	objectOffsetCursor int
	rowOffsetCursor    int
	currentBatch       *batch.Batch
	batchLength        int
	objects            []*ObjectEntry
	fs                 fileservice.FileService
	mp                 *mpool.MPool
}

func NewAObjectHandle(ctx context.Context, isTombstone bool, start, end types.TS, objects []*ObjectEntry, fs fileservice.FileService, mp *mpool.MPool) (*AObjectHandle, error) {
	handle := &AObjectHandle{
		isTombstone: isTombstone,
		start:       start,
		end:         end,
		objects:     objects,
		fs:          fs,
		mp:          mp,
	}
	err := handle.getNextAObject(ctx)
	return handle, err
}

func (h *AObjectHandle) getNextAObject(ctx context.Context) (err error) {
	if h.isEnd() {
		return
	}
	currentObjectStats := h.objects[h.objectOffsetCursor].ObjectStats
	h.currentBatch, err = readObjects(currentObjectStats, 0, h.fs, h.isTombstone, ctx)
	h.batchLength = h.currentBatch.Vecs[0].Length()
	if h.isTombstone {
		updateTombstoneBatch(h.currentBatch, h.start, h.end, h.mp)
	} else {
		updateDataBatch(h.currentBatch, h.start, h.end, h.mp)
	}
	return
}
func (h *AObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects)
}

func (h *AObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	start := h.rowOffsetCursor
	end := start + 1
	if *bat == nil {
		*bat = batch.NewWithSize(len(h.currentBatch.Vecs))
		(*bat).Attrs = append((*bat).Attrs, h.currentBatch.Attrs...)
		for _, vec := range h.currentBatch.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				return err
			}
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	} else {
		for i, vec := range (*bat).Vecs {
			appendFromEntry(h.currentBatch.Vecs[i], vec, h.rowOffsetCursor, mp)
		}
	}
	h.rowOffsetCursor++
	if h.rowOffsetCursor >= h.batchLength {
		h.currentBatch.Clean(h.mp)
		h.rowOffsetCursor = 0
		h.objectOffsetCursor++
		if !h.isEnd() {
			h.getNextAObject(ctx)
		}
	}
	return
}
func (h *AObjectHandle) NextTS() types.TS {
	if h.isEnd() {
		return types.TS{}
	}
	commitTSVec := h.currentBatch.Vecs[len(h.currentBatch.Vecs)]
	return vector.GetFixedAtNoTypeCheck[types.TS](commitTSVec, h.rowOffsetCursor)
}

type baseHandle struct {
	aobjHandle     *AObjectHandle
	cnObjectHandle *CNObjectHandle
	inMemoryHandle *BatchHandle
}

const (
	NextChangeHandle_AObj = iota
	NextChangeHandle_CNObj
	NextChangeHandle_InMemory

	NextChangeHandle_Tombstone
	NextChangeHandle_Data
)

func NewBaseHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, tombstone bool, fs fileservice.FileService, ctx context.Context) (p *baseHandle, err error) {
	p = &baseHandle{}
	var iter btree.IterG[ObjectEntry]
	if tombstone {
		iter = state.tombstoneObjectsNameIndex.Copy().Iter()
	} else {
		iter = state.dataObjectsNameIndex.Copy().Iter()
	}
	defer iter.Release()
	rowIter := state.rows.Copy().Iter()
	defer rowIter.Release()
	p.inMemoryHandle, err = p.newBatchHandleWithRowIterator(ctx, rowIter, start, end, tombstone, mp)
	if err != nil {
		return
	}
	aobj, cnObj := p.getObjectEntries(iter, start, end)
	p.aobjHandle, err = NewAObjectHandle(ctx, tombstone, start, end, aobj, fs, mp)
	if err != nil {
		return
	}
	p.cnObjectHandle = NewCNObjectHandle(tombstone, cnObj, fs, mp)
	return
}
func (p *baseHandle) Close() {
	p.inMemoryHandle.Close()
}
func (p *baseHandle) nextTS() (types.TS, int) {
	inMemoryTS := p.inMemoryHandle.NextTS()
	aobjTS := p.aobjHandle.NextTS()
	cnObjTS := p.cnObjectHandle.NextTS()
	if !inMemoryTS.IsEmpty() && inMemoryTS.LessEq(&aobjTS) && inMemoryTS.LessEq(&cnObjTS) {
		return inMemoryTS, NextChangeHandle_InMemory
	}
	if !aobjTS.IsEmpty() && aobjTS.LessEq(&cnObjTS) {
		return aobjTS, NextChangeHandle_AObj
	}
	return cnObjTS, NextChangeHandle_CNObj
}
func (p *baseHandle) NextTS() types.TS {
	ts, _ := p.nextTS()
	return ts
}
func (p *baseHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	_, typ := p.nextTS()
	switch typ {
	case NextChangeHandle_AObj:
		err = p.aobjHandle.Next(ctx, bat, mp)
	case NextChangeHandle_InMemory:
		err = p.inMemoryHandle.Next(bat, mp)
	case NextChangeHandle_CNObj:
		err = p.cnObjectHandle.Next(ctx, bat, mp)
	}
	return
}
func (p *baseHandle) newBatchHandleWithRowIterator(ctx context.Context, iter btree.IterG[RowEntry], start, end types.TS, tombstone bool, mp *mpool.MPool) (h *BatchHandle, err error) {
	bat := p.getBatchesFromRowIterator(iter, start, end, tombstone, mp)
	if bat == nil {
		return nil, moerr.GetOkExpectedEOF()
	}
	h, err = NewRowHandle(bat, mp, ctx)
	if err != nil {
		closeBatch(bat, mp)
	}
	return
}
func (p *baseHandle) getBatchesFromRowIterator(iter btree.IterG[RowEntry], start, end types.TS, tombstone bool, mp *mpool.MPool) (bat *batch.Batch) {
	for iter.Next() {
		entry := iter.Item()
		if checkTS(start, end, entry.Time) {
			if !entry.Deleted && !tombstone {
				fillInInsertBatch(&bat, &entry, mp)
			}
			if entry.Deleted && tombstone {
				fillInDeleteBatch(&bat, &entry, mp)
			}
		}
	}
	return
}
func (p *baseHandle) getObjectEntries(objIter btree.IterG[ObjectEntry], start, end types.TS) (aobj, cnObj []*ObjectEntry) {
	aobj = make([]*ObjectEntry, 0)
	cnObj = make([]*ObjectEntry, 0)
	for objIter.Next() {
		entry := objIter.Item()
		if entry.GetAppendable() {
			if entry.CreateTime.Greater(&end) {
				continue
			}
			if !entry.DeleteTime.IsEmpty() && entry.DeleteTime.Less(&start) {
				continue
			}
			aobj = append(aobj, &entry)
		} else {
			if !entry.ObjectStats.GetCNCreated() {
				continue
			}
			if entry.CreateTime.Less(&start) || entry.CreateTime.Greater(&end) {
				continue
			}
			cnObj = append(cnObj, &entry)
		}
	}
	return
}

type ChangeHandler struct {
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
	coarseMaxRow    int
}

func NewChangesHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32, fs fileservice.FileService, ctx context.Context) (changeHandle *ChangeHandler, err error) {
	if state.minTS.Greater(&start) {
		return nil, moerr.NewErrStaleReadNoCtx(state.minTS.ToString(), start.ToString())
	}
	changeHandle = &ChangeHandler{
		coarseMaxRow: int(maxRow),
	}
	changeHandle.tombstoneHandle, err = NewBaseHandler(state, start, end, mp, true, fs, ctx)
	if err != nil {
		return
	}
	changeHandle.dataHandle, err = NewBaseHandler(state, start, end, mp, false, fs, ctx)
	if err != nil {
		changeHandle.tombstoneHandle.Close()
	}
	return
}

func (p *ChangeHandler) Close() error {
	p.dataHandle.Close()
	p.tombstoneHandle.Close()
	return nil
}
func (p *ChangeHandler) decideNextHandle() int {
	tombstoneTS := p.tombstoneHandle.NextTS()
	dataTS := p.dataHandle.NextTS()
	if !tombstoneTS.IsEmpty() && tombstoneTS.LessEq(&dataTS) {
		return NextChangeHandle_Tombstone
	}
	return NextChangeHandle_Data
}
func (p *ChangeHandler) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	for {
		typ := p.decideNextHandle()
		switch typ {
		case NextChangeHandle_Data:
			err = p.dataHandle.Next(ctx, &data, mp)
			if err == nil && data.Vecs[0].Length() > p.coarseMaxRow {
				return
			}
		case NextChangeHandle_Tombstone:
			err = p.tombstoneHandle.Next(ctx, &tombstone, mp)
			if err == nil && tombstone.Vecs[0].Length() > p.coarseMaxRow {
				return
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			err = nil
			break
		}
		if err != nil {
			break
		}
	}
	return
}

func applyTSFilterForBatch(bat *batch.Batch, sortIdx int, start, end types.TS) error {
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
	applyTSFilterForBatch(bat, 1, start, end)
	sortBatch(bat, 1, mp)
}
func updateDataBatch(bat *batch.Batch, start, end types.TS, mp *mpool.MPool) {
	bat.Vecs[len(bat.Vecs)-2].Free(mp) // rowid
	bat.Vecs = append(bat.Vecs[:len(bat.Vecs)-2], bat.Vecs[len(bat.Vecs)-1])
	applyTSFilterForBatch(bat, len(bat.Vecs)-1, start, end)
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
