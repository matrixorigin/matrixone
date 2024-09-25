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

	goSort "sort"

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
)

const (
	ChangesHandle_Object uint8 = iota
	ChangesHandle_Row
)

const (
	RowHandle_DataBatchIDX uint8 = iota
	RowHandle_TombstoneBatchIDX
)

const (
	SmallBatchThreshold = 8192
	CoarseMaxRow        = 8192
)

type BatchHandle struct {
	rowOffsetCursor int
	mp              *mpool.MPool

	batches     *batch.Batch
	batchLength int
	ctx         context.Context
}

func NewRowHandle(data *batch.Batch, mp *mpool.MPool, ctx context.Context) (handle *BatchHandle) {
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

func (r *BatchHandle) init(quick bool, mp *mpool.MPool) (err error) {
	if quick || r == nil {
		return
	}
	err = sortBatch(r.batches, len(r.batches.Vecs)-1, mp)
	return
}
func (r *BatchHandle) IsEmpty() bool {
	if r == nil {
		return true
	}
	return r.batchLength == 0
}
func (r *BatchHandle) Rows() int {
	if r == nil {
		return 0
	}
	return r.batchLength
}
func (r *BatchHandle) isEnd() bool {
	return r == nil || r.batches == nil || r.rowOffsetCursor >= r.batchLength
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
	err = r.next(data, mp, r.rowOffsetCursor, r.rowOffsetCursor+1)
	if err != nil {
		return
	}
	r.rowOffsetCursor++
	return
}

func (r *BatchHandle) QuickNext(data **batch.Batch, mp *mpool.MPool) (err error) {
	if r.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = r.next(data, mp, r.rowOffsetCursor, r.batchLength)
	if err != nil {
		return
	}
	r.rowOffsetCursor = r.batchLength
	return
}

func (r *BatchHandle) next(bat **batch.Batch, mp *mpool.MPool, start, end int) (err error) {
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
func (h *CNObjectHandle) IsEmpty() bool {
	return len(h.objects) == 0
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
		*bat = batch.NewWithSize(0)
		(*bat).Attrs = append((*bat).Attrs, data.Attrs...)
		for _, vec := range data.Vecs {
			newVec := vector.NewVec(*vec.GetType())
			if err != nil {
				return err
			}
			(*bat).Vecs = append((*bat).Vecs, newVec)
		}
	}
	srcLen := data.Vecs[0].Length()
	sels := make([]int64, srcLen)
	for j := 0; j < srcLen; j++ {
		sels[j] = int64(j)
	}
	for i, vec := range (*bat).Vecs {
		src := data.Vecs[i]
		vec.Union(src, sels, mp)
	}
	h.blkOffsetCursor++
	if h.blkOffsetCursor >= int(currentObject.BlkCnt()) {
		h.blkOffsetCursor = 0
		h.objectOffsetCursor++
	}
	return
}

func (h *CNObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	return h.Next(ctx, data, mp)
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
	quick              bool
	fs                 fileservice.FileService
	mp                 *mpool.MPool
}

func NewAObjectHandle(ctx context.Context, isTombstone bool, start, end types.TS, objects []*ObjectEntry, fs fileservice.FileService, mp *mpool.MPool) *AObjectHandle {
	handle := &AObjectHandle{
		isTombstone: isTombstone,
		start:       start,
		end:         end,
		objects:     objects,
		fs:          fs,
		mp:          mp,
	}
	return handle
}
func (h *AObjectHandle) init(ctx context.Context, quick bool) (err error) {
	h.quick = quick
	err = h.getNextAObject(ctx)
	return
}
func (h *AObjectHandle) IsEmpty() bool {
	return len(h.objects) == 0
}
func (h *AObjectHandle) RowCount() int {
	cnt := 0
	for _, obj := range h.objects {
		cnt += int(obj.ObjectStats.Rows())
	}
	return cnt
}
func (h *AObjectHandle) getNextAObject(ctx context.Context) (err error) {
	for {
		if h.isEnd() {
			return
		}
		currentObjectStats := h.objects[h.objectOffsetCursor].ObjectStats
		h.currentBatch, err = readObjects(currentObjectStats, 0, h.fs, h.isTombstone, ctx)
		if h.isTombstone {
			updateTombstoneBatch(h.currentBatch, h.start, h.end, !h.quick, h.mp)
		} else {
			updateDataBatch(h.currentBatch, h.start, h.end, h.mp)
		}
		h.batchLength = h.currentBatch.Vecs[0].Length()
		if h.batchLength > 0 {
			return
		}
		h.objectOffsetCursor++
	}
}
func (h *AObjectHandle) isEnd() bool {
	return h.objectOffsetCursor >= len(h.objects)
}

func (h *AObjectHandle) QuickNext(ctx context.Context, data **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	err = h.next(ctx, data, mp, h.rowOffsetCursor, h.batchLength)
	if err != nil {
		return
	}
	return
}

func (h *AObjectHandle) Next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	return h.next(ctx, bat, mp, h.rowOffsetCursor, h.rowOffsetCursor+1)
}
func (h *AObjectHandle) next(ctx context.Context, bat **batch.Batch, mp *mpool.MPool, start, end int) (err error) {
	if h.isEnd() {
		return moerr.GetOkExpectedEOF()
	}
	if *bat == nil {
		*bat = batch.NewWithSize(len(h.currentBatch.Vecs))
		(*bat).Attrs = append((*bat).Attrs, h.currentBatch.Attrs...)
		for i, vec := range h.currentBatch.Vecs {
			newVec, err := vec.CloneWindow(start, end, mp)
			if err != nil {
				return err
			}
			(*bat).Vecs[i] = newVec
		}
	} else {
		for i, vec := range (*bat).Vecs {
			appendFromEntry(h.currentBatch.Vecs[i], vec, h.rowOffsetCursor, mp)
		}
	}
	h.rowOffsetCursor = end
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
	commitTSVec := h.currentBatch.Vecs[len(h.currentBatch.Vecs)-1]
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
	p.inMemoryHandle = p.newBatchHandleWithRowIterator(ctx, rowIter, start, end, tombstone, mp)
	aobj, cnObj := p.getObjectEntries(iter, start, end)
	p.aobjHandle = NewAObjectHandle(ctx, tombstone, start, end, aobj, fs, mp)
	p.cnObjectHandle = NewCNObjectHandle(tombstone, cnObj, fs, mp)
	return
}
func (p *baseHandle) init(ctx context.Context, quick bool, mp *mpool.MPool) (err error) {
	err = p.aobjHandle.init(ctx, quick)
	if err != nil {
		return
	}
	err = p.inMemoryHandle.init(quick, mp)
	return
}
func (p *baseHandle) IsEmpty() bool {
	return p.aobjHandle.IsEmpty() && p.inMemoryHandle.IsEmpty() && p.cnObjectHandle.IsEmpty()
}

func (p *baseHandle) IsSmall() bool {
	if !p.cnObjectHandle.IsEmpty() {
		return false
	}
	count := p.aobjHandle.RowCount() + p.inMemoryHandle.Rows()
	return count < SmallBatchThreshold
}
func (p *baseHandle) Close() {
	if p.inMemoryHandle != nil {
		p.inMemoryHandle.Close()
	}
}
func (p *baseHandle) less(a, b types.TS) bool {
	if a.IsEmpty() {
		return false
	}
	if b.IsEmpty() {
		return true
	}
	return a.LessEq(&b)
}
func (p *baseHandle) nextTS() (types.TS, int) {
	inMemoryTS := p.inMemoryHandle.NextTS()
	aobjTS := p.aobjHandle.NextTS()
	cnObjTS := p.cnObjectHandle.NextTS()
	if p.less(inMemoryTS, aobjTS) && p.less(inMemoryTS, cnObjTS) {
		return inMemoryTS, NextChangeHandle_InMemory
	}
	if p.less(aobjTS, cnObjTS) {
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
func (p *baseHandle) QuickNext(ctx context.Context, bat **batch.Batch, mp *mpool.MPool) (err error) {
	if p.aobjHandle != nil {
		err = p.aobjHandle.QuickNext(ctx, bat, mp)
		if err == nil {
			return
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.aobjHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	if p.inMemoryHandle != nil {
		err = p.inMemoryHandle.QuickNext(bat, mp)
		if err == nil {
			return
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			p.inMemoryHandle.Close()
			p.inMemoryHandle = nil
			err = nil
		}
		if err != nil {
			return
		}
	}
	err = p.cnObjectHandle.QuickNext(ctx, bat, mp)
	return
}
func (p *baseHandle) newBatchHandleWithRowIterator(ctx context.Context, iter btree.IterG[RowEntry], start, end types.TS, tombstone bool, mp *mpool.MPool) (h *BatchHandle) {
	bat := p.getBatchesFromRowIterator(iter, start, end, tombstone, mp)
	if bat == nil {
		return nil
	}
	h = NewRowHandle(bat, mp, ctx)
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
	goSort.Slice(aobj, func(i, j int) bool {
		return aobj[i].CreateTime.Less(&aobj[j].CreateTime)
	})
	goSort.Slice(cnObj, func(i, j int) bool {
		return cnObj[i].CreateTime.Less(&cnObj[j].CreateTime)
	})
	return
}

type ChangeHandler struct {
	tombstoneHandle *baseHandle
	dataHandle      *baseHandle
	coarseMaxRow    int
	quick           bool
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
		return
	}
	changeHandle.decideMode()
	err = changeHandle.dataHandle.init(ctx, changeHandle.quick, mp)
	if err != nil {
		changeHandle.dataHandle.Close()
		changeHandle.tombstoneHandle.Close()
		return
	}
	err = changeHandle.tombstoneHandle.init(ctx, changeHandle.quick, mp)
	return
}

func (p *ChangeHandler) Close() error {
	p.dataHandle.Close()
	p.tombstoneHandle.Close()
	return nil
}
func (p *ChangeHandler) decideMode() {
	if p.tombstoneHandle.IsEmpty() {
		p.quick = true
		return
	}
	if p.dataHandle.IsEmpty() {
		p.quick = true
		return
	}
	if p.dataHandle.IsSmall() && p.tombstoneHandle.IsSmall() {
		p.quick = true
	}
}
func (p *ChangeHandler) decideNextHandle() int {
	tombstoneTS := p.tombstoneHandle.NextTS()
	dataTS := p.dataHandle.NextTS()
	if dataTS.IsEmpty() {
		return NextChangeHandle_Tombstone
	}
	if !tombstoneTS.IsEmpty() && tombstoneTS.LessEq(&dataTS) {
		return NextChangeHandle_Tombstone
	}
	return NextChangeHandle_Data
}
func (p *ChangeHandler) quickNext(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, err error) {
	err = p.dataHandle.QuickNext(ctx, &data, mp)
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		return
	}
	err = p.tombstoneHandle.QuickNext(ctx, &tombstone, mp)
	if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
		err = nil
	}
	return
}
func (p *ChangeHandler) Next(ctx context.Context, mp *mpool.MPool) (data, tombstone *batch.Batch, hint engine.ChangesHandle_Hint, err error) {
	hint = engine.ChangesHandle_Tail_done
	if p.quick {
		data, tombstone, err = p.quickNext(ctx, mp)
		return
	}
	for {
		typ := p.decideNextHandle()
		switch typ {
		case NextChangeHandle_Data:
			err = p.dataHandle.Next(ctx, &data, mp)
			if err == nil && data.Vecs[0].Length() >= p.coarseMaxRow {
				return
			}
		case NextChangeHandle_Tombstone:
			err = p.tombstoneHandle.Next(ctx, &tombstone, mp)
			if err == nil && tombstone.Vecs[0].Length() >= p.coarseMaxRow {
				return
			}
		}
		if moerr.IsMoErrCode(err, moerr.OkExpectedEOF) {
			err = nil
			return
		}
		if err != nil {
			return
		}
	}
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
	data.Attrs = append(data.Attrs, objectio.DefaultCommitTS_Attr)
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
			objectio.TombstoneAttr_PK_Attr,
			objectio.DefaultCommitTS_Attr,
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

func updateTombstoneBatch(bat *batch.Batch, start, end types.TS, sort bool, mp *mpool.MPool) {
	bat.Vecs[0].Free(mp) // rowid
	//bat.Vecs[2].Free(mp) // phyaddr
	bat.Vecs = []*vector.Vector{bat.Vecs[1], bat.Vecs[2]}
	bat.Attrs = []string{
		objectio.TombstoneAttr_PK_Attr,
		objectio.DefaultCommitTS_Attr}
	applyTSFilterForBatch(bat, 1, start, end)
	if sort {
		sortBatch(bat, 1, mp)
	}
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
	bat.Attrs = []string{objectio.TombstoneAttr_PK_Attr, objectio.DefaultCommitTS_Attr}
}
func updateCNDataBatch(bat *batch.Batch, commitTS types.TS, mp *mpool.MPool) {
	commitTSVec, err := vector.NewConstFixed(types.T_TS.ToType(), commitTS, bat.Vecs[0].Length(), mp)
	if err != nil {
		return
	}
	bat.Vecs = append(bat.Vecs, commitTSVec)
}
