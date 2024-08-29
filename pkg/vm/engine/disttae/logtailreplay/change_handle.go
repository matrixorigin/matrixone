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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	taeCatalog "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/tidwall/btree"
)

const (
	Checkpoint uint8 = iota
	Tail_wip
	Tail_done
)

const (
	DataObject uint8 = iota
	TombstoneObject
	DataRow
	TombstoneRow
	End
)

const (
	RowHandle_DataBatchIDX uint8 = iota
	RowHandle_TombstoneBatchIDX
)

type RowHandle struct {
	rows       btree.IterG[RowEntry]
	start, end types.TS
	maxRow     uint32
	mp         *mpool.MPool

	batches     [2]*batch.Batch
	batchLength [2]int
	dataOffsets [2]int
}

func NewRowHandle(changeHandle *TailHandler) (handle *RowHandle, err error) {
	handle = &RowHandle{
		rows:  changeHandle.rows,
		start: changeHandle.start,
		end:   changeHandle.end,
		mp:    changeHandle.mp,
	}
	err = handle.init()
	return
}
func (r *RowHandle) Close() {
	for _, bat := range r.batches {
		if bat != nil {
			for _, vec := range bat.Vecs {
				if !vec.NeedDup() {
					vec.Free(r.mp)
				}
			}
		}
	}
}
func (r *RowHandle) Next() (data, tombstone *batch.Batch, err error) {
	data, err = r.next(RowHandle_DataBatchIDX)
	if err != nil {
		return
	}
	tombstone, err = r.next(RowHandle_TombstoneBatchIDX)
	if data == nil && tombstone == nil {
		return nil, nil, moerr.GetOkExpectedEOF()
	}
	return
}
func (r *RowHandle) next(idx uint8) (bat *batch.Batch, err error) {
	src := r.batches[idx]
	if src == nil {
		return nil, nil
	}
	start := r.dataOffsets[idx]
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
	r.dataOffsets[idx] = end
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
func (r *RowHandle) init() error {
	r.getData()
	err := r.sort(RowHandle_DataBatchIDX, 1)
	if err != nil {
		return err
	}
	err = r.sort(RowHandle_TombstoneBatchIDX, 1)
	return err
}
func (r *RowHandle) sort(idx uint8, sortIdx int) error {
	bat := r.batches[idx]
	if bat == nil {
		return nil
	}
	if bat.Vecs[sortIdx].GetType().Oid != types.T_TS {
		panic(fmt.Sprintf("logic error, batch type %d, sort idx %d", idx, sortIdx))
	}
	sortedIdx := make([]int64, bat.Vecs[0].Length())
	for i := 0; i < len(sortedIdx); i++ {
		sortedIdx[i] = int64(i)
	}
	sort.Sort(false, false, true, sortedIdx, bat.Vecs[sortIdx])
	for i := 0; i < len(bat.Vecs); i++ {
		err := bat.Vecs[i].Shuffle(sortedIdx, r.mp)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *RowHandle) getData() {
	for r.rows.Next() {
		entry := r.rows.Item()
		if checkTS(r.start, r.end, entry.Time) {
			if !entry.Deleted {
				fillInInsertBatch(&r.batches[RowHandle_DataBatchIDX], &entry, r.mp)
			} else {
				fillInDeleteBatch(&r.batches[RowHandle_TombstoneBatchIDX], &entry, r.mp)
			}
		}
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

type TailHandler struct {
	dataObjects     btree.IterG[ObjectEntry]
	tombstoneObjets btree.IterG[ObjectEntry]
	rows            btree.IterG[RowEntry] // use value type to avoid locking on elements

	start, end  types.TS
	currentType uint8
	tid         uint64
	maxRow      uint32
	mp          *mpool.MPool
}

func NewTailHandler(state *PartitionState, start, end types.TS, mp *mpool.MPool, maxRow uint32) *TailHandler {
	return &TailHandler{
		dataObjects:     state.dataObjects.Copy().Iter(),
		tombstoneObjets: state.tombstoneObjets.Copy().Iter(),
		rows:            state.rows.Copy().Iter(),
		start:           start,
		end:             end,
		tid:             state.tid,
		maxRow:          maxRow,
		mp:              mp,
	}
}
func (p *TailHandler) Close() {
	p.dataObjects.Release()
	p.tombstoneObjets.Release()
	p.rows.Release()
}
func (p *TailHandler) Next() (data, tombstone *batch.Batch, hint uint8, err error) {
	for {
		switch p.currentType {
		case DataObject:
			bat := p.dataObject(false, p.mp)
			if bat != nil {
				return bat, nil, Tail_wip, nil
			}
			p.currentType = TombstoneObject
		case TombstoneObject:
			bat := p.dataObject(true, p.mp)
			if bat != nil {
				return nil, bat, Tail_wip, nil
			}
			p.currentType = DataRow
		case DataRow:
			bat := p.getData(p.mp, false)
			if bat != nil {
				return bat, nil, Tail_wip, nil
			}
			p.currentType = TombstoneRow
			p.rows.First()
		case TombstoneRow:
			bat := p.getData(p.mp, false)
			if bat != nil {
				return nil, bat, Tail_wip, nil
			}
			p.currentType = End
			return nil, nil, Tail_done, nil
		case End:
			return nil, nil, Tail_done, nil
		default:
			panic(fmt.Sprintf("invalid type %d", p.currentType))
		}
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

func fillInObjectBatch(bat **batch.Batch, entry *ObjectEntry, mp *mpool.MPool) {
	if *bat == nil {
		(*bat) = batch.NewWithSize(4)
		(*bat).SetAttributes([]string{
			taeCatalog.ObjectAttr_ObjectStats,
			taeCatalog.EntryNode_CreateAt,
			taeCatalog.EntryNode_DeleteAt,
			taeCatalog.AttrCommitTs,
		})
		(*bat).Vecs[0] = vector.NewVec(types.T_varchar.ToType())
		(*bat).Vecs[1] = vector.NewVec(types.T_TS.ToType())
		(*bat).Vecs[2] = vector.NewVec(types.T_TS.ToType())
		(*bat).Vecs[3] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendBytes((*bat).Vecs[0], entry.ObjectStats[:], false, mp)
	vector.AppendFixed((*bat).Vecs[1], entry.CreateTime, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.DeleteTime, false, mp)
	vector.AppendFixed((*bat).Vecs[3], entry.CommitTS, false, mp)
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
			taeCatalog.AttrPKVal,
			taeCatalog.AttrCommitTs,
		})
		(*bat).Vecs[0] = vector.NewVec(types.T_Rowid.ToType())
		(*bat).Vecs[1] = vector.NewVec(types.T_varchar.ToType())
		(*bat).Vecs[2] = vector.NewVec(types.T_TS.ToType())
	}
	vector.AppendFixed((*bat).Vecs[0], entry.RowID, false, mp)
	vector.AppendBytes((*bat).Vecs[1], entry.PrimaryIndexBytes, false, mp)
	vector.AppendFixed((*bat).Vecs[2], entry.Time, false, mp)
}
func isCreatedByCN(entry *ObjectEntry) bool {
	return entry.ObjectStats.GetCNCreated()
}

func checkTS(start, end types.TS, ts types.TS) bool {
	return ts.LessEq(&end) && ts.GreaterEq(&start)
}
func (p *TailHandler) dataObject(isTombstone bool, mp *mpool.MPool) (bat *batch.Batch) {
	var iter *btree.IterG[ObjectEntry]
	if isTombstone {
		iter = &p.tombstoneObjets
	} else {
		iter = &p.dataObjects
	}
	for iter.Next() {
		entry := iter.Item()
		if entry.Appendable {
			if entry.Appendable {
				if entry.DeleteTime.GreaterEq(&p.start) || entry.CreateTime.LessEq(&p.end) {
					fillInObjectBatch(&bat, &entry, mp)
					if bat.Vecs[0].Length() == int(p.maxRow) {
						break
					}
				}
			}
		} else {
			if checkTS(p.start, p.end, entry.CreateTime) && isCreatedByCN(&entry) {
				fillInObjectBatch(&bat, &entry, mp)
				if bat.Vecs[0].Length() == int(p.maxRow) {
					break
				}
			}
		}
	}
	return
}
