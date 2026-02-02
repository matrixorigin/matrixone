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
	"bytes"
	"container/heap"
	"context"
	"fmt"
	"math"
	"runtime/trace"
	"strings"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"go.uber.org/zap"

	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	metricv2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	IndexScaleZero        = 0
	IndexScaleOne         = 1
	IndexScaleTiny        = 10
	MuchGreaterThanFactor = 100
)

type PartitionState struct {
	service  string
	prefetch bool

	// also modify the Copy method if adding fields
	tid uint64

	// data
	rows *btree.BTreeG[*RowEntry] // use value type to avoid locking on elements

	checkpoints []string
	//current partitionState can serve snapshot read only if start <= ts <= end
	start types.TS
	end   types.TS

	// index

	dataObjectsNameIndex      *btree.BTreeG[objectio.ObjectEntry]
	tombstoneObjectsNameIndex *btree.BTreeG[objectio.ObjectEntry]

	rowPrimaryKeyIndex       *btree.BTreeG[*PrimaryIndexEntry]
	inMemTombstoneRowIdIndex *btree.BTreeG[*PrimaryIndexEntry]

	dataObjectTSIndex       *btree.BTreeG[ObjectIndexByTSEntry]
	tombstoneObjectDTSIndex *btree.BTreeG[objectio.ObjectEntry]

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool

	lastFlushTimestamp types.TS

	// some data need to be shared between all states
	// should have been in the Partition structure, but doing that requires much more codes changes
	// so just put it here.
	shared *sharedStates
}

func (p *PartitionState) GetStart() types.TS {
	return p.start
}

func (p *PartitionState) GetEnd() types.TS {
	return p.end
}

func (p *PartitionState) LogEntry(entry *api.Entry, msg string) {
	data, _ := batch.ProtoBatchToBatch(entry.Bat)
	logutil.Info(
		msg,
		zap.String("table-name", entry.TableName),
		zap.Uint64("table-id", p.tid),
		zap.String("ps", fmt.Sprintf("%p", p)),
		zap.String("data", common.MoBatchToString(data, 1000)),
	)
}

func (p *PartitionState) Desc(short bool) string {
	buf := bytes.Buffer{}
	buf.WriteString(fmt.Sprintf("tid= %d, dataObjectCnt= %d, tombstoneObjectCnt= %d, rowsCnt= %d",
		p.tid,
		p.dataObjectsNameIndex.Len(),
		p.tombstoneObjectsNameIndex.Len(),
		p.rows.Len()))

	if short {
		return buf.String()
	}

	buf.WriteString("\n\nRows:\n")

	str := p.LogAllRowEntry()
	buf.WriteString(str)

	return buf.String()
}

func (p *PartitionState) String() string {
	return p.Desc(false)
}

func (p *PartitionState) HandleObjectEntry(
	ctx context.Context,
	fs fileservice.FileService,
	objectEntry objectio.ObjectEntry,
	isTombstone bool,
) (err error) {
	if isTombstone {
		return p.handleTombstoneObjectEntry(ctx, fs, objectEntry)
	} else {
		return p.handleDataObjectEntry(ctx, fs, objectEntry)
	}
}

func (p *PartitionState) handleDataObjectEntry(
	ctx context.Context,
	fs fileservice.FileService,
	objEntry objectio.ObjectEntry,
) (err error) {
	commitTS := objEntry.CreateTime
	if !objEntry.DeleteTime.IsEmpty() {
		commitTS = objEntry.DeleteTime
	}
	if commitTS.GT(&p.lastFlushTimestamp) {
		p.lastFlushTimestamp = commitTS
	}

	if objEntry.Size() == 0 || (objEntry.GetAppendable() && objEntry.DeleteTime.IsEmpty()) {
		// CN doesn't consume the create event of appendable object
		return
	}

	old, exist := p.dataObjectsNameIndex.Get(objEntry)
	if exist {
		// why check the deleteTime here? consider this situation:
		// 		1. insert on an object, then these insert operations recorded into a CKP.
		// 		2. and delete this object, this operation recorded into WAL.
		// 		3. restart
		// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
		// the delete record in WAL could be overwritten by insert record in CKP,
		// causing logic err of the objects' visibility(dead object back to life!!).
		//
		// if this happened, just skip this object will be fine,
		if !old.DeleteTime.IsEmpty() {
			return
		}
	} else {
		e := ObjectIndexByTSEntry{
			Time:         objEntry.CreateTime,
			ShortObjName: *objEntry.ObjectShortName(),
			IsDelete:     false,
			IsAppendable: objEntry.GetAppendable(),
		}
		p.dataObjectTSIndex.Set(e)
	}

	p.dataObjectsNameIndex.Set(objEntry)

	// Need to insert an ee in dataObjectTSIndex, when soft delete appendable object.
	if !objEntry.DeleteTime.IsEmpty() {
		e := ObjectIndexByTSEntry{
			Time:         objEntry.DeleteTime,
			IsDelete:     true,
			ShortObjName: *objEntry.ObjectShortName(),
			IsAppendable: objEntry.GetAppendable(),
		}
		p.dataObjectTSIndex.Set(e)
	}

	// for appendable object, gc rows when delete object
	if objEntry.GetAppendable() && !objEntry.DeleteTime.IsEmpty() {
		var numDeleted int64
		iter := p.rows.Copy().Iter()
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		blkCnt := objEntry.ObjectStats.BlkCnt()
		if blkCnt != 1 {
			panic("logic error")
		}
		blkID := objectio.NewBlockidWithObjectID(objID, 0)
		pivot := &RowEntry{
			// aobj has only one blk
			BlockID: blkID,
		}
		for ok := iter.Seek(pivot); ok; ok = iter.Next() {
			entry := iter.Item()
			if entry.BlockID != blkID {
				break
			}

			// cannot gc the inmem tombstone at this point
			if entry.Deleted {
				continue
			}

			// if the inserting block is appendable, need to delete the rows for it;
			// if the inserting block is non-appendable and has delta location, need to delete
			// the deletes for it.
			if entry.Time.LE(&objEntry.DeleteTime) {
				// delete the row
				p.rows.Delete(entry)

				// delete the row's primary index
				if len(entry.PrimaryIndexBytes) > 0 {
					p.rowPrimaryKeyIndex.Delete(&PrimaryIndexEntry{
						Bytes:      entry.PrimaryIndexBytes,
						RowEntryID: entry.ID,
						Time:       entry.Time,
					})
				}
				numDeleted++
			}

			//it's tricky here.
			//Due to consuming lazily the checkpoint,
			//we have to take the following scenario into account:
			//1. CN receives deletes for a non-appendable block from the log tail,
			//   then apply the deletes into PartitionState.rows.
			//2. CN receives block meta of the above non-appendable block to be inserted
			//   from the checkpoint, then apply the block meta into PartitionState.blocks.
			// So , if the above scenario happens, we need to set the non-appendable block into
			// PartitionState.dirtyBlocks.
			//if !objEntry.EntryState && !objEntry.HasDeltaLoc {
			//	p.dirtyBlocks.Set(entry.BlockID)
			//	break
			//}
		}
		iter.Release()

		// if there are no rows for the block, delete the block from the dirty
		//if objEntry.EntryState && scanCnt == blockDeleted && p.dirtyBlocks.Len() > 0 {
		//	p.dirtyBlocks.Delete(*blkID)
		//}
	}

	p.prefetchObject(fs, objEntry)

	return
}
func (p *PartitionState) handleTombstoneObjectEntry(
	ctx context.Context,
	fs fileservice.FileService,
	objEntry objectio.ObjectEntry,
) (err error) {
	commitTS := objEntry.CreateTime
	if !objEntry.DeleteTime.IsEmpty() {
		commitTS = objEntry.DeleteTime
	}
	if commitTS.GT(&p.lastFlushTimestamp) {
		p.lastFlushTimestamp = commitTS
	}
	if objEntry.Size() == 0 || (objEntry.GetAppendable() && objEntry.DeleteTime.IsEmpty()) {
		return
	}

	old, exist := p.tombstoneObjectsNameIndex.Get(objEntry)
	if exist {
		// why check the deleteTime here? consider this situation:
		// 		1. insert on an object, then these insert operations recorded into a CKP.
		// 		2. and delete this object, this operation recorded into WAL.
		// 		3. restart
		// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
		// the delete record in WAL could be overwritten by insert record in CKP,
		// causing logic err of the objects' visibility(dead object back to life!!).
		//
		// if this happened, just skip this object will be fine,
		if !old.DeleteTime.IsEmpty() {
			return
		}
	}

	p.tombstoneObjectsNameIndex.Set(objEntry)
	{ // update or set DTSIndex for objEntry
		tmpObj := objEntry
		tmpObj.DeleteTime = types.TS{}
		// if already exists, delete it first
		p.tombstoneObjectDTSIndex.Delete(tmpObj)
		p.tombstoneObjectDTSIndex.Set(objEntry)
	}

	// for appendable object, gc rows when delete object
	if !objEntry.GetAppendable() {
		return
	}

	truncatePoint := objEntry.DeleteTime

	var deletedRow *RowEntry
	var tbIter = p.inMemTombstoneRowIdIndex.Copy().Iter()
	defer tbIter.Release()

	for ok := tbIter.Seek(&PrimaryIndexEntry{
		Bytes: objEntry.ObjectName().ObjectId()[:],
		Time:  types.MaxTs(),
	}); ok; ok = tbIter.Next() {
		if truncatePoint.LT(&tbIter.Item().Time) {
			continue
		}

		current := types.Objectid(tbIter.Item().Bytes)
		if !objEntry.ObjectName().ObjectId().EQ(&current) {
			break
		}

		if deletedRow, exist = p.rows.Get(&RowEntry{
			ID:      tbIter.Item().RowEntryID,
			BlockID: tbIter.Item().BlockID,
			RowID:   tbIter.Item().RowID,
			Time:    tbIter.Item().Time,
		}); !exist {
			continue
		}

		p.rows.Delete(deletedRow)
		p.inMemTombstoneRowIdIndex.Delete(tbIter.Item())
		if len(deletedRow.PrimaryIndexBytes) > 0 {
			p.rowPrimaryKeyIndex.Delete(&PrimaryIndexEntry{
				Bytes:      deletedRow.PrimaryIndexBytes,
				RowEntryID: deletedRow.ID,
				Time:       deletedRow.Time,
			})
		}
	}

	p.prefetchObject(fs, objEntry)

	return
}
func (p *PartitionState) HandleLogtailEntry(
	ctx context.Context,
	fs fileservice.FileService,
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
	pool *mpool.MPool,
) {
	txnTrace.GetService(p.service).ApplyLogtail(entry, 1)
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsDataObjectList(entry.TableName) {
			if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
				p.LogEntry(entry, "INJECT-TRACE-PS-OBJ-INS")
			}
			p.HandleDataObjectList(ctx, entry, fs, pool)
		} else if IsTombstoneObjectList(entry.TableName) {
			if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
				p.LogEntry(entry, "INJECT-TRACE-PS-OBJ-DEL")
			}
			p.HandleTombstoneObjectList(ctx, entry, fs, pool)
		} else {
			if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
				p.LogEntry(entry, "INJECT-TRACE-PS-MEM-INS")
			}
			p.HandleRowsInsert(ctx, entry.Bat, primarySeqnum, packer, pool)
		}

	case api.Entry_Delete:
		if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
			p.LogEntry(entry, "INJECT-TRACE-PS-MEM-DEL")
		}
		p.HandleRowsDelete(ctx, entry.Bat, packer, pool)
	case api.Entry_DataObject:
		if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
			p.LogEntry(entry, "INJECT-TRACE-PS-OBJ-INS")
		}
		p.HandleDataObjectList(ctx, entry, fs, pool)
	case api.Entry_TombstoneObject:
		if ok, _ := objectio.PartitionStateInjected(entry.DatabaseName, entry.TableName); ok {
			p.LogEntry(entry, "INJECT-TRACE-PS-OBJ-DEL")
		}
		p.HandleTombstoneObjectList(ctx, entry, fs, pool)
	default:
		logutil.Panicf("unsupported logtail entry type: %s", entry.String())
	}
}

func (p *PartitionState) HandleDataObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	var numDeleted, blockDeleted int64

	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	defer statsVec.Free(pool)

	vec := mustVectorFromProto(ee.Bat.Vecs[5])
	defer vec.Free(pool)
	createTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[6])
	defer vec.Free(pool)
	deleteTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[7])
	defer vec.Free(pool)
	startTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[9])
	defer vec.Free(pool)
	commitTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	for idx := 0; idx < statsVec.Length(); idx++ {
		if t := commitTSCol[idx]; t.GT(&p.lastFlushTimestamp) {
			p.lastFlushTimestamp = t
		}
		var objEntry objectio.ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		if objEntry.Size() == 0 || (objEntry.GetAppendable() && objEntry.DeleteTime.IsEmpty()) {
			// CN doesn't consume the create event of appendable object
			continue
		}

		old, exist := p.dataObjectsNameIndex.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		} else {
			e := ObjectIndexByTSEntry{
				Time:         createTSCol[idx],
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     false,
				IsAppendable: objEntry.GetAppendable(),
			}
			p.dataObjectTSIndex.Set(e)
		}

		p.dataObjectsNameIndex.Set(objEntry)

		//Need to insert an ee in dataObjectTSIndex, when soft delete appendable object.
		if !deleteTSCol[idx].IsEmpty() {
			e := ObjectIndexByTSEntry{
				Time:         deleteTSCol[idx],
				IsDelete:     true,
				ShortObjName: *objEntry.ObjectShortName(),
				IsAppendable: objEntry.GetAppendable(),
			}
			p.dataObjectTSIndex.Set(e)
		}

		// for appendable object, gc rows when delete object
		iter := p.rows.Copy().Iter()
		defer iter.Release()
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		trunctPoint := startTSCol[idx]
		blkCnt := objEntry.ObjectStats.BlkCnt()
		for i := uint32(0); i < blkCnt; i++ {

			blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
			pivot := &RowEntry{
				// aobj has only one blk
				BlockID: blkID,
			}
			for ok := iter.Seek(pivot); ok; ok = iter.Next() {
				entry := iter.Item()
				if entry.BlockID != blkID {
					break
				}

				// cannot gc the inmem tombstone at this point
				if entry.Deleted {
					continue
				}

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if objEntry.GetAppendable() {
					if entry.Time.LE(&trunctPoint) {
						// delete the row
						p.rows.Delete(entry)

						// delete the row's primary index
						if len(entry.PrimaryIndexBytes) > 0 {
							p.rowPrimaryKeyIndex.Delete(&PrimaryIndexEntry{
								Bytes:      entry.PrimaryIndexBytes,
								RowEntryID: entry.ID,
								Time:       entry.Time,
							})
						}
						numDeleted++
						blockDeleted++
					}
				}

				//it's tricky here.
				//Due to consuming lazily the checkpoint,
				//we have to take the following scenario into account:
				//1. CN receives deletes for a non-appendable block from the log tail,
				//   then apply the deletes into PartitionState.rows.
				//2. CN receives block meta of the above non-appendable block to be inserted
				//   from the checkpoint, then apply the block meta into PartitionState.blocks.
				// So , if the above scenario happens, we need to set the non-appendable block into
				// PartitionState.dirtyBlocks.
				//if !objEntry.EntryState && !objEntry.HasDeltaLoc {
				//	p.dirtyBlocks.Set(entry.BlockID)
				//	break
				//}
			}

			// if there are no rows for the block, delete the block from the dirty
			//if objEntry.EntryState && scanCnt == blockDeleted && p.dirtyBlocks.Len() > 0 {
			//	p.dirtyBlocks.Delete(*blkID)
			//}
		}

		p.prefetchObject(fs, objEntry)
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
	})
}

func (p *PartitionState) prefetchObject(fs fileservice.FileService, obj objectio.ObjectEntry) {
	if p.prefetch && fs != nil {
		ioutil.Prefetch(p.service, fs, obj.BlockLocation(uint16(0), objectio.BlockMaxRows))
	}
}

func (p *PartitionState) HandleTombstoneObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	statsVec := mustVectorFromProto(ee.Bat.Vecs[2])
	defer statsVec.Free(pool)

	vec := mustVectorFromProto(ee.Bat.Vecs[5])
	defer vec.Free(pool)
	createTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[6])
	defer vec.Free(pool)
	deleteTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[7])
	defer vec.Free(pool)
	startTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(ee.Bat.Vecs[9])
	defer vec.Free(pool)
	commitTSCol := vector.MustFixedColWithTypeCheck[types.TS](vec)

	var tbIter = p.inMemTombstoneRowIdIndex.Copy().Iter()
	defer tbIter.Release()

	for idx := 0; idx < statsVec.Length(); idx++ {
		if t := commitTSCol[idx]; t.GT(&p.lastFlushTimestamp) {
			p.lastFlushTimestamp = t
		}
		var objEntry objectio.ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		if objEntry.Size() == 0 || (objEntry.GetAppendable() && objEntry.DeleteTime.IsEmpty()) {
			continue
		}

		old, exist := p.tombstoneObjectsNameIndex.Get(objEntry)
		if exist {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine,
			if !old.DeleteTime.IsEmpty() {
				continue
			}
		}

		p.tombstoneObjectsNameIndex.Set(objEntry)
		{ // update or set DTSIndex for objEntry
			tmpObj := objEntry
			tmpObj.DeleteTime = types.TS{}
			// if already exists, delete it first
			p.tombstoneObjectDTSIndex.Delete(tmpObj)
			p.tombstoneObjectDTSIndex.Set(objEntry)
		}

		// for appendable object, gc rows when delete object
		if !objEntry.GetAppendable() {
			continue
		}

		truncatePoint := startTSCol[idx]

		var deletedRow *RowEntry

		for ok := tbIter.Seek(&PrimaryIndexEntry{
			Bytes: objEntry.ObjectName().ObjectId()[:],
			Time:  types.MaxTs(),
		}); ok; ok = tbIter.Next() {
			if truncatePoint.LT(&tbIter.Item().Time) {
				continue
			}

			current := types.Objectid(tbIter.Item().Bytes)
			if !objEntry.ObjectName().ObjectId().EQ(&current) {
				break
			}

			if deletedRow, exist = p.rows.Get(&RowEntry{
				ID:      tbIter.Item().RowEntryID,
				BlockID: tbIter.Item().BlockID,
				RowID:   tbIter.Item().RowID,
				Time:    tbIter.Item().Time,
			}); !exist {
				continue
			}

			p.rows.Delete(deletedRow)
			p.inMemTombstoneRowIdIndex.Delete(tbIter.Item())
			if len(deletedRow.PrimaryIndexBytes) > 0 {
				p.rowPrimaryKeyIndex.Delete(&PrimaryIndexEntry{
					Bytes:      deletedRow.PrimaryIndexBytes,
					RowEntryID: deletedRow.ID,
					Time:       deletedRow.Time,
				})
			}
		}

		p.prefetchObject(fs, objEntry)
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
	})
}

func (p *PartitionState) HandleRowsDelete(
	ctx context.Context,
	input *api.Batch,
	packer *types.Packer,
	pool *mpool.MPool,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsDelete")
	defer task.End()

	vec := mustVectorFromProto(input.Vecs[0])
	defer vec.Free(pool)
	rowIDVector := vector.MustFixedColWithTypeCheck[types.Rowid](vec)

	vec = mustVectorFromProto(input.Vecs[1])
	defer vec.Free(pool)
	timeVector := vector.MustFixedColWithTypeCheck[types.TS](vec)

	vec = mustVectorFromProto(input.Vecs[3])
	defer vec.Free(pool)
	tbRowIdVector := vector.MustFixedColWithTypeCheck[types.Rowid](vec)

	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}

	var primaryKeys [][]byte
	if len(input.Vecs) > 2 {
		// has primary key
		primaryKeys = readutil.EncodePrimaryKeyVector(
			batch.Vecs[2],
			packer,
		)
	}

	numDeletes := int64(0)
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := &RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numDeletes++
		}

		entry.Deleted = true
		if i < len(primaryKeys) {
			entry.PrimaryIndexBytes = primaryKeys[i]
		}
		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		p.rows.Set(entry)

		//handle memory deletes for non-appendable block.
		//p.dirtyBlocks.Set(blockID)

		// primary key
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			pe := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
				Deleted:    entry.Deleted,
			}
			p.rowPrimaryKeyIndex.Set(pe)
		}

		tbRowId := tbRowIdVector[i]
		index := PrimaryIndexEntry{
			Bytes:      tbRowId.BorrowObjectID()[:],
			BlockID:    entry.BlockID,
			RowID:      entry.RowID,
			Time:       entry.Time,
			RowEntryID: entry.ID,
			Deleted:    entry.Deleted,
		}

		p.inMemTombstoneRowIdIndex.Set(&index)
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
	})
}

func (p *PartitionState) HandleRowsInsert(
	ctx context.Context,
	input *api.Batch,
	primarySeqnum int,
	packer *types.Packer,
	pool *mpool.MPool,
) (
	primaryKeys [][]byte,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsInsert")
	defer task.End()

	vec := mustVectorFromProto(input.Vecs[0])
	defer vec.Free(pool)
	rowIDVector := vector.MustFixedColWithTypeCheck[types.Rowid](vec)

	vec = mustVectorFromProto(input.Vecs[1])
	defer vec.Free(pool)
	timeVector := vector.MustFixedColWithTypeCheck[types.TS](vec)

	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}
	primaryKeys = readutil.EncodePrimaryKeyVector(
		batch.Vecs[2+primarySeqnum],
		packer,
	)

	var numInserted int64
	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := &RowEntry{
			BlockID: blockID,
			RowID:   rowID,
			Time:    timeVector[i],
		}
		entry, ok := p.rows.Get(pivot)
		if !ok {
			entry = pivot
			entry.ID = atomic.AddInt64(&nextRowEntryID, 1)
			numInserted++
		}

		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		entry.PrimaryIndexBytes = primaryKeys[i]
		p.rows.Set(entry)

		{
			pe := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
				Deleted:    entry.Deleted,
			}
			p.rowPrimaryKeyIndex.Set(pe)
		}
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
	})

	return
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		service:                   p.service,
		tid:                       p.tid,
		rows:                      p.rows.Copy(),
		dataObjectsNameIndex:      p.dataObjectsNameIndex.Copy(),
		tombstoneObjectsNameIndex: p.tombstoneObjectsNameIndex.Copy(),
		rowPrimaryKeyIndex:        p.rowPrimaryKeyIndex.Copy(),
		inMemTombstoneRowIdIndex:  p.inMemTombstoneRowIdIndex.Copy(),
		noData:                    p.noData,
		dataObjectTSIndex:         p.dataObjectTSIndex.Copy(),
		tombstoneObjectDTSIndex:   p.tombstoneObjectDTSIndex.Copy(),
		shared:                    p.shared,
		lastFlushTimestamp:        p.lastFlushTimestamp,
		start:                     p.start,
		end:                       p.end,
		prefetch:                  p.prefetch,
	}
	if len(p.checkpoints) > 0 {
		state.checkpoints = make([]string, len(p.checkpoints))
		copy(state.checkpoints, p.checkpoints)
	}
	return &state
}

func (p *PartitionState) CacheCkpDuration(
	start types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
}

func (p *PartitionState) AppendCheckpoint(
	checkpoint string,
	partiton *Partition) {
	if partiton.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionState) consumeCheckpoints(
	fn func(checkpoint string, state *PartitionState) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint, p); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}

func NewPartitionState(
	service string,
	noData bool,
	tid uint64,
	prefetch bool,
) *PartitionState {
	opts := btree.Options{
		Degree:  32, // may good for heap alloc
		NoLocks: true,
	}

	ps := &PartitionState{
		service:                   service,
		tid:                       tid,
		noData:                    noData,
		rows:                      btree.NewBTreeGOptions((*RowEntry).Less, opts),
		dataObjectsNameIndex:      btree.NewBTreeGOptions(objectio.ObjectEntry.ObjectNameIndexLess, opts),
		tombstoneObjectsNameIndex: btree.NewBTreeGOptions(objectio.ObjectEntry.ObjectNameIndexLess, opts),
		rowPrimaryKeyIndex:        btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		inMemTombstoneRowIdIndex:  btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		dataObjectTSIndex:         btree.NewBTreeGOptions(ObjectIndexByTSEntry.Less, opts),
		tombstoneObjectDTSIndex:   btree.NewBTreeGOptions(objectio.ObjectEntry.ObjectDTSIndexLess, opts),
		shared:                    new(sharedStates),
		start:                     types.MaxTs(),
		prefetch:                  prefetch,
	}
	logutil.Info(
		"partition.state.created",
		zap.Uint64("table-id", tid),
		zap.String("service", service),
		zap.String("addr", fmt.Sprintf("%p", ps)),
	)
	return ps
}

func (p *PartitionState) truncateTombstoneObjects(
	dbId uint64,
	tblId uint64,
	ts types.TS) {

	var gcLog bytes.Buffer

	iter := p.tombstoneObjectDTSIndex.Copy().Iter()
	defer iter.Release()

	for iter.Next() {
		entry := iter.Item()
		if entry.DeleteTime.IsEmpty() || entry.DeleteTime.GT(&ts) {
			break
		}

		gcLog.WriteString(fmt.Sprintf("%s; ", entry.ObjectName().String()))

		p.tombstoneObjectsNameIndex.Delete(entry)
		p.tombstoneObjectDTSIndex.Delete(entry)
	}

	if gcLog.Len() > 0 {
		logutil.Info(
			"partition.state.gc.tombstone.index",
			zap.String("db.tbl", fmt.Sprintf("%d.%d", dbId, tblId)),
			zap.String("ts", ts.ToString()),
			zap.String("files", gcLog.String()))
	}
}

func (p *PartitionState) truncate(ids [2]uint64, ts types.TS) (updated bool) {
	if p.start.GT(&ts) {
		updated = true
		return
	}
	p.start = ts

	p.truncateTombstoneObjects(ids[0], ids[1], ts)

	gced := false
	pivot := ObjectIndexByTSEntry{
		Time:         ts.Next(),
		ShortObjName: objectio.ObjectNameShort{},
		IsDelete:     true,
	}
	iter := p.dataObjectTSIndex.Copy().Iter()
	ok := iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	objIDsToDelete := make(map[objectio.ObjectNameShort]struct{}, 0)
	var objectsToDeleteBuilder strings.Builder
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.GT(&ts) {
			continue
		}
		if entry.IsDelete {
			objIDsToDelete[entry.ShortObjName] = struct{}{}
			if gced {
				objectsToDeleteBuilder.WriteString(", ")
			}
			objectsToDeleteBuilder.WriteString(entry.ShortObjName.ShortString())
			gced = true
		}
	}
	iter.Release()
	objectsToDelete := objectsToDeleteBuilder.String()

	iter = p.dataObjectTSIndex.Copy().Iter()
	ok = iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.GT(&ts) {
			continue
		}
		if _, ok := objIDsToDelete[entry.ShortObjName]; ok {
			p.dataObjectTSIndex.Delete(entry)
		}
	}
	iter.Release()
	if gced {
		logutil.Info(
			"partition.state.gc.data.object",
			zap.String("ts", ts.ToString()),
			zap.String("db.tbl", fmt.Sprintf("%d.%d", ids[0], ids[1])),
			zap.String("files", objectsToDelete),
			zap.String("ps", fmt.Sprintf("%p", p)),
		)
	}

	objectsToDeleteBuilder.Reset()
	objIter := p.dataObjectsNameIndex.Copy().Iter()
	defer objIter.Release()
	objGced := false
	firstCalled := false
	for {
		if !firstCalled {
			if !objIter.First() {
				break
			}
			firstCalled = true
		} else {
			if !objIter.Next() {
				break
			}
		}

		objEntry := objIter.Item()

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LE(&ts) {
			p.dataObjectsNameIndex.Delete(objEntry)
			if objGced {
				objectsToDeleteBuilder.WriteString(", ")
			}
			objectsToDeleteBuilder.WriteString(objEntry.ObjectShortName().ShortString())
			objGced = true
		}
	}
	objsToDelete := objectsToDeleteBuilder.String()
	if objGced {
		logutil.Info(
			"partition.state.gc.name.index",
			zap.String("ts", ts.ToString()),
			zap.String("db.tbl", fmt.Sprintf("%d.%d", ids[0], ids[1])),
			zap.String("files", objsToDelete),
			zap.String("ps", fmt.Sprintf("%p", p)),
		)
	}
	updated = true
	return
}

func (p *PartitionState) PKExistInMemBetween(
	from types.TS,
	to types.TS,
	keys [][]byte,
) (bool, bool) {
	iter := p.rowPrimaryKeyIndex.Iter()
	pivot := &RowEntry{
		Time: types.BuildTS(math.MaxInt64, math.MaxUint32),
	}
	idxEntry := &PrimaryIndexEntry{}
	defer iter.Release()

	for _, key := range keys {

		idxEntry.Bytes = key
		idxEntry.Time = types.MaxTs()

		for ok := iter.Seek(idxEntry); ok; ok = iter.Next() {

			entry := iter.Item()

			if !bytes.Equal(entry.Bytes, key) {
				break
			}

			if entry.Time.GE(&from) {
				return true, false
			}

			//some legacy deletion entries may not be indexed since old TN maybe
			//don't take pk in log tail when delete row , so check all rows for changes.
			pivot.BlockID = entry.BlockID
			pivot.RowID = entry.RowID
			rowIter := p.rows.Iter()
			seek := false
			for {
				if !seek {
					seek = true
					if !rowIter.Seek(pivot) {
						break
					}
				} else {
					if !rowIter.Next() {
						break
					}
				}
				row := rowIter.Item()
				if row.BlockID.Compare(&entry.BlockID) != 0 {
					break
				}
				if !row.RowID.EQ(&entry.RowID) {
					break
				}
				if row.Time.GE(&from) {
					rowIter.Release()
					return true, false
				}
			}
			rowIter.Release()
		}

		iter.First()
	}

	if p.lastFlushTimestamp.LE(&from) {
		return false, false
	}
	return false, true
}

func (p *PartitionState) Checkpoints() []string {
	return p.checkpoints
}

func (p *PartitionState) RowExists(rowID types.Rowid, ts types.TS) bool {
	iter := p.rows.Iter()
	defer iter.Release()

	blockID := rowID.CloneBlockID()
	for ok := iter.Seek(&RowEntry{
		BlockID: blockID,
		RowID:   rowID,
		Time:    ts,
	}); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.BlockID != blockID {
			break
		}
		if entry.RowID != rowID {
			break
		}
		if entry.Time.GT(&ts) {
			// not visible
			continue
		}
		if entry.Deleted {
			// deleted
			return false
		}
		return true
	}

	return false
}

func (p *PartitionState) CanServe(ts types.TS) bool {
	return ts.GE(&p.start) && ts.LE(&p.end)
}

func (p *PartitionState) UpdateDuration(start types.TS, end types.TS) {
	p.start = start
	p.end = end
}

func (p *PartitionState) GetDuration() (types.TS, types.TS) {
	return p.start, p.end
}

func (p *PartitionState) IsValid() bool {
	return p.start.LE(&p.end)
}

func (p *PartitionState) IsEmpty() bool {
	return p.start == types.MaxTs()
}

func (p *PartitionState) LogAllRowEntry() string {
	var buf bytes.Buffer
	_ = p.ScanRows(false, func(entry *RowEntry) (bool, error) {
		buf.WriteString(entry.String())
		buf.WriteString("\n")
		return true, nil
	})
	return buf.String()
}

func (p *PartitionState) ScanRows(
	reverse bool,
	onItem func(entry *RowEntry) (bool, error),
) (err error) {
	var ok bool

	if !reverse {
		p.rows.Scan(func(item *RowEntry) bool {
			if ok, err = onItem(item); err != nil || !ok {
				return false
			}
			return true
		})
	} else {
		p.rows.Reverse(func(item *RowEntry) bool {
			if ok, err = onItem(item); err != nil || !ok {
				return false
			}
			return true
		})
	}

	return
}

func (p *PartitionState) CheckRowIdDeletedInMem(ts types.TS, rowId types.Rowid) bool {
	iter := p.rows.Iter()
	defer iter.Release()

	if !iter.Seek(&RowEntry{
		Time:    ts,
		BlockID: rowId.CloneBlockID(),
		RowID:   rowId,
	}) {
		return false
	}

	item := iter.Item()
	if !item.Deleted {
		return false
	}
	return item.RowID.EQ(&rowId)
}

// CountRows returns the total number of visible rows at the given snapshot.
// CountRows = CollectDataStats.Rows - CollectTombstoneStats.Rows
func (p *PartitionState) CountRows(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
) (uint64, error) {
	dataStats := p.CollectDataStats(snapshot)
	tombstoneStats, err := p.CollectTombstoneStats(ctx, snapshot, fs)
	if err != nil {
		return 0, err
	}
	if tombstoneStats.Rows > dataStats.Rows {
		return 0, nil
	}
	// Record estimate/actual ratio: high ratio (e.g. 100x) means estimate is much larger than actual (bad signal).
	estimateRows, _ := p.estimateTombstoneRowsOnly(snapshot)
	actual := tombstoneStats.Rows
	if actual == 0 {
		actual = 1 // avoid div by zero; ratio is still meaningful (estimate/1)
	}
	ratio := float64(estimateRows) / float64(actual)
	metricv2.StarcountEstimateOverActualRatioHistogram.Observe(ratio)
	return dataStats.Rows - tombstoneStats.Rows, nil
}

// estimateTombstoneRowsOnly returns (estimated rows, object count) from metadata only (no S3 I/O).
// Same visibility logic as EstimateCommittedTombstoneCount; used by CountRows to compute estimate/actual ratio.
func (p *PartitionState) estimateTombstoneRowsOnly(snapshot types.TS) (estimatedRows int, objectCount int) {
	iter := p.tombstoneObjectsNameIndex.Iter()
	defer iter.Release()
	for ok := iter.First(); ok; ok = iter.Next() {
		obj := iter.Item()
		if obj.CreateTime.GT(&snapshot) {
			continue
		}
		if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LE(&snapshot) {
			continue
		}
		estimatedRows += int(obj.Rows())
		objectCount++
	}
	return estimatedRows, objectCount
}

// EstimateCommittedTombstoneCount returns an estimated count of committed tombstone rows
// by summing up the row counts from visible tombstone object metadata.
// This is very lightweight (only reads metadata, no S3 I/O) and can be used to decide
// whether to use StarCount optimization.
//
// Note: This is an upper bound estimate because:
// - It includes duplicate rowids (not deduplicated)
// - It includes tombstones pointing to invisible data objects
// The actual count from CollectTombstoneStats will be lower after deduplication and visibility filtering.
func (p *PartitionState) EstimateCommittedTombstoneCount(snapshot types.TS) int {
	estimatedRows, objectCount := p.estimateTombstoneRowsOnly(snapshot)
	metricv2.StarcountEstimateTombstoneObjectsHistogram.Observe(float64(objectCount))
	return estimatedRows
}

// DataStats contains statistics for data objects and rows.
type DataStats struct {
	// Rows: exact count of visible data rows at snapshot
	// - Non-appendable objects: exact count from ObjectStats
	// - Appendable rows: exact count from in-memory rows btree
	Rows uint64

	// Size: total data size in bytes (partially estimated)
	// - Non-appendable objects: exact size from ObjectStats.Size()
	// - Appendable rows: estimated using average row size from non-appendable objects
	//   or batch size if no non-appendable objects exist
	Size float64

	ObjectCnt int // Exact count of visible non-appendable data objects
	BlockCnt  int // Exact count of visible data blocks
}

// CollectDataStats returns comprehensive statistics for data objects and rows at the given snapshot.
func (p *PartitionState) CollectDataStats(snapshot types.TS) DataStats {
	var stats DataStats
	var estimatedOneRowSize float64
	var nonAppendableRows uint64

	// Count non-appendable data objects
	iter := p.dataObjectsNameIndex.Iter()
	defer iter.Release()
	for ok := iter.First(); ok; ok = iter.Next() {
		obj := iter.Item()
		if obj.CreateTime.GT(&snapshot) {
			continue
		}
		if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LE(&snapshot) {
			continue
		}
		if !obj.GetAppendable() {
			stats.ObjectCnt++
			stats.BlockCnt += int(obj.BlkCnt())
			stats.Rows += uint64(obj.Rows())
			stats.Size += float64(obj.Size())
			nonAppendableRows += uint64(obj.Rows())
		}
	}

	// Calculate estimated row size from non-appendable objects
	if nonAppendableRows > 0 {
		estimatedOneRowSize = stats.Size / float64(nonAppendableRows)
	}

	// Count appendable rows (scan all non-deleted entries)
	p.rows.Scan(func(entry *RowEntry) bool {
		if entry.Time.GT(&snapshot) {
			return true
		}
		if !entry.Deleted {
			stats.Rows++
			if estimatedOneRowSize > 0 {
				stats.Size += estimatedOneRowSize
			} else if entry.Batch != nil {
				stats.Size += float64(entry.Batch.Size()) / float64(entry.Batch.RowCount())
			}
		}
		return true
	})

	return stats
}

// TombstoneStats contains statistics for tombstone objects and deleted rows.
type TombstoneStats struct {
	// Rows: exact count of deleted rows (deduplicated and filtered by data object visibility)
	// - Tombstone objects: read from S3 and deduplicated
	// - In-memory tombstones: exact count from inMemTombstoneRowIdIndex
	// - Only counts deletions on visible data objects
	Rows uint64

	// Size: exact total size of tombstone objects in bytes
	// - Read from ObjectStats.Size() for each visible tombstone object
	// - Does not include in-memory tombstone size (negligible)
	Size float64

	ObjectCnt int // Exact count of visible tombstone objects
	BlockCnt  int // Exact count of visible tombstone blocks
}

// CollectTombstoneStats returns comprehensive statistics for tombstone objects and deleted rows at the given snapshot.
// This combines object counting with row counting in a single pass for better performance.
func (p *PartitionState) CollectTombstoneStats(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
) (TombstoneStats, error) {
	var stats TombstoneStats

	// Collect all visible tombstone objects
	iter := p.tombstoneObjectsNameIndex.Iter()
	defer iter.Release()

	var visibleObjects []objectio.ObjectEntry
	estimatedRows := 0
	for ok := iter.First(); ok; ok = iter.Next() {
		obj := iter.Item()
		if obj.CreateTime.GT(&snapshot) {
			continue
		}
		if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LE(&snapshot) {
			continue
		}
		visibleObjects = append(visibleObjects, obj)
		stats.ObjectCnt++
		stats.BlockCnt += int(obj.BlkCnt())
		stats.Size += float64(obj.Size())
		estimatedRows += int(obj.Rows())
	}

	// Decision: use merge-based deduplication for large datasets
	// Threshold considerations:
	// - Single object: use linear if no in-memory tombstones (O(N) time, O(1) memory)
	// - Map: ~20.6 bytes/row, fast O(N), but memory grows with row count
	// - Merge: ~3 KB fixed, slower O(N log K), but memory-safe for concurrent queries
	//
	// Thresholds:
	// 1. Single object + no in-memory: use linear (no duplicates possible)
	// 2. Multiple objects + large dataset (>5M rows ≈ 103 MB map): use merge for safety
	// 3. Huge dataset (>50M rows ≈ 1 GB map): always use merge regardless of object count
	//
	// Examples:
	// - 1 object, 1B rows, no in-mem: use linear (no duplicates, fast)
	// - 1 object, 1B rows, with in-mem: use merge (need dedup with in-mem)
	// - 3 objects, 100M rows: use merge (1 GB map would be risky)
	// - 100 objects, 1M rows: use merge (many objects, likely concurrent queries)
	// - 2 objects, 1M rows: use map (20 MB, fast and safe)

	// Single object without in-memory tombstones: use linear deduplication
	if len(visibleObjects) == 1 && p.inMemTombstoneRowIdIndex.Len() == 0 {
		return p.countTombstoneStatsLinear(ctx, snapshot, fs, visibleObjects[0], stats)
	}

	useMerge := len(visibleObjects) >= 4 && estimatedRows > 5000000 || estimatedRows > 50000000

	if useMerge {
		return p.countTombstoneStatsWithMerge(ctx, snapshot, fs, visibleObjects, stats)
	}
	return p.countTombstoneStatsWithMap(ctx, snapshot, fs, visibleObjects, stats)
}

// IsDataObjectVisible checks if a data object is visible at the given snapshot.
//
// Background:
// - CN can delete rows on both appendable and non-appendable data objects
// - CN can flush tombstones to S3 independently (before data object is flushed)
// - Therefore, tombstone files may reference both appendable and non-appendable objects
//
// This function checks:
// 1. Non-appendable objects in dataObjectsNameIndex (O(log n) lookup)
// 2. Appendable objects in rows btree (O(n) scan, but cached by caller)
func (p *PartitionState) IsDataObjectVisible(objId *types.Objectid, snapshot types.TS) bool {
	// Build a dummy ObjectEntry with the target objectid for lookup
	var stats objectio.ObjectStats
	objectio.SetObjectStatsObjectName(&stats, objectio.BuildObjectNameWithObjectID(objId))

	entry := objectio.ObjectEntry{ObjectStats: stats}

	// Check non-appendable objects index (fast O(log n) lookup)
	if obj, exists := p.dataObjectsNameIndex.Get(entry); exists {
		// Check visibility at snapshot
		if obj.CreateTime.GT(&snapshot) {
			return false
		}
		if !obj.DeleteTime.IsEmpty() && obj.DeleteTime.LE(&snapshot) {
			return false
		}
		return true
	}

	// Check appendable objects in in-memory rows
	// Use Seek to quickly locate rows for this object (rows are sorted by BlockID)
	// This is O(log n + k) where k is the number of rows in this object
	iter := p.rows.Iter()
	defer iter.Release()

	// Create a pivot entry with the target objectid
	// BlockID is sorted by objectid first, so we can seek to the first block of this object
	pivotBlockID := objectio.NewBlockidWithObjectID(objId, 0)
	pivot := &RowEntry{BlockID: pivotBlockID}

	// Seek to the first row of this object
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		row := iter.Item()

		// Check if we've moved past this object
		if !row.BlockID.Object().EQ(objId) {
			break
		}

		// Found a visible data row for this object
		if row.Time.LE(&snapshot) && !row.Deleted {
			return true
		}
	}

	return false
}

// countTombstoneStatsLinear uses linear deduplication for single object
// Single object is already sorted, no cross-object duplicates possible
func (p *PartitionState) countTombstoneStatsLinear(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
	obj objectio.ObjectEntry,
	stats TombstoneStats,
) (TombstoneStats, error) {
	cnCreated := obj.GetCNCreated()
	isAppendable := obj.GetAppendable()
	needCheckCommitTs := !cnCreated || isAppendable

	var hidden objectio.HiddenColumnSelection
	if needCheckCommitTs {
		hidden = objectio.HiddenColumnSelection_CommitTS
	} else {
		hidden = objectio.HiddenColumnSelection_None
	}
	attrs := objectio.GetTombstoneAttrs(hidden)
	persistedDeletes := containers.NewVectors(len(attrs))

	var readErr error
	var lastRowId types.Rowid
	var lastRowIdSet bool

	objectio.ForeachBlkInObjStatsList(true, nil,
		func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
			var release func()
			if _, release, readErr = ioutil.ReadDeletes(
				ctx, blk.MetaLoc[:], fs, cnCreated, persistedDeletes, nil,
			); readErr != nil {
				return false
			}
			defer release()

			rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](&persistedDeletes[0])

			var commitTSs []types.TS
			if needCheckCommitTs && len(persistedDeletes) > 2 {
				commitTSs = vector.MustFixedColNoTypeCheck[types.TS](&persistedDeletes[len(persistedDeletes)-1])
			}

			var lastObjId types.Objectid
			var lastVisible bool
			var lastObjIdSet bool

			for j := 0; j < len(rowIds); j++ {
				// Linear deduplication: check if same as last rowid
				if lastRowIdSet && rowIds[j].EQ(&lastRowId) {
					continue
				}

				if needCheckCommitTs && len(commitTSs) > 0 && commitTSs[j].GT(&snapshot) {
					continue
				}

				objId := rowIds[j].BorrowObjectID()

				if !lastObjIdSet || !objId.EQ(&lastObjId) {
					lastObjId = *objId
					lastObjIdSet = true
					lastVisible = p.IsDataObjectVisible(objId, snapshot)
				}

				if lastVisible {
					stats.Rows++
					lastRowId = rowIds[j]
					lastRowIdSet = true
				}
			}

			return true
		}, obj.ObjectStats)

	if readErr != nil {
		return stats, readErr
	}

	// No in-memory tombstones in this path (checked by caller)
	return stats, nil
}

// countTombstoneStatsWithMap uses map-based deduplication for small datasets
func (p *PartitionState) countTombstoneStatsWithMap(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
	objects []objectio.ObjectEntry,
	stats TombstoneStats,
) (TombstoneStats, error) {
	estimatedSize := 0
	for _, obj := range objects {
		estimatedSize += int(obj.Rows())
	}
	estimatedSize += p.inMemTombstoneRowIdIndex.Len()

	if estimatedSize > 10000000 {
		estimatedSize = 10000000
	} else if estimatedSize < 128 {
		estimatedSize = 128
	}

	seenRowIds := make(map[types.Rowid]struct{}, estimatedSize)

	for _, obj := range objects {
		cnCreated := obj.GetCNCreated()
		isAppendable := obj.GetAppendable()
		needCheckCommitTs := !cnCreated || isAppendable

		var hidden objectio.HiddenColumnSelection
		if needCheckCommitTs {
			hidden = objectio.HiddenColumnSelection_CommitTS
		} else {
			hidden = objectio.HiddenColumnSelection_None
		}
		attrs := objectio.GetTombstoneAttrs(hidden)
		persistedDeletes := containers.NewVectors(len(attrs))

		var readErr error
		var lastRowId types.Rowid
		var lastRowIdSet bool

		objectio.ForeachBlkInObjStatsList(true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				var release func()
				if _, release, readErr = ioutil.ReadDeletes(
					ctx, blk.MetaLoc[:], fs, cnCreated, persistedDeletes, nil,
				); readErr != nil {
					return false
				}
				defer release()

				rowIds := vector.MustFixedColNoTypeCheck[types.Rowid](&persistedDeletes[0])

				var commitTSs []types.TS
				if needCheckCommitTs && len(persistedDeletes) > 2 {
					commitTSs = vector.MustFixedColNoTypeCheck[types.TS](&persistedDeletes[len(persistedDeletes)-1])
				}

				var lastObjId types.Objectid
				var lastVisible bool
				var lastObjIdSet bool

				for j := 0; j < len(rowIds); j++ {
					if lastRowIdSet && rowIds[j].EQ(&lastRowId) {
						continue
					}

					if needCheckCommitTs && len(commitTSs) > 0 && commitTSs[j].GT(&snapshot) {
						continue
					}

					objId := rowIds[j].BorrowObjectID()

					if !lastObjIdSet || !objId.EQ(&lastObjId) {
						lastObjId = *objId
						lastObjIdSet = true
						lastVisible = p.IsDataObjectVisible(objId, snapshot)
					}

					if lastVisible {
						if _, seen := seenRowIds[rowIds[j]]; !seen {
							stats.Rows++
							seenRowIds[rowIds[j]] = struct{}{}
						}
						lastRowId = rowIds[j]
						lastRowIdSet = true
					}
				}

				return true
			}, obj.ObjectStats)

		if readErr != nil {
			return stats, readErr
		}
	}

	// Count in-memory tombstones
	var lastObjId types.Objectid
	var lastVisible bool
	var lastObjIdSet bool
	var lastRowId types.Rowid
	var lastRowIdSet bool

	tombIter := p.inMemTombstoneRowIdIndex.Iter()
	defer tombIter.Release()

	for ok := tombIter.First(); ok; ok = tombIter.Next() {
		entry := tombIter.Item()

		if entry.Time.GT(&snapshot) {
			continue
		}

		if lastRowIdSet && entry.RowID.EQ(&lastRowId) {
			continue
		}

		objId := entry.RowID.BorrowObjectID()

		if !lastObjIdSet || !objId.EQ(&lastObjId) {
			lastObjId = *objId
			lastObjIdSet = true
			lastVisible = p.IsDataObjectVisible(objId, snapshot)
		}

		if lastVisible {
			if _, seen := seenRowIds[entry.RowID]; !seen {
				stats.Rows++
				seenRowIds[entry.RowID] = struct{}{}
			}
			lastRowId = entry.RowID
			lastRowIdSet = true
		}
	}

	return stats, nil
}

// tombstoneBlockIterator iterates over RowIDs in a tombstone object block by block
type tombstoneBlockIterator struct {
	obj          objectio.ObjectEntry
	blocks       []objectio.BlockInfo
	blockIdx     int
	rowIds       []types.Rowid
	commitTSs    []types.TS
	rowIdx       int
	needCheckTS  bool
	snapshot     types.TS
	persistedDel containers.Vectors
	ctx          context.Context
	fs           fileservice.FileService
	err          error
	p            *PartitionState
	release      func() // Release function for current block
	isInMemory   bool   // True if this is an in-memory tombstone iterator
	inMemIter    btree.IterG[*PrimaryIndexEntry]
	inMemEntry   *PrimaryIndexEntry
}

func (it *tombstoneBlockIterator) loadNextBlock() bool {
	// Release previous block
	if it.release != nil {
		it.release()
		it.release = nil
	}

	if it.blockIdx >= len(it.blocks) {
		return false
	}

	blk := it.blocks[it.blockIdx]
	it.blockIdx++

	cnCreated := it.obj.GetCNCreated()
	if _, it.release, it.err = ioutil.ReadDeletes(
		it.ctx, blk.MetaLoc[:], it.fs, cnCreated, it.persistedDel, nil,
	); it.err != nil {
		return false
	}

	it.rowIds = vector.MustFixedColNoTypeCheck[types.Rowid](&it.persistedDel[0])

	if it.needCheckTS && len(it.persistedDel) > 2 {
		it.commitTSs = vector.MustFixedColNoTypeCheck[types.TS](&it.persistedDel[len(it.persistedDel)-1])
	} else {
		it.commitTSs = nil
	}

	it.rowIdx = 0
	return true
}

func (it *tombstoneBlockIterator) next() bool {
	if it.isInMemory {
		// In-memory tombstone iterator
		for {
			if !it.inMemIter.Next() {
				return false
			}
			it.inMemEntry = it.inMemIter.Item()

			if it.inMemEntry.Time.GT(&it.snapshot) {
				continue
			}

			objId := it.inMemEntry.RowID.BorrowObjectID()
			if !it.p.IsDataObjectVisible(objId, it.snapshot) {
				continue
			}

			return true
		}
	}

	// Persisted tombstone iterator
	for {
		for it.rowIdx < len(it.rowIds) {
			if it.needCheckTS && len(it.commitTSs) > 0 && it.commitTSs[it.rowIdx].GT(&it.snapshot) {
				it.rowIdx++
				continue
			}

			objId := it.rowIds[it.rowIdx].BorrowObjectID()
			if !it.p.IsDataObjectVisible(objId, it.snapshot) {
				it.rowIdx++
				continue
			}

			return true
		}

		if !it.loadNextBlock() {
			return false
		}
	}
}

func (it *tombstoneBlockIterator) current() types.Rowid {
	if it.isInMemory {
		return it.inMemEntry.RowID
	}
	return it.rowIds[it.rowIdx]
}

func (it *tombstoneBlockIterator) advance() {
	if !it.isInMemory {
		it.rowIdx++
	}
	// For in-memory, advance happens in next()
}

// heapItem represents an item in the min-heap for merge-based deduplication
type heapItem struct {
	rowId types.Rowid
	iter  *tombstoneBlockIterator
}

type minHeap []heapItem

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return h[i].rowId.LT(&h[j].rowId) }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *minHeap) Push(x interface{}) {
	*h = append(*h, x.(heapItem))
}

func (h *minHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// countTombstoneStatsWithMerge uses streaming merge-based deduplication for large datasets
func (p *PartitionState) countTombstoneStatsWithMerge(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
	objects []objectio.ObjectEntry,
	stats TombstoneStats,
) (TombstoneStats, error) {
	iterators := make([]*tombstoneBlockIterator, 0, len(objects))

	for _, obj := range objects {
		cnCreated := obj.GetCNCreated()
		isAppendable := obj.GetAppendable()
		needCheckCommitTs := !cnCreated || isAppendable

		var hidden objectio.HiddenColumnSelection
		if needCheckCommitTs {
			hidden = objectio.HiddenColumnSelection_CommitTS
		} else {
			hidden = objectio.HiddenColumnSelection_None
		}
		attrs := objectio.GetTombstoneAttrs(hidden)
		persistedDeletes := containers.NewVectors(len(attrs))

		var blocks []objectio.BlockInfo
		objectio.ForeachBlkInObjStatsList(true, nil,
			func(blk objectio.BlockInfo, blkMeta objectio.BlockObject) bool {
				blocks = append(blocks, blk)
				return true
			}, obj.ObjectStats)

		if len(blocks) == 0 {
			continue
		}

		it := &tombstoneBlockIterator{
			obj:          obj,
			blocks:       blocks,
			blockIdx:     0,
			needCheckTS:  needCheckCommitTs,
			snapshot:     snapshot,
			persistedDel: persistedDeletes,
			ctx:          ctx,
			fs:           fs,
			p:            p,
		}

		if it.next() {
			iterators = append(iterators, it)
		}
	}

	// Add in-memory tombstones as an iterator
	inMemIter := p.inMemTombstoneRowIdIndex.Iter()
	defer inMemIter.Release()
	inMemIt := &tombstoneBlockIterator{
		isInMemory: true,
		inMemIter:  inMemIter,
		snapshot:   snapshot,
		p:          p,
	}
	if inMemIt.next() {
		iterators = append(iterators, inMemIt)
	}

	h := make(minHeap, 0, len(iterators))
	for _, it := range iterators {
		h = append(h, heapItem{rowId: it.current(), iter: it})
	}
	heap.Init(&h)

	var lastRowId types.Rowid
	var lastRowIdSet bool

	for h.Len() > 0 {
		item := heap.Pop(&h).(heapItem)
		rowId := item.rowId

		if !lastRowIdSet || !rowId.EQ(&lastRowId) {
			stats.Rows++
			lastRowId = rowId
			lastRowIdSet = true
		}

		item.iter.advance()
		if item.iter.next() {
			heap.Push(&h, heapItem{rowId: item.iter.current(), iter: item.iter})
		}
	}

	for _, it := range iterators {
		if it.release != nil {
			it.release()
		}
		if it.err != nil {
			return stats, it.err
		}
	}

	// In-memory tombstones are already included in the heap merge above
	return stats, nil
}

// TableStats contains comprehensive table statistics including row counts, sizes, and object counts.
// TableStats contains comprehensive table statistics at a snapshot.
type TableStats struct {
	// TotalRows: exact count of visible data rows after applying deletions
	// Calculated as: dataStats.Rows - tombstoneStats.Rows
	TotalRows float64

	// TotalSize: estimated size of visible data in bytes (excludes deleted data)
	// - Data size: exact for non-appendable, estimated for appendable rows
	// - Deleted data size is estimated and subtracted: (dataStats.Size / dataStats.Rows) * tombstoneStats.Rows
	// - Does NOT include tombstone object size (tombstone is metadata overhead)
	TotalSize float64

	// Object and block counts (all exact)
	DataObjectCnt      int // Number of visible non-appendable data objects
	DataBlockCnt       int // Number of visible data blocks
	TombstoneObjectCnt int // Number of visible tombstone objects
	TombstoneBlockCnt  int // Number of visible tombstone blocks
}

// CalculateTableStats calculates comprehensive table statistics at the given snapshot.
func (p *PartitionState) CalculateTableStats(
	ctx context.Context,
	snapshot types.TS,
	fs fileservice.FileService,
) (TableStats, error) {
	dataStats := p.CollectDataStats(snapshot)
	tombstoneStats, err := p.CollectTombstoneStats(ctx, snapshot, fs)
	if err != nil {
		return TableStats{}, err
	}

	// Calculate visible rows (data rows - deleted rows)
	var visibleRows uint64
	if tombstoneStats.Rows < dataStats.Rows {
		visibleRows = dataStats.Rows - tombstoneStats.Rows
	} else {
		visibleRows = 0
	}

	return TableStats{
		TotalRows:          float64(visibleRows),
		TotalSize:          dataStats.Size + tombstoneStats.Size,
		DataObjectCnt:      dataStats.ObjectCnt,
		DataBlockCnt:       dataStats.BlockCnt,
		TombstoneObjectCnt: tombstoneStats.ObjectCnt,
		TombstoneBlockCnt:  tombstoneStats.BlockCnt,
	}, nil
}
