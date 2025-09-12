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
	"context"
	"fmt"
	"math"
	"runtime/trace"
	"strings"
	"sync/atomic"

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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	IndexScaleZero        = 0
	IndexScaleOne         = 1
	IndexScaleTiny        = 10
	MuchGreaterThanFactor = 100
)

type PartitionState struct {
	service string

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

func (p *PartitionState) HandleObjectEntry(ctx context.Context, objectEntry objectio.ObjectEntry, isTombstone bool) (err error) {
	if isTombstone {
		return p.handleTombstoneObjectEntry(ctx, objectEntry)
	} else {
		return p.handleDataObjectEntry(ctx, objectEntry)
	}
}

func (p *PartitionState) handleDataObjectEntry(ctx context.Context, objEntry objectio.ObjectEntry) (err error) {
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
		perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
			c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
		})
	}

	return
}
func (p *PartitionState) handleTombstoneObjectEntry(ctx context.Context, objEntry objectio.ObjectEntry) (err error) {
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
	var numDeleted int64
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

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
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
			iter.Release()

			// if there are no rows for the block, delete the block from the dirty
			//if objEntry.EntryState && scanCnt == blockDeleted && p.dirtyBlocks.Len() > 0 {
			//	p.dirtyBlocks.Delete(*blkID)
			//}
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

func (p *PartitionState) HandleTombstoneObjectList(
	ctx context.Context,
	ee *api.Entry,
	fs fileservice.FileService,
	pool *mpool.MPool) {

	var numDeleted int64
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
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
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
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeletes)
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
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
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
	}
	logutil.Info(
		"PS-CREATED",
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
			"PS-GC-TombstoneIndex",
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
	if gced {
		logutil.Info(
			"PS-GC-DataObject",
			zap.String("ts", ts.ToString()),
			zap.String("db.tbl", fmt.Sprintf("%d.%d", ids[0], ids[1])),
			zap.String("files", objectsToDelete),
			zap.String("ps", fmt.Sprintf("%p", p)),
		)
	}

	objectsToDeleteBuilder.Reset()
	objIter := p.dataObjectsNameIndex.Copy().Iter()
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
			"PS-GC-NameIndex",
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
