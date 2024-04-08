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
	"runtime/trace"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	txnTrace "github.com/matrixorigin/matrixone/pkg/txn/trace"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/reusee/pt"
	"github.com/tidwall/btree"
)

type PartitionState struct {
	// also modify the Copy method if adding fields

	// data
	prioritySource pt.PrioritySource
	rows           *pt.Treap[RowEntry]
	//table data objects
	dataObjects           *pt.Treap[ObjectEntry]
	dataObjectsByCreateTS *pt.Treap[ObjectIndexByCreateTSEntry]
	//TODO:: It's transient, should be removed in future PR.
	blockDeltas *pt.Treap[BlockDeltaEntry]
	checkpoints []string

	// index
	primaryIndex *pt.Treap[*PrimaryIndexEntry]
	//for non-appendable block's memory deletes, used to getting dirty
	// non-appendable blocks quickly.
	//TODO::remove it
	dirtyBlocks *pt.Treap[types.Blockid]
	//index for objects by timestamp.
	objectIndexByTS *btree.BTreeG[ObjectIndexByTSEntry]

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool

	// some data need to be shared between all states
	// should have been in the Partition structure, but doing that requires much more codes changes
	// so just put it here.
	shared *sharedStates

	// blocks deleted before minTS is hard deleted.
	// partition state can't serve txn with snapshotTS less than minTS
	minTS types.TS
}

// sharedStates is shared among all PartitionStates
type sharedStates struct {
	sync.Mutex
	// last block flush timestamp for table
	lastFlushTimestamp types.TS
}

// RowEntry represents a version of a row
type RowEntry struct {
	BlockID types.Blockid // we need to iter by block id, so put it first to allow faster iteration
	RowID   types.Rowid
	Time    types.TS

	ID                int64 // a unique version id, for primary index building and validating
	Deleted           bool
	Batch             *batch.Batch
	Offset            int64
	PrimaryIndexBytes []byte
}

func (r RowEntry) Compare(r2 RowEntry) int {
	if res := r.BlockID.Compare(r2.BlockID); res != 0 {
		return res
	}
	if r.RowID.Less(r2.RowID) {
		return -1
	} else if r2.RowID.Less(r.RowID) {
		return 1
	}
	if r2.Time.Less(&r.Time) {
		return -1
	}
	if r.Time.Less(&r2.Time) {
		return 1
	}
	return 0
}

type BlockEntry struct {
	objectio.BlockInfo

	CreateTime types.TS
	DeleteTime types.TS
}

type BlockDeltaEntry struct {
	BlockID types.Blockid

	CommitTs types.TS
	DeltaLoc objectio.ObjectLocation
}

func (b BlockDeltaEntry) Compare(to BlockDeltaEntry) int {
	return b.BlockID.Compare(to.BlockID)
}

func (b BlockDeltaEntry) DeltaLocation() objectio.Location {
	return b.DeltaLoc[:]
}

type ObjectInfo struct {
	objectio.ObjectStats

	EntryState  bool
	Sorted      bool
	HasDeltaLoc bool
	CommitTS    types.TS
	CreateTime  types.TS
	DeleteTime  types.TS
}

func (o ObjectInfo) String() string {
	return fmt.Sprintf(
		"%s; entryState: %v; sorted: %v; hasDeltaLoc: %v; commitTS: %s; createTS: %s; deleteTS: %s",
		o.ObjectStats.String(), o.EntryState,
		o.Sorted, o.HasDeltaLoc, o.CommitTS.ToString(),
		o.CreateTime.ToString(), o.DeleteTime.ToString())
}

func (o ObjectInfo) Location() objectio.Location {
	return o.ObjectLocation()
}

type ObjectEntry struct {
	ObjectInfo
}

func (o ObjectEntry) Compare(than ObjectEntry) int {
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:])
}

func (o ObjectEntry) IsEmpty() bool {
	return o.Size() == 0
}

func (o *ObjectEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(&o.DeleteTime))
}

func (o ObjectEntry) Location() objectio.Location {
	return o.ObjectLocation()
}

func (o ObjectInfo) StatsValid() bool {
	return o.Rows() != 0
}

type ObjectIndexByCreateTSEntry struct {
	ObjectInfo
}

func (o ObjectIndexByCreateTSEntry) Compare(to ObjectIndexByCreateTSEntry) int {
	// desc
	if o.CreateTime.GreaterEq(&to.CreateTime) {
		return -1
	}
	if to.CreateTime.GreaterEq(&o.CreateTime) {
		return 1
	}
	// unique
	return bytes.Compare(o.ObjectShortName()[:], to.ObjectShortName()[:])
}

func (o *ObjectIndexByCreateTSEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(&ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(&o.DeleteTime))
}

type PrimaryIndexEntry struct {
	Bytes      []byte
	RowEntryID int64

	// fields for validating
	BlockID types.Blockid
	RowID   types.Rowid
	Time    types.TS
}

func (p *PrimaryIndexEntry) Compare(p2 *PrimaryIndexEntry) int {
	if res := bytes.Compare(p.Bytes, p2.Bytes); res != 0 {
		return res
	}
	if p.RowEntryID < p2.RowEntryID {
		return -1
	} else if p.RowEntryID > p2.RowEntryID {
		return 1
	}
	return 0
}

type ObjectIndexByTSEntry struct {
	Time         types.TS // insert or delete time
	ShortObjName objectio.ObjectNameShort

	IsDelete     bool
	IsAppendable bool
}

func (b ObjectIndexByTSEntry) Less(than ObjectIndexByTSEntry) bool {
	// asc
	if b.Time.Less(&than.Time) {
		return true
	}
	if than.Time.Less(&b.Time) {
		return false
	}

	cmp := bytes.Compare(b.ShortObjName[:], than.ShortObjName[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}

	//if b.IsDelete && !than.IsDelete {
	//	return true
	//}
	//if !b.IsDelete && than.IsDelete {
	//	return false
	//}

	return false
}

func NewPartitionState(noData bool) *PartitionState {
	opts := btree.Options{
		Degree: 64,
	}
	return &PartitionState{
		prioritySource:  pt.NewPrioritySource(),
		noData:          noData,
		objectIndexByTS: btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		prioritySource:        pt.NewPrioritySource(),
		rows:                  p.rows,
		dataObjects:           p.dataObjects,
		dataObjectsByCreateTS: p.dataObjectsByCreateTS,
		blockDeltas:           p.blockDeltas,
		primaryIndex:          p.primaryIndex,
		noData:                p.noData,
		dirtyBlocks:           p.dirtyBlocks,
		objectIndexByTS:       p.objectIndexByTS.Copy(),
		shared:                p.shared,
	}
	if len(p.checkpoints) > 0 {
		state.checkpoints = make([]string, len(p.checkpoints))
		copy(state.checkpoints, p.checkpoints)
	}
	return &state
}

func (p *PartitionState) RowExists(rowID types.Rowid, ts types.TS) bool {
	iter := p.rows.NewIter()
	defer iter.Close()

	blockID := rowID.CloneBlockID()
	for entry, ok := iter.Seek(RowEntry{
		BlockID: blockID,
		RowID:   rowID,
		Time:    ts,
	}); ok; entry, ok = iter.Next() {
		if entry.BlockID != blockID {
			break
		}
		if entry.RowID != rowID {
			break
		}
		if entry.Time.Greater(&ts) {
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

func (p *PartitionState) HandleLogtailEntry(
	ctx context.Context,
	fs fileservice.FileService,
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleLogtailEntry")
	defer task.End()

	txnTrace.GetService().ApplyLogtail(entry, 1)
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsBlkTable(entry.TableName) {
			p.HandleMetadataInsert(ctx, fs, entry.Bat)
		} else if IsObjTable(entry.TableName) {
			p.HandleObjectInsert(ctx, entry.Bat, fs)
		} else {
			p.HandleRowsInsert(ctx, entry.Bat, primarySeqnum, packer)
		}
	case api.Entry_Delete:
		if IsBlkTable(entry.TableName) {
			p.HandleMetadataDelete(ctx, entry.TableId, entry.Bat)
		} else if IsObjTable(entry.TableName) {
			p.HandleObjectDelete(entry.TableId, entry.Bat)
		} else {
			p.HandleRowsDelete(ctx, entry.Bat, packer)
		}
	default:
		panic("unknown entry type")
	}
}

func (p *PartitionState) HandleObjectDelete(
	tableID uint64,
	bat *api.Batch) {
	statsVec := mustVectorFromProto(bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[8]))
	commitTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[11]))

	for idx := 0; idx < len(stateCol); idx++ {
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))

		if objEntry.ObjectStats.BlkCnt() == 0 || objEntry.ObjectStats.Rows() == 0 {
			continue
		}

		objEntry.EntryState = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]
		p.objectDeleteHelper(tableID, objEntry, deleteTSCol[idx])
	}
}

func (p *PartitionState) HandleObjectInsert(ctx context.Context, bat *api.Batch, fs fileservice.FileService) {

	var numDeleted, blockDeleted, scanCnt int64
	statsVec := mustVectorFromProto(bat.Vecs[2])
	stateCol := vector.MustFixedCol[bool](mustVectorFromProto(bat.Vecs[3]))
	sortedCol := vector.MustFixedCol[bool](mustVectorFromProto(bat.Vecs[4]))
	createTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[7]))
	deleteTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[8]))
	startTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[9]))
	commitTSCol := vector.MustFixedCol[types.TS](mustVectorFromProto(bat.Vecs[11]))

	for idx := 0; idx < len(stateCol); idx++ {
		p.shared.Lock()
		if t := commitTSCol[idx]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()
		var objEntry ObjectEntry

		objEntry.ObjectStats = objectio.ObjectStats(statsVec.GetBytesAt(idx))
		if objEntry.ObjectStats.BlkCnt() == 0 || objEntry.ObjectStats.Rows() == 0 {
			logutil.Errorf("skip empty object stats when HandleObjectInsert, %s\n", objEntry.String())
			continue
		}

		objEntry.EntryState = stateCol[idx]
		objEntry.CreateTime = createTSCol[idx]
		objEntry.DeleteTime = deleteTSCol[idx]
		objEntry.CommitTS = commitTSCol[idx]
		objEntry.Sorted = sortedCol[idx]

		old, exist := p.dataObjects.Get(objEntry)
		if exist {
			objEntry.HasDeltaLoc = old.HasDeltaLoc
		}
		if exist && !old.IsEmpty() {
			// why check the deleteTime here? consider this situation:
			// 		1. insert on an object, then these insert operations recorded into a CKP.
			// 		2. and delete this object, this operation recorded into WAL.
			// 		3. restart
			// 		4. replay CKP(lazily) into partition state --> replay WAL into partition state
			// the delete record in WAL could be overwritten by insert record in CKP,
			// causing logic err of the objects' visibility(dead object back to life!!).
			//
			// if this happened, just skip this object will be fine, why chose to
			// update the object Stats and leave others unchanged?
			//
			// in single txn, the pushed log tail has orders: meta insert, object insert.
			// as long as delta location generated, there will be meta insert followed by object insert pushed to cn.
			// in the normal case, the handleMetaInsert will construct objects with empty stats(rows = 0)
			// and will be updated by HandleObjectInsert later. if we skip this object in such case (non-above situation),
			// the object stats will be remained empty, has potential impact on where the stats.rows be used.
			//
			// so the final logic is that only update the object stats
			// when an object already exists in the partition state and has the deleteTime value.
			if !old.DeleteTime.IsEmpty() {
				// leave these field unchanged
				objEntry.DeleteTime = old.DeleteTime
				objEntry.CommitTS = old.CommitTS
				objEntry.EntryState = old.EntryState
				objEntry.CreateTime = old.CreateTime
				objEntry.Sorted = old.Sorted

				// only update object stats
			}
		} else {
			e := ObjectIndexByTSEntry{
				Time:         createTSCol[idx],
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     false,

				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(e)
		}
		//prefetch the object meta
		if err := blockio.PrefetchMeta(fs, objEntry.Location()); err != nil {
			logutil.Errorf("prefetch object meta failed. %v", err)
		}

		p.dataObjects, _ = p.dataObjects.Upsert(objEntry, p.prioritySource(), false)
		p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Upsert(ObjectIndexByCreateTSEntry(objEntry), p.prioritySource(), false)
		{
			//Need to insert an entry in objectIndexByTS, when soft delete appendable object.
			e := ObjectIndexByTSEntry{
				ShortObjName: *objEntry.ObjectShortName(),

				IsAppendable: objEntry.EntryState,
			}
			if !deleteTSCol[idx].IsEmpty() {
				e.Time = deleteTSCol[idx]
				e.IsDelete = true
				p.objectIndexByTS.Set(e)
			}
		}

		if objEntry.EntryState && objEntry.DeleteTime.IsEmpty() {
			panic("logic error")
		}
		// for appendable object, gc rows when delete object
		objID := objEntry.ObjectStats.ObjectName().ObjectId()
		trunctPoint := startTSCol[idx]
		blkCnt := objEntry.BlkCnt()
		for i := uint32(0); i < blkCnt; i++ {

			blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
			pivot := RowEntry{
				// aobj has only one blk
				BlockID: *blkID,
			}
			iter := p.rows.NewIter()
			for entry, ok := iter.Seek(pivot); ok; entry, ok = iter.Next() {
				if entry.BlockID != *blkID {
					break
				}
				scanCnt++

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if objEntry.EntryState {
					if entry.Time.LessEq(&trunctPoint) {
						// delete the row
						p.rows, _ = p.rows.Remove(entry, false)

						// delete the row's primary index
						if objEntry.EntryState && len(entry.PrimaryIndexBytes) > 0 {
							p.primaryIndex, _ = p.primaryIndex.Remove(&PrimaryIndexEntry{
								Bytes:      entry.PrimaryIndexBytes,
								RowEntryID: entry.ID,
							}, false)
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
				if !objEntry.EntryState && !objEntry.HasDeltaLoc {
					p.dirtyBlocks, _ = p.dirtyBlocks.Upsert(entry.BlockID, p.prioritySource(), false)
					break
				}
			}
			iter.Close()

			// if there are no rows for the block, delete the block from the dirty
			if objEntry.EntryState && scanCnt == blockDeleted && p.dirtyBlocks.Length() > 0 {
				p.dirtyBlocks, _ = p.dirtyBlocks.Remove(*blkID, false)
			}
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

var nextRowEntryID = int64(1)

func (p *PartitionState) HandleRowsInsert(
	ctx context.Context,
	input *api.Batch,
	primarySeqnum int,
	packer *types.Packer,
) (
	primaryKeys [][]byte,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsInsert")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}
	primaryKeys = EncodePrimaryKeyVector(
		batch.Vecs[2+primarySeqnum],
		packer,
	)

	var newRows []RowEntry
	var newPrimaryIndex []*PrimaryIndexEntry
	useUnion := len(rowIDVector) > 8
	if useUnion {
		newRows = make([]RowEntry, 0, len(rowIDVector))
		newPrimaryIndex = make([]*PrimaryIndexEntry, 0, len(rowIDVector))
	}

	var numInserted int64
	for i, rowID := range rowIDVector {

		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
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
		if useUnion {
			newRows = append(newRows, entry)
		} else {
			p.rows, _ = p.rows.Upsert(entry, p.prioritySource(), false)
		}

		{
			entry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			if useUnion {
				newPrimaryIndex = append(newPrimaryIndex, entry)
			} else {
				p.primaryIndex, _ = p.primaryIndex.Upsert(entry, p.prioritySource(), false)
			}
		}
	}

	p.rows = p.rows.Union(pt.Build(p.prioritySource, sortUnique(newRows, RowEntry.Compare)), false)
	p.primaryIndex = p.primaryIndex.Union(pt.Build(p.prioritySource, sortUnique(newPrimaryIndex, (*PrimaryIndexEntry).Compare)), false)

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
	})

	return
}

func (p *PartitionState) HandleRowsDelete(
	ctx context.Context,
	input *api.Batch,
	packer *types.Packer,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsDelete")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}

	var primaryKeys [][]byte
	if len(input.Vecs) > 2 {
		// has primary key
		primaryKeys = EncodePrimaryKeyVector(
			batch.Vecs[2],
			packer,
		)
	}

	numDeletes := int64(0)
	for i, rowID := range rowIDVector {

		blockID := rowID.CloneBlockID()
		pivot := RowEntry{
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
		p.rows, _ = p.rows.Upsert(entry, p.prioritySource(), false)

		//handle memory deletes for non-appendable block.
		p.dirtyBlocks, _ = p.dirtyBlocks.Upsert(blockID, p.prioritySource(), false)

		// primary key
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			entry := &PrimaryIndexEntry{
				Bytes:      primaryKeys[i],
				RowEntryID: entry.ID,
				BlockID:    blockID,
				RowID:      rowID,
				Time:       entry.Time,
			}
			p.primaryIndex, _ = p.primaryIndex.Upsert(entry, p.prioritySource(), false)
		}

	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeletes)
	})
}

func (p *PartitionState) HandleMetadataInsert(
	ctx context.Context,
	fs fileservice.FileService,
	input *api.Batch,
) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleMetadataInsert")
	defer task.End()

	createTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	blockIDVector := vector.MustFixedCol[types.Blockid](mustVectorFromProto(input.Vecs[2]))
	entryStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[3]))
	sortedStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[4]))
	metaLocationVector := mustVectorFromProto(input.Vecs[5])
	deltaLocationVector := mustVectorFromProto(input.Vecs[6])
	commitTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[7]))
	//segmentIDVector := vector.MustFixedCol[types.Uuid](mustVectorFromProto(input.Vecs[8]))
	memTruncTSVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[9]))

	var rowsToDelete []RowEntry
	var primaryKeysToDelete []*PrimaryIndexEntry
	var numInserted, numDeleted int64
	for i, blockID := range blockIDVector {
		p.shared.Lock()
		if t := commitTimeVector[i]; t.Greater(&p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()

		pivot := BlockDeltaEntry{
			BlockID: blockID,
		}
		blockEntry, ok := p.blockDeltas.Get(pivot)
		if !ok {
			blockEntry = pivot
			numInserted++
		} else if blockEntry.CommitTs.GreaterEq(&commitTimeVector[i]) {
			// it possible to get an older version blk from lazy loaded checkpoint
			return
		}

		// the following codes handle block which be inserted or updated by a newer delta location.
		// Notice that only delta location can be updated by a newer delta location.
		if location := objectio.Location(deltaLocationVector.GetBytesAt(i)); !location.IsEmpty() {
			blockEntry.DeltaLoc = *(*[objectio.LocationLen]byte)(unsafe.Pointer(&location[0]))
		}
		if t := commitTimeVector[i]; !t.IsEmpty() {
			blockEntry.CommitTs = t
		}

		isAppendable := entryStateVector[i]
		isEmptyDelta := blockEntry.DeltaLocation().IsEmpty()

		if !isEmptyDelta {
			p.blockDeltas, _ = p.blockDeltas.Upsert(blockEntry, p.prioritySource(), false)
		}

		{
			scanCnt := int64(0)
			blockDeleted := int64(0)
			trunctPoint := memTruncTSVector[i]
			iter := p.rows.NewIter()
			pivot := RowEntry{
				BlockID: blockID,
			}
			for entry, ok := iter.Seek(pivot); ok; entry, ok = iter.Next() {
				if entry.BlockID != blockID {
					break
				}
				scanCnt++
				//it's tricky here.
				//Due to consuming lazily the checkpoint,
				//we have to take the following scenario into account:
				//1. CN receives deletes for a non-appendable block from the log tail,
				//   then apply the deletes into PartitionState.rows.
				//2. CN receives block meta of the above non-appendable block to be inserted
				//   from the checkpoint, then apply the block meta into PartitionState.blocks.
				// So , if the above scenario happens, we need to set the non-appendable block into
				// PartitionState.dirtyBlocks.
				if !isAppendable && isEmptyDelta {
					p.dirtyBlocks, _ = p.dirtyBlocks.Upsert(blockID, p.prioritySource(), false)
					break
				}

				// if the inserting block is appendable, need to delete the rows for it;
				// if the inserting block is non-appendable and has delta location, need to delete
				// the deletes for it.
				if isAppendable || (!isAppendable && !isEmptyDelta) {
					if entry.Time.LessEq(&trunctPoint) {
						// delete the row
						rowsToDelete = append(rowsToDelete, entry)

						// delete the row's primary index
						if isAppendable && len(entry.PrimaryIndexBytes) > 0 {
							primaryKeysToDelete = append(primaryKeysToDelete, &PrimaryIndexEntry{
								Bytes:      entry.PrimaryIndexBytes,
								RowEntryID: entry.ID,
							})
						}
						numDeleted++
						blockDeleted++
					}
				}
			}
			iter.Close()

			// if there are no rows for the block, delete the block from the dirty
			if scanCnt == blockDeleted && p.dirtyBlocks.Length() > 0 {
				p.dirtyBlocks, _ = p.dirtyBlocks.Remove(blockID, false)
			}
		}

		//create object by block insert to set objEntry.HasDeltaLoc
		//when lazy load, maybe deltalocation is consumed before object is created
		{
			objPivot := ObjectEntry{}
			if metaLoc := objectio.Location(metaLocationVector.GetBytesAt(i)); !metaLoc.IsEmpty() {
				objectio.SetObjectStatsLocation(&objPivot.ObjectStats, metaLoc)
			} else {
				// After block is removed,
				// HandleMetadataInsert only handle deltaloc.
				// Meta location is empty.
				objID := blockID.Object()
				objName := objectio.BuildObjectNameWithObjectID(objID)
				objectio.SetObjectStatsObjectName(&objPivot.ObjectStats, objName)
			}
			objEntry, ok := p.dataObjects.Get(objPivot)
			if ok {
				// don't need to update objEntry, except for HasDeltaLoc and blkCnt
				if !isEmptyDelta {
					objEntry.HasDeltaLoc = true
				}

				blkCnt := blockID.Sequence() + 1
				if uint32(blkCnt) > objEntry.BlkCnt() {
					objectio.SetObjectStatsBlkCnt(&objEntry.ObjectStats, uint32(blkCnt))
				}
				p.dataObjects, _ = p.dataObjects.Upsert(objEntry, p.prioritySource(), false)
				// For deltaloc batch after block is removed,
				// objEntry.CreateTime is empty.
				// and it's temporary.
				// Related dataObjectsByCreateTS will be set in HandleObjectInsert.
				if !objEntry.CreateTime.IsEmpty() {
					p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Upsert(ObjectIndexByCreateTSEntry(objEntry), p.prioritySource(), false)
				}
				return
			}
			objEntry = objPivot
			objEntry.EntryState = entryStateVector[i]
			objEntry.Sorted = sortedStateVector[i]
			if !isEmptyDelta {
				objEntry.HasDeltaLoc = true
			}
			objEntry.CommitTS = commitTimeVector[i]
			createTS := createTimeVector[i]
			// after blk is removed, create ts is empty
			if !createTS.IsEmpty() {
				objEntry.CreateTime = createTS
			}

			blkCnt := blockID.Sequence() + 1
			if uint32(blkCnt) > objEntry.BlkCnt() {
				objectio.SetObjectStatsBlkCnt(&objEntry.ObjectStats, uint32(blkCnt))
			}

			p.dataObjects, _ = p.dataObjects.Upsert(objEntry, p.prioritySource(), false)

			if !objEntry.CreateTime.IsEmpty() {
				p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Upsert(ObjectIndexByCreateTSEntry(objEntry), p.prioritySource(), false)
			}

			{
				e := ObjectIndexByTSEntry{
					Time:         createTimeVector[i],
					ShortObjName: *objEntry.ObjectShortName(),
					IsDelete:     false,

					IsAppendable: objEntry.EntryState,
				}
				p.objectIndexByTS.Set(e)
			}
		}

	}

	p.rows = p.rows.BulkRemove(pt.Build(p.prioritySource, sortUnique(rowsToDelete, RowEntry.Compare)), false)
	p.primaryIndex = p.primaryIndex.BulkRemove(pt.Build(p.prioritySource, sortUnique(primaryKeysToDelete, (*PrimaryIndexEntry).Compare)), false)

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.MetadataInsertEntries.Add(1)
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
		c.DistTAE.Logtail.InsertBlocks.Add(numInserted)
	})
}

func (p *PartitionState) objectDeleteHelper(
	tableID uint64,
	pivot ObjectEntry,
	deleteTime types.TS) {
	objEntry, ok := p.dataObjects.Get(pivot)
	//TODO non-appendable block' delete maybe arrive before its insert?
	if !ok {
		panic(fmt.Sprintf("invalid block id. %v", pivot.String()))
	}

	if objEntry.DeleteTime.IsEmpty() {
		// apply first delete
		objEntry.DeleteTime = deleteTime
		p.dataObjects, _ = p.dataObjects.Upsert(objEntry, p.prioritySource(), false)
		p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Upsert(ObjectIndexByCreateTSEntry(objEntry), p.prioritySource(), false)

		{
			e := ObjectIndexByTSEntry{
				Time:         objEntry.DeleteTime,
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     true,

				IsAppendable: objEntry.EntryState,
			}
			txnTrace.GetService().ApplyDeleteObject(
				tableID,
				objEntry.DeleteTime.ToTimestamp(),
				"",
				"delete-object")
			p.objectIndexByTS.Set(e)
		}
	} else {
		// update deletetime, if incoming delete ts is less
		if objEntry.DeleteTime.Greater(&deleteTime) {
			old := ObjectIndexByTSEntry{
				Time:         objEntry.DeleteTime,
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     true,

				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Delete(old)
			objEntry.DeleteTime = deleteTime
			p.dataObjects, _ = p.dataObjects.Upsert(objEntry, p.prioritySource(), false)
			p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Upsert(ObjectIndexByCreateTSEntry(objEntry), p.prioritySource(), false)

			new := ObjectIndexByTSEntry{
				Time:         objEntry.DeleteTime,
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     true,

				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(new)
		} else if objEntry.DeleteTime.Equal(&deleteTime) {
			//FIXME:: should we do something here?
			e := ObjectIndexByTSEntry{
				Time:         objEntry.DeleteTime,
				ShortObjName: *objEntry.ObjectShortName(),
				IsDelete:     true,

				IsAppendable: objEntry.EntryState,
			}
			p.objectIndexByTS.Set(e)
		}
	}
}

func (p *PartitionState) HandleMetadataDelete(
	ctx context.Context,
	tableID uint64,
	input *api.Batch) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleMetadataDelete")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	deleteTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))

	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		pivot := ObjectEntry{}
		objectio.SetObjectStatsShortName(&pivot.ObjectStats, objectio.ShortName(&blockID))
		p.objectDeleteHelper(tableID, pivot, deleteTimeVector[i])
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.MetadataDeleteEntries.Add(1)
	})
}

func (p *PartitionState) AppendCheckpoint(checkpoint string, partiton *Partition) {
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

func (p *PartitionState) truncate(ids [2]uint64, ts types.TS) {
	if p.minTS.Greater(&ts) {
		logutil.Errorf("logic error: current minTS %v, incoming ts %v", p.minTS.ToString(), ts.ToString())
		return
	}
	p.minTS = ts
	gced := false
	pivot := ObjectIndexByTSEntry{
		Time:         ts.Next(),
		ShortObjName: objectio.ObjectNameShort{},
		IsDelete:     true,
	}
	iter := p.objectIndexByTS.Copy().Iter()
	ok := iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	objIDsToDelete := make(map[objectio.ObjectNameShort]struct{}, 0)
	objectsToDelete := ""
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if entry.IsDelete {
			objIDsToDelete[entry.ShortObjName] = struct{}{}
			if gced {
				objectsToDelete = fmt.Sprintf("%s, %v", objectsToDelete, entry.ShortObjName)
			} else {
				objectsToDelete = fmt.Sprintf("%s%v", objectsToDelete, entry.ShortObjName)
			}
			gced = true
		}
	}
	iter = p.objectIndexByTS.Copy().Iter()
	ok = iter.Seek(pivot)
	if !ok {
		ok = iter.Last()
	}
	for ; ok; ok = iter.Prev() {
		entry := iter.Item()
		if entry.Time.Greater(&ts) {
			continue
		}
		if _, ok := objIDsToDelete[entry.ShortObjName]; ok {
			p.objectIndexByTS.Delete(entry)
		}
	}
	if gced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objectsToDelete)
	}

	objsToDelete := ""
	objIter := p.dataObjects.NewIter()
	objGced := false

	for {
		objEntry, ok := objIter.Next()
		if !ok {
			break
		}

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LessEq(&ts) {
			p.dataObjects, _ = p.dataObjects.Remove(objEntry, false)
			//p.dataObjectsByCreateTS.Delete(ObjectIndexByCreateTSEntry{
			//	//CreateTime:   objEntry.CreateTime,
			//	//ShortObjName: objEntry.ShortObjName,
			//	ObjectInfo: objEntry.ObjectInfo,
			//})
			p.dataObjectsByCreateTS, _ = p.dataObjectsByCreateTS.Remove(ObjectIndexByCreateTSEntry(objEntry), false)
			if objGced {
				objsToDelete = fmt.Sprintf("%s, %s", objsToDelete, objEntry.Location().Name().String())
			} else {
				objsToDelete = fmt.Sprintf("%s%s", objsToDelete, objEntry.Location().Name().String())
			}
			objGced = true
		}
	}
	if objGced {
		logutil.Infof("GC partition_state at %v for table %d:%s", ts.ToString(), ids[1], objsToDelete)
	}
}

func (p *PartitionState) LastFlushTimestamp() types.TS {
	p.shared.Lock()
	defer p.shared.Unlock()
	return p.shared.lastFlushTimestamp
}
