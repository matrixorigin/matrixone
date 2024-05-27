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
	"time"
	"unsafe"

	"github.com/tidwall/btree"

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
)

type PartitionState struct {
	// also modify the Copy method if adding fields

	tombstones *btree.BTreeG[ObjTombstoneRowEntry]
	dataRows   *btree.BTreeG[ObjDataRowEntry]
	//TODO::build a index for dataRows by timestamp for truncate entry by ts when handle object/block insert.
	//dataRowsIndexByTS *btree.BTreeG[DataRowsIndexByTSEntry]

	//table data objects
	dataObjects *btree.BTreeG[ObjectEntry]
	//TODO:: It's transient, should be removed in future PR.
	blockDeltas *btree.BTreeG[BlockDeltaEntry]
	checkpoints []string
	start       types.TS
	end         types.TS

	//for non-appendable block's memory deletes, used to getting dirty
	// non-appendable blocks quickly.
	//TODO::remove it
	dirtyBlocks *btree.BTreeG[types.Blockid]
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

type ObjTombstoneRowEntry struct {
	ShortObjName objectio.ObjectNameShort

	//tombstones belong to this object.
	Tombstones *atomic.Pointer[btree.BTreeG[TombstoneRowEntry]]
}

func (o ObjTombstoneRowEntry) Less(than ObjTombstoneRowEntry) bool {
	return bytes.Compare(o.ShortObjName[:], than.ShortObjName[:]) < 0
}

func (o ObjTombstoneRowEntry) MutateTombstones() (*btree.BTreeG[TombstoneRowEntry], func()) {
	curTombstones := o.Tombstones.Load()
	tombstones := curTombstones.Copy()
	return tombstones, func() {
		if !o.Tombstones.CompareAndSwap(curTombstones, tombstones) {
			panic("concurrent mutation of tombstones")
		}
	}
}

type ObjDataRowEntry struct {
	ShortObjName objectio.ObjectNameShort

	DataRows *atomic.Pointer[btree.BTreeG[DataRowEntry]]
}

func (o ObjDataRowEntry) Less(than ObjDataRowEntry) bool {
	return bytes.Compare(o.ShortObjName[:], than.ShortObjName[:]) < 0
}

func (o ObjDataRowEntry) MutateRows() (*btree.BTreeG[DataRowEntry], func()) {
	curRows := o.DataRows.Load()
	rows := curRows.Copy()
	return rows, func() {
		if !o.DataRows.CompareAndSwap(curRows, rows) {
			panic("concurrent mutation of data rows")
		}
	}
}

type TombstoneRowEntry struct {
	BlockID types.Blockid
	Offset  uint32

	// PK maybe nil , in case of be compatible with old TN.
	PK   []byte
	Time types.TS
}

func (t TombstoneRowEntry) Less(than TombstoneRowEntry) bool {
	//asc
	cmp := t.BlockID.Compare(than.BlockID)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	return t.Offset < than.Offset
}

type DataRowEntry struct {
	//The same PK maybe been inserted and deleted ,then inserted.
	//PK + Time is unique.
	PK   []byte
	Time types.TS

	RowID  types.Rowid
	Batch  *batch.Batch
	Offset int64
}

func (d DataRowEntry) Less(than DataRowEntry) bool {
	//asc
	cmp := bytes.Compare(d.PK, than.PK)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	//asc
	return d.Time.Compare(&than.Time) < 0

	//desc
	//if than.Time.Less(&d.Time) {
	//	return true
	//}
	//if d.Time.Less(&than.Time) {
	//	return false
	//}
	//return false
}

type BlockEntry struct {
	objectio.BlockInfo

	CreateTime types.TS
	DeleteTime types.TS
}

func (b BlockEntry) Less(than BlockEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
}

type BlockDeltaEntry struct {
	BlockID types.Blockid

	CommitTs types.TS
	DeltaLoc objectio.ObjectLocation
}

func (b BlockDeltaEntry) Less(than BlockDeltaEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
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

func (o ObjectEntry) Less(than ObjectEntry) bool {
	return bytes.Compare((*o.ObjectShortName())[:], (*than.ObjectShortName())[:]) < 0
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
	return o.ObjectStats.Rows() != 0
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
		noData:          noData,
		tombstones:      btree.NewBTreeGOptions((ObjTombstoneRowEntry).Less, opts),
		dataObjects:     btree.NewBTreeGOptions((ObjectEntry).Less, opts),
		blockDeltas:     btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		dataRows:        btree.NewBTreeGOptions((ObjDataRowEntry).Less, opts),
		dirtyBlocks:     btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS: btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:          new(sharedStates),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		tombstones:      p.tombstones.Copy(),
		dataObjects:     p.dataObjects.Copy(),
		blockDeltas:     p.blockDeltas.Copy(),
		dataRows:        p.dataRows.Copy(),
		noData:          p.noData,
		dirtyBlocks:     p.dirtyBlocks.Copy(),
		objectIndexByTS: p.objectIndexByTS.Copy(),
		shared:          p.shared,
		start:           p.start,
		end:             p.end,
	}
	if len(p.checkpoints) > 0 {
		state.checkpoints = make([]string, len(p.checkpoints))
		copy(state.checkpoints, p.checkpoints)
	}
	return &state
}

func (p *PartitionState) HandleLogtailEntry(
	ctx context.Context,
	fs fileservice.FileService,
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
) {
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
			start := time.Now()
			p.HandleRowsDelete(ctx, entry.Bat, packer)
			if entry.TableName == "bugt2" {
				logutil.Infof("xxxx handle delete cost %v", time.Since(start))
			}
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

	var numDeleted int64
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

		p.dataObjects.Set(objEntry)
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
		if objEntry.EntryState {
			objID := objEntry.ObjectStats.ObjectName().ObjectId()
			trunctPoint := startTSCol[idx]
			blkCnt := objEntry.ObjectStats.BlkCnt()
			// aobj has only one blk
			for i := uint32(0); i < blkCnt; i++ {
				blkID := objectio.NewBlockidWithObjectID(objID, uint16(i))
				numDeleted += p.truncateDataRowsByBlk(*blkID, trunctPoint)
				numDeleted += p.truncateTombstonesByBlk(*blkID, trunctPoint)
			}
		}
	}
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
	})
}

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

	var numInserted int64
	for i, rowID := range rowIDVector {

		blockID := rowID.CloneBlockID()
		objPivot := ObjDataRowEntry{
			ShortObjName: *objectio.ShortName(&blockID),
		}
		objEntry, ok := p.dataRows.Get(objPivot)
		if !ok {
			objEntry = objPivot
			objEntry.DataRows = &atomic.Pointer[btree.BTreeG[DataRowEntry]]{}
			opts := btree.Options{
				Degree: 64,
			}
			objEntry.DataRows.Store(
				btree.NewBTreeGOptions(DataRowEntry.Less, opts))

			p.dataRows.Set(objEntry)
		}

		rows, done := objEntry.MutateRows()

		pivot := DataRowEntry{
			PK:   primaryKeys[i],
			Time: timeVector[i],
		}
		entry, ok := rows.Get(pivot)
		if !ok {
			entry = pivot
			numInserted++
		}

		entry.RowID = rowID

		if !p.noData {
			entry.Batch = batch
			entry.Offset = int64(i)
		}
		rows.Set(entry)

		done()

	}

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
		objPivot := ObjTombstoneRowEntry{
			ShortObjName: *objectio.ShortName(&blockID),
		}
		objEntry, ok := p.tombstones.Get(objPivot)
		if !ok {
			objEntry = objPivot
			objEntry.Tombstones = &atomic.Pointer[btree.BTreeG[TombstoneRowEntry]]{}
			opts := btree.Options{
				Degree: 64,
			}
			objEntry.Tombstones.Store(
				btree.NewBTreeGOptions(TombstoneRowEntry.Less, opts))

			p.tombstones.Set(objEntry)
		}

		tombstones, done := objEntry.MutateTombstones()
		pivot := TombstoneRowEntry{
			BlockID: blockID,
			Offset:  rowID.GetRowOffset(),
		}
		entry, ok := tombstones.Get(pivot)
		if !ok {
			entry = pivot
			numDeletes++
		}
		entry.Time = timeVector[i]
		if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
			entry.PK = primaryKeys[i]
		}
		tombstones.Set(entry)
		done()

		//handle memory deletes for non-appendable block.
		p.dirtyBlocks.Set(blockID)

	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
		c.DistTAE.Logtail.DeleteRows.Add(numDeletes)
	})
}

func (p *PartitionState) truncateDataRowsByBlk(
	bid types.Blockid,
	ts types.TS,
) (numDeleted int64) {
	//truncate deletes
	objPivot := ObjDataRowEntry{
		ShortObjName: *objectio.ShortName(&bid),
	}
	objIter := p.dataRows.Copy().Iter()
	defer objIter.Release()
	if ok := objIter.Seek(objPivot); !ok {
		return
	}
	item := objIter.Item()
	if !bytes.Equal(item.ShortObjName[:], objPivot.ShortObjName[:]) {
		return
	}

	rows, done := objIter.Item().MutateRows()

	iter := rows.Copy().Iter()
	defer iter.Release()

	for ok := iter.First(); ok; ok = iter.Next() {
		item := iter.Item()
		if *item.RowID.BorrowBlockID() != bid {
			continue
		}
		if item.Time.LessEq(&ts) {
			rows.Delete(item)
			numDeleted++
		}
	}

	done()

	return
}

func (p *PartitionState) truncateTombstonesByBlk(
	bid types.Blockid,
	ts types.TS) (numDeleted int64) {

	scanCnt := int64(0)
	blockDeleted := int64(0)

	//truncate deletes
	objPivot := ObjTombstoneRowEntry{
		ShortObjName: *objectio.ShortName(&bid),
	}
	objIter := p.tombstones.Copy().Iter()
	defer objIter.Release()
	if ok := objIter.Seek(objPivot); !ok {
		return
	}
	item := objIter.Item()
	if !bytes.Equal(item.ShortObjName[:], objPivot.ShortObjName[:]) {
		return
	}

	tombstones, done := item.MutateTombstones()
	iter := tombstones.Copy().Iter()
	defer iter.Release()
	pivot := TombstoneRowEntry{
		BlockID: bid,
	}
	for ok := iter.Seek(pivot); ok; ok = iter.Next() {
		entry := iter.Item()
		if entry.BlockID != bid {
			break
		}
		scanCnt++
		if entry.Time.LessEq(&ts) {
			tombstones.Delete(entry)
			numDeleted++
			blockDeleted++
		}
	}
	done()

	// if there are no rows for the block, delete the block from the dirty
	if scanCnt == blockDeleted && p.dirtyBlocks.Len() > 0 {
		p.dirtyBlocks.Delete(bid)
	}
	return
}

func (p *PartitionState) HandleMetadataInsert(
	ctx context.Context,
	fs fileservice.FileService,
	input *api.Batch) {
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
			continue
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
			p.blockDeltas.Set(blockEntry)
		}

		if isAppendable {
			numDeleted += p.truncateDataRowsByBlk(blockID, memTruncTSVector[i])
			numDeleted += p.truncateTombstonesByBlk(blockID, memTruncTSVector[i])
		} else if !isEmptyDelta {
			numDeleted += p.truncateTombstonesByBlk(blockID, memTruncTSVector[i])
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
				p.dataObjects.Set(objEntry)
				// For deltaloc batch after block is removed,
				// objEntry.CreateTime is empty.
				// and it's temporary.
				// Related dataObjectsByCreateTS will be set in HandleObjectInsert.
				//
				// the created ts index have been removed now
				//if !objEntry.CreateTime.IsEmpty() {
				//	p.dataObjectsByCreateTS.Set(ObjectIndexByCreateTSEntry(objEntry))
				//}
			} else {

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

				p.dataObjects.Set(objEntry)

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

	}

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
		p.dataObjects.Set(objEntry)

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
			p.dataObjects.Set(objEntry)

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

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.MetadataDeleteEntries.Add(1)
	})
}

func (p *PartitionState) CacheCkpDuration(
	start types.TS,
	end types.TS,
	partition *Partition) {
	if partition.checkpointConsumed.Load() {
		panic("checkpoints already consumed")
	}
	p.start = start
	p.end = end
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
	objIter := p.dataObjects.Copy().Iter()
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

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LessEq(&ts) {
			p.dataObjects.Delete(objEntry)
			//p.dataObjectsByCreateTS.Delete(ObjectIndexByCreateTSEntry{
			//	//CreateTime:   objEntry.CreateTime,
			//	//ShortObjName: objEntry.ShortObjName,
			//	ObjectInfo: objEntry.ObjectInfo,
			//})
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
