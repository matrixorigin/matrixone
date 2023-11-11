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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"net/http"
	"runtime/trace"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moprobe"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/tidwall/btree"
)

var partitionStateProfileHandler = fileservice.NewProfileHandler()

func init() {
	http.Handle("/debug/cn-partition-state", partitionStateProfileHandler)
}

type PartitionState struct {
	// also modify the Copy method if adding fields

	// data
	rows *btree.BTreeG[RowEntry] // use value type to avoid locking on elements
	//table data objects
	dataObjects           *btree.BTreeG[ObjectEntry]
	dataObjectsByCreateTS *btree.BTreeG[ObjectIndexByCreateTSEntry]
	//TODO:: It's transient, should be removed in future PR.
	blockDeltas *btree.BTreeG[BlockDeltaEntry]
	checkpoints []string

	// index
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
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

func (r RowEntry) Less(than RowEntry) bool {
	// asc
	cmp := r.BlockID.Compare(than.BlockID)
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	// asc
	if r.RowID.Less(than.RowID) {
		return true
	}
	if than.RowID.Less(r.RowID) {
		return false
	}
	// desc
	if than.Time.Less(r.Time) {
		return true
	}
	if r.Time.Less(than.Time) {
		return false
	}
	return false
}

type BlockEntry struct {
	catalog.BlockInfo

	CreateTime types.TS
	DeleteTime types.TS
}

func (b BlockEntry) Less(than BlockEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
}

type BlockDeltaEntry struct {
	BlockID types.Blockid

	CommitTs types.TS
	DeltaLoc catalog.ObjectLocation
}

func (b BlockDeltaEntry) Less(than BlockDeltaEntry) bool {
	return b.BlockID.Compare(than.BlockID) < 0
}

func (b BlockDeltaEntry) DeltaLocation() objectio.Location {
	return b.DeltaLoc[:]
}

type ObjectInfo struct {
	Loc         objectio.Location
	EntryState  bool
	Sorted      bool
	HasDeltaLoc bool
	SegmentID   types.Uuid
	CommitTS    types.TS
	CreateTime  types.TS
	DeleteTime  types.TS
	BlkCnt      uint16
}

func (o ObjectInfo) Location() objectio.Location {
	return o.Loc
}

type ObjectEntry struct {
	ShortObjName objectio.ObjectNameShort

	ObjectInfo
}

func (o ObjectEntry) Less(than ObjectEntry) bool {
	return bytes.Compare(o.ShortObjName[:], than.ShortObjName[:]) < 0
}

func (o *ObjectEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(o.DeleteTime))
}

func (o ObjectEntry) Location() objectio.Location {
	return o.Loc
}

type ObjectIndexByCreateTSEntry struct {
	CreateTime   types.TS
	ShortObjName objectio.ObjectNameShort

	ObjectInfo
}

func (o ObjectIndexByCreateTSEntry) Less(than ObjectIndexByCreateTSEntry) bool {
	//asc
	if o.CreateTime.Less(than.CreateTime) {

		return true
	}
	if than.CreateTime.Less(o.CreateTime) {
		return false
	}

	cmp := bytes.Compare(o.ShortObjName[:], than.ShortObjName[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}
	return false
}

func (o *ObjectIndexByCreateTSEntry) Visible(ts types.TS) bool {
	return o.CreateTime.LessEq(ts) &&
		(o.DeleteTime.IsEmpty() || ts.Less(o.DeleteTime))
}

type PrimaryIndexEntry struct {
	Bytes      []byte
	RowEntryID int64

	// fields for validating
	BlockID types.Blockid
	RowID   types.Rowid
	Time    types.TS
}

func (p *PrimaryIndexEntry) Less(than *PrimaryIndexEntry) bool {
	if res := bytes.Compare(p.Bytes, than.Bytes); res < 0 {
		return true
	} else if res > 0 {
		return false
	}
	return p.RowEntryID < than.RowEntryID
}

type ObjectIndexByTSEntry struct {
	Time         types.TS // insert or delete time
	ShortObjName objectio.ObjectNameShort
	IsDelete     bool

	IsAppendable bool
}

func (b ObjectIndexByTSEntry) Less(than ObjectIndexByTSEntry) bool {
	// asc
	if b.Time.Less(than.Time) {
		return true
	}
	if than.Time.Less(b.Time) {
		return false
	}

	cmp := bytes.Compare(b.ShortObjName[:], than.ShortObjName[:])
	if cmp < 0 {
		return true
	}
	if cmp > 0 {
		return false
	}

	if b.IsDelete && !than.IsDelete {
		return true
	}
	if !b.IsDelete && than.IsDelete {
		return false
	}

	return false
}

func NewPartitionState(noData bool) *PartitionState {
	opts := btree.Options{
		Degree: 4,
	}
	return &PartitionState{
		noData:                noData,
		rows:                  btree.NewBTreeGOptions((RowEntry).Less, opts),
		dataObjects:           btree.NewBTreeGOptions((ObjectEntry).Less, opts),
		dataObjectsByCreateTS: btree.NewBTreeGOptions((ObjectIndexByCreateTSEntry).Less, opts),
		blockDeltas:           btree.NewBTreeGOptions((BlockDeltaEntry).Less, opts),
		primaryIndex:          btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		dirtyBlocks:           btree.NewBTreeGOptions((types.Blockid).Less, opts),
		objectIndexByTS:       btree.NewBTreeGOptions((ObjectIndexByTSEntry).Less, opts),
		shared:                new(sharedStates),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		rows:                  p.rows.Copy(),
		dataObjects:           p.dataObjects.Copy(),
		dataObjectsByCreateTS: p.dataObjectsByCreateTS.Copy(),
		blockDeltas:           p.blockDeltas.Copy(),
		primaryIndex:          p.primaryIndex.Copy(),
		noData:                p.noData,
		dirtyBlocks:           p.dirtyBlocks.Copy(),
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
	iter := p.rows.Iter()
	defer iter.Release()

	blockID := rowID.CloneBlockID()
	for ok := iter.Seek(RowEntry{
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
		if entry.Time.Greater(ts) {
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
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsBlkTable(entry.TableName) {
			p.HandleMetadataInsert(ctx, fs, entry.Bat)
		} else {
			p.HandleRowsInsert(ctx, entry.Bat, primarySeqnum, packer)
		}
	case api.Entry_Delete:
		if IsBlkTable(entry.TableName) {
			p.HandleMetadataDelete(ctx, entry.Bat)
		} else if IsSegTable(entry.TableName) {
			// TODO p.HandleSegDelete(ctx, entry.Bat)
		} else {
			p.HandleRowsDelete(ctx, entry.Bat, packer)
		}
	default:
		panic("unknown entry type")
	}
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

	t0 := time.Now()
	defer func() {
		partitionStateProfileHandler.AddSample(time.Since(t0))
	}()

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
		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleInsert, func() {

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
			p.rows.Set(entry)

			{
				entry := &PrimaryIndexEntry{
					Bytes:      primaryKeys[i],
					RowEntryID: entry.ID,
					BlockID:    blockID,
					RowID:      rowID,
					Time:       entry.Time,
				}
				p.primaryIndex.Set(entry)
			}

		})
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

	t0 := time.Now()
	defer func() {
		partitionStateProfileHandler.AddSample(time.Since(t0))
	}()

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
		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleDel, func() {

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
			p.rows.Set(entry)

			//handle memory deletes for non-appendable block.
			p.dirtyBlocks.Set(blockID)

			// primary key
			if i < len(primaryKeys) && len(primaryKeys[i]) > 0 {
				entry := &PrimaryIndexEntry{
					Bytes:      primaryKeys[i],
					RowEntryID: entry.ID,
					BlockID:    blockID,
					RowID:      rowID,
					Time:       entry.Time,
				}
				p.primaryIndex.Set(entry)
			}

		})
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
	input *api.Batch) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleMetadataInsert")
	defer task.End()

	t0 := time.Now()
	defer func() {
		partitionStateProfileHandler.AddSample(time.Since(t0))
	}()

	createTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	blockIDVector := vector.MustFixedCol[types.Blockid](mustVectorFromProto(input.Vecs[2]))
	entryStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[3]))
	sortedStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[4]))
	metaLocationVector := mustVectorFromProto(input.Vecs[5])
	deltaLocationVector := mustVectorFromProto(input.Vecs[6])
	commitTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[7]))
	segmentIDVector := vector.MustFixedCol[types.Uuid](mustVectorFromProto(input.Vecs[8]))
	memTruncTSVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[9]))

	var numInserted, numDeleted int64
	for i, blockID := range blockIDVector {
		p.shared.Lock()
		if t := commitTimeVector[i]; t.Greater(p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()

		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleMetaInsert, func() {

			pivot := BlockDeltaEntry{
				BlockID: blockID,
			}
			blockEntry, ok := p.blockDeltas.Get(pivot)
			if !ok {
				blockEntry = pivot
				numInserted++
			} else if blockEntry.CommitTs.GreaterEq(commitTimeVector[i]) {
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
				p.blockDeltas.Set(blockEntry)
			}

			{
				scanCnt := int64(0)
				trunctPoint := memTruncTSVector[i]
				iter := p.rows.Copy().Iter()
				pivot := RowEntry{
					BlockID: blockID,
				}
				for ok := iter.Seek(pivot); ok; ok = iter.Next() {
					entry := iter.Item()
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
						p.dirtyBlocks.Set(blockID)
						break
					}

					// if the inserting block is appendable, need to delete the rows for it;
					// if the inserting block is non-appendable and has delta location, need to delete
					// the deletes for it.
					if isAppendable || (!isAppendable && !isEmptyDelta) {
						if entry.Time.LessEq(trunctPoint) {
							// delete the row
							p.rows.Delete(entry)

							// delete the row's primary index
							if isAppendable && len(entry.PrimaryIndexBytes) > 0 {
								p.primaryIndex.Delete(&PrimaryIndexEntry{
									Bytes:      entry.PrimaryIndexBytes,
									RowEntryID: entry.ID,
								})
							}
							numDeleted++
						}
					}
				}
				iter.Release()

				// if there are no rows for the block, delete the block from the dirty
				if scanCnt == numDeleted && p.dirtyBlocks.Len() > 0 {
					p.dirtyBlocks.Delete(blockID)
				}
			}

			//create object by block insert
			{
				objPivot := ObjectEntry{
					ShortObjName: *(objectio.Location(metaLocationVector.GetBytesAt(i)).ShortName()),
				}
				objEntry, ok := p.dataObjects.Get(objPivot)
				if ok {
					// don't need to update objEntry, except for HasDeltaLoc and blkCnt
					if !isEmptyDelta {
						objEntry.HasDeltaLoc = true
					}

					blkCnt := blockID.Sequence() + 1
					if blkCnt > objEntry.BlkCnt {
						objEntry.BlkCnt = blkCnt
					}

					p.dataObjects.Set(objEntry)
					p.dataObjectsByCreateTS.Set(ObjectIndexByCreateTSEntry{
						CreateTime:   objEntry.CreateTime,
						ShortObjName: objEntry.ShortObjName,

						ObjectInfo: objEntry.ObjectInfo,
					})
					return
				}
				objEntry = objPivot
				if metaLocation := objectio.Location(metaLocationVector.GetBytesAt(i)); !metaLocation.IsEmpty() {
					objEntry.Loc = metaLocation
				}
				objEntry.EntryState = entryStateVector[i]
				objEntry.Sorted = sortedStateVector[i]
				if !isEmptyDelta {
					objEntry.HasDeltaLoc = true
				}
				objEntry.SegmentID = segmentIDVector[i]
				objEntry.CommitTS = commitTimeVector[i]
				objEntry.CreateTime = createTimeVector[i]

				blkCnt := blockID.Sequence() + 1
				if blkCnt > objEntry.BlkCnt {
					objEntry.BlkCnt = blkCnt
				}

				p.dataObjects.Set(objEntry)

				//prefetch the object meta
				if err := blockio.PrefetchMeta(fs, objEntry.Loc); err != nil {
					logutil.Errorf("prefetch object meta failed. %v", err)
				}

				p.dataObjectsByCreateTS.Set(ObjectIndexByCreateTSEntry{
					CreateTime:   objEntry.CreateTime,
					ShortObjName: objEntry.ShortObjName,

					ObjectInfo: objEntry.ObjectInfo,
				})

				{
					e := ObjectIndexByTSEntry{
						Time:         createTimeVector[i],
						ShortObjName: objEntry.ShortObjName,
						IsDelete:     false,

						IsAppendable: objEntry.EntryState,
					}
					p.objectIndexByTS.Set(e)
				}
			}

		})
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.MetadataInsertEntries.Add(1)
		c.DistTAE.Logtail.ActiveRows.Add(-numDeleted)
		c.DistTAE.Logtail.InsertBlocks.Add(numInserted)
	})
}

func (p *PartitionState) HandleMetadataDelete(ctx context.Context, input *api.Batch) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleMetadataDelete")
	defer task.End()

	t0 := time.Now()
	defer func() {
		partitionStateProfileHandler.AddSample(time.Since(t0))
	}()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	deleteTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))

	for i, rowID := range rowIDVector {
		blockID := rowID.CloneBlockID()
		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleMetaDelete, func() {

			pivot := ObjectEntry{
				ShortObjName: *objectio.ShortName(&blockID),
			}
			objEntry, ok := p.dataObjects.Get(pivot)
			//TODO non-appendable block' delete maybe arrive before its insert?
			if !ok {
				panic(fmt.Sprintf("invalid block id. %x", rowID))
			}

			if objEntry.DeleteTime.IsEmpty() {
				// apply first delete
				objEntry.DeleteTime = deleteTimeVector[i]

				p.dataObjects.Set(objEntry)

				p.dataObjectsByCreateTS.Set(ObjectIndexByCreateTSEntry{
					CreateTime:   objEntry.CreateTime,
					ShortObjName: objEntry.ShortObjName,

					ObjectInfo: objEntry.ObjectInfo,
				})

				{
					e := ObjectIndexByTSEntry{
						Time:         objEntry.DeleteTime,
						ShortObjName: objEntry.ShortObjName,
						IsDelete:     true,

						IsAppendable: objEntry.EntryState,
					}
					p.objectIndexByTS.Set(e)
				}
			} else {
				// update deletetime, if incoming delete ts is less
				if objEntry.DeleteTime.Greater(deleteTimeVector[i]) {
					old := ObjectIndexByTSEntry{
						Time:         objEntry.DeleteTime,
						ShortObjName: objEntry.ShortObjName,
						IsDelete:     true,

						IsAppendable: objEntry.EntryState,
						//ObjectInfo:   objEntry.ObjectInfo,
					}
					p.objectIndexByTS.Delete(old)
					objEntry.DeleteTime = deleteTimeVector[i]
					p.dataObjects.Set(objEntry)

					p.dataObjectsByCreateTS.Set(ObjectIndexByCreateTSEntry{
						CreateTime:   objEntry.CreateTime,
						ShortObjName: objEntry.ShortObjName,

						ObjectInfo: objEntry.ObjectInfo,
					})

					new := ObjectIndexByTSEntry{
						Time:         objEntry.DeleteTime,
						ShortObjName: objEntry.ShortObjName,
						IsDelete:     true,

						IsAppendable: objEntry.EntryState,
						//ObjectInfo:   objEntry.ObjectInfo,
					}
					p.objectIndexByTS.Set(new)
				} else if objEntry.DeleteTime.Equal(deleteTimeVector[i]) {
					//FIXME:: should we do something here?
					e := ObjectIndexByTSEntry{
						Time:         objEntry.DeleteTime,
						ShortObjName: objEntry.ShortObjName,
						IsDelete:     true,

						IsAppendable: objEntry.EntryState,
					}
					p.objectIndexByTS.Set(e)
				}
			}
		})
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
	if p.minTS.Greater(ts) {
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
		if entry.Time.Greater(ts) {
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
		if entry.Time.Greater(ts) {
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

		if !objEntry.DeleteTime.IsEmpty() && objEntry.DeleteTime.LessEq(ts) {
			p.dataObjects.Delete(objEntry)
			p.dataObjectsByCreateTS.Delete(ObjectIndexByCreateTSEntry{
				CreateTime:   objEntry.CreateTime,
				ShortObjName: objEntry.ShortObjName,
			})
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
