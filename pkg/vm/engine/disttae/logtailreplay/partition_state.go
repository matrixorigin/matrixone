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
	rows        *btree.BTreeG[RowEntry] // use value type to avoid locking on elements
	blocks      *btree.BTreeG[BlockEntry]
	checkpoints []string

	// index
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	//for non-appendable block's memory deletes, used to getting dirty
	// non-appendable blocks quickly.
	dirtyBlocks *btree.BTreeG[BlockEntry]
	//index for blocks by timestamp.
	//TODO gc entries
	blockIndexByTS *btree.BTreeG[BlockIndexByTSEntry]

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool

	// some data need to be shared between all states
	// should have been in the Partition structure, but doing that requires much more codes changes
	// so just put it here.
	shared *sharedStates
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

func (b *BlockEntry) Visible(ts types.TS) bool {
	return b.CreateTime.LessEq(ts) &&
		(b.DeleteTime.IsEmpty() || ts.Less(b.DeleteTime))
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

type BlockIndexByTSEntry struct {
	Time     types.TS // insert or delete time
	BlockID  types.Blockid
	IsDelete bool

	IsAppendable bool
}

func (b BlockIndexByTSEntry) Less(than BlockIndexByTSEntry) bool {
	// asc
	if b.Time.Less(than.Time) {
		return true
	}
	if than.Time.Less(b.Time) {
		return false
	}

	cmp := b.BlockID.Compare(than.BlockID)
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
		noData:         noData,
		rows:           btree.NewBTreeGOptions((RowEntry).Less, opts),
		blocks:         btree.NewBTreeGOptions((BlockEntry).Less, opts),
		primaryIndex:   btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
		dirtyBlocks:    btree.NewBTreeGOptions((BlockEntry).Less, opts),
		blockIndexByTS: btree.NewBTreeGOptions((BlockIndexByTSEntry).Less, opts),
		shared:         new(sharedStates),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	state := PartitionState{
		rows:           p.rows.Copy(),
		blocks:         p.blocks.Copy(),
		primaryIndex:   p.primaryIndex.Copy(),
		noData:         p.noData,
		dirtyBlocks:    p.dirtyBlocks.Copy(),
		blockIndexByTS: p.blockIndexByTS.Copy(),
		shared:         p.shared,
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
	entry *api.Entry,
	primarySeqnum int,
	packer *types.Packer,
) {
	switch entry.EntryType {
	case api.Entry_Insert:
		if IsBlkTable(entry.TableName) {
			p.HandleMetadataInsert(ctx, entry.Bat)
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
	if primarySeqnum >= 0 {
		primaryKeys = EncodePrimaryKeyVector(
			batch.Vecs[2+primarySeqnum],
			packer,
		)
	}

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
			if i < len(primaryKeys) {
				entry.PrimaryIndexBytes = primaryKeys[i]
			}

			p.rows.Set(entry)

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
			}

			entry.Deleted = true
			if !p.noData {
				entry.Batch = batch
				entry.Offset = int64(i)
			}
			p.rows.Set(entry)

			//handle memory deletes for non-appendable block.
			bPivot := BlockEntry{
				BlockInfo: catalog.BlockInfo{
					BlockID: blockID,
				},
			}
			be, ok := p.blocks.Get(bPivot)
			if ok && !be.EntryState {
				p.dirtyBlocks.Set(be)
			}

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
	})
}

func (p *PartitionState) HandleMetadataInsert(ctx context.Context, input *api.Batch) {
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

	var numInserted, numDeleted int64
	for i, blockID := range blockIDVector {
		p.shared.Lock()
		if t := commitTimeVector[i]; t.Greater(p.shared.lastFlushTimestamp) {
			p.shared.lastFlushTimestamp = t
		}
		p.shared.Unlock()

		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleMetaInsert, func() {

			pivot := BlockEntry{
				BlockInfo: catalog.BlockInfo{
					BlockID: blockID,
				},
			}
			blockEntry, ok := p.blocks.Get(pivot)
			if !ok {
				blockEntry = pivot
				numInserted++
			}

			if location := objectio.Location(metaLocationVector.GetBytesAt(i)); !location.IsEmpty() {
				blockEntry.MetaLoc = *(*[objectio.LocationLen]byte)(unsafe.Pointer(&location[0]))
			}
			if location := objectio.Location(deltaLocationVector.GetBytesAt(i)); !location.IsEmpty() {
				blockEntry.DeltaLoc = *(*[objectio.LocationLen]byte)(unsafe.Pointer(&location[0]))
			}
			if id := segmentIDVector[i]; objectio.IsEmptySegid(&id) {
				blockEntry.SegmentID = id
			}
			blockEntry.Sorted = sortedStateVector[i]
			if t := createTimeVector[i]; !t.IsEmpty() {
				blockEntry.CreateTime = t
			}
			if t := commitTimeVector[i]; !t.IsEmpty() {
				blockEntry.CommitTs = t
			}
			blockEntry.EntryState = entryStateVector[i]

			p.blocks.Set(blockEntry)

			{
				e := BlockIndexByTSEntry{
					Time:         blockEntry.CreateTime,
					BlockID:      blockID,
					IsDelete:     false,
					IsAppendable: blockEntry.EntryState,
				}
				p.blockIndexByTS.Set(e)
			}

			{
				iter := p.rows.Copy().Iter()
				pivot := RowEntry{
					BlockID: blockID,
				}
				for ok := iter.Seek(pivot); ok; ok = iter.Next() {
					entry := iter.Item()
					if entry.BlockID != blockID {
						break
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
					if !entryStateVector[i] && blockEntry.DeltaLocation().IsEmpty() {
						//if entry.Deleted {
						p.dirtyBlocks.Set(blockEntry)
						//}
						//for better performance, we can break here.
						break
					}

					// if the inserting block is appendable, need to delete the rows for it;
					// if the inserting block is non-appendable and has delta location, need to delete
					// the deletes for it.
					if entryStateVector[i] ||
						(!entryStateVector[i] && !blockEntry.DeltaLocation().IsEmpty()) {
						p.rows.Delete(entry)
						numDeleted++

					}
					if entryStateVector[i] {
						if len(entry.PrimaryIndexBytes) > 0 {
							p.primaryIndex.Delete(&PrimaryIndexEntry{
								Bytes:      entry.PrimaryIndexBytes,
								RowEntryID: entry.ID,
							})
						}
					}
				}
				iter.Release()
				//if the inserting block is non-appendable and has delta location,
				//then delete it from the dirtyBlocks.
				if !entryStateVector[i] && !blockEntry.DeltaLocation().IsEmpty() {
					p.dirtyBlocks.Delete(blockEntry)
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

			pivot := BlockEntry{
				BlockInfo: catalog.BlockInfo{
					BlockID: blockID,
				},
			}
			entry, ok := p.blocks.Get(pivot)
			//TODO non-appendable block' delete maybe arrive before its insert?
			if !ok {
				panic(fmt.Sprintf("invalid block id. %x", rowID))
			}

			entry.DeleteTime = deleteTimeVector[i]

			p.blocks.Set(entry)

			{
				e := BlockIndexByTSEntry{
					Time:         entry.DeleteTime,
					BlockID:      blockID,
					IsDelete:     true,
					IsAppendable: entry.EntryState,
				}
				p.blockIndexByTS.Set(e)
			}

		})
	}

	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.MetadataDeleteEntries.Add(1)
	})
}

func (p *PartitionState) BlockVisible(blockID types.Blockid, ts types.TS) bool {
	pivot := BlockEntry{
		BlockInfo: catalog.BlockInfo{
			BlockID: blockID,
		},
	}
	entry, ok := p.blocks.Get(pivot)
	if !ok {
		return false
	}
	return entry.Visible(ts)
}

func (p *PartitionState) AppendCheckpoint(checkpoint string) {
	p.checkpoints = append(p.checkpoints, checkpoint)
}

func (p *PartitionState) ConsumeCheckpoints(
	fn func(checkpoint string) error,
) error {
	for _, checkpoint := range p.checkpoints {
		if err := fn(checkpoint); err != nil {
			return err
		}
	}
	p.checkpoints = p.checkpoints[:0]
	return nil
}
