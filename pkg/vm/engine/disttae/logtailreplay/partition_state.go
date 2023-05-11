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
	"sync/atomic"
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
	rows         *btree.BTreeG[RowEntry] // use value type to avoid locking on elements
	blocks       *btree.BTreeG[BlockEntry]
	primaryIndex *btree.BTreeG[*PrimaryIndexEntry]
	checkpoints  []string

	// noData indicates whether to retain data batch
	// for primary key dedup, reading data is not required
	noData bool
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
}

func (p *PrimaryIndexEntry) Less(than *PrimaryIndexEntry) bool {
	if res := bytes.Compare(p.Bytes, than.Bytes); res < 0 {
		return true
	} else if res > 0 {
		return false
	}
	return p.RowEntryID < than.RowEntryID
}

func NewPartitionState(noData bool) *PartitionState {
	opts := btree.Options{
		Degree: 4,
	}
	return &PartitionState{
		noData:       noData,
		rows:         btree.NewBTreeGOptions((RowEntry).Less, opts),
		blocks:       btree.NewBTreeGOptions((BlockEntry).Less, opts),
		primaryIndex: btree.NewBTreeGOptions((*PrimaryIndexEntry).Less, opts),
	}
}

func (p *PartitionState) Copy() *PartitionState {
	checkpoints := make([]string, len(p.checkpoints))
	copy(checkpoints, p.checkpoints)
	return &PartitionState{
		rows:         p.rows.Copy(),
		blocks:       p.blocks.Copy(),
		primaryIndex: p.primaryIndex.Copy(),
		checkpoints:  checkpoints,
		noData:       p.noData,
	}
}

func (p *PartitionState) RowExists(rowID types.Rowid, ts types.TS) bool {
	iter := p.rows.Iter()
	defer iter.Release()

	blockID := *rowID.GetBlockid()
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
			p.HandleRowsDelete(ctx, entry.Bat)
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

			blockID := *rowID.GetBlockid()
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
				}
				p.primaryIndex.Set(entry)
			}

		})
	}

	partitionStateProfileHandler.AddSample()
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.InsertEntries.Add(1)
		c.DistTAE.Logtail.InsertRows.Add(numInserted)
		c.DistTAE.Logtail.ActiveRows.Add(numInserted)
	})

	return
}

func (p *PartitionState) HandleRowsDelete(ctx context.Context, input *api.Batch) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleRowsDelete")
	defer task.End()

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	timeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	batch, err := batch.ProtoBatchToBatch(input)
	if err != nil {
		panic(err)
	}

	for i, rowID := range rowIDVector {
		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleDel, func() {

			blockID := *rowID.GetBlockid()
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
		})
	}

	partitionStateProfileHandler.AddSample()
	perfcounter.Update(ctx, func(c *perfcounter.CounterSet) {
		c.DistTAE.Logtail.Entries.Add(1)
		c.DistTAE.Logtail.DeleteEntries.Add(1)
	})
}

func (p *PartitionState) HandleMetadataInsert(ctx context.Context, input *api.Batch) {
	ctx, task := trace.NewTask(ctx, "PartitionState.HandleMetadataInsert")
	defer task.End()

	createTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))
	blockIDVector := vector.MustFixedCol[types.Blockid](mustVectorFromProto(input.Vecs[2]))
	entryStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[3]))
	sortedStateVector := vector.MustFixedCol[bool](mustVectorFromProto(input.Vecs[4]))
	metaLocationVector := vector.MustBytesCol(mustVectorFromProto(input.Vecs[5]))
	deltaLocationVector := vector.MustBytesCol(mustVectorFromProto(input.Vecs[6]))
	commitTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[7]))
	segmentIDVector := vector.MustFixedCol[types.Uuid](mustVectorFromProto(input.Vecs[8]))

	var numInserted, numDeleted int64
	for i, blockID := range blockIDVector {
		moprobe.WithRegion(ctx, moprobe.PartitionStateHandleMetaInsert, func() {

			pivot := BlockEntry{
				BlockInfo: catalog.BlockInfo{
					BlockID: blockID,
				},
			}
			entry, ok := p.blocks.Get(pivot)
			if !ok {
				entry = pivot
				numInserted++
			}

			if location := objectio.Location(metaLocationVector[i]); !location.IsEmpty() {
				entry.MetaLoc = *(*[objectio.LocationLen]byte)(unsafe.Pointer(&location[0]))
			}
			if location := objectio.Location(deltaLocationVector[i]); !location.IsEmpty() {
				entry.DeltaLoc = *(*[objectio.LocationLen]byte)(unsafe.Pointer(&location[0]))
			}
			if id := segmentIDVector[i]; objectio.IsEmptySegid(&id) {
				entry.SegmentID = id
			}
			entry.Sorted = sortedStateVector[i]
			if t := createTimeVector[i]; !t.IsEmpty() {
				entry.CreateTime = t
			}
			if t := commitTimeVector[i]; !t.IsEmpty() {
				entry.CommitTs = t
			}
			entry.EntryState = entryStateVector[i]

			p.blocks.Set(entry)

			if entryStateVector[i] {
				iter := p.rows.Copy().Iter()
				pivot := RowEntry{
					BlockID: blockID,
				}
				for ok := iter.Seek(pivot); ok; ok = iter.Next() {
					entry := iter.Item()
					if entry.BlockID != blockID {
						break
					}
					// delete row entry
					p.rows.Delete(entry)
					numDeleted++
					// delete primary index entry
					if len(entry.PrimaryIndexBytes) > 0 {
						p.primaryIndex.Delete(&PrimaryIndexEntry{
							Bytes:      entry.PrimaryIndexBytes,
							RowEntryID: entry.ID,
						})
					}
				}
				iter.Release()
			}

		})
	}

	partitionStateProfileHandler.AddSample()
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

	rowIDVector := vector.MustFixedCol[types.Rowid](mustVectorFromProto(input.Vecs[0]))
	deleteTimeVector := vector.MustFixedCol[types.TS](mustVectorFromProto(input.Vecs[1]))

	for i, rowID := range rowIDVector {
		blockID := *rowID.GetBlockid()
		trace.WithRegion(ctx, "handle a row", func() {

			pivot := BlockEntry{
				BlockInfo: catalog.BlockInfo{
					BlockID: blockID,
				},
			}
			entry, ok := p.blocks.Get(pivot)
			if !ok {
				panic(fmt.Sprintf("invalid block id. %x", rowID))
			}

			entry.DeleteTime = deleteTimeVector[i]

			p.blocks.Set(entry)
		})
	}

	partitionStateProfileHandler.AddSample()
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
