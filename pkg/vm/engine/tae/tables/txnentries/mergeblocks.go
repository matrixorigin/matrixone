// Copyright 2021 Matrix Origin
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

package txnentries

import (
	"sync"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type mergeBlocksEntry struct {
	sync.RWMutex
	txn         txnif.AsyncTxn
	relation    handle.Relation
	droppedSegs []*catalog.SegmentEntry
	deletes     []*roaring.Bitmap
	createdSegs []*catalog.SegmentEntry
	droppedBlks []*catalog.BlockEntry
	createdBlks []*catalog.BlockEntry
	mapping     []uint32
	fromAddr    []uint32
	toAddr      []uint32
	scheduler   tasks.TaskScheduler
	skippedBlks []int
}

func NewMergeBlocksEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedSegs, createdSegs []*catalog.SegmentEntry,
	droppedBlks, createdBlks []*catalog.BlockEntry,
	mapping, fromAddr, toAddr []uint32,
	deletes []*roaring.Bitmap,
	skipBlks []int,
	scheduler tasks.TaskScheduler) *mergeBlocksEntry {
	return &mergeBlocksEntry{
		txn:         txn,
		relation:    relation,
		createdSegs: createdSegs,
		droppedSegs: droppedSegs,
		createdBlks: createdBlks,
		droppedBlks: droppedBlks,
		mapping:     mapping,
		fromAddr:    fromAddr,
		toAddr:      toAddr,
		scheduler:   scheduler,
		deletes:     deletes,
		skippedBlks: skipBlks,
	}
}

func (entry *mergeBlocksEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (entry *mergeBlocksEntry) ApplyRollback(index *wal.Index) (err error) {
	//TODO::?
	return
}

func (entry *mergeBlocksEntry) ApplyCommit(index *wal.Index) (err error) {
	return
}

func (entry *mergeBlocksEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	droppedSegs := make([]*common.ID, 0)
	for _, blk := range entry.droppedSegs {
		id := blk.AsCommonID()
		droppedSegs = append(droppedSegs, id)
	}
	createdSegs := make([]*common.ID, 0)
	for _, blk := range entry.createdSegs {
		id := blk.AsCommonID()
		createdSegs = append(createdSegs, id)
	}
	droppedBlks := make([]*common.ID, 0)
	for _, blk := range entry.droppedBlks {
		id := blk.AsCommonID()
		droppedBlks = append(droppedBlks, id)
	}
	createdBlks := make([]*common.ID, 0)
	for _, blk := range entry.createdBlks {
		createdBlks = append(createdBlks, blk.AsCommonID())
	}
	cmd = newMergeBlocksCmd(
		entry.relation.ID(),
		droppedSegs,
		createdSegs,
		droppedBlks,
		createdBlks,
		entry.mapping,
		entry.fromAddr,
		entry.toAddr,
		entry.txn,
		csn)
	return
}

func (entry *mergeBlocksEntry) Set1PC()     {}
func (entry *mergeBlocksEntry) Is1PC() bool { return false }
func (entry *mergeBlocksEntry) resolveAddr(fromPos int, fromOffset uint32) (toPos int, toOffset uint32) {
	totalFromOffset := entry.fromAddr[fromPos] + fromOffset
	totalToOffset := entry.mapping[totalFromOffset]
	left, right := 0, len(entry.toAddr)-1
	for left <= right {
		toPos = (left + right) / 2
		if entry.toAddr[toPos] < totalToOffset {
			left = toPos + 1
		} else if entry.toAddr[toPos] > totalToOffset {
			right = toPos - 1
		} else {
			break
		}
	}

	// if toPos == 0 && entry.toAddr[toPos] < totalToOffset {
	if entry.toAddr[toPos] > totalToOffset {
		toPos = toPos - 1
	}
	toOffset = totalToOffset - entry.toAddr[toPos]
	// logutil.Infof("mapping=%v", entry.mapping)
	// logutil.Infof("fromPos=%d, fromOff=%d", fromPos, fromOffset)
	// logutil.Infof("fromAddr=%v", entry.fromAddr)
	// logutil.Infof("toAddr=%v", entry.toAddr)
	// logutil.Infof("toPos=%d, toOffset=%d", toPos, toOffset)
	return
}

func (entry *mergeBlocksEntry) isSkipped(fromPos int) bool {
	for _, offset := range entry.skippedBlks {
		if offset == fromPos {
			return true
		}
	}
	return false
}

func (entry *mergeBlocksEntry) transferBlockDeletes(
	dropped *catalog.BlockEntry,
	blks []handle.Block,
	fromPos int,
	skippedCnt int) (err error) {
	id := dropped.AsCommonID()
	page := model.NewTransferHashPage(id, time.Now())
	var (
		length uint32
		view   *model.BlockView
	)
	if fromPos-skippedCnt+1 == len(entry.fromAddr) {
		length = uint32(len(entry.mapping)) - entry.fromAddr[fromPos-skippedCnt]
	} else {
		length = entry.fromAddr[fromPos-skippedCnt+1] - entry.fromAddr[fromPos-skippedCnt]
	}
	for i := uint32(0); i < length; i++ {
		if entry.deletes[fromPos] != nil && entry.deletes[fromPos].Contains(i) {
			continue
		}
		newOffset := i
		if entry.deletes[fromPos] != nil {
			newOffset = i - uint32(entry.deletes[fromPos].Rank(i))
		}
		toPos, toRow := entry.resolveAddr(fromPos-skippedCnt, newOffset)
		toId := entry.createdBlks[toPos].AsCommonID()
		prefix := toId.BlockID[:]
		rowid := model.EncodePhyAddrKeyWithPrefix(prefix, uint32(i))
		page.Train(toRow, rowid)
	}
	_ = entry.scheduler.AddTransferPage(page)

	dataBlock := dropped.GetBlockData()
	if view, err = dataBlock.CollectChangesInRange(
		entry.txn.GetStartTS(),
		entry.txn.GetCommitTS()); err != nil || view == nil {
		return
	}

	deletes := view.DeleteMask
	for colIdx, column := range view.Columns {
		view.DeleteMask = compute.ShuffleByDeletes(
			deletes, entry.deletes[fromPos])
		for row, v := range column.UpdateVals {
			toPos, toRow := entry.resolveAddr(fromPos-skippedCnt, row)
			if err = blks[toPos].Update(toRow, uint16(colIdx), v); err != nil {
				return
			}
		}
	}
	view.DeleteMask = compute.ShuffleByDeletes(view.DeleteMask, entry.deletes[fromPos])
	if view.DeleteMask != nil {
		it := view.DeleteMask.Iterator()
		for it.HasNext() {
			row := it.Next()
			toPos, toRow := entry.resolveAddr(fromPos-skippedCnt, row)
			if err = blks[toPos].RangeDelete(toRow, toRow, handle.DT_MergeCompact); err != nil {
				return
			}
		}
	}
	return
}

func (entry *mergeBlocksEntry) PrepareCommit() (err error) {
	blks := make([]handle.Block, len(entry.createdBlks))
	for i, meta := range entry.createdBlks {
		id := meta.AsCommonID()
		seg, err := entry.relation.GetSegment(id.SegmentID())
		if err != nil {
			return err
		}
		blk, err := seg.GetBlock(id.BlockID)
		if err != nil {
			return err
		}
		blks[i] = blk
	}

	skippedCnt := 0
	ids := make([]*common.ID, 0)

	for fromPos, dropped := range entry.droppedBlks {
		if entry.isSkipped(fromPos) {
			skippedCnt++
			continue
		}

		if err = entry.transferBlockDeletes(
			dropped,
			blks,
			fromPos,
			skippedCnt); err != nil {
			break
		}
		ids = append(ids, dropped.AsCommonID())
	}
	if err != nil {
		for _, id := range ids {
			_ = entry.scheduler.DeleteTransferPage(id)
		}
	}
	return
}
