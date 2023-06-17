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

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type mergeBlocksEntry struct {
	sync.RWMutex
	txn         txnif.AsyncTxn
	relation    handle.Relation
	droppedSegs []*catalog.SegmentEntry
	deletes     []*nulls.Bitmap
	createdSegs []*catalog.SegmentEntry
	droppedBlks []*catalog.BlockEntry
	createdBlks []*catalog.BlockEntry
	mapping     []uint32
	fromAddr    []uint32
	toAddr      []uint32
	skippedBlks []int

	rt *model.Runtime
}

func NewMergeBlocksEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedSegs, createdSegs []*catalog.SegmentEntry,
	droppedBlks, createdBlks []*catalog.BlockEntry,
	mapping, fromAddr, toAddr []uint32,
	deletes []*nulls.Bitmap,
	skipBlks []int,
	rt *model.Runtime,
) *mergeBlocksEntry {
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
		deletes:     deletes,
		skippedBlks: skipBlks,
		rt:          rt,
	}
}

func (entry *mergeBlocksEntry) PrepareRollback() (err error) {
	// TODO: remove block file? (should be scheduled and executed async)
	return
}
func (entry *mergeBlocksEntry) ApplyRollback() (err error) {
	//TODO::?
	return
}

func (entry *mergeBlocksEntry) ApplyCommit() (err error) {
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

	posInFromAddr := fromPos - skippedCnt
	if posInFromAddr+1 == len(entry.fromAddr) {
		length = uint32(len(entry.mapping)) - entry.fromAddr[posInFromAddr]
	} else {
		length = entry.fromAddr[posInFromAddr+1] - entry.fromAddr[posInFromAddr]
	}

	var (
		offsetInDropped              uint32
		offsetInOldBlkBeforeApplyDel uint32
	)
	deleteMap := entry.deletes[fromPos]
	delCnt := uint32(0)
	if deleteMap != nil {
		delCnt = uint32(deleteMap.GetCardinality())
	}
	for ; offsetInOldBlkBeforeApplyDel < delCnt+length; offsetInOldBlkBeforeApplyDel++ {
		if deleteMap != nil && deleteMap.Contains(uint64(offsetInOldBlkBeforeApplyDel)) {
			continue
		}
		// add a record
		toPos, toRow := entry.resolveAddr(posInFromAddr, offsetInDropped)
		rowid := objectio.NewRowid(&entry.createdBlks[toPos].ID, toRow)
		// offset in oldblk -> new rowid
		page.Train(offsetInOldBlkBeforeApplyDel, *rowid)

		// bump
		offsetInDropped++
	}
	if offsetInDropped != length {
		panic("tranfer logic error")
	}

	_ = entry.rt.TransferTable.AddPage(page)

	dataBlock := dropped.GetBlockData()
	if view, err = dataBlock.CollectChangesInRange(
		entry.txn.GetContext(),
		entry.txn.GetStartTS(),
		entry.txn.GetCommitTS()); err != nil || view == nil {
		return
	}

	view.DeleteMask = compute.ShuffleByDeletes(view.DeleteMask, entry.deletes[fromPos])
	if !view.DeleteMask.IsEmpty() {
		it := view.DeleteMask.GetBitmap().Iterator()
		for it.HasNext() {
			row := it.Next()
			toPos, toRow := entry.resolveAddr(fromPos-skippedCnt, uint32(row))
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
		defer seg.Close()
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
			_ = entry.rt.TransferTable.DeletePage(id)
		}
	}
	return
}
