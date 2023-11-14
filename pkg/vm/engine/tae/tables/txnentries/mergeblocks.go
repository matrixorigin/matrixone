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
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type mergeBlocksEntry struct {
	sync.RWMutex
	txn           txnif.AsyncTxn
	relation      handle.Relation
	droppedSegs   []*catalog.SegmentEntry
	deletes       []*nulls.Bitmap
	createdSegs   []*catalog.SegmentEntry
	droppedBlks   []*catalog.BlockEntry
	createdBlks   []*catalog.BlockEntry
	transMappings *BlkTransferBooking
	mapping       []uint32
	fromAddr      []uint32
	toAddr        []uint32
	skippedBlks   []int

	rt *dbutils.Runtime
}

func NewMergeBlocksEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedSegs, createdSegs []*catalog.SegmentEntry,
	droppedBlks, createdBlks []*catalog.BlockEntry,
	transMappings *BlkTransferBooking,
	mapping, fromAddr, toAddr []uint32,
	deletes []*nulls.Bitmap,
	skipBlks []int,
	rt *dbutils.Runtime,
) *mergeBlocksEntry {
	return &mergeBlocksEntry{
		txn:           txn,
		relation:      relation,
		createdSegs:   createdSegs,
		droppedSegs:   droppedSegs,
		createdBlks:   createdBlks,
		droppedBlks:   droppedBlks,
		transMappings: transMappings,
		mapping:       mapping,
		fromAddr:      fromAddr,
		toAddr:        toAddr,
		deletes:       deletes,
		skippedBlks:   skipBlks,
		rt:            rt,
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
	delTbls []*model.TransDels,
	blkidx int) (err error) {

	mapping := entry.transMappings.Mappings[blkidx]
	if len(mapping) == 0 {
		panic("cannot tranfer empty block")
	}
	tblEntry := dropped.GetSegment().GetTable()
	isTransient := !tblEntry.GetLastestSchema().HasPK()
	id := dropped.AsCommonID()
	page := model.NewTransferHashPage(id, time.Now(), isTransient)
	for srcRow, dst := range mapping {
		blkid := blks[dst.Idx].ID()
		page.Train(uint32(srcRow), *objectio.NewRowid(&blkid, uint32(dst.Row)))
	}
	_ = entry.rt.TransferTable.AddPage(page)

	dataBlock := dropped.GetBlockData()

	bat, err := dataBlock.CollectDeleteInRange(
		entry.txn.GetContext(),
		entry.txn.GetStartTS().Next(),
		entry.txn.GetPrepareTS(),
		false,
		common.MergeAllocator,
	)
	if err != nil {
		return err
	}
	if bat == nil || bat.Length() == 0 {
		return nil
	}

	tblEntry.Stats.Lock()
	tblEntry.DeletedDirties = append(tblEntry.DeletedDirties, dropped)
	tblEntry.Stats.Unlock()
	rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.PhyAddrColumnName).GetDownstreamVector())
	ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

	count := len(rowid)
	for i := 0; i < count; i++ {
		row := rowid[i].GetRowOffset()
		destpos, ok := mapping[int(row)]
		if !ok {
			panic(fmt.Sprintf("%s find no transfer mapping for row %d", dropped.ID.String(), row))
		}
		if delTbls[destpos.Idx] == nil {
			delTbls[destpos.Idx] = model.NewTransDels(entry.txn.GetPrepareTS())
		}
		delTbls[destpos.Idx].Mapping[destpos.Row] = ts[i]
		if err = blks[destpos.Idx].RangeDelete(
			uint32(destpos.Row), uint32(destpos.Row), handle.DT_MergeCompact, common.MergeAllocator,
		); err != nil {
			return err
		}
	}
	return
}

func (entry *mergeBlocksEntry) PrepareCommit() (err error) {
	blks := make([]handle.Block, len(entry.createdBlks))
	delTbls := make([]*model.TransDels, len(entry.createdBlks))
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

	ids := make([]*common.ID, 0)

	for idx, dropped := range entry.droppedBlks {
		if entry.isSkipped(idx) {
			if len(entry.transMappings.Mappings[idx]) != 0 {
				panic("empty block do not match")
			}
			continue
		}

		if err = entry.transferBlockDeletes(
			dropped,
			blks,
			delTbls,
			idx); err != nil {
			break
		}
		ids = append(ids, dropped.AsCommonID())
	}
	if err == nil {
		for i, delTbl := range delTbls {
			if delTbl != nil {
				destid := blks[i].ID()
				entry.rt.TransferDelsMap.SetDelsForBlk(destid, delTbl)
			}
		}
	}
	if err != nil {
		for _, id := range ids {
			_ = entry.rt.TransferTable.DeletePage(id)
		}
	}

	return
}
