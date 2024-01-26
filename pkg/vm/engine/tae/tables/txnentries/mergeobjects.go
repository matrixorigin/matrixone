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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type mergeObjectsEntry struct {
	sync.RWMutex
	txn           txnif.AsyncTxn
	relation      handle.Relation
	droppedObjs   []*catalog.ObjectEntry
	createdObjs   []*catalog.ObjectEntry
	droppedBlks   []*catalog.BlockEntry
	createdBlks   []*catalog.BlockEntry
	transMappings *api.BlkTransferBooking

	rt      *dbutils.Runtime
	pageIds []*common.ID
}

func NewMergeObjectsEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	droppedBlks, createdBlks []*catalog.BlockEntry,
	transMappings *api.BlkTransferBooking,
	rt *dbutils.Runtime,
) *mergeObjectsEntry {
	entry := &mergeObjectsEntry{
		txn:           txn,
		relation:      relation,
		createdObjs:   createdObjs,
		droppedObjs:   droppedObjs,
		createdBlks:   createdBlks,
		droppedBlks:   droppedBlks,
		transMappings: transMappings,
		rt:            rt,
	}
	entry.prepareTransferPage()
	return entry
}

func (entry *mergeObjectsEntry) prepareTransferPage() {
	for i, blk := range entry.droppedBlks {
		if len(entry.transMappings.Mappings[i].M) == 0 {
			continue
		}
		mapping := entry.transMappings.Mappings[i].M
		if len(mapping) == 0 {
			panic("cannot tranfer empty block")
		}
		tblEntry := blk.GetObject().GetTable()
		isTransient := !tblEntry.GetLastestSchemaLocked().HasPK()
		id := blk.AsCommonID()
		page := model.NewTransferHashPage(id, time.Now(), isTransient)
		for srcRow, dst := range mapping {
			blkid := entry.createdBlks[dst.Idx].ID
			page.Train(uint32(srcRow), *objectio.NewRowid(&blkid, uint32(dst.Row)))
		}
		entry.pageIds = append(entry.pageIds, id)
		_ = entry.rt.TransferTable.AddPage(page)
	}
}

func (entry *mergeObjectsEntry) PrepareRollback() (err error) {
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}
	entry.pageIds = nil
	return
}

func (entry *mergeObjectsEntry) ApplyRollback() (err error) {
	//TODO::?
	return
}

func (entry *mergeObjectsEntry) ApplyCommit() (err error) {
	return
}

func (entry *mergeObjectsEntry) MakeCommand(csn uint32) (cmd txnif.TxnCmd, err error) {
	droppedObjs := make([]*common.ID, 0)
	for _, blk := range entry.droppedObjs {
		id := blk.AsCommonID()
		droppedObjs = append(droppedObjs, id)
	}
	createdObjs := make([]*common.ID, 0)
	for _, blk := range entry.createdObjs {
		id := blk.AsCommonID()
		createdObjs = append(createdObjs, id)
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
		droppedObjs,
		createdObjs,
		droppedBlks,
		createdBlks,
		entry.txn,
		csn)
	return
}

func (entry *mergeObjectsEntry) Set1PC()     {}
func (entry *mergeObjectsEntry) Is1PC() bool { return false }

func (entry *mergeObjectsEntry) transferBlockDeletes(
	dropped *catalog.BlockEntry,
	blks []handle.Block,
	delTbls []*model.TransDels,
	blkidx int) (err error) {

	mapping := entry.transMappings.Mappings[blkidx].M
	if len(mapping) == 0 {
		panic("cannot tranfer empty block")
	}
	dataBlock := dropped.GetBlockData()
	tblEntry := dropped.GetObject().GetTable()

	startTS := entry.txn.GetStartTS()
	bat, err := dataBlock.CollectDeleteInRange(
		entry.txn.GetContext(),
		startTS.Next(),
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
		destpos, ok := mapping[int32(row)]
		if !ok {
			panic(fmt.Sprintf("%s find no transfer mapping for row %d", dropped.ID.String(), row))
		}
		if delTbls[destpos.Idx] == nil {
			delTbls[destpos.Idx] = model.NewTransDels(entry.txn.GetPrepareTS())
		}
		delTbls[destpos.Idx].Mapping[int(destpos.Row)] = ts[i]
		if err = blks[destpos.Idx].RangeDelete(
			uint32(destpos.Row), uint32(destpos.Row), handle.DT_MergeCompact, common.MergeAllocator,
		); err != nil {
			return err
		}
	}
	return
}

func (entry *mergeObjectsEntry) PrepareCommit() (err error) {
	blks := make([]handle.Block, len(entry.createdBlks))
	delTbls := make([]*model.TransDels, len(entry.createdBlks))
	for i, meta := range entry.createdBlks {
		id := meta.AsCommonID()
		obj, err := entry.relation.GetObject(id.ObjectID())
		if err != nil {
			return err
		}
		defer obj.Close()
		blk, err := obj.GetBlock(id.BlockID)
		if err != nil {
			return err
		}
		blks[i] = blk
	}

	ids := make([]*common.ID, 0)

	if len(entry.droppedBlks) != len(entry.transMappings.Mappings) {
		panic(fmt.Sprintf("bad length %v != %v", len(entry.droppedBlks), len(entry.transMappings.Mappings)))
	}

	for idx, dropped := range entry.droppedBlks {
		if len(entry.transMappings.Mappings[idx].M) == 0 {
			continue
		}
		if err = entry.transferBlockDeletes(
			dropped,
			blks,
			delTbls,
			idx,
		); err != nil {
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
