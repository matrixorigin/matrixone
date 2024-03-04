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
	txn                txnif.AsyncTxn
	relation           handle.Relation
	droppedObjs        []*catalog.ObjectEntry
	createdObjs        []*catalog.ObjectEntry
	createdBlkCnt      []int
	totalCreatedBlkCnt int
	transMappings      *api.BlkTransferBooking

	rt      *dbutils.Runtime
	pageIds []*common.ID
}

func NewMergeObjectsEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	transMappings *api.BlkTransferBooking,
	rt *dbutils.Runtime,
) *mergeObjectsEntry {
	createdBlkCnt := make([]int, len(createdObjs))
	totalCreatedBlkCnt := 0
	for i, obj := range createdObjs {
		createdBlkCnt[i] = totalCreatedBlkCnt
		totalCreatedBlkCnt += obj.BlockCnt()
	}
	entry := &mergeObjectsEntry{
		txn:                txn,
		relation:           relation,
		createdObjs:        createdObjs,
		totalCreatedBlkCnt: totalCreatedBlkCnt,
		createdBlkCnt:      createdBlkCnt,
		droppedObjs:        droppedObjs,
		transMappings:      transMappings,
		rt:                 rt,
	}
	entry.prepareTransferPage()
	return entry
}

func (entry *mergeObjectsEntry) prepareTransferPage() {
	k := 0
	for _, obj := range entry.droppedObjs {
		for j := 0; j < obj.BlockCnt(); j++ {
			if len(entry.transMappings.Mappings[k].M) == 0 {
				k++
				continue
			}
			mapping := entry.transMappings.Mappings[k].M
			if len(mapping) == 0 {
				panic("cannot tranfer empty block")
			}
			tblEntry := obj.GetTable()
			isTransient := !tblEntry.GetLastestSchema().HasPK()
			id := obj.AsCommonID()
			id.SetBlockOffset(uint16(j))
			page := model.NewTransferHashPage(id, time.Now(), isTransient)
			for srcRow, dst := range mapping {
				objID := entry.createdObjs[0].ID
				blkID := objectio.NewBlockidWithObjectID(&objID, uint16(dst.Idx))
				page.Train(uint32(srcRow), *objectio.NewRowid(blkID, uint32(dst.Row)))
			}
			entry.pageIds = append(entry.pageIds, id)
			_ = entry.rt.TransferTable.AddPage(page)
			k++
		}
	}
	if k != len(entry.transMappings.Mappings) {
		panic(fmt.Sprintf("k %v, mapping %v", k, len(entry.transMappings.Mappings)))
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
	cmd = newMergeBlocksCmd(
		entry.relation.ID(),
		droppedObjs,
		createdObjs,
		entry.txn,
		csn)
	return
}

func (entry *mergeObjectsEntry) Set1PC()     {}
func (entry *mergeObjectsEntry) Is1PC() bool { return false }

func (entry *mergeObjectsEntry) transferBlockDeletes(
	dropped *catalog.ObjectEntry,
	created handle.Object,
	delTbls []*model.TransDels,
	blkidxStart int) (err error) {

	dataBlock := dropped.GetBlockData()
	tblEntry := dropped.GetTable()

	bat, _, err := dataBlock.CollectDeleteInRange(
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
	if err != nil {
		panic(err)
	}
	for i := 0; i < count; i++ {
		row := rowid[i].GetRowOffset()
		blkOffset := rowid[i].GetBlockOffset()
		mapping := entry.transMappings.Mappings[blkidxStart+int(blkOffset)].M
		if len(mapping) == 0 {
			panic("cannot tranfer empty block")
		}
		destpos, ok := mapping[int32(row)]
		if !ok {
			panic(fmt.Sprintf("%s find no transfer mapping for row %d", dropped.ID.String(), row))
		}
		if delTbls[destpos.Idx] == nil {
			delTbls[destpos.Idx] = model.NewTransDels(entry.txn.GetPrepareTS())
		}
		delTbls[destpos.Idx].Mapping[int(destpos.Row)] = ts[i]
		if err = created.RangeDelete(
			uint16(destpos.Idx), uint32(destpos.Row), uint32(destpos.Row), handle.DT_MergeCompact, common.MergeAllocator,
		); err != nil {
			return err
		}
	}
	return
}

func (entry *mergeObjectsEntry) PrepareCommit() (err error) {
	delTbls := make([]*model.TransDels, entry.totalCreatedBlkCnt)
	if len(entry.createdBlkCnt) == 0 {
		return
	}
	created, err := entry.relation.GetObject(&entry.createdObjs[0].ID)
	if err != nil {
		return
	}

	ids := make([]*common.ID, 0)

	blkIdxStart := 0
	k := 0
	for _, dropped := range entry.droppedObjs {
		hasMapping := false
		for i := 0; i < dropped.BlockCnt(); i++ {
			if len(entry.transMappings.Mappings[k+i].M) != 0 {
				hasMapping = true
				break
			}
		}
		k += dropped.BlockCnt()
		if !hasMapping {
			blkIdxStart += dropped.BlockCnt()
			continue
		}
		if err = entry.transferBlockDeletes(
			dropped,
			created,
			delTbls,
			blkIdxStart,
		); err != nil {
			break
		}
		blkIdxStart += dropped.BlockCnt()
	}
	if k != len(entry.transMappings.Mappings) {
		panic(fmt.Sprintf("bad length %v != %v", k, len(entry.transMappings.Mappings)))
	}
	objID := entry.createdObjs[0].ID
	if err == nil {
		for i, delTbl := range delTbls {
			if delTbl != nil {
				destid := objectio.NewBlockidWithObjectID(&objID, uint16(i))
				entry.rt.TransferDelsMap.SetDelsForBlk(*destid, delTbl)
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
