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
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
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

	rt                   *dbutils.Runtime
	pageIds              []*common.ID
	delTbls              []*model.TransDels
	collectTs            types.TS
	transCntBeforeCommit int
	nextRoundDirties     map[*catalog.ObjectEntry]struct{}
}

func NewMergeObjectsEntry(
	txn txnif.AsyncTxn,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	transMappings *api.BlkTransferBooking,
	rt *dbutils.Runtime,
) (*mergeObjectsEntry, error) {
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

	if totalCreatedBlkCnt > 0 {
		entry.delTbls = make([]*model.TransDels, totalCreatedBlkCnt)
		entry.nextRoundDirties = make(map[*catalog.ObjectEntry]struct{})
		entry.collectTs = rt.Now()
		var err error
		// phase 1 transfer
		entry.transCntBeforeCommit, err = entry.collectDelsAndTransfer(entry.txn.GetStartTS(), entry.collectTs)
		if err != nil {
			return nil, err
		}
	}

	entry.prepareTransferPage()
	return entry, nil
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
				objID := entry.createdObjs[dst.ObjIdx].ID
				blkID := objectio.NewBlockidWithObjectID(&objID, uint16(dst.BlkIdx))
				page.Train(uint32(srcRow), *objectio.NewRowid(blkID, uint32(dst.RowIdx)))
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

// ATTENTION !!! (from, to] !!!
func (entry *mergeObjectsEntry) transferObjectDeletes(
	dropped *catalog.ObjectEntry,
	from, to types.TS,
	blkOffsetBase int) (transCnt int, err error) {

	dataBlock := dropped.GetObjectData()

	bat, _, err := dataBlock.CollectDeleteInRange(
		entry.txn.GetContext(),
		from.Next(),
		to,
		false,
		common.MergeAllocator,
	)
	if err != nil {
		return
	}
	if bat == nil || bat.Length() == 0 {
		return
	}

	entry.nextRoundDirties[dropped] = struct{}{}

	rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.PhyAddrColumnName).GetDownstreamVector())
	ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

	count := len(rowid)
	transCnt += count
	for i := 0; i < count; i++ {
		row := rowid[i].GetRowOffset()
		blkOffsetInObj := int(rowid[i].GetBlockOffset())
		blkOffset := blkOffsetBase + blkOffsetInObj
		mapping := entry.transMappings.Mappings[blkOffset].M
		if len(mapping) == 0 {
			// this block had been all deleted, skip
			// Note: it is possible that the block is empty, but not the object
			continue
		}
		destpos, ok := mapping[int32(row)]
		if !ok {
			_min, _max := int32(math.MaxInt32), int32(0)
			for k := range mapping {
				if k < _min {
					_min = k
				}
				if k > _max {
					_max = k
				}
			}
			panic(fmt.Sprintf(
				"%s-%d find no transfer mapping for row %d, mapping range (%d, %d)",
				dropped.ID.String(), blkOffsetInObj, row, _min, _max))
		}
		if entry.delTbls[destpos.BlkIdx] == nil {
			entry.delTbls[destpos.BlkIdx] = model.NewTransDels(entry.txn.GetPrepareTS())
		}
		entry.delTbls[destpos.BlkIdx].Mapping[int(destpos.RowIdx)] = ts[i]
		targetObj, err := entry.relation.GetObject(&entry.createdObjs[destpos.ObjIdx].ID)
		if err != nil {
			return 0, err
		}
		if err = targetObj.RangeDelete(
			uint16(destpos.BlkIdx), uint32(destpos.RowIdx), uint32(destpos.RowIdx), handle.DT_MergeCompact, common.MergeAllocator,
		); err != nil {
			return 0, err
		}
	}
	return
}

// ATTENTION !!! (from, to] !!!
func (entry *mergeObjectsEntry) collectDelsAndTransfer(from, to types.TS) (transCnt int, err error) {
	if len(entry.createdBlkCnt) == 0 {
		return
	}

	blksOffsetBase := 0
	for _, dropped := range entry.droppedObjs {
		// handle object transfer
		hasMappingInThisObj := false
		blkCnt := dropped.BlockCnt()
		for iblk := 0; iblk < blkCnt; iblk++ {
			if len(entry.transMappings.Mappings[blksOffsetBase+iblk].M) != 0 {
				hasMappingInThisObj = true
				break
			}
		}
		if !hasMappingInThisObj {
			// this object had been all deleted, skip
			blksOffsetBase += dropped.BlockCnt()
			continue
		}

		cnt := 0
		cnt, err = entry.transferObjectDeletes(dropped, from, to, blksOffsetBase)
		if err != nil {
			return
		}
		transCnt += cnt
		blksOffsetBase += dropped.BlockCnt()
	}
	return
}

func (entry *mergeObjectsEntry) PrepareCommit() (err error) {
	inst := time.Now()
	defer func() {
		v2.TaskCommitMergeObjectsDurationHistogram.Observe(time.Since(inst).Seconds())
	}()
	if len(entry.createdBlkCnt) == 0 {
		return
	}

	// phase 2 transfer
	transCnt, err := entry.collectDelsAndTransfer(entry.collectTs, entry.txn.GetPrepareTS())
	if err != nil {
		return nil
	}

	tblEntry := entry.droppedObjs[0].GetTable()
	tblEntry.Stats.Lock()
	for dropped := range entry.nextRoundDirties {
		tblEntry.DeletedDirties = append(tblEntry.DeletedDirties, dropped)
	}
	tblEntry.Stats.Unlock()

	objID := entry.createdObjs[0].ID
	for i, delTbl := range entry.delTbls {
		if delTbl != nil {
			destid := objectio.NewBlockidWithObjectID(&objID, uint16(i))
			entry.rt.TransferDelsMap.SetDelsForBlk(*destid, delTbl)
		}
	}
	logutil.Infof("mergeblocks commit %v, [%v,%v], trans %d on %d objects, %d in commit queue",
		entry.relation.ID(),
		entry.txn.GetStartTS().ToString(),
		entry.txn.GetCommitTS().ToString(),
		entry.transCntBeforeCommit+transCnt,
		len(entry.nextRoundDirties),
		transCnt,
	)

	return
}
