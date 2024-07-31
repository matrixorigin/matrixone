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
	"context"
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
	"go.uber.org/zap"
)

type mergeObjectsEntry struct {
	sync.RWMutex
	txn           txnif.AsyncTxn
	taskName      string
	relation      handle.Relation
	droppedObjs   []*catalog.ObjectEntry
	createdObjs   []*catalog.ObjectEntry
	transMappings api.TransferMaps
	skipTransfer  bool

	rt                   *dbutils.Runtime
	pageIds              []*common.ID
	delTbls              map[objectio.ObjectId]map[uint16]struct{}
	collectTs            types.TS
	transCntBeforeCommit int
	nextRoundDirties     map[*catalog.ObjectEntry]struct{}
}

func NewMergeObjectsEntry(
	ctx context.Context,
	txn txnif.AsyncTxn,
	taskName string,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	transMappings api.TransferMaps,
	rt *dbutils.Runtime,
) (*mergeObjectsEntry, error) {
	totalCreatedBlkCnt := 0
	for i, obj := range createdObjs {
		createdObjs[i] = obj.GetLatestNode()
		totalCreatedBlkCnt += createdObjs[i].BlockCnt()
	}
	entry := &mergeObjectsEntry{
		txn:           txn,
		relation:      relation,
		createdObjs:   createdObjs,
		droppedObjs:   droppedObjs,
		transMappings: transMappings,
		skipTransfer:  transMappings == nil,
		rt:            rt,
		taskName:      taskName,
	}

	if !entry.skipTransfer && totalCreatedBlkCnt > 0 {
		entry.delTbls = make(map[types.Objectid]map[uint16]struct{})
		entry.nextRoundDirties = make(map[*catalog.ObjectEntry]struct{})
		entry.collectTs = rt.Now()
		var err error
		// phase 1 transfer
		entry.transCntBeforeCommit, _, err = entry.collectDelsAndTransfer(entry.txn.GetStartTS(), entry.collectTs)
		if err != nil {
			return nil, err
		}
		entry.prepareTransferPage(ctx)
	}
	return entry, nil
}

func (entry *mergeObjectsEntry) prepareTransferPage(ctx context.Context) {
	k := 0
	pagesToSet := make([][]*model.TransferHashPage, 0, len(entry.droppedObjs))
	bts := time.Now().Add(time.Hour)
	createdObjIDs := make([]*objectio.ObjectId, 0, len(entry.createdObjs))
	for _, obj := range entry.createdObjs {
		createdObjIDs = append(createdObjIDs, obj.ID())
	}
	for _, obj := range entry.droppedObjs {
		ioVector := model.InitTransferPageIO()
		pages := make([]*model.TransferHashPage, 0, obj.BlockCnt())
		var duration time.Duration
		var start time.Time
		for j := 0; j < obj.BlockCnt(); j++ {
			m := entry.transMappings[k]
			k++
			if len(m) == 0 {
				continue
			}
			tblEntry := obj.GetTable()
			isTransient := !tblEntry.GetLastestSchema().HasPK()
			id := obj.AsCommonID()
			id.SetBlockOffset(uint16(j))
			page := model.NewTransferHashPage(id, bts, isTransient, entry.rt.LocalFs.Service, model.GetTTL(), model.GetDiskTTL(), createdObjIDs)
			page.Train(m)

			start = time.Now()
			err := model.AddTransferPage(page, ioVector)
			if err != nil {
				return
			}
			duration += time.Since(start)

			entry.pageIds = append(entry.pageIds, id)
			pages = append(pages, page)
		}

		start = time.Now()
		model.WriteTransferPage(ctx, entry.rt.LocalFs.Service, pages, *ioVector)
		pagesToSet = append(pagesToSet, pages)
		duration += time.Since(start)
		v2.TransferPageMergeLatencyHistogram.Observe(duration.Seconds())
	}

	now := time.Now()
	for _, pages := range pagesToSet {
		for _, page := range pages {
			if page.BornTS() != bts {
				page.SetBornTS(now.Add(time.Minute))
			} else {
				page.SetBornTS(now)
			}
			entry.rt.TransferTable.AddPage(page)
		}
	}

	if k != len(entry.transMappings) {
		logutil.Fatal(fmt.Sprintf("k %v, mapping %v", k, len(entry.transMappings)))
	}
}

func (entry *mergeObjectsEntry) PrepareRollback() (err error) {
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}
	for objectID, blkMap := range entry.delTbls {
		for blkOffset := range blkMap {
			blkID := objectio.NewBlockidWithObjectID(&objectID, blkOffset)
			entry.rt.TransferDelsMap.DeleteDelsForBlk(*blkID)
		}
	}
	entry.pageIds = nil
	return
}

func (entry *mergeObjectsEntry) ApplyRollback() (err error) {
	//TODO::?
	return
}

func (entry *mergeObjectsEntry) ApplyCommit(_ string) (err error) {
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

// ATTENTION !!! (from, to] !!!
func (entry *mergeObjectsEntry) transferObjectDeletes(
	dropped *catalog.ObjectEntry,
	from, to types.TS,
	blkOffsetBase int) (transCnt int, collect, transfer time.Duration, err error) {

	dataBlock := dropped.GetObjectData()

	inst := time.Now()
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
	collect = time.Since(inst)
	if bat == nil || bat.Length() == 0 {
		return
	}
	inst = time.Now()
	defer func() { transfer = time.Since(inst) }()

	entry.nextRoundDirties[dropped] = struct{}{}

	rowid := vector.MustFixedCol[types.Rowid](bat.GetVectorByName(catalog.PhyAddrColumnName).GetDownstreamVector())
	ts := vector.MustFixedCol[types.TS](bat.GetVectorByName(catalog.AttrCommitTs).GetDownstreamVector())

	count := len(rowid)
	transCnt += count
	for i := 0; i < count; i++ {
		row := rowid[i].GetRowOffset()
		blkOffsetInObj := int(rowid[i].GetBlockOffset())
		blkOffset := blkOffsetBase + blkOffsetInObj
		mapping := entry.transMappings[blkOffset]
		if len(mapping) == 0 {
			// this block had been all deleted, skip
			// Note: it is possible that the block is empty, but not the object
			continue
		}
		destpos, ok := mapping[row]
		if !ok {
			_min, _max := uint32(math.MaxUint32), uint32(0)
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
				dropped.ID().String(), blkOffsetInObj, row, _min, _max))
		}
		if entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()] == nil {
			entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()] = make(map[uint16]struct{})
		}
		entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()][destpos.BlkIdx] = struct{}{}
		blkID := objectio.NewBlockidWithObjectID(entry.createdObjs[destpos.ObjIdx].ID(), destpos.BlkIdx)
		entry.rt.TransferDelsMap.SetDelsForBlk(*blkID, int(destpos.RowIdx), entry.txn.GetPrepareTS(), ts[i])
		var targetObj handle.Object
		targetObj, err = entry.relation.GetObject(entry.createdObjs[destpos.ObjIdx].ID())
		if err != nil {
			return
		}
		if err = targetObj.RangeDelete(
			destpos.BlkIdx, destpos.RowIdx, destpos.RowIdx, handle.DT_MergeCompact, common.MergeAllocator,
		); err != nil {
			return
		}
	}
	return
}

type tempStat struct {
	transObj                           int
	pcost, ccost, tcost, mpt, mct, mtt time.Duration
}

func (s *tempStat) String() string {
	return fmt.Sprintf("transObj %d, pcost %v, ccost %v, tcost %v, mpt %v, mct %v, mtt %v",
		s.transObj, s.pcost, s.ccost, s.tcost, s.mpt, s.mct, s.mtt)
}

// ATTENTION !!! (from, to] !!!
func (entry *mergeObjectsEntry) collectDelsAndTransfer(from, to types.TS) (transCnt int, stat tempStat, err error) {
	if len(entry.createdObjs) == 0 {
		return
	}

	blksOffsetBase := 0
	var pcost, ccost, tcost time.Duration
	var mpt, mct, mtt time.Duration
	transobj := 0
	for _, dropped := range entry.droppedObjs {
		inst := time.Now()
		// handle object transfer
		hasMappingInThisObj := false
		blkCnt := dropped.BlockCnt()
		for iblk := 0; iblk < blkCnt; iblk++ {
			if len(entry.transMappings[blksOffsetBase+iblk]) != 0 {
				hasMappingInThisObj = true
				break
			}
		}
		if !hasMappingInThisObj {
			// this object had been all deleted, skip
			blksOffsetBase += blkCnt
			continue
		}
		pcost += time.Since(inst)
		if pcost > mpt {
			mpt = pcost
		}
		cnt := 0

		var ct, tt time.Duration
		cnt, ct, tt, err = entry.transferObjectDeletes(dropped, from, to, blksOffsetBase)
		if err != nil {
			return
		}
		if ct > mct {
			mct = ct
		}
		if tt > mtt {
			mtt = tt
		}
		ccost += ct
		tcost += tt
		transCnt += cnt
		transobj++
		blksOffsetBase += blkCnt
	}
	stat = tempStat{
		transObj: transobj,
		pcost:    pcost,
		ccost:    ccost,
		tcost:    tcost,
		mpt:      mpt,
		mct:      mct,
		mtt:      mtt,
	}
	return
}

func (entry *mergeObjectsEntry) PrepareCommit() (err error) {
	inst := time.Now()
	defer func() {
		v2.TaskCommitMergeObjectsDurationHistogram.Observe(time.Since(inst).Seconds())
	}()
	if len(entry.createdObjs) == 0 || entry.skipTransfer {
		logutil.Info(
			"[MERGE-PREPARE-COMMIT]",
			zap.Uint64("table-id", entry.relation.ID()),
			zap.Int("created-objs", len(entry.createdObjs)),
			zap.Bool("skip-transfer", entry.skipTransfer),
			zap.String("task", entry.taskName),
			zap.String("commit-ts", entry.txn.GetPrepareTS().ToString()),
		)
		return
	}

	// phase 2 transfer
	transCnt, stat, err := entry.collectDelsAndTransfer(entry.collectTs, entry.txn.GetPrepareTS())
	if err != nil {
		return nil
	}

	inst1 := time.Now()
	tblEntry := entry.droppedObjs[0].GetTable()
	tblEntry.Stats.Lock()
	for dropped := range entry.nextRoundDirties {
		tblEntry.DeletedDirties = append(tblEntry.DeletedDirties, dropped)
	}
	tblEntry.Stats.Unlock()

	total := time.Since(inst)
	fields := make([]zap.Field, 0, 9)
	fields = append(fields,
		zap.Uint64("table-id", entry.relation.ID()),
		zap.String("task", entry.taskName),
		zap.Int("total-transfer", entry.transCntBeforeCommit+transCnt),
		zap.Int("in-queue-transfer", transCnt),
		zap.Int("objs", len(entry.nextRoundDirties)),
		zap.Duration("total-cost", total),
		zap.Duration("this-tran-cost", time.Since(inst1)),
		zap.String("commit-ts", entry.txn.GetPrepareTS().ToString()),
	)

	if total > 300*time.Millisecond {
		fields = append(fields, zap.String("stat", stat.String()))
		logutil.Info(
			"[MERGE-PREPARE-COMMIT-SLOW]",
			fields...,
		)
	} else {
		logutil.Info(
			"[MERGE-PREPARE-COMMIT]",
			fields...,
		)
	}

	return
}
