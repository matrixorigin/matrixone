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
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type mergeObjectsEntry struct {
	sync.RWMutex
	txn           txnif.AsyncTxn
	taskName      string
	relation      handle.Relation
	droppedObjs   []*catalog.ObjectEntry
	createdObjs   []*catalog.ObjectEntry
	transferTable *mergesort.TransferTable
	skipTransfer  bool

	rt                   *dbutils.Runtime
	pageIds              []*common.ID
	isTombstone          bool
	delTbls              map[objectio.ObjectId]map[uint16]struct{}
	transferredDels      transferredDeleteSet
	collectTs            types.TS
	sourceSnapshot       types.TS
	transCntBeforeCommit int

	// lifecycleWholeArchive is a narrow mode used by Lifecycle Whole Object
	// retirement. It reuses the normal Merge WAL command and Catalog/GC path,
	// but has no created Object or transfer table. At Prepare it must reject
	// every Tombstone committed after the source snapshot: otherwise a row
	// deleted while the Archive was being written could be restored later.
	lifecycleWholeArchive bool
	lifecycleRewrite      bool
	finalPrepareDeadline  time.Time
	maxDeltaRows          uint64
	maxDeltaBytes         uint64
	maxDeltaBlocks        uint32
	deltaRows             uint64
	deltaBytes            uint64
	deltaBlocks           map[types.Blockid]struct{}
}

func NewMergeObjectsEntry(
	ctx context.Context,
	txn txnif.AsyncTxn,
	taskName string,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	transferTable *mergesort.TransferTable,
	isTombstone bool,
	rt *dbutils.Runtime,
) (_ *mergeObjectsEntry, err error) {
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
		transferTable: transferTable,
		skipTransfer:  transferTable == nil,
		rt:            rt,
		isTombstone:   isTombstone,
		taskName:      taskName,
	}
	defer func() {
		if err != nil {
			entry.RollbackTransferState()
		}
	}()

	startTS := entry.txn.GetStartTS()
	if entry.rt.BigDeleteHinter.HasBigDelAfter(entry.relation.ID(), &startTS) {
		return nil, moerr.NewInternalErrorNoCtxf("LockMerge give up in NewMergeObjectsEntry %v", entry.taskName)
	}

	if !entry.skipTransfer && totalCreatedBlkCnt > 0 {
		entry.delTbls = make(map[types.Objectid]map[uint16]struct{})
		entry.transferredDels = make(transferredDeleteSet)
		entry.collectTs = rt.Now()
		objectio.WaitInjected(objectio.FJ_DataMergeAfterCollectTS)
		if _, _, injected := fault.TriggerFault(objectio.FJ_TransferSlow); injected {
			time.Sleep(time.Second)
		}
		// phase 1 transfer
		entry.transCntBeforeCommit, _, err = entry.collectDelsAndTransfer(ctx, entry.txn.GetStartTS(), entry.collectTs)
		if err != nil {
			return nil, err
		}
		if err = entry.prepareTransferPage(ctx); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

// NewLifecycleWholeObjectsEntry creates the transaction entry for an exact
// Whole Object retirement. The caller must already have installed the source
// Object DropIntent through Relation.SoftDeleteObject.
//
// This deliberately remains a thin specialization of mergeObjectsEntry:
// MakeCommand, WAL replay and physical reclamation stay on the ordinary Merge
// path, while Lifecycle adds only its source-snapshot validation contract.
func NewLifecycleWholeObjectsEntry(
	txn txnif.AsyncTxn,
	taskName string,
	relation handle.Relation,
	droppedObjs []*catalog.ObjectEntry,
	sourceSnapshot types.TS,
	finalPrepareDeadline time.Time,
	rt *dbutils.Runtime,
) (*mergeObjectsEntry, error) {
	if len(droppedObjs) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("Lifecycle Whole retirement has no source Object")
	}
	if sourceSnapshot.IsEmpty() {
		return nil, moerr.NewInvalidInputNoCtx("Lifecycle Whole retirement has an empty source snapshot")
	}
	if finalPrepareDeadline.IsZero() {
		return nil, moerr.NewInvalidInputNoCtx("Lifecycle Whole retirement has no prepare deadline")
	}
	return &mergeObjectsEntry{
		txn:                   txn,
		taskName:              taskName,
		relation:              relation,
		droppedObjs:           droppedObjs,
		skipTransfer:          true,
		rt:                    rt,
		collectTs:             sourceSnapshot,
		sourceSnapshot:        sourceSnapshot,
		lifecycleWholeArchive: true,
		finalPrepareDeadline:  finalPrepareDeadline,
	}, nil
}

// NewLifecycleRewriteObjectsEntry prepares the existing Merge transfer entry
// from an immutable external booking. Lifecycle differs from ordinary Merge in
// only three ways: its delete window starts at the source snapshot, every
// missing destination aborts, and Root-owned created files are not deleted by
// transaction rollback.
func NewLifecycleRewriteObjectsEntry(
	ctx context.Context,
	txn txnif.AsyncTxn,
	taskName string,
	relation handle.Relation,
	droppedObjs, createdObjs []*catalog.ObjectEntry,
	transferTable *mergesort.TransferTable,
	sourceSnapshot types.TS,
	finalPrepareDeadline time.Time,
	maxDeltaRows, maxDeltaBytes uint64,
	maxDeltaBlocks uint32,
	rt *dbutils.Runtime,
) (_ *mergeObjectsEntry, err error) {
	var entry *mergeObjectsEntry
	defer func() {
		if err == nil {
			return
		}
		if entry != nil {
			entry.RollbackTransferState()
		} else if transferTable != nil {
			// Once passed to this constructor, the decoded TransferTable has
			// exactly one owner even when validation rejects the entry before
			// its runtime state can be built.
			transferTable.Release()
		}
	}()
	if len(droppedObjs) != 1 ||
		len(createdObjs) == 0 ||
		transferTable == nil ||
		sourceSnapshot.IsEmpty() ||
		!finalPrepareDeadline.After(time.Now()) ||
		maxDeltaRows == 0 ||
		maxDeltaBytes == 0 ||
		maxDeltaBlocks == 0 {
		return nil, moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite entry is outside the certified contract",
		)
	}
	totalCreatedBlkCnt := 0
	for index, object := range createdObjs {
		createdObjs[index] = object.GetLatestNode()
		totalCreatedBlkCnt += createdObjs[index].BlockCnt()
	}
	if totalCreatedBlkCnt == 0 {
		return nil, moerr.NewInvalidInputNoCtx(
			"Lifecycle Rewrite created no live blocks",
		)
	}
	entry = &mergeObjectsEntry{
		txn:                  txn,
		taskName:             taskName,
		relation:             relation,
		droppedObjs:          droppedObjs,
		createdObjs:          createdObjs,
		transferTable:        transferTable,
		rt:                   rt,
		collectTs:            rt.Now(),
		sourceSnapshot:       sourceSnapshot,
		lifecycleRewrite:     true,
		finalPrepareDeadline: finalPrepareDeadline,
		maxDeltaRows:         maxDeltaRows,
		maxDeltaBytes:        maxDeltaBytes,
		maxDeltaBlocks:       maxDeltaBlocks,
		deltaBlocks:          make(map[types.Blockid]struct{}),
		delTbls:              make(map[types.Objectid]map[uint16]struct{}),
	}
	if entry.rt.BigDeleteHinter.HasBigDelAfter(
		entry.relation.ID(),
		&sourceSnapshot,
	) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"Lifecycle Rewrite gives up after a BigDelete: %v",
			entry.taskName,
		)
	}
	phaseOneContext, cancel := context.WithDeadline(
		ctx,
		finalPrepareDeadline,
	)
	defer cancel()
	entry.transCntBeforeCommit, _, err = entry.collectDelsAndTransfer(
		phaseOneContext,
		sourceSnapshot,
		entry.collectTs,
	)
	if err != nil {
		return nil, err
	}
	if err = entry.prepareTransferPage(phaseOneContext); err != nil {
		return nil, err
	}
	return entry, nil
}

func (entry *mergeObjectsEntry) prepareTransferPage(ctx context.Context) error {
	if entry.isTombstone {
		return nil
	}
	type transferPageStatus struct {
		page      *model.TransferHashPage
		persisted bool
	}
	k := 0
	pagesToSet := make([]transferPageStatus, 0, len(entry.droppedObjs))
	bts := time.Now().Add(time.Hour)
	createdObjIDs := make([]*objectio.ObjectId, 0, len(entry.createdObjs))
	for _, obj := range entry.createdObjs {
		createdObjIDs = append(createdObjIDs, obj.ID())
	}
	writeDisabled := false
	for _, obj := range entry.droppedObjs {
		ioVector := model.InitTransferPageIO()
		pages := make([]*model.TransferHashPage, 0, obj.BlockCnt())
		var marshalBufs []*bytes.Buffer
		var duration time.Duration
		var start time.Time
		for j := 0; j < obj.BlockCnt(); j++ {
			m := entry.transferTable.GetBlockMap(k)
			k++
			if m == nil {
				continue
			}
			tblEntry := obj.GetTable()
			isTransient := !tblEntry.GetLastestSchema(false).HasPK()
			id := obj.AsCommonID()
			id.SetBlockOffset(uint16(j))
			page := model.NewTransferHashPage(id, bts, isTransient, entry.rt.TmpFS, model.GetTTL(), model.GetDiskTTL(), createdObjIDs)
			page.TrainDetached(m)

			start = time.Now()
			err := model.AddTransferPage(page, ioVector, &marshalBufs)
			if err != nil {
				return err
			}
			duration += time.Since(start)

			entry.pageIds = append(entry.pageIds, id)
			pages = append(pages, page)
		}

		start = time.Now()
		persisted := false
		if !writeDisabled {
			transferFS, err := model.GetTransferFS(entry.rt.TmpFS)
			if err != nil {
				return err
			}
			if writeErr := model.WriteTransferPage(ctx, transferFS, pages, *ioVector, marshalBufs); writeErr != nil {
				writeDisabled = true
				logutil.Warnf("[MergeObjects] persist transfer page failed (page count %d), keeping in-memory pages for remaining objects: %v",
					len(pages), writeErr)
			} else {
				persisted = true
			}
		} else {
			model.ReleaseMarshalBufs(marshalBufs)
		}
		for _, page := range pages {
			pagesToSet = append(pagesToSet, transferPageStatus{
				page:      page,
				persisted: persisted,
			})
		}
		duration += time.Since(start)
		v2.TransferPageMergeLatencyHistogram.Observe(duration.Seconds())
	}

	now := time.Now()
	for _, status := range pagesToSet {
		if status.persisted {
			status.page.SetBornTS(now)
		} else {
			// Extend bornTS so in-memory hashmap survives the full diskTTL
			// window instead of being evicted after the short ttl (5s).
			status.page.SetBornTS(now.Add(model.GetDiskTTL() - model.GetTTL()))
		}
		entry.rt.TransferTable.AddPage(status.page)
	}

	if k != entry.transferTable.Len() {
		logutil.Fatal(fmt.Sprintf("k %v, mapping %v", k, entry.transferTable.Len()))
	}
	return nil
}

// RollbackTransferState releases the state created while preparing delete
// transfer. It deliberately does not remove output object files: before the
// entry is registered, that remains the merge task's responsibility.
func (entry *mergeObjectsEntry) RollbackTransferState() {
	if entry.transferTable != nil {
		entry.transferTable.Release()
		entry.transferTable = nil
	}
	for _, id := range entry.pageIds {
		_ = entry.rt.TransferTable.DeletePage(id)
	}
	for objectID, blkMap := range entry.delTbls {
		for blkOffset := range blkMap {
			blkID := objectio.NewBlockidWithObjectID(&objectID, blkOffset)
			entry.rt.TransferDelsMap.DeleteDelsForBlk(blkID)
		}
	}
	entry.pageIds = nil
	entry.delTbls = nil
	entry.transferredDels = nil
}

func (entry *mergeObjectsEntry) PrepareRollback() (err error) {
	entry.RollbackTransferState()

	if entry.lifecycleRewrite {
		// Lifecycle output files belong to Cleanup Root until commit succeeds.
		// A definitive abort is reclaimed by the Root sweeper.
		return nil
	}
	fs := entry.rt.Fs
	// for io task, dispatch by round robin, scope can be nil
	entry.rt.Scheduler.ScheduleScopedFn(&tasks.Context{}, tasks.IOTask, nil, func() error {
		// TODO: variable as timeout
		ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Minute, moerr.CausePrepareRollback2)

		defer cancel()
		for _, obj := range entry.createdObjs {
			_ = fs.Delete(ctx, obj.ObjectName().String())
		}
		return nil
	})
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
	ctx context.Context,
	dropped *catalog.ObjectEntry,
	from, to types.TS,
	blkOffsetBase int,
) (transCnt int, collect, transfer time.Duration, err error) {
	first := from.Next()
	if to.LT(&first) {
		return
	}
	inst := time.Now()
	maxRows := uint64(0)
	if entry.lifecycleRewrite {
		if entry.deltaRows >= entry.maxDeltaRows {
			return 0, 0, 0, moerr.NewInternalErrorNoCtx(
				"Lifecycle Rewrite post-snapshot Tombstone budget exceeded",
			)
		}
		remaining := entry.maxDeltaRows - entry.deltaRows
		if remaining < math.MaxUint64 {
			maxRows = remaining + 1
		}
	}
	bat, err := tables.TombstoneRangeScanByObjectWithMaxRows(
		ctx,
		dropped.GetTable(),
		*dropped.ID(),
		first,
		to,
		common.MergeAllocator,
		entry.rt.VectorPool.Small,
		maxRows,
	)
	if err != nil {
		return
	}
	collect = time.Since(inst)
	if bat == nil || bat.Length() == 0 {
		return
	}
	defer bat.Close()
	if entry.lifecycleRewrite {
		entry.deltaRows += uint64(bat.Length())
		entry.deltaBytes += uint64(bat.Allocated())
		if entry.deltaRows > entry.maxDeltaRows ||
			entry.deltaBytes > entry.maxDeltaBytes {
			err = moerr.NewInternalErrorNoCtx(
				"Lifecycle Rewrite post-snapshot Tombstone budget exceeded",
			)
			return
		}
	}
	inst = time.Now()
	defer func() { transfer = time.Since(inst) }()

	rowid := vector.MustFixedColWithTypeCheck[types.Rowid](bat.GetVectorByName(objectio.TombstoneAttr_Rowid_Attr).GetDownstreamVector())
	ts := vector.MustFixedColWithTypeCheck[types.TS](bat.GetVectorByName(objectio.TombstoneAttr_CommitTs_Attr).GetDownstreamVector())
	deletesPK := bat.GetVectorByName(objectio.TombstoneAttr_PK_Attr)

	count := len(rowid)
	if entry.lifecycleRewrite {
		for _, value := range rowid {
			entry.deltaBlocks[*value.BorrowBlockID()] = struct{}{}
		}
		if len(entry.deltaBlocks) > int(entry.maxDeltaBlocks) {
			err = moerr.NewInternalErrorNoCtx(
				"Lifecycle Rewrite post-snapshot block budget exceeded",
			)
			return
		}
	}
	pendingDels := make(transferredDeleteSet)
	var rowIDVec, pkVec containers.Vector
	defer func() {
		if rowIDVec != nil {
			rowIDVec.Close()
		}
		if pkVec != nil {
			pkVec.Close()
		}
	}()
	for i := 0; i < count; i++ {
		if entry.transferredDels.contains(rowid[i]) || pendingDels.contains(rowid[i]) {
			continue
		}
		row := rowid[i].GetRowOffset()
		blkOffsetInObj := int(rowid[i].GetBlockOffset())
		blkOffset := blkOffsetBase + blkOffsetInObj
		mapping := entry.transferTable.GetBlockMap(blkOffset)
		if mapping == nil {
			if entry.lifecycleRewrite {
				err = moerr.NewTxnWWConflictNoCtx(
					entry.relation.ID(),
					"Lifecycle Rewrite post-snapshot DELETE has no destination",
				)
				return
			}
			// this block had been all deleted, skip
			// Note: it is possible that the block is empty, but not the object
			continue
		}
		if uint32(len(mapping)) <= row || mapping[row].ObjIdx == api.NoTransfer {
			if entry.lifecycleRewrite {
				err = moerr.NewTxnWWConflictNoCtx(
					entry.relation.ID(),
					"Lifecycle Rewrite post-snapshot DELETE targets a retired row",
				)
				return
			}
			err = moerr.NewInternalErrorNoCtxf("%s-%d find no transfer mapping for row %d (mapping len %d)",
				dropped.ID().String(), blkOffsetInObj, row, len(mapping))
			return
		}
		destpos := mapping[row]
		if entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()] == nil {
			entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()] = make(map[uint16]struct{})
		}
		entry.delTbls[*entry.createdObjs[destpos.ObjIdx].ID()][destpos.BlkIdx] = struct{}{}
		blkID := objectio.NewBlockidWithObjectID(entry.createdObjs[destpos.ObjIdx].ID(), destpos.BlkIdx)
		entry.rt.TransferDelsMap.SetDelsForBlk(blkID, int(destpos.RowIdx), entry.txn.GetPrepareTS(), ts[i])
		var targetObj handle.Object
		targetObj, err = entry.relation.GetObject(entry.createdObjs[destpos.ObjIdx].ID(), entry.isTombstone)
		if err != nil {
			return
		}
		id := targetObj.Fingerprint()
		id.SetBlockOffset(destpos.BlkIdx)
		if pkVec == nil {
			pkVec = containers.MakeVector(*deletesPK.GetType(), entry.rt.VectorPool.Small.GetMPool())
		}
		if rowIDVec == nil {
			rowIDVec = containers.MakeVector(types.T_Rowid.ToType(), entry.rt.VectorPool.Small.GetMPool())
		}
		rowID := types.NewRowIDWithObjectIDBlkNumAndRowID(*targetObj.GetID(), destpos.BlkIdx, destpos.RowIdx)
		rowIDVec.Append(rowID, false)
		pkVec.Append(deletesPK.Get(i), false)
		pendingDels.add(rowid[i])
		transCnt++
	}
	if rowIDVec != nil {
		err = entry.relation.DeleteByPhyAddrKeys(rowIDVec, pkVec, handle.DT_MergeCompact)
		if err == nil {
			entry.transferredDels.merge(pendingDels)
			if _, sarg, injected := fault.TriggerFault(objectio.FJ_TransferErrorAfterTransfer); injected {
				err = moerr.NewInternalErrorNoCtx(sarg)
			}
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
func (entry *mergeObjectsEntry) collectDelsAndTransfer(
	ctx context.Context, from, to types.TS,
) (transCnt int, stat tempStat, err error) {
	if _, sarg, injected := fault.TriggerFault(objectio.FJ_TransferError); injected {
		err = moerr.NewInternalErrorNoCtx(sarg)
		return
	}
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
			if entry.transferTable.GetBlockMap(blksOffsetBase+iblk) != nil {
				hasMappingInThisObj = true
				break
			}
		}
		if !hasMappingInThisObj && !entry.lifecycleRewrite {
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
		cnt, ct, tt, err = entry.transferObjectDeletes(ctx, dropped, from, to, blksOffsetBase)
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
		// Release the transfer table (returns slab to pool) now that
		// both phase-1 and phase-2 transfers are done.
		if entry.transferTable != nil {
			entry.transferTable.Release()
			entry.transferTable = nil
		}
		if entry.isTombstone {
			v2.TaskCommitTombstoneMergeDurationHistogram.Observe(time.Since(inst).Seconds())
		} else {
			v2.TaskCommitDataMergeDurationHistogram.Observe(time.Since(inst).Seconds())
		}
	}()
	if entry.lifecycleWholeArchive {
		return entry.validateLifecycleWholePostSnapshotDeletes()
	}
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

	startTS := entry.txn.GetStartTS()
	if entry.lifecycleRewrite {
		startTS = entry.sourceSnapshot
	}
	if entry.rt.BigDeleteHinter.HasBigDelAfter(entry.relation.ID(), &startTS) {
		return moerr.NewInternalErrorNoCtxf("LockMerge give up in queue %v", entry.taskName)
	}

	// phase 2 transfer
	ctx := context.Background()
	var cancel context.CancelFunc
	if entry.lifecycleRewrite {
		ctx, cancel = context.WithDeadline(ctx, entry.finalPrepareDeadline)
		defer cancel()
	}
	transCnt, stat, err := entry.collectDelsAndTransfer(ctx, entry.collectTs, entry.txn.GetPrepareTS().Prev())
	if err != nil {
		return err
	}

	inst1 := time.Now()

	total := time.Since(inst)
	fields := make([]zap.Field, 0, 9)
	fields = append(fields,
		zap.Uint64("table-id", entry.relation.ID()),
		zap.String("task", entry.taskName),
		zap.Int("total-transfer", entry.transCntBeforeCommit+transCnt),
		zap.Int("in-queue-transfer", transCnt),
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

func (entry *mergeObjectsEntry) validateLifecycleWholePostSnapshotDeletes() error {
	prepareTS := entry.txn.GetPrepareTS()
	if prepareTS.LE(&entry.collectTs) {
		return moerr.NewTxnWWConflictNoCtx(
			entry.relation.ID(),
			"Lifecycle prepare timestamp does not follow source snapshot",
		)
	}
	ctx, cancel := context.WithDeadline(context.Background(), entry.finalPrepareDeadline)
	defer cancel()
	from := entry.collectTs.Next()
	to := prepareTS.Prev()
	if to.LT(&from) {
		return nil
	}
	for _, dropped := range entry.droppedObjs {
		hasPostSnapshotDelete, err := tables.HasTombstoneInRangeByObject(
			ctx,
			dropped.GetTable(),
			*dropped.ID(),
			from,
			to,
			common.MergeAllocator,
			entry.rt.VectorPool.Small,
		)
		if err != nil {
			return err
		}
		if hasPostSnapshotDelete {
			return moerr.NewTxnWWConflictNoCtx(
				entry.relation.ID(),
				"Lifecycle Whole source has a post-snapshot delete",
			)
		}
	}
	return nil
}
