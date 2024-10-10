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

package jobs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type mergeObjectsTask struct {
	*tasks.BaseTask
	txn               txnif.AsyncTxn
	rt                *dbutils.Runtime
	mergedObjs        []*catalog.ObjectEntry
	mergedObjsHandle  []handle.Object
	mergedBlkCnt      []int
	totalMergedBlkCnt int
	createdBObjs      []*catalog.ObjectEntry
	commitEntry       *api.MergeCommitEntry
	transferMaps      api.TransferMaps
	tableEntry        *catalog.TableEntry
	did, tid          uint64
	isTombstone       bool

	blkCnt     []int
	nMergedBlk []int
	schema     *catalog.Schema
	idxs       []int
	attrs      []string

	targetObjSize uint32

	createAt time.Time
}

func NewMergeObjectsTask(
	ctx *tasks.Context,
	txn txnif.AsyncTxn,
	mergedObjs []*catalog.ObjectEntry,
	rt *dbutils.Runtime,
	targetObjSize uint32,
	isTombstone bool) (task *mergeObjectsTask, err error) {
	if len(mergedObjs) == 0 {
		panic("empty mergedObjs")
	}
	task = &mergeObjectsTask{
		txn:              txn,
		rt:               rt,
		did:              mergedObjs[0].GetTable().GetDB().ID,
		tid:              mergedObjs[0].GetTable().ID,
		mergedObjs:       mergedObjs,
		mergedObjsHandle: make([]handle.Object, len(mergedObjs)),
		mergedBlkCnt:     make([]int, len(mergedObjs)),
		isTombstone:      isTombstone,
		nMergedBlk:       make([]int, len(mergedObjs)),
		blkCnt:           make([]int, len(mergedObjs)),
		targetObjSize:    targetObjSize,
		createAt:         time.Now(),
	}

	database, err := txn.GetDatabaseByID(task.did)
	if err != nil {
		return
	}
	rel, err := database.GetRelationByID(task.tid)
	if err != nil {
		return
	}

	for i, obj := range mergedObjs {
		if obj.IsTombstone != isTombstone {
			panic(fmt.Sprintf("task.IsTombstone %v, object %v %v", isTombstone, obj.ID().String(), obj.IsTombstone))
		}
		task.mergedBlkCnt[i] = task.totalMergedBlkCnt
		task.blkCnt[i] = obj.BlockCnt()
		task.totalMergedBlkCnt += task.blkCnt[i]

		objHandle, err := rel.GetObject(obj.ID(), isTombstone)
		if err != nil {
			return nil, err
		}
		task.mergedObjsHandle[i] = objHandle
	}

	task.schema = rel.Schema(isTombstone).(*catalog.Schema)
	task.tableEntry = rel.GetMeta().(*catalog.TableEntry)
	task.idxs = make([]int, 0, len(task.schema.ColDefs)-1)
	task.attrs = make([]string, 0, len(task.schema.ColDefs)-1)
	for _, def := range task.schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		task.idxs = append(task.idxs, def.Idx)
		task.attrs = append(task.attrs, def.Name)
	}
	if isTombstone {
		task.idxs = append(task.idxs, objectio.SEQNUM_COMMITTS)
		task.attrs = append(task.attrs, objectio.TombstoneAttr_CommitTs_Attr)
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *mergeObjectsTask) GetObjectCnt() int {
	return len(task.mergedObjs)
}

func (task *mergeObjectsTask) GetBlkCnts() []int {
	return task.blkCnt
}

func (task *mergeObjectsTask) GetAccBlkCnts() []int {
	return task.mergedBlkCnt
}

func (task *mergeObjectsTask) GetBlockMaxRows() uint32 {
	return task.schema.Extra.BlockMaxRows
}

func (task *mergeObjectsTask) GetObjectMaxBlocks() uint16 {
	return uint16(task.schema.Extra.ObjectMaxBlocks)
}

func (task *mergeObjectsTask) GetTargetObjSize() uint32 {
	return task.targetObjSize
}

func (task *mergeObjectsTask) GetSortKeyPos() int {
	sortKeyPos := -1
	if task.schema.HasSortKey() {
		sortKeyPos = task.schema.GetSingleSortKeyIdx()
	}
	return sortKeyPos
}

func (task *mergeObjectsTask) GetSortKeyType() types.Type {
	if task.schema.HasSortKey() {
		return task.schema.GetSingleSortKeyType()
	}
	return types.Type{}
}

// impl DisposableVecPool
func (task *mergeObjectsTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := task.rt.VectorPool.Transient.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (task *mergeObjectsTask) GetMPool() *mpool.MPool {
	return task.rt.VectorPool.Transient.GetMPool()
}

func (task *mergeObjectsTask) HostHintName() string { return "DN" }

func (task *mergeObjectsTask) LoadNextBatch(ctx context.Context, objIdx uint32) (*batch.Batch, *nulls.Nulls, func(), error) {
	if objIdx >= uint32(len(task.mergedObjs)) {
		panic("invalid objIdx")
	}
	if task.nMergedBlk[objIdx] >= task.blkCnt[objIdx] {
		return nil, nil, nil, mergesort.ErrNoMoreBlocks
	}
	var err error
	var data *containers.Batch
	releaseF := func() {
		if data != nil {
			data.Close()
		}
	}
	defer func() {
		if err != nil {
			releaseF()
		}
	}()

	obj := task.mergedObjsHandle[objIdx]
	if task.isTombstone {
		err = obj.Scan(ctx, &data, uint16(task.nMergedBlk[objIdx]), task.idxs, common.MergeAllocator)
	} else {
		err = obj.HybridScan(ctx, &data, uint16(task.nMergedBlk[objIdx]), task.idxs, common.MergeAllocator)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if task.isTombstone {
		err = data.Vecs[0].Foreach(func(v any, isNull bool, row int) error {
			rowID := v.(types.Rowid)
			objectID := rowID.BorrowObjectID()
			obj, err := task.tableEntry.GetObjectByID(objectID, false)
			if err != nil || obj.HasDropCommitted() {
				if data.Deletes == nil {
					data.Deletes = &nulls.Nulls{}
				}
				data.Deletes.Add(uint64(row))
				return nil
			}
			return nil
		}, nil)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if len(task.attrs) != len(data.Vecs) {
		panic(fmt.Sprintf("mismatch %v, %v, %v", task.attrs, len(task.attrs), len(data.Vecs)))
	}
	task.nMergedBlk[objIdx]++

	bat := batch.New(true, task.attrs)
	for i, idx := range task.idxs {
		if idx == objectio.SEQNUM_COMMITTS {
			id := slices.Index(task.attrs, objectio.TombstoneAttr_CommitTs_Attr)
			bat.Vecs[id] = data.Vecs[i].GetDownstreamVector()
		} else {
			bat.Vecs[idx] = data.Vecs[i].GetDownstreamVector()
		}
	}
	bat.SetRowCount(data.Length())
	return bat, data.Deletes, releaseF, nil
}

func (task *mergeObjectsTask) GetCommitEntry() *api.MergeCommitEntry {
	if task.commitEntry == nil {
		return task.prepareCommitEntry()
	}
	return task.commitEntry
}

func (task *mergeObjectsTask) InitTransferMaps(blkCnt int) {
	task.transferMaps = make(api.TransferMaps, blkCnt)
	for i := range task.transferMaps {
		task.transferMaps[i] = make(api.TransferMap)
	}
}

func (task *mergeObjectsTask) GetTransferMaps() api.TransferMaps {
	return task.transferMaps
}

func (task *mergeObjectsTask) prepareCommitEntry() *api.MergeCommitEntry {
	commitEntry := &api.MergeCommitEntry{}
	commitEntry.DbId = task.did
	commitEntry.TblId = task.tid
	commitEntry.TableName = task.schema.Name
	commitEntry.StartTs = task.txn.GetStartTS().ToTimestamp()
	for _, o := range task.mergedObjs {
		obj := *o.GetObjectStats()
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, obj[:])
	}
	task.commitEntry = commitEntry
	// leave mapping to ReadMergeAndWrite
	return commitEntry
}

func (task *mergeObjectsTask) PrepareNewWriter() *blockio.BlockWriter {
	seqnums := make([]uint16, 0, len(task.schema.ColDefs)-1)
	for _, def := range task.schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		seqnums = append(seqnums, def.SeqNum)
	}
	if task.isTombstone {
		seqnums = append(seqnums, objectio.SEQNUM_COMMITTS)
	}
	sortkeyIsPK := false
	sortkeyPos := -1

	if task.schema.HasPK() {
		sortkeyPos = task.schema.GetSingleSortKeyIdx()
		sortkeyIsPK = true
	} else if task.schema.HasSortKey() {
		sortkeyPos = task.schema.GetSingleSortKeyIdx()
	}

	return blockio.ConstructWriter(
		task.schema.Version,
		seqnums,
		sortkeyPos,
		sortkeyIsPK,
		task.isTombstone,
		task.rt.Fs.Service,
	)
}

func (task *mergeObjectsTask) DoTransfer() bool {
	return !task.isTombstone && !strings.Contains(task.schema.Comment, pkgcatalog.MO_COMMENT_NO_DEL_HINT)
}

func (task *mergeObjectsTask) Name() string {
	return fmt.Sprintf("[MT-%d]-%d-%s", task.ID(), task.tid, task.schema.Name)
}

func (task *mergeObjectsTask) Execute(ctx context.Context) (err error) {
	phaseDesc := ""
	defer func() {
		if err != nil {
			logutil.Error("[MERGE-ERR]",
				zap.String("task", task.Name()),
				zap.String("phase", phaseDesc),
				zap.Error(err),
			)
		}
	}()

	if time.Since(task.createAt) > time.Second*10 {
		logutil.Warn(
			"[MERGE-SLOW-SCHED]",
			zap.String("task", task.Name()),
			common.AnyField("duration", time.Since(task.createAt)),
		)
	}

	sortkeyPos := -1
	if task.schema.HasSortKey() {
		sortkeyPos = task.schema.GetSingleSortKeyIdx()
	}
	if task.rt.LockMergeService.IsLockedByUser(task.tid, task.schema.Name) {
		return moerr.NewInternalErrorNoCtxf("LockMerge give up in exec %v", task.Name())
	}
	phaseDesc = "1-DoMergeAndWrite"
	if err = mergesort.DoMergeAndWrite(ctx, task.txn.String(), sortkeyPos, task, task.isTombstone); err != nil {
		return err
	}

	phaseDesc = "2-HandleMergeEntryInTxn"
	if task.createdBObjs, err = HandleMergeEntryInTxn(ctx, task.txn, task.Name(), task.commitEntry, task.transferMaps, task.rt, task.isTombstone); err != nil {
		return err
	}

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.MergeBlocks.Add(1)
	})
	return nil
}

func HandleMergeEntryInTxn(
	ctx context.Context,
	txn txnif.AsyncTxn,
	taskName string,
	entry *api.MergeCommitEntry,
	booking api.TransferMaps,
	rt *dbutils.Runtime,
	isTombstone bool,
) ([]*catalog.ObjectEntry, error) {
	database, err := txn.GetDatabaseByID(entry.DbId)
	if err != nil {
		return nil, err
	}
	rel, err := database.GetRelationByID(entry.TblId)
	if err != nil {
		return nil, err
	}

	mergedObjs := make([]*catalog.ObjectEntry, 0, len(entry.MergedObjs))
	createdObjs := make([]*catalog.ObjectEntry, 0, len(entry.CreatedObjs))
	ids := make([]*common.ID, 0, len(entry.MergedObjs)*2)

	// drop merged blocks and objects
	for _, item := range entry.MergedObjs {
		drop := objectio.ObjectStats(item)
		objID := drop.ObjectName().ObjectId()
		obj, err := rel.GetObject(objID, isTombstone)
		if err != nil {
			if moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
				logutil.Infof("[MERGE-EOB] LockMerge %v %v", objID.ShortStringEx(), err)
			}
			return nil, err
		}
		mergedObjs = append(mergedObjs, obj.GetMeta().(*catalog.ObjectEntry))
		if err = rel.SoftDeleteObject(objID, isTombstone); err != nil {
			return nil, err
		}
	}

	sorted := false
	if rel.GetMeta().(*catalog.TableEntry).GetLastestSchema(isTombstone).HasSortKey() {
		sorted = true
	}
	// construct new object,
	for _, stats := range entry.CreatedObjs {
		stats := objectio.ObjectStats(stats)
		objID := stats.ObjectName().ObjectId()
		// set stats and sorted property
		objstats := objectio.NewObjectStatsWithObjectID(objID, false, sorted, false)
		err := objectio.SetObjectStats(objstats, &stats)
		if err != nil {
			return nil, err
		}
		obj, err := rel.CreateNonAppendableObject(
			isTombstone,
			new(objectio.CreateObjOpt).WithObjectStats(objstats).WithIsTombstone(isTombstone))
		if err != nil {
			return nil, err
		}
		createdObjs = append(createdObjs, obj.GetMeta().(*catalog.ObjectEntry))
	}

	txnEntry, err := txnentries.NewMergeObjectsEntry(
		ctx,
		txn,
		taskName,
		rel,
		mergedObjs,
		createdObjs,
		booking,
		isTombstone,
		rt,
	)
	if err != nil {
		return nil, err
	}

	if isTombstone {
		if err = txn.LogTxnEntry(entry.DbId, entry.TblId, txnEntry, nil, ids); err != nil {
			return nil, err
		}
	} else {
		if err = txn.LogTxnEntry(entry.DbId, entry.TblId, txnEntry, ids, nil); err != nil {
			return nil, err
		}
	}

	return createdObjs, nil
}

func (task *mergeObjectsTask) GetTotalSize() uint64 {
	totalSize := uint64(0)
	for _, obj := range task.mergedObjs {
		totalSize += uint64(obj.OriginSize())
	}
	return totalSize
}

func (task *mergeObjectsTask) GetTotalRowCnt() uint32 {
	totalRowCnt := 0
	for _, obj := range task.mergedObjs {
		totalRowCnt += int(obj.Rows())
	}
	return uint32(totalRowCnt)
}

// for UT
func (task *mergeObjectsTask) GetCreatedObjects() []*catalog.ObjectEntry {
	return task.createdBObjs
}
