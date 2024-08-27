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
	"strings"
	"time"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
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
	rel               handle.Relation
	did, tid          uint64
	isTombstone       bool

	doTransfer bool

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
		txn:          txn,
		rt:           rt,
		mergedObjs:   mergedObjs,
		createdBObjs: make([]*catalog.ObjectEntry, 0),
		mergedBlkCnt: make([]int, len(mergedObjs)),
		isTombstone:  isTombstone,
		nMergedBlk:   make([]int, len(mergedObjs)),
		blkCnt:       make([]int, len(mergedObjs)),

		targetObjSize: targetObjSize,

		createAt: time.Now(),
	}
	for i, obj := range mergedObjs {
		if obj.IsTombstone != isTombstone {
			panic(fmt.Sprintf("task.IsTombstone %v, object %v %v", isTombstone, obj.ID().String(), obj.IsTombstone))
		}
		task.mergedBlkCnt[i] = task.totalMergedBlkCnt
		task.blkCnt[i] = obj.BlockCnt()
		task.totalMergedBlkCnt += task.blkCnt[i]
	}

	task.did = mergedObjs[0].GetTable().GetDB().ID
	database, err := txn.GetDatabaseByID(task.did)
	if err != nil {
		return
	}
	task.tid = mergedObjs[0].GetTable().ID
	task.rel, err = database.GetRelationByID(task.tid)
	if err != nil {
		return
	}
	for _, meta := range mergedObjs {
		obj, err := task.rel.GetObject(meta.ID(), isTombstone)
		if err != nil {
			return nil, err
		}
		task.mergedObjsHandle = append(task.mergedObjsHandle, obj)
	}
	task.schema = task.rel.Schema(isTombstone).(*catalog.Schema)
	task.doTransfer = !strings.Contains(task.schema.Comment, pkgcatalog.MO_COMMENT_NO_DEL_HINT)
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
		task.idxs = append(task.idxs, catalog.COLIDX_COMMITS)
		task.attrs = append(task.attrs, catalog.AttrCommitTs)
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
	return task.schema.BlockMaxRows
}

func (task *mergeObjectsTask) GetObjectMaxBlocks() uint16 {
	return task.schema.ObjectMaxBlocks
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
	var view *containers.Batch
	releaseF := func() {
		if view != nil {
			view.Close()
		}
	}
	defer func() {
		if err != nil {
			releaseF()
		}
	}()

	obj := task.mergedObjsHandle[objIdx]
	if task.isTombstone {
		err = obj.Scan(ctx, &view, uint16(task.nMergedBlk[objIdx]), task.idxs, common.MergeAllocator)
	} else {
		err = obj.HybridScan(ctx, &view, uint16(task.nMergedBlk[objIdx]), task.idxs, common.MergeAllocator)
	}
	if err != nil {
		return nil, nil, nil, err
	}
	if task.isTombstone {
		rowIDs := view.Vecs[0]
		tbl := task.rel.GetMeta().(*catalog.TableEntry)
		rowIDs.Foreach(func(v any, isNull bool, row int) error {
			rowID := v.(types.Rowid)
			objectID := rowID.BorrowObjectID()
			obj, err := tbl.GetObjectByID(objectID, false)
			if err != nil {
				view.Deletes.Add(uint64(row))
				return nil
			}
			if obj.HasDropCommitted() {
				if view.Deletes == nil {
					view.Deletes = &nulls.Nulls{}
				}
				view.Deletes.Add(uint64(row))
			}
			return nil
		}, nil)
	}
	if len(task.attrs) != len(view.Vecs) {
		panic(fmt.Sprintf("mismatch %v, %v, %v", task.attrs, len(task.attrs), len(view.Vecs)))
	}
	task.nMergedBlk[objIdx]++

	bat := batch.New(true, task.attrs)
	for i, idx := range task.idxs {
		if idx == catalog.COLIDX_COMMITS {
			id := 0
			for i, attr := range task.attrs {
				if attr == catalog.AttrCommitTs {
					id = i
				}
			}
			bat.Vecs[id] = view.Vecs[i].GetDownstreamVector()
		} else {

			bat.Vecs[idx] = view.Vecs[i].GetDownstreamVector()
		}
	}
	bat.SetRowCount(view.Length())
	return bat, view.Deletes, releaseF, nil
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
	schema := task.rel.Schema(false).(*catalog.Schema)
	commitEntry := &api.MergeCommitEntry{}
	commitEntry.DbId = task.did
	commitEntry.TblId = task.tid
	commitEntry.TableName = schema.Name
	commitEntry.StartTs = task.txn.GetStartTS().ToTimestamp()
	for _, o := range task.mergedObjs {
		obj := o.GetObjectStats()
		commitEntry.MergedObjs = append(commitEntry.MergedObjs, obj.Clone().Marshal())
	}
	task.commitEntry = commitEntry
	// leave mapping to ReadMergeAndWrite
	return commitEntry
}

func (task *mergeObjectsTask) PrepareNewWriter() *blockio.BlockWriter {
	schema := task.rel.Schema(task.isTombstone).(*catalog.Schema)
	seqnums := make([]uint16, 0, len(schema.ColDefs)-1)
	for _, def := range schema.ColDefs {
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

	if schema.HasPK() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
		sortkeyIsPK = true
	} else if schema.HasSortKey() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
	}

	return mergesort.GetNewWriter(task.rt.Fs.Service, schema.Version, seqnums, sortkeyPos, sortkeyIsPK, task.isTombstone)
}

func (task *mergeObjectsTask) DoTransfer() bool {
	return task.doTransfer
}

func (task *mergeObjectsTask) Name() string {
	return fmt.Sprintf("[MT-%d]-%d-%s", task.ID(), task.rel.ID(), task.schema.Name)
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

	schema := task.rel.Schema(task.isTombstone).(*catalog.Schema)
	sortkeyPos := -1
	if schema.HasSortKey() {
		sortkeyPos = schema.GetSingleSortKeyIdx()
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
		objectio.SetObjectStats(objstats, &stats)
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
		totalSize += uint64(obj.GetOriginSize())
	}
	return totalSize
}

func (task *mergeObjectsTask) GetTotalRowCnt() uint32 {
	totalRowCnt := 0
	for _, obj := range task.mergedObjs {
		totalRowCnt += obj.GetRows()
	}
	return uint32(totalRowCnt)
}

// for UT
func (task *mergeObjectsTask) GetCreatedObjects() []*catalog.ObjectEntry {
	return task.createdBObjs
}
