// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
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
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
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
	"go.uber.org/zap/zapcore"
)

type TestFlushBailoutPos1 struct{}
type TestFlushBailoutPos2 struct{}

var FlushTableTailTaskFactory = func(
	metas []*catalog.ObjectEntry, rt *dbutils.Runtime, endTs types.TS, /* end of dirty range*/
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewFlushTableTailTask(ctx, txn, metas, rt, endTs)
	}
}

type flushTableTailTask struct {
	*tasks.BaseTask
	txn        txnif.AsyncTxn
	rt         *dbutils.Runtime
	dirtyEndTs types.TS

	scopes []common.ID
	schema *catalog.Schema

	rel  handle.Relation
	dbid uint64

	// record the row mapping from deleted blocks to created blocks
	transMappings *api.BlkTransferBooking

	aObjMetas         []*catalog.ObjectEntry
	delSrcMetas       []*catalog.ObjectEntry
	aObjHandles       []handle.Object
	delSrcHandles     []handle.Object
	createdObjHandles handle.Object

	dirtyLen                 int
	createdMergedObjectName  string
	createdDeletesObjectName string

	mergeRowsCnt, aObjDeletesCnt, nObjDeletesCnt int
}

// A note about flush start timestamp
//
// As the last **committed** time, not the newest allcated time,
// is used in NewFlushTableTailTask, there will be a situation that
// some commiting appends prepared between committed-time and aobj-freeze-time
// are ignored during the data collection stage of flushing,
// which leads to transfer-row-not-found problem.
//
// The proposed solution is to add a check function in NewFlushTableTailTask
// to figure out if there exist an AppendNode with a bigger prepared time
// than flush-start-ts, and if so, retry the flush task
//
// Two question:
//
// 1. How about deletes prepared in that special time range?
//    Never mind, deletes will be transfered when committing the flush task
// 2. Is it guaranteed that the check function is able to see all possible AppendNodes?
//    Probably no, because getting appender and attaching AppendNode are not atomic group opertions.
//    Imagine:
//
//                freeze  check
// committed  x1     |     |     x2
// prepared          |     |  o2
// preparing    i2   |     |
//
// - x1 is the last committed time.
// - getting appender(i2 in graph) is before the freezing
// - attaching AppendNode successfully (o2 in graph) after the check
// - finishing commit at x2
//
// So in order for the check function to work, a dedicated lock is added
// on ablock to ensure that NO AppendNode will be attatched to ablock
// after the very moment when the ablock is freezed.
//
// In the first version proposal, the check in NewFlushTableTailTask is omitted,
// because the existing PrepareCompact in ablock already handles that thing.
// If the last AppendNode in an ablock is not committed, PrepareCompact will
// return false to reschedule the task. However, commiting AppendNode doesn't
// guarantee that the committs has been updated. It's still possible to get a
// old startts which is not able to collect all appends in the ablock.

func NewFlushTableTailTask(
	ctx *tasks.Context,
	txn txnif.AsyncTxn,
	objs []*catalog.ObjectEntry,
	rt *dbutils.Runtime,
	dirtyEndTs types.TS,
) (task *flushTableTailTask, err error) {
	task = &flushTableTailTask{
		txn:        txn,
		rt:         rt,
		dirtyEndTs: dirtyEndTs,
	}
	meta := objs[0]
	dbId := meta.GetTable().GetDB().ID
	task.dbid = dbId
	database, err := txn.UnsafeGetDatabase(dbId)
	if err != nil {
		return
	}
	tableId := meta.GetTable().ID
	rel, err := database.UnsafeGetRelation(tableId)
	task.rel = rel
	if err != nil {
		return
	}
	task.schema = rel.Schema().(*catalog.Schema)

	for _, obj := range objs {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(&obj.ID)
		if err != nil {
			return
		}
		if hdl.IsAppendable() {
			task.aObjMetas = append(task.aObjMetas, obj)
			task.aObjHandles = append(task.aObjHandles, hdl)
			if obj.GetObjectData().CheckFlushTaskRetry(txn.GetStartTS()) {
				return nil, txnif.ErrTxnNeedRetry
			}
		} else {
			task.delSrcMetas = append(task.delSrcMetas, obj)
			task.delSrcHandles = append(task.delSrcHandles, hdl)
		}
	}
	task.transMappings = mergesort.NewBlkTransferBooking(len(task.aObjHandles))

	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)

	tblEntry := rel.GetMeta().(*catalog.TableEntry)
	tblEntry.Stats.RLock()
	defer tblEntry.Stats.RUnlock()
	task.dirtyLen = len(tblEntry.DeletedDirties)
	for _, obj := range tblEntry.DeletedDirties {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(&obj.ID)
		if err != nil {
			return
		}
		task.delSrcMetas = append(task.delSrcMetas, obj)
		task.delSrcHandles = append(task.delSrcHandles, hdl)
	}
	return
}

// Scopes is used in conflict checking in scheduler. For ScopedTask interface
func (task *flushTableTailTask) Scopes() []common.ID { return task.scopes }

// Name is for ScopedTask interface
func (task *flushTableTailTask) Name() string {
	return fmt.Sprintf("[%d]FT-%d-%s", task.ID(), task.rel.ID(), task.schema.Name)
}

func (task *flushTableTailTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	enc.AddString("endTs", task.dirtyEndTs.ToString())
	objs := ""
	for _, obj := range task.aObjMetas {
		objs = fmt.Sprintf("%s%s,", objs, obj.ID.ShortStringEx())
	}
	enc.AddString("a-objs", objs)
	// delsrc := ""
	// for _, del := range task.delSrcMetas {
	// 	delsrc = fmt.Sprintf("%s%s,", delsrc, del.ID.ShortStringEx())
	// }
	// enc.AddString("deletes-src", delsrc)
	enc.AddInt("delete-obj-ndv", len(task.delSrcMetas))

	toObjs := ""
	if task.createdObjHandles != nil {
		id := task.createdObjHandles.GetID()
		toObjs = fmt.Sprintf("%s%s,", toObjs, id.ShortStringEx())
	}
	if toObjs != "" {
		enc.AddString("to-objs", toObjs)
	}
	return
}

func (task *flushTableTailTask) Execute(ctx context.Context) (err error) {
	logutil.Info("[Start]", common.OperationField(task.Name()), common.OperandField(task),
		common.OperandField(len(task.aObjHandles)+len(task.delSrcHandles)))

	phaseDesc := ""
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField(task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseDesc),
			)
		}
	}()
	now := time.Now()

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-flushing appendable blocks for snapshot"
	snapshotSubtasks, err := task.flushAObjsForSnapshot(ctx)
	if err != nil {
		return
	}
	defer func() {
		releaseFlushObjTasks(snapshotSubtasks, err)
	}()

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-write all deletes from naobjs"
	// just collect deletes, do not soft delete it, leave that to merge task.
	deleteTask, emptyMap, err := task.flushAllDeletesFromDelSrc(ctx)
	if err != nil {
		return
	}
	defer func() {
		relaseFlushDelTask(deleteTask, err)
	}()
	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-merge aobjects"
	// merge aobjects, no need to wait, it is a sync procedure, that is why put it
	// after flushAObjsForSnapshot and flushAllDeletesFromNObjs
	if err = task.mergeAObjs(ctx); err != nil {
		return
	}

	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		err = moerr.NewInternalErrorNoCtx("test merge bail out")
		return
	}

	/////////////////////
	//// phase seperator
	///////////////////
	phaseDesc = "1-waiting flushing appendable blocks for snapshot"
	// wait flush tasks
	if err = task.waitFlushAObjForSnapshot(ctx, snapshotSubtasks); err != nil {
		return
	}

	/////////////////////
	//// phase seperator
	///////////////////

	phaseDesc = "1-wait flushing all deletes from naobjs"
	if err = task.waitFlushAllDeletesFromDelSrc(ctx, deleteTask, emptyMap); err != nil {
		return
	}

	phaseDesc = "1-wait LogTxnEntry"
	txnEntry, err := txnentries.NewFlushTableTailEntry(
		task.txn,
		task.ID(),
		task.transMappings,
		task.rel.GetMeta().(*catalog.TableEntry),
		task.aObjMetas,
		task.delSrcMetas,
		task.aObjHandles,
		task.delSrcHandles,
		task.createdObjHandles,
		task.createdDeletesObjectName,
		task.createdMergedObjectName,
		task.dirtyLen,
		task.rt,
		task.dirtyEndTs,
	)
	if err != nil {
		return err
	}
	readset := make([]*common.ID, 0, len(task.aObjMetas)+len(task.delSrcMetas))
	for _, obj := range task.aObjMetas {
		readset = append(readset, obj.AsCommonID())
	}
	for _, obj := range task.delSrcMetas[:(len(task.delSrcMetas) - task.dirtyLen)] {
		readset = append(readset, obj.AsCommonID())
	}
	if err = task.txn.LogTxnEntry(
		task.dbid,
		task.rel.ID(),
		txnEntry,
		readset,
	); err != nil {
		return
	}
	/////////////////////

	duration := time.Since(now)
	logutil.Info("[End]", common.OperationField(task.Name()),
		common.AnyField("txn-start-ts", task.txn.GetStartTS().ToString()),
		zap.Int("aobj-deletes", task.aObjDeletesCnt),
		zap.Int("aobj-merge-rows", task.mergeRowsCnt),
		zap.Int("nobj-deletes", task.nObjDeletesCnt),
		common.DurationField(duration),
		common.OperandField(task))

	v2.TaskFlushTableTailDurationHistogram.Observe(duration.Seconds())

	sleep, name, exist := fault.TriggerFault("slow_flush")
	if exist && name == task.schema.Name {
		time.Sleep(time.Duration(sleep) * time.Second)
	}
	return
}

// prepareAObjSortedData read the data from appendable blocks, sort them if sort key exists
func (task *flushTableTailTask) prepareAObjSortedData(
	ctx context.Context, objIdx int, idxs []int, sortKeyPos int,
) (bat *containers.Batch, empty bool, err error) {
	if len(idxs) <= 0 {
		logutil.Infof("[FlushTabletail] no mergeable columns")
		return nil, true, nil
	}
	obj := task.aObjHandles[objIdx]

	views, err := obj.GetColumnDataByIds(ctx, 0, idxs, common.MergeAllocator)
	if err != nil {
		return
	}
	bat = containers.NewBatch()
	rowCntBeforeApplyDelete := views.Columns[0].Length()
	deletes := views.DeleteMask
	views.ApplyDeletes()
	defer views.Close()
	for i, colidx := range idxs {
		colview := views.Columns[i]
		if colview == nil {
			empty = true
			return
		}
		vec := colview.Orphan()
		if vec.Length() == 0 {
			empty = true
			vec.Close()
			bat.Close()
			return
		}
		bat.AddVector(task.schema.ColDefs[colidx].Name, vec.TryConvertConst())
	}

	if deletes != nil {
		task.aObjDeletesCnt += deletes.GetCardinality()
	}

	var sortMapping []int64
	if sortKeyPos >= 0 {
		if objIdx == 0 {
			logutil.Infof("flushtabletail sort obj on %s", bat.Attrs[sortKeyPos])
		}
		sortMapping, err = mergesort.SortBlockColumns(bat.Vecs, sortKeyPos, task.rt.VectorPool.Transient)
		if err != nil {
			return
		}
	}
	mergesort.AddSortPhaseMapping(task.transMappings, objIdx, rowCntBeforeApplyDelete, deletes, sortMapping)
	return
}

// mergeAObjs merge the data from appendable blocks, and write the merged data to new block,
// recording row mapping in blkTransferBooking struct
func (task *flushTableTailTask) mergeAObjs(ctx context.Context) (err error) {
	if len(task.aObjMetas) == 0 {
		return nil
	}

	// prepare columns idx and sortKey to read sorted batch
	schema := task.schema
	seqnums := make([]uint16, 0, len(schema.ColDefs))
	readColIdxs := make([]int, 0, len(schema.ColDefs))
	sortKeyIdx := -1
	sortKeyPos := -1
	if schema.HasSortKey() {
		sortKeyIdx = schema.GetSingleSortKeyIdx()
	}
	for i, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		readColIdxs = append(readColIdxs, def.Idx)
		if def.Idx == sortKeyIdx {
			sortKeyPos = i
		}
		seqnums = append(seqnums, def.SeqNum)
	}

	// read from aobjects
	readedBats := make([]*containers.Batch, 0, len(task.aObjHandles))
	for _, block := range task.aObjHandles {
		err = block.Prefetch(readColIdxs)
		if err != nil {
			return
		}
	}
	for i := range task.aObjHandles {
		bat, empty, err := task.prepareAObjSortedData(ctx, i, readColIdxs, sortKeyPos)
		if err != nil {
			return err
		}
		if empty {
			continue
		}
		readedBats = append(readedBats, bat)
	}

	for _, bat := range readedBats {
		defer bat.Close()
	}

	if len(readedBats) == 0 {
		// just soft delete all Objects
		for _, obj := range task.aObjHandles {
			tbl := obj.GetRelation()
			if err = tbl.SoftDeleteObject(obj.GetID()); err != nil {
				return err
			}
		}
		mergesort.CleanTransMapping(task.transMappings)
		return nil
	}

	// create new object to hold merged blocks
	var toObjectEntry *catalog.ObjectEntry
	var toObjectHandle handle.Object
	if toObjectHandle, err = task.rel.CreateNonAppendableObject(false, nil); err != nil {
		return
	}
	toObjectEntry = toObjectHandle.GetMeta().(*catalog.ObjectEntry)
	toObjectEntry.SetSorted()

	// prepare merge
	// pick the sort key or first column to run first merge, determing the ordering
	sortVecs := make([]containers.Vector, 0, len(readedBats))
	// fromLayout describes the layout of the input batch, which is a list of batch length
	fromLayout := make([]uint32, 0, len(readedBats))
	// toLayout describes the layout of the output batch, i.e. [8192, 8192, 8192, 4242]
	toLayout := make([]uint32, 0, len(readedBats))
	totalRowCnt := 0
	if sortKeyPos < 0 {
		// no pk, just pick the first column to reshape
		sortKeyPos = 0
	}
	for _, bat := range readedBats {
		vec := bat.Vecs[sortKeyPos]
		fromLayout = append(fromLayout, uint32(vec.Length()))
		totalRowCnt += vec.Length()
		sortVecs = append(sortVecs, vec)
	}
	task.mergeRowsCnt = totalRowCnt
	rowsLeft := totalRowCnt
	for rowsLeft > 0 {
		if rowsLeft > int(schema.BlockMaxRows) {
			toLayout = append(toLayout, schema.BlockMaxRows)
			rowsLeft -= int(schema.BlockMaxRows)
		} else {
			toLayout = append(toLayout, uint32(rowsLeft))
			break
		}
	}

	// do first sort
	var orderedVecs []containers.Vector
	var sortedIdx []uint32
	if schema.HasSortKey() {
		// mergesort is needed, allocate sortedidx and mapping
		allocSz := totalRowCnt * 4
		// sortedIdx is used to shuffle other columns according to the order of the sort key
		sortIdxNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		// sortedidx will be used to shuffle other column, defer free
		defer common.MergeAllocator.Free(sortIdxNode)
		sortedIdx = unsafe.Slice((*uint32)(unsafe.Pointer(&sortIdxNode[0])), totalRowCnt)

		mappingNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		mapping := unsafe.Slice((*uint32)(unsafe.Pointer(&mappingNode[0])), totalRowCnt)

		// modify sortidx and mapping
		orderedVecs = mergesort.MergeColumn(sortVecs, sortedIdx, mapping, fromLayout, toLayout, task.rt.VectorPool.Transient)
		mergesort.UpdateMappingAfterMerge(task.transMappings, mapping, fromLayout, toLayout)
		// free mapping, which is never used again
		common.MergeAllocator.Free(mappingNode)
	} else {
		// just do reshape
		orderedVecs = mergesort.ReshapeColumn(sortVecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		// UpdateMappingAfterMerge will handle the nil mapping
		mergesort.UpdateMappingAfterMerge(task.transMappings, nil, fromLayout, toLayout)
	}
	for _, vec := range orderedVecs {
		defer vec.Close()
	}

	// make all columns ordered and prepared writtenBatches
	writtenBatches := make([]*containers.Batch, 0, len(orderedVecs))
	task.createdObjHandles = toObjectHandle
	for i := 0; i < len(orderedVecs); i++ {
		writtenBatches = append(writtenBatches, containers.NewBatch())
	}
	vecs := make([]containers.Vector, 0, len(readedBats))
	for i, idx := range readColIdxs {
		// skip rowid and sort(reshape) column in the first run
		if schema.ColDefs[idx].IsPhyAddr() {
			continue
		}
		if i == sortKeyPos {
			for i, vec := range orderedVecs {
				writtenBatches[i].AddVector(schema.ColDefs[idx].Name, vec)
			}
			continue
		}
		vecs = vecs[:0]
		for _, bat := range readedBats {
			vecs = append(vecs, bat.Vecs[i])
		}
		var outvecs []containers.Vector
		if schema.HasSortKey() {
			outvecs = mergesort.ShuffleColumn(vecs, sortedIdx, fromLayout, toLayout, task.rt.VectorPool.Transient)
		} else {
			outvecs = mergesort.ReshapeColumn(vecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		}

		for i, vec := range outvecs {
			writtenBatches[i].AddVector(schema.ColDefs[idx].Name, vec)
			defer vec.Close()
		}
	}

	// write!
	name := objectio.BuildObjectNameWithObjectID(&toObjectEntry.ID)
	writer, err := blockio.NewBlockWriterNew(task.rt.Fs.Service, name, schema.Version, seqnums)
	if err != nil {
		return err
	}
	if schema.HasPK() {
		pkIdx := schema.GetSingleSortKeyIdx()
		writer.SetPrimaryKey(uint16(pkIdx))
	} else if schema.HasSortKey() {
		writer.SetSortKey(uint16(schema.GetSingleSortKeyIdx()))
	}
	for _, bat := range writtenBatches {
		_, err = writer.WriteBatch(containers.ToCNBatch(bat))
		if err != nil {
			return err
		}
	}
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	task.createdMergedObjectName = name.String()

	// update new status for created blocks
	err = toObjectHandle.UpdateStats(writer.Stats())
	if err != nil {
		return
	}
	err = toObjectHandle.GetMeta().(*catalog.ObjectEntry).GetObjectData().Init()
	if err != nil {
		return
	}

	// soft delete all aobjs
	for _, obj := range task.aObjHandles {
		tbl := obj.GetRelation()
		if err = tbl.SoftDeleteObject(obj.GetID()); err != nil {
			return err
		}
	}

	return nil
}

// flushAObjsForSnapshot schedule io task to flush aobjects for snapshot read. this function will not release any data in io task
func (task *flushTableTailTask) flushAObjsForSnapshot(ctx context.Context) (subtasks []*flushObjTask, err error) {
	defer func() {
		if err != nil {
			releaseFlushObjTasks(subtasks, err)
		}
	}()
	subtasks = make([]*flushObjTask, len(task.aObjMetas))
	// fire flush task
	for i, obj := range task.aObjMetas {
		var data, deletes *containers.Batch
		var dataVer *containers.BatchWithVersion
		objData := obj.GetObjectData()
		if dataVer, err = objData.CollectAppendInRange(
			types.TS{}, task.txn.GetStartTS(), true, common.MergeAllocator,
		); err != nil {
			return
		}
		data = dataVer.Batch
		if data == nil || data.Length() == 0 {
			// the new appendable block might has no data when we flush the table, just skip it
			// In previous impl, runner will only pass non-empty obj to NewCompactBlackTask
			continue
		}
		// do not close data, leave that to wait phase
		if deletes, _, err = objData.CollectDeleteInRange(
			ctx, types.TS{}, task.txn.GetStartTS(), true, common.MergeAllocator,
		); err != nil {
			return
		}
		if deletes != nil {
			// make sure every batch in deltaloc object is sorted by rowid
			_, err := mergesort.SortBlockColumns(deletes.Vecs, 0, task.rt.VectorPool.Transient)
			if err != nil {
				return nil, err
			}
		}

		aobjectTask := NewFlushObjTask(
			tasks.WaitableCtx,
			dataVer.Version,
			dataVer.Seqnums,
			objData.GetFs(),
			obj,
			data,
			deletes,
			true,
		)
		if err = task.rt.Scheduler.Schedule(aobjectTask); err != nil {
			return
		}
		subtasks[i] = aobjectTask
	}
	return
}

// waitFlushAObjForSnapshot waits all io tasks about flushing aobject for snapshot read, update locations
func (task *flushTableTailTask) waitFlushAObjForSnapshot(ctx context.Context, subtasks []*flushObjTask) (err error) {
	ictx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	for i, subtask := range subtasks {
		if subtask == nil {
			continue
		}
		if err = subtask.WaitDone(ictx); err != nil {
			return
		}
		if err = task.aObjHandles[i].UpdateStats(subtask.stat); err != nil {
			return
		}
		if subtask.delta == nil {
			continue
		}
		deltaLoc := blockio.EncodeLocation(
			subtask.name,
			subtask.blocks[1].GetExtent(),
			uint32(subtask.delta.Length()),
			subtask.blocks[1].GetID())

		if err = task.aObjHandles[i].UpdateDeltaLoc(0, deltaLoc); err != nil {
			return err
		}
	}
	return nil
}

// flushAllDeletesFromDelSrc collects all deletes from objs and flush them into one obj
func (task *flushTableTailTask) flushAllDeletesFromDelSrc(ctx context.Context) (subtask *flushDeletesTask, emtpyDelObjIdx []*bitmap.Bitmap, err error) {
	var bufferBatch *containers.Batch
	defer func() {
		if err != nil && bufferBatch != nil {
			bufferBatch.Close()
		}
	}()
	emtpyDelObjIdx = make([]*bitmap.Bitmap, len(task.delSrcMetas))
	for i, obj := range task.delSrcMetas {
		objData := obj.GetObjectData()
		var deletes *containers.Batch
		emptyDelObjs := &bitmap.Bitmap{}
		emptyDelObjs.InitWithSize(int64(obj.BlockCnt()))
		for j := 0; j < obj.BlockCnt(); j++ {
			found, _ := objData.HasDeleteIntentsPreparedInByBlock(uint16(j), types.TS{}, task.txn.GetStartTS())
			if !found {
				emptyDelObjs.Add(uint64(j))
				continue
			}
			if deletes, err = objData.CollectDeleteInRangeByBlock(
				ctx, uint16(j), types.TS{}, task.txn.GetStartTS(), true, common.MergeAllocator,
			); err != nil {
				return
			}
			if deletes == nil || deletes.Length() == 0 {
				emptyDelObjs.Add(uint64(j))
				continue
			}
			if bufferBatch == nil {
				bufferBatch = makeDeletesTempBatch(deletes, task.rt.VectorPool.Transient)
			}
			task.nObjDeletesCnt += deletes.Length()
			// deletes is closed by Extend
			bufferBatch.Extend(deletes)
		}
		emtpyDelObjIdx[i] = emptyDelObjs
	}
	if bufferBatch != nil {
		// make sure every batch in deltaloc object is sorted by rowid
		_, err = mergesort.SortBlockColumns(bufferBatch.Vecs, 0, task.rt.VectorPool.Transient)
		if err != nil {
			return
		}
		subtask = NewFlushDeletesTask(tasks.WaitableCtx, task.rt.Fs, bufferBatch)
		if err = task.rt.Scheduler.Schedule(subtask); err != nil {
			return
		}
	}
	return
}

// waitFlushAllDeletesFromDelSrc waits all io tasks about flushing deletes from objs, update locations but skip those in emtpyDelObjIdx
func (task *flushTableTailTask) waitFlushAllDeletesFromDelSrc(ctx context.Context, subtask *flushDeletesTask, emtpyDelObjIdx []*bitmap.Bitmap) (err error) {
	if subtask == nil {
		return
	}
	ictx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	if err = subtask.WaitDone(ictx); err != nil {
		return err
	}
	task.createdDeletesObjectName = subtask.name.String()
	deltaLoc := blockio.EncodeLocation(
		subtask.name,
		subtask.blocks[0].GetExtent(),
		uint32(subtask.delta.Length()),
		subtask.blocks[0].GetID())

	v2.TaskFlushDeletesCountHistogram.Observe(float64(task.nObjDeletesCnt))
	v2.TaskFlushDeletesSizeHistogram.Observe(float64(deltaLoc.Extent().End()))
	logutil.Infof("[FlushTabletail] task %d update %s for approximate %d objs", task.ID(), deltaLoc, len(task.delSrcHandles))
	for i, hdl := range task.delSrcHandles {
		for j := 0; j < hdl.GetMeta().(*catalog.ObjectEntry).BlockCnt(); j++ {
			if emtpyDelObjIdx[i] != nil && emtpyDelObjIdx[i].Contains(uint64(j)) {
				continue
			}
			if err = hdl.UpdateDeltaLoc(uint16(j), deltaLoc); err != nil {
				return err
			}

		}
	}
	return
}

func makeDeletesTempBatch(template *containers.Batch, pool *containers.VectorPool) *containers.Batch {
	bat := containers.NewBatchWithCapacity(len(template.Attrs))
	for i, name := range template.Attrs {
		bat.AddVector(name, pool.GetVector(template.Vecs[i].GetType()))
	}
	return bat
}

func relaseFlushDelTask(task *flushDeletesTask, err error) {
	if err != nil && task != nil {
		logutil.Infof("[FlushTabletail] release flush del task bat because of err %v", err)
		ictx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second, /*6*time.Minute,*/
		)
		defer cancel()
		task.WaitDone(ictx)
	}
	if task != nil && task.delta != nil {
		task.delta.Close()
	}
}

func releaseFlushObjTasks(subtasks []*flushObjTask, err error) {
	if err != nil {
		logutil.Infof("[FlushTabletail] release flush aobj bat because of err %v", err)
		// add a timeout to avoid WaitDone block the whole process
		ictx, cancel := context.WithTimeout(
			context.Background(),
			10*time.Second, /*6*time.Minute,*/
		)
		defer cancel()
		for _, subtask := range subtasks {
			if subtask != nil {
				// wait done, otherwise the data might be released before flush, and cause data race
				subtask.WaitDone(ictx)
			}
		}
	}
	for _, subtask := range subtasks {
		if subtask != nil && subtask.data != nil {
			subtask.data.Close()
		}
		if subtask != nil && subtask.delta != nil {
			subtask.delta.Close()
		}
	}
}

// For unit test
func (task *flushTableTailTask) GetCreatedObjects() handle.Object {
	return task.createdObjHandles
}
