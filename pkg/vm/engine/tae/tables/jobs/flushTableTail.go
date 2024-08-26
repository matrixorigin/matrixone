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
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type TestFlushBailoutPos1 struct{}
type TestFlushBailoutPos2 struct{}

var FlushTableTailTaskFactory = func(
	metas, tombstones []*catalog.ObjectEntry, rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return NewFlushTableTailTask(ctx, txn, metas, tombstones, rt)
	}
}

type flushTableTailTask struct {
	*tasks.BaseTask
	txn txnif.AsyncTxn
	rt  *dbutils.Runtime

	scopes          []common.ID
	schema          *catalog.Schema
	tombstoneSchema *catalog.Schema

	rel  handle.Relation
	dbid uint64

	// record the row mapping from deleted blocks to created blocks
	transMappings api.TransferMaps
	doTransfer    bool

	aObjMetas         []*catalog.ObjectEntry
	aObjHandles       []handle.Object
	createdObjHandles handle.Object

	aTombstoneMetas         []*catalog.ObjectEntry
	aTombstoneHandles       []handle.Object
	createdTombstoneHandles handle.Object

	createdMergedObjectName    string
	createdMergedTombstoneName string

	mergeRowsCnt, aObjDeletesCnt, tombstoneMergeRowsCnt int
	createAt                                            time.Time
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
	tombStones []*catalog.ObjectEntry,
	rt *dbutils.Runtime,
) (task *flushTableTailTask, err error) {
	task = &flushTableTailTask{
		txn: txn,
		rt:  rt,
	}

	var meta *catalog.ObjectEntry
	if len(objs) != 0 {
		meta = objs[0]
	} else {
		meta = tombStones[0]
	}
	dbId := meta.GetTable().GetDB().ID
	task.dbid = dbId
	var database handle.Database
	database, err = txn.UnsafeGetDatabase(dbId)
	if err != nil {
		return
	}
	tableId := meta.GetTable().ID
	var rel handle.Relation
	rel, err = database.UnsafeGetRelation(tableId)
	task.rel = rel
	if err != nil {
		return
	}
	task.schema = rel.Schema(false).(*catalog.Schema)

	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)

	for _, obj := range objs {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(obj.ID(), false)
		if err != nil {
			return
		}
		if obj.IsTombstone {
			panic(fmt.Sprintf("logic err, obj %v is tombstone", obj.ID().String()))
		}
		if !hdl.IsAppendable() {
			panic(fmt.Sprintf("logic err %v is nonappendable", hdl.GetID().String()))
		}
		if obj.HasDropCommitted() {
			continue
		}
		if obj.GetObjectData().CheckFlushTaskRetry(txn.GetStartTS()) {
			logutil.Info(
				"[FLUSH-NEED-RETRY]",
				zap.String("task", task.Name()),
				common.AnyField("obj", obj.ID().String()),
			)
			return nil, txnif.ErrTxnNeedRetry
		}
		task.aObjMetas = append(task.aObjMetas, obj)
		task.aObjHandles = append(task.aObjHandles, hdl)
	}

	task.tombstoneSchema = rel.Schema(true).(*catalog.Schema)

	for _, obj := range tombStones {
		task.scopes = append(task.scopes, *obj.AsCommonID())
		var hdl handle.Object
		hdl, err = rel.GetObject(obj.ID(), true)
		if err != nil {
			return
		}
		if !obj.IsTombstone {
			panic(fmt.Sprintf("logic err, obj %v is not tombstone", obj.ID().String()))
		}
		if !hdl.IsAppendable() {
			panic(fmt.Sprintf("logic err %v is nonappendable", hdl.GetID().String()))
		}
		if obj.HasDropCommitted() {
			continue
		}
		if obj.GetObjectData().CheckFlushTaskRetry(txn.GetStartTS()) {
			logutil.Infof("[FlushTabletail] obj %v needs retry", obj.ID().String())
			return nil, txnif.ErrTxnNeedRetry
		}
		task.aTombstoneMetas = append(task.aTombstoneMetas, obj)
		task.aTombstoneHandles = append(task.aTombstoneHandles, hdl)
	}

	task.doTransfer = !strings.Contains(task.schema.Comment, pkgcatalog.MO_COMMENT_NO_DEL_HINT)
	if task.doTransfer {
		task.transMappings = make(api.TransferMaps, len(task.aObjHandles))
		for i := range len(task.aObjHandles) {
			task.transMappings[i] = make(api.TransferMap)
		}
	}

	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)

	task.createAt = time.Now()
	return
}

// impl DisposableVecPool
func (task *flushTableTailTask) GetVector(typ *types.Type) (*vector.Vector, func()) {
	v := task.rt.VectorPool.Transient.GetVector(typ)
	return v.GetDownstreamVector(), v.Close
}

func (task *flushTableTailTask) GetMPool() *mpool.MPool {
	return task.rt.VectorPool.Transient.GetMPool()
}

// Scopes is used in conflict checking in scheduler. For ScopedTask interface
func (task *flushTableTailTask) Scopes() []common.ID { return task.scopes }

// Name is for ScopedTask interface
func (task *flushTableTailTask) Name() string {
	return fmt.Sprintf("[FT-%d]%d-%s", task.ID(), task.rel.ID(), task.schema.Name)
}

func (task *flushTableTailTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	objs := ""
	for _, obj := range task.aObjMetas {
		objs = fmt.Sprintf("%s%s,", objs, obj.ID().ShortStringEx())
	}
	enc.AddString("a-objs", objs)
	tombstones := ""
	for _, obj := range task.aTombstoneMetas {
		tombstones = fmt.Sprintf("%s%s,", tombstones, obj.ID().ShortStringEx())
	}
	enc.AddString("a-tombstones", tombstones)

	toObjs := ""
	if task.createdObjHandles != nil {
		id := task.createdObjHandles.GetID()
		toObjs = fmt.Sprintf("%s%s,", toObjs, id.ShortStringEx())
	}
	if toObjs != "" {
		enc.AddString("to-objs", toObjs)
	}

	toTombstones := ""
	if task.createdTombstoneHandles != nil {
		id := task.createdTombstoneHandles.GetID()
		toTombstones = fmt.Sprintf("%s%s,", toTombstones, id.ShortStringEx())
	}
	if toTombstones != "" {
		enc.AddString("to-tombstones", toTombstones)
	}
	return
}

var (
	SlowFlushIOTask      = 10 * time.Second
	SlowFlushTaskOverall = 60 * time.Second
	SlowDelCollect       = 10 * time.Second
	SlowDelCollectNObj   = 10
)

func (task *flushTableTailTask) Execute(ctx context.Context) (err error) {
	logutil.Info(
		"[FLUSH-START]",
		zap.String("task", task.Name()),
		zap.Any("extra-info", task),
		common.AnyField("txn-info", task.txn.String()),
		zap.Int("aobj-ndv", len(task.aObjHandles)+len(task.aTombstoneHandles)),
	)

	phaseDesc := ""
	defer func() {
		if err != nil {
			logutil.Error("[FLUSH-ERR]",
				zap.String("task", task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseDesc),
			)
		}
	}()
	statWait := time.Since(task.createAt)
	now := time.Now()

	/////////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-flushing appendable objects for snapshot"
	inst := time.Now()
	snapshotSubtasks, err := task.flushAObjsForSnapshot(ctx, false)
	statFlushAobj := time.Since(inst)
	defer func() {
		releaseFlushObjTasks(task, snapshotSubtasks, err)
	}()
	if err != nil {
		return
	}

	/////////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-flushing appendable tombstones for snapshot"
	inst = time.Now()
	tombstoneSnapshotSubtasks, err := task.flushAObjsForSnapshot(ctx, true)
	statFlushTombStone := time.Since(inst)
	if err != nil {
		return
	}
	defer func() {
		releaseFlushObjTasks(task, tombstoneSnapshotSubtasks, err)
	}()

	/////////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-merge aobjects"
	// merge aobjects, no need to wait, it is a sync procedure, that is why put it
	// after flushAObjsForSnapshot
	inst = time.Now()
	if err = task.mergeAObjs(ctx, false); err != nil {
		return
	}
	statMergeAobj := time.Since(inst)

	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		err = moerr.NewInternalErrorNoCtx("test merge bail out")
		return
	}

	/////////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-merge atombstones"
	// merge atombstones, no need to wait, it is a sync procedure, that is why put it
	// after flushAObjsForSnapshot
	inst = time.Now()
	if err = task.mergeAObjs(ctx, true); err != nil {
		return
	}
	statMergeATombstones := time.Since(inst)

	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		err = moerr.NewInternalErrorNoCtx("test merge bail out")
		return
	}

	///////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-merging persisted tombstones"
	inst = time.Now()
	// ignore error
	_ = task.mergePersistedTombstones(ctx)
	statWaitTombstoneMerge := time.Since(inst)

	/////////////////////
	//// phase separator
	///////////////////
	phaseDesc = "1-waiting flushing appendable blocks for snapshot"
	// wait flush tasks
	inst = time.Now()
	if err = task.waitFlushAObjForSnapshot(ctx, snapshotSubtasks, false); err != nil {
		return
	}
	statWaitAobj := time.Since(inst)

	/////////////////////
	//// phase separator
	///////////////////

	phaseDesc = "1-waiting flushing appendable tombstones for snapshot"
	inst = time.Now()
	if err = task.waitFlushAObjForSnapshot(ctx, tombstoneSnapshotSubtasks, true); err != nil {
		return
	}
	statWaitTombstones := time.Since(inst)

	phaseDesc = "1-wait LogTxnEntry"
	inst = time.Now()
	txnEntry, err := txnentries.NewFlushTableTailEntry(
		ctx,
		task.txn,
		task.Name(),
		task.transMappings,
		task.rel.GetMeta().(*catalog.TableEntry),
		task.aObjMetas,
		task.aObjHandles,
		task.createdObjHandles,
		task.createdMergedObjectName,
		task.aTombstoneMetas,
		task.aTombstoneHandles,
		task.createdTombstoneHandles,
		task.createdMergedTombstoneName,
		task.rt,
	)
	if err != nil {
		return err
	}
	if err = task.txn.LogTxnEntry(
		task.dbid,
		task.rel.ID(),
		txnEntry,
		nil,
		nil,
	); err != nil {
		return
	}
	statNewFlushEntry := time.Since(inst)
	/////////////////////

	duration := time.Since(now)
	logutil.Info("[FLUSH-END]",
		zap.String("task", task.Name()),
		zap.Int("aobj-deletes", task.aObjDeletesCnt),
		zap.Int("aobj-merge-rows", task.mergeRowsCnt),
		zap.Int("tombstone-rows", task.tombstoneMergeRowsCnt),
		common.DurationField(duration),
		zap.Any("extra-info", task))
	v2.TaskFlushTableTailDurationHistogram.Observe(duration.Seconds())

	if time.Since(task.createAt) > SlowFlushTaskOverall {
		logutil.Info(
			"[FLUSH-SUMMARY]",
			zap.String("task", task.Name()),
			common.AnyField("wait-execute", statWait),
			common.AnyField("schedule-flush-aobj", statFlushAobj),
			common.AnyField("schedule-flush-dels", statFlushTombStone),
			common.AnyField("do-merge-data", statMergeAobj),
			common.AnyField("do-merge-tombstone", statMergeATombstones),
			common.AnyField("wait-aobj-flush", statWaitAobj),
			common.AnyField("wait-dels-flush", statWaitTombstones),
			common.AnyField("log-txn-entry", statNewFlushEntry),
			common.AnyField("tombstone-merge", statWaitTombstoneMerge),
		)
	}

	sleep, name, exist := fault.TriggerFault("slow_flush")
	if exist && name == task.schema.Name {
		time.Sleep(time.Duration(sleep) * time.Second)
	}
	return
}

// prepareAObjSortedData read the data from appendable blocks, sort them if sort key exists
func (task *flushTableTailTask) prepareAObjSortedData(
	ctx context.Context, objIdx int, idxs []int, sortKeyPos int, isTombstone bool,
) (bat *containers.Batch, empty bool, err error) {
	if len(idxs) <= 0 {
		logutil.Info(
			"NO-MERGEABLE-COLUMNS",
			zap.String("task", task.Name()),
		)
		return nil, true, nil
	}

	var obj handle.Object
	if isTombstone {
		obj = task.aTombstoneHandles[objIdx]
		err = obj.Scan(ctx, &bat, 0, idxs, common.MergeAllocator)
	} else {
		obj = task.aObjHandles[objIdx]
		err = obj.HybridScan(ctx, &bat, 0, idxs, common.MergeAllocator)
	}

	if err != nil {
		return
	}
	for i := range idxs {
		if vec := bat.Vecs[i]; vec == nil || vec.Length() == 0 {
			empty = true
			bat.Close()
			bat = nil
			return
		}
	}
	totalRowCnt := bat.Length()
	task.aObjDeletesCnt += bat.Deletes.GetCardinality()

	if isTombstone && bat.Deletes != nil {
		panic(fmt.Sprintf("logic err, tombstone %v has deletes", obj.GetID().String()))
	}

	var sortMapping []int64
	if sortKeyPos >= 0 {
		if objIdx == 0 {
			logutil.Info("[FLUSH-STEP]", zap.String("task", task.Name()), common.AnyField("sort-key", bat.Attrs[sortKeyPos]))
		}
		sortMapping, err = mergesort.SortBlockColumns(bat.Vecs, sortKeyPos, task.rt.VectorPool.Transient)
		if bat.Deletes != nil {
			nulls.Filter(bat.Deletes, sortMapping, false)
		}
		if err != nil {
			return
		}
	}

	if isTombstone {
		return
	}
	if task.doTransfer {
		mergesort.AddSortPhaseMapping(task.transMappings[objIdx], totalRowCnt, sortMapping)
	}
	return
}

// mergeAObjs merge the data from appendable blocks, and write the merged data to new block,
// recording row mapping in blkTransferBooking struct
func (task *flushTableTailTask) mergeAObjs(ctx context.Context, isTombstone bool) (err error) {
	var objMetas []*catalog.ObjectEntry
	var objHandles []handle.Object
	if isTombstone {
		objHandles = task.aTombstoneHandles
		objMetas = task.aTombstoneMetas
	} else {
		objHandles = task.aObjHandles
		objMetas = task.aObjMetas
	}

	if len(objMetas) == 0 {
		return nil
	}
	// prepare columns idx and sortKey to read sorted batch
	var schema *catalog.Schema
	if isTombstone {
		schema = task.tombstoneSchema
	} else {
		schema = task.schema
	}
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
	if isTombstone {
		readColIdxs = append(readColIdxs, catalog.COLIDX_COMMITS)
		seqnums = append(seqnums, objectio.SEQNUM_COMMITTS)
	}

	// read from aobjects
	readedBats := make([]*containers.Batch, 0, len(objHandles))
	for _, block := range objHandles {
		err = block.Prefetch(readColIdxs)
		if err != nil {
			return
		}
	}
	for _, block := range objHandles {
		if err = block.Prefetch(readColIdxs); err != nil {
			return
		}
	}
	for i := range objHandles {
		bat, empty, err := task.prepareAObjSortedData(ctx, i, readColIdxs, sortKeyPos, isTombstone)
		if err != nil {
			return err
		}
		if empty {
			continue
		}
		readedBats = append(readedBats, bat)
	}

	// prepare merge
	// toLayout describes the layout of the output batch, i.e. [8192, 8192, 8192, 4242]
	toLayout := make([]uint32, 0, len(readedBats))

	totalRowCnt := 0
	if sortKeyPos < 0 {
		// no pk, just pick the first column to reshape
		sortKeyPos = 0
	}
	for _, bat := range readedBats {
		vec := bat.Vecs[sortKeyPos]
		totalRowCnt += vec.Length()
	}
	if !isTombstone {
		totalRowCnt -= task.aObjDeletesCnt
	}
	if isTombstone {
		task.tombstoneMergeRowsCnt = totalRowCnt
	} else {
		task.mergeRowsCnt = totalRowCnt
	}

	if totalRowCnt == 0 {
		// just soft delete all Objects
		for _, obj := range objHandles {
			tbl := obj.GetRelation()
			if err = tbl.SoftDeleteObject(obj.GetID(), isTombstone); err != nil {
				return err
			}
		}
		if !isTombstone && task.doTransfer {
			mergesort.CleanTransMapping(task.transMappings)
		}
		return nil
	}
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
	var writtenBatches []*batch.Batch
	var releaseF func()
	var mapping []int
	if schema.HasSortKey() {
		writtenBatches, releaseF, mapping, err = mergesort.MergeAObj(ctx, task, readedBats, sortKeyPos, toLayout)
		if err != nil {
			return
		}
	} else {
		writtenBatches, releaseF, mapping, err = mergesort.ReshapeBatches(readedBats, toLayout, task)
		if err != nil {
			return
		}
	}
	defer releaseF()
	if !isTombstone && task.doTransfer {
		mergesort.UpdateMappingAfterMerge(task.transMappings, mapping, toLayout)
	}

	// write!
	// create new object to hold merged blocks
	var createdObjectHandle handle.Object
	if isTombstone {
		if task.createdTombstoneHandles, err = task.rel.CreateNonAppendableObject(isTombstone, nil); err != nil {
			return
		}
		createdObjectHandle = task.createdTombstoneHandles
	} else {
		if task.createdObjHandles, err = task.rel.CreateNonAppendableObject(isTombstone, nil); err != nil {
			return
		}
		createdObjectHandle = task.createdObjHandles
	}
	toObjectEntry := createdObjectHandle.GetMeta().(*catalog.ObjectEntry)
	toObjectEntry.SetSorted()
	name := objectio.BuildObjectNameWithObjectID(toObjectEntry.ID())
	writer, err := blockio.NewBlockWriterNew(task.rt.Fs.Service, name, schema.Version, seqnums)
	if err != nil {
		return err
	}

	if schema.HasPK() {
		pkIdx := schema.GetSingleSortKeyIdx()
		if isTombstone {
			writer.SetDataType(objectio.SchemaTombstone)
			writer.SetPrimaryKeyWithType(
				uint16(catalog.TombstonePrimaryKeyIdx),
				index.HBF,
				index.ObjectPrefixFn,
				index.BlockPrefixFn,
			)
		} else {
			writer.SetPrimaryKey(uint16(pkIdx))
		}
	} else if schema.HasSortKey() {
		writer.SetSortKey(uint16(schema.GetSingleSortKeyIdx()))
	}
	for _, bat := range writtenBatches {
		_, err = writer.WriteBatch(bat)
		if err != nil {
			return err
		}
	}
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	if isTombstone {
		task.createdMergedTombstoneName = name.String()
	} else {
		task.createdMergedObjectName = name.String()
	}

	// update new status for created blocks
	stats := writer.Stats()
	stats.SetSorted()
	err = createdObjectHandle.UpdateStats(stats)
	if err != nil {
		return
	}
	err = createdObjectHandle.GetMeta().(*catalog.ObjectEntry).GetObjectData().Init()
	if err != nil {
		return
	}

	// soft delete all aobjs
	for _, obj := range objHandles {
		tbl := obj.GetRelation()
		if err = tbl.SoftDeleteObject(obj.GetID(), isTombstone); err != nil {
			return err
		}
	}

	return nil
}

// flushAObjsForSnapshot schedule io task to flush aobjects for snapshot read. this function will not release any data in io task
func (task *flushTableTailTask) flushAObjsForSnapshot(ctx context.Context, isTombstone bool) (subtasks []*flushObjTask, err error) {
	defer func() {
		if err != nil {
			releaseFlushObjTasks(task, subtasks, err)
		}
	}()

	var metas []*catalog.ObjectEntry
	if isTombstone {
		metas = task.aTombstoneMetas
	} else {
		metas = task.aObjMetas
	}

	subtasks = make([]*flushObjTask, len(metas))
	// fire flush task
	for i, obj := range metas {
		dataVers := make(map[uint32]*containers.BatchWithVersion)
		if err = tables.RangeScanInMemoryByObject(
			ctx, obj, dataVers, types.TS{}, task.txn.GetStartTS(), common.MergeAllocator,
		); err != nil {
			return
		}
		if len(dataVers) == 0 {
			// the new appendable block might has no data when we flush the table, just skip it
			// In previous impl, runner will only pass non-empty obj to NewCompactBlackTask
			continue
		}
		if len(dataVers) != 1 {
			panic("logic err")
		}
		var dataVer *containers.BatchWithVersion
		for _, data := range dataVers {
			dataVer = data
			break
		}

		// do not close data, leave that to wait phase
		if isTombstone {
			_, err = mergesort.SortBlockColumns(dataVer.Vecs, catalog.TombstonePrimaryKeyIdx, task.rt.VectorPool.Transient)
			if err != nil {
				logutil.Info(
					"[FLUSH-AOBJ-ERR]",
					common.AnyField("error", err),
					zap.String("task", task.Name()),
					zap.String("obj", obj.ID().String()),
				)
				dataVer.Close()
				return
			}
		}

		aobjectTask := NewFlushObjTask(
			tasks.WaitableCtx,
			dataVer.Version,
			dataVer.Seqnums,
			task.rt.Fs,
			obj,
			dataVer.Batch,
			nil,
			true,
			task.Name(),
		)
		if err = task.rt.Scheduler.Schedule(aobjectTask); err != nil {
			return
		}
		subtasks[i] = aobjectTask
	}
	return
}

// waitFlushAObjForSnapshot waits all io tasks about flushing aobject for snapshot read, update locations
func (task *flushTableTailTask) waitFlushAObjForSnapshot(ctx context.Context, subtasks []*flushObjTask, isTombstone bool) (err error) {
	ictx, cancel := context.WithTimeout(ctx, 6*time.Minute)
	defer cancel()
	var handles []handle.Object
	if isTombstone {
		handles = task.aTombstoneHandles
	} else {
		handles = task.aObjHandles
	}
	for i, subtask := range subtasks {
		if subtask == nil {
			continue
		}
		if err = subtask.WaitDone(ictx); err != nil {
			return
		}
		stat := subtask.stat.Clone()
		stat.SetAppendable()
		if err = handles[i].UpdateStats(*stat); err != nil {
			return
		}
	}
	return nil
}

func (task *flushTableTailTask) mergePersistedTombstones(ctx context.Context) error {
	tombstones := make([]*catalog.ObjectEntry, 0)
	tombstoneIter := task.rel.MakeObjectItOnSnap(true)
	for tombstoneIter.Next() {
		tombstone := tombstoneIter.GetObject().GetMeta().(*catalog.ObjectEntry)
		if tombstone.IsCommitted() && !tombstone.IsAppendable() {
			tombstones = append(tombstones, tombstone)
		}
	}
	if len(tombstones) < 2 {
		return nil
	}
	scopes := make([]common.ID, 0, len(tombstones))
	for _, obj := range tombstones {
		scopes = append(scopes, *obj.AsCommonID())
	}
	tombstoneTask, err := NewMergeObjectsTask(
		tasks.WaitableCtx,
		task.txn,
		tombstones,
		task.rt,
		common.DefaultMaxOsizeObjMB*common.Const1MBytes,
		true,
	)
	if err != nil {
		return err
	}
	return tombstoneTask.Execute(ctx)
}

func releaseFlushObjTasks(ftask *flushTableTailTask, subtasks []*flushObjTask, err error) {
	if err != nil {
		logutil.Info(
			"[FLUSH-AOBJ-ERR]",
			common.AnyField("error", err),
			zap.String("task", ftask.Name()),
		)
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
