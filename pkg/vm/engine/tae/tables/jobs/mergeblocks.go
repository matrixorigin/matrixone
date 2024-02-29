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
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/logutil"
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
	"go.uber.org/zap/zapcore"
)

// CompactObjectTaskFactory merge non-appendable blocks of an appendable-Object
// into a new non-appendable Object.
var CompactObjectTaskFactory = func(
	mergedObjs []*catalog.ObjectEntry, rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, mergedObjs, nil, rt)
	}
}

var MergeBlocksIntoObjectTaskFctory = func(
	mergedObjs []*catalog.ObjectEntry, toObjEntry *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, mergedObjs, toObjEntry, rt)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn               txnif.AsyncTxn
	rt                *dbutils.Runtime
	toObjEntry        *catalog.ObjectEntry
	createdObjs       []*catalog.ObjectEntry
	mergedObjs        []*catalog.ObjectEntry
	mergedBlkCnt      []int
	totalMergedBlkCnt int
	transMappings     *txnentries.BlkTransferBooking
	compacted         []handle.Object
	rel               handle.Relation
	scopes            []common.ID
	deletes           []*nulls.Bitmap
}

func NewMergeBlocksTask(
	ctx *tasks.Context, txn txnif.AsyncTxn,
	mergedObjs []*catalog.ObjectEntry, toObjEntry *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:          txn,
		rt:           rt,
		mergedObjs:   mergedObjs,
		compacted:    make([]handle.Object, 0),
		mergedBlkCnt: make([]int, len(mergedObjs)),
		toObjEntry:   toObjEntry,
	}
	for i, obj := range mergedObjs {
		task.mergedBlkCnt[i] = task.totalMergedBlkCnt
		task.totalMergedBlkCnt += obj.BlockCnt()
	}
	task.transMappings = txnentries.NewBlkTransferBooking(task.totalMergedBlkCnt)
	dbId := mergedObjs[0].GetTable().GetDB().ID
	database, err := txn.GetDatabaseByID(dbId)
	if err != nil {
		return
	}
	relId := mergedObjs[0].GetTable().ID
	task.rel, err = database.GetRelationByID(relId)
	if err != nil {
		return
	}
	for _, meta := range mergedObjs {
		obj, err := task.rel.GetObject(&meta.ID)
		if err != nil {
			return nil, err
		}
		task.compacted = append(task.compacted, obj)
		task.scopes = append(task.scopes, *meta.AsCommonID())
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *mergeBlocksTask) Scopes() []common.ID { return task.scopes }

func (task *mergeBlocksTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	objs := ""
	for _, obj := range task.mergedObjs {
		objs = fmt.Sprintf("%s%s,", objs, common.ShortObjId(obj.ID))
	}
	enc.AddString("from-objs", objs)

	toObjs := ""
	for _, obj := range task.createdObjs {
		toObjs = fmt.Sprintf("%s%s,", toObjs, common.ShortObjId(obj.ID))
	}
	if toObjs != "" {
		enc.AddString("to-objs", toObjs)
	}
	return
}

func (task *mergeBlocksTask) Execute(ctx context.Context) (err error) {
	phaseNumber := 0
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr] Mergeblocks", common.OperationField(task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()
	now := time.Now()

	// merge data according to the schema at startTs
	schema := task.rel.Schema().(*catalog.Schema)
	logutil.Info("[Start] Mergeblocks", common.OperationField(task.Name()),
		common.OperandField(schema.Name),
		common.OperandField(task))
	sortVecs := make([]containers.Vector, 0)
	fromLayout := make([]uint32, 0)
	skipBlks := make([]int, 0)
	length := 0
	ids := make([]*common.ID, 0, task.totalMergedBlkCnt)
	task.deletes = make([]*nulls.Bitmap, task.totalMergedBlkCnt)

	// Prepare sort key resources
	// If there's no sort key, use first column to do reshaping
	var sortColDef *catalog.ColDef
	if schema.HasSortKey() {
		sortColDef = schema.GetSingleSortKey()
		logutil.Infof("Mergeblocks on sort column %s\n", sortColDef.Name)
	} else {
		sortColDef = schema.ColDefs[0]
	}
	phaseNumber = 1
	seqnums := make([]uint16, 0, len(schema.ColDefs)-1)
	Idxs := make([]int, 0, len(schema.ColDefs))
	views := make([]*containers.BlockView, task.totalMergedBlkCnt)
	for _, def := range schema.ColDefs {
		Idxs = append(Idxs, def.Idx)
		if def.IsPhyAddr() {
			continue
		}
		seqnums = append(seqnums, def.SeqNum)
	}
	for _, obj := range task.compacted {
		err = obj.Prefetch(Idxs)
		if err != nil {
			return
		}
	}

	blkOffset := 0
	for i, object := range task.compacted {
		for j := 0; j < object.GetMeta().(*catalog.ObjectEntry).BlockCnt(); j++ {
			blkOffset = task.mergedBlkCnt[i] + j
			if views[blkOffset], err = object.GetColumnDataByIds(ctx, uint16(j), Idxs, common.MergeAllocator); err != nil {
				return
			}
			defer views[blkOffset].Close()

			task.deletes[blkOffset] = views[blkOffset].DeleteMask
			rowCntBeforeApplyDelete := views[blkOffset].Columns[0].Length()

			views[blkOffset].ApplyDeletes()
			vec := views[blkOffset].Columns[sortColDef.Idx].GetData()
			defer vec.Close()
			if vec.Length() == 0 {
				skipBlks = append(skipBlks, blkOffset)
				continue
			}
			task.transMappings.AddSortPhaseMapping(blkOffset, rowCntBeforeApplyDelete, task.deletes[blkOffset], nil /*it is sorted, no mapping*/)
			sortVecs = append(sortVecs, vec.TryConvertConst())
			fromLayout = append(fromLayout, uint32(vec.Length()))
			length += vec.Length()
			ids = append(ids, object.Fingerprint())
		}
	}

	if length == 0 {
		// all is deleted, nothing to merge, just delete
		for _, compacted := range task.compacted {
			if err = task.rel.SoftDeleteObject(compacted.GetID()); err != nil {
				return err
			}
			task.transMappings.Clean()
		}
		return nil
	}

	var toObjEntry handle.Object
	if task.toObjEntry == nil {
		if toObjEntry, err = task.rel.CreateNonAppendableObject(false); err != nil {
			return err
		}
		task.toObjEntry = toObjEntry.GetMeta().(*catalog.ObjectEntry)
		task.toObjEntry.SetSorted()
		task.createdObjs = append(task.createdObjs, task.toObjEntry)
	} else {
		panic("warning: merge to a existing Object")
	}

	toLayout := make([]uint32, 0)
	maxrow := schema.BlockMaxRows
	totalRows := length
	for totalRows > 0 {
		if totalRows > int(maxrow) {
			toLayout = append(toLayout, maxrow)
			totalRows -= int(maxrow)
		} else {
			toLayout = append(toLayout, uint32(totalRows))
			break
		}
	}

	// merge sort the sort key
	var sortedVecs []containers.Vector
	var sortedIdx []uint32
	if schema.HasSortKey() {
		// mergesort is needed, allocate sortedidx and mapping
		allocSz := length * 4
		// sortedIdx is used to shuffle other columns according to the order of the sort key
		sortIdxNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		// sortedidx will be used to shuffle other column, defer free
		defer common.MergeAllocator.Free(sortIdxNode)
		sortedIdx = unsafe.Slice((*uint32)(unsafe.Pointer(&sortIdxNode[0])), length)

		mappingNode, err := common.MergeAllocator.Alloc(allocSz)
		if err != nil {
			panic(err)
		}
		mapping := unsafe.Slice((*uint32)(unsafe.Pointer(&mappingNode[0])), length)

		// modify sortidx and mapping
		sortedVecs = mergesort.MergeColumn(sortVecs, sortedIdx, mapping, fromLayout, toLayout, task.rt.VectorPool.Transient)
		task.transMappings.UpdateMappingAfterMerge(mapping, fromLayout, toLayout)
		// free mapping, which is never used again
		common.MergeAllocator.Free(mappingNode)
	} else {
		// just do reshape
		sortedVecs = mergesort.ReshapeColumn(sortVecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		// UpdateMappingAfterMerge will handle the nil mapping
		task.transMappings.UpdateMappingAfterMerge(nil, fromLayout, toLayout)
	}

	batchs := make([]*containers.Batch, 0)
	phaseNumber = 2
	for _, vec := range sortedVecs {
		defer vec.Close()
		batchs = append(batchs, containers.NewBatch())
	}

	// Build and flush block index if sort key is defined
	// Flush sort key it correlates to only one column

	phaseNumber = 3
	vecs := make([]containers.Vector, 0, len(views))
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}

		// just put the sorted column to the write batch
		if def.Idx == sortColDef.Idx {
			for i, vec := range sortedVecs {
				batchs[i].AddVector(def.Name, vec)
			}
			continue
		}

		vecs = vecs[:0]
		totalBlkCnt := 0
		for i := range task.compacted {
			for j := 0; j < task.compacted[i].BlkCnt(); j++ {
				vec := views[totalBlkCnt+j].Columns[def.Idx].Orphan().TryConvertConst()
				defer vec.Close()
				if vec.Length() == 0 {
					continue
				}
				vecs = append(vecs, vec)
			}
			totalBlkCnt += task.compacted[i].BlkCnt()
		}
		var outvecs []containers.Vector
		if schema.HasSortKey() {
			outvecs = mergesort.ShuffleColumn(vecs, sortedIdx, fromLayout, toLayout, task.rt.VectorPool.Transient)
		} else {
			outvecs = mergesort.ReshapeColumn(vecs, fromLayout, toLayout, task.rt.VectorPool.Transient)
		}
		for i, vec := range outvecs {
			batchs[i].AddVector(def.Name, vec)
			defer vec.Close()
		}
	}

	phaseNumber = 4
	name := objectio.BuildObjectNameWithObjectID(&task.toObjEntry.ID)
	writer, err := blockio.NewBlockWriterNew(task.mergedObjs[0].GetBlockData().GetFs().Service, name, schema.Version, seqnums)
	if err != nil {
		return err
	}
	if schema.HasPK() {
		pkIdx := schema.GetSingleSortKeyIdx()
		writer.SetPrimaryKey(uint16(pkIdx))
	} else if schema.HasSortKey() {
		writer.SetSortKey(uint16(schema.GetSingleSortKeyIdx()))
	}
	for _, bat := range batchs {
		_, err = writer.WriteBatch(containers.ToCNBatch(bat))
		if err != nil {
			return err
		}
	}
	_, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	phaseNumber = 5
	if err = toObjEntry.UpdateStats(writer.Stats()); err != nil {
		return err
	}
	for _, blk := range task.createdObjs {
		if err = blk.GetBlockData().Init(); err != nil {
			return err
		}
	}

	phaseNumber = 6
	for _, entry := range task.mergedObjs {
		if err = task.rel.SoftDeleteObject(&entry.ID); err != nil {
			return err
		}
	}

	phaseNumber = 7
	table := task.toObjEntry.GetTable()
	txnEntry := txnentries.NewMergeBlocksEntry(
		task.txn,
		task.rel,
		task.mergedObjs,
		task.createdObjs,
		task.transMappings,
		task.deletes,
		skipBlks,
		task.rt,
	)
	if err = task.txn.LogTxnEntry(table.GetDB().ID, table.ID, txnEntry, ids); err != nil {
		return err
	}

	logutil.Info("[Done] Mergeblocks",
		common.AnyField("txn-start-ts", task.txn.GetStartTS().ToString()),
		common.OperationField(task.Name()),
		common.OperandField(schema.Name),
		common.OperandField(task),
		common.DurationField(time.Since(now)))

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Object.MergeBlocks.Add(1)
	})
	return err
}

func (task *mergeBlocksTask) GetCreatedObjects() []*catalog.ObjectEntry {
	return task.createdObjs
}
