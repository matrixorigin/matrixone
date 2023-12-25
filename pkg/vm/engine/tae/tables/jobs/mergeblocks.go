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
	mergedBlks []*catalog.BlockEntry, rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		mergedObjs := make([]*catalog.ObjectEntry, 1)
		mergedObjs[0] = mergedBlks[0].GetObject()
		return NewMergeBlocksTask(ctx, txn, mergedBlks, mergedObjs, nil, rt)
	}
}

var MergeBlocksIntoObjectTaskFctory = func(
	mergedBlks []*catalog.BlockEntry, toObjEntry *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, mergedBlks, nil, toObjEntry, rt)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn           txnif.AsyncTxn
	rt            *dbutils.Runtime
	toObjEntry    *catalog.ObjectEntry
	createdObjs   []*catalog.ObjectEntry
	mergedObjs    []*catalog.ObjectEntry
	mergedBlks    []*catalog.BlockEntry
	createdBlks   []*catalog.BlockEntry
	transMappings *txnentries.BlkTransferBooking
	compacted     []handle.Block
	rel           handle.Relation
	scopes        []common.ID
	deletes       []*nulls.Bitmap
}

func NewMergeBlocksTask(
	ctx *tasks.Context, txn txnif.AsyncTxn,
	mergedBlks []*catalog.BlockEntry, mergedObjs []*catalog.ObjectEntry, toObjEntry *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:         txn,
		rt:          rt,
		mergedBlks:  mergedBlks,
		mergedObjs:  mergedObjs,
		createdBlks: make([]*catalog.BlockEntry, 0),
		compacted:   make([]handle.Block, 0),
		toObjEntry:  toObjEntry,
	}
	task.transMappings = txnentries.NewBlkTransferBooking(len(task.mergedBlks))
	dbId := mergedBlks[0].GetObject().GetTable().GetDB().ID
	database, err := txn.GetDatabaseByID(dbId)
	if err != nil {
		return
	}
	relId := mergedBlks[0].GetObject().GetTable().ID
	task.rel, err = database.GetRelationByID(relId)
	if err != nil {
		return
	}
	for _, meta := range mergedBlks {
		obj, err := task.rel.GetObject(&meta.GetObject().ID)
		if err != nil {
			return nil, err
		}
		defer obj.Close()
		blk, err := obj.GetBlock(meta.ID)
		if err != nil {
			return nil, err
		}
		task.compacted = append(task.compacted, blk)
		task.scopes = append(task.scopes, *meta.AsCommonID())
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *mergeBlocksTask) Scopes() []common.ID { return task.scopes }

func mergeColumns(
	srcVecs []containers.Vector,
	sortedIdx *[]uint32,
	isPrimary bool,
	fromLayout,
	toLayout []uint32,
	sort bool,
	pool *containers.VectorPool) (retVecs []containers.Vector, mapping []uint32) {
	if len(srcVecs) == 0 {
		return
	}
	if sort {
		if isPrimary {
			retVecs, mapping = mergesort.MergeSortedColumn(srcVecs, sortedIdx, fromLayout, toLayout, pool)
		} else {
			retVecs = mergesort.ShuffleColumn(srcVecs, *sortedIdx, fromLayout, toLayout, pool)
		}
	} else {
		retVecs, mapping = mergeColumnWithOutSort(srcVecs, fromLayout, toLayout, pool)
	}
	return
}

func mergeColumnWithOutSort(
	column []containers.Vector, fromLayout, toLayout []uint32, pool *containers.VectorPool,
) (ret []containers.Vector, mapping []uint32) {
	totalLength := uint32(0)
	for _, i := range toLayout {
		totalLength += i
	}
	mapping = make([]uint32, totalLength)
	for i := range mapping {
		mapping[i] = uint32(i)
	}
	ret = mergesort.Reshape(column, fromLayout, toLayout, pool)
	return
}

func (task *mergeBlocksTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	objs := ""
	for _, obj := range task.mergedObjs {
		objs = fmt.Sprintf("%s%s,", objs, common.ShortObjId(obj.ID))
	}
	enc.AddString("from-objs", objs)

	toblks := ""
	for _, blk := range task.createdBlks {
		toblks = fmt.Sprintf("%s%s,", toblks, blk.ID.ShortStringEx())
	}
	if toblks != "" {
		enc.AddString("to-blks", toblks)
	}
	return
}

func (task *mergeBlocksTask) Execute(ctx context.Context) (err error) {
	task.rt.Throttle.AcquireCompactionQuota()
	defer task.rt.Throttle.ReleaseCompactionQuota()
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
	rows := make([]uint32, 0)
	skipBlks := make([]int, 0)
	length := 0
	fromAddr := make([]uint32, 0, len(task.compacted))
	ids := make([]*common.ID, 0, len(task.compacted))
	task.deletes = make([]*nulls.Bitmap, len(task.compacted))

	// Prepare sort key resources
	// If there's no sort key, use physical address key
	var sortColDef *catalog.ColDef
	if schema.HasSortKey() {
		sortColDef = schema.GetSingleSortKey()
		logutil.Infof("Mergeblocks on sort column %s\n", sortColDef.Name)
	} else {
		sortColDef = schema.PhyAddrKey
	}
	phaseNumber = 1
	seqnums := make([]uint16, 0, len(schema.ColDefs)-1)
	Idxs := make([]int, 0, len(schema.ColDefs))
	views := make([]*containers.BlockView, len(task.compacted))
	for _, def := range schema.ColDefs {
		Idxs = append(Idxs, def.Idx)
		if def.IsPhyAddr() {
			continue
		}
		seqnums = append(seqnums, def.SeqNum)
	}
	for _, block := range task.compacted {
		err = block.Prefetch(Idxs)
		if err != nil {
			return
		}
	}

	for i, block := range task.compacted {
		if views[i], err = block.GetColumnDataByIds(ctx, Idxs, common.MergeAllocator); err != nil {
			return
		}
		defer views[i].Close()

		task.deletes[i] = views[i].DeleteMask
		rowCntBeforeApplyDelete := views[i].Columns[0].Length()

		views[i].ApplyDeletes()
		vec := views[i].Columns[sortColDef.Idx].GetData()
		defer vec.Close()
		if vec.Length() == 0 {
			skipBlks = append(skipBlks, i)
			continue
		}
		task.transMappings.AddSortPhaseMapping(i, rowCntBeforeApplyDelete, task.deletes[i], nil /*it is sorted, no mapping*/)
		sortVecs = append(sortVecs, vec.TryConvertConst())
		rows = append(rows, uint32(vec.Length()))
		fromAddr = append(fromAddr, uint32(length))
		length += vec.Length()
		ids = append(ids, block.Fingerprint())
	}

	if length == 0 {
		// all is deleted, nothing to merge, just delete
		for _, compacted := range task.compacted {
			obj := compacted.GetObject()
			if err = obj.SoftDeleteBlock(compacted.ID()); err != nil {
				return err
			}
		}
		for _, entry := range task.mergedObjs {
			if err = task.rel.SoftDeleteObject(&entry.ID); err != nil {
				return err
			}
		}
		task.transMappings.Clean()
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

	to := make([]uint32, 0)
	maxrow := schema.BlockMaxRows
	totalRows := length
	for totalRows > 0 {
		if totalRows > int(maxrow) {
			to = append(to, maxrow)
			totalRows -= int(maxrow)
		} else {
			to = append(to, uint32(totalRows))
			break
		}
	}

	// merge sort the sort key
	allocSz := length * 4
	node, err := common.MergeAllocator.Alloc(allocSz)
	if err != nil {
		panic(err)
	}
	defer common.MergeAllocator.Free(node)
	sortedIdx := unsafe.Slice((*uint32)(unsafe.Pointer(&node[0])), length)

	vecs, mapping := mergeColumns(sortVecs, &sortedIdx, true, rows, to, schema.HasSortKey(), task.rt.VectorPool.Transient)
	task.transMappings.UpdateMappingAfterMerge(mapping, rows, to)
	// logutil.Infof("mapping is %v", mapping)
	// logutil.Infof("sortedIdx is %v", sortedIdx)
	length = 0
	var blk handle.Block
	toAddr := make([]uint32, 0, len(vecs))
	// index meta for every created block
	// Prepare new block placeholder
	// Build and flush block index if sort key is defined
	// Flush sort key it correlates to only one column
	batchs := make([]*containers.Batch, 0)
	blockHandles := make([]handle.Block, 0)
	phaseNumber = 2
	for i, vec := range vecs {
		toAddr = append(toAddr, uint32(length))
		length += vec.Length()
		blk, err = toObjEntry.CreateNonAppendableBlock(
			new(objectio.CreateBlockOpt).WithFileIdx(0).WithBlkIdx(uint16(i)))
		if err != nil {
			return err
		}
		task.createdBlks = append(task.createdBlks, blk.GetMeta().(*catalog.BlockEntry))
		blockHandles = append(blockHandles, blk)
		batch := containers.NewBatch()
		batchs = append(batchs, batch)
		vec.Close()
	}

	// Build and flush block index if sort key is defined
	// Flush sort key it correlates to only one column

	phaseNumber = 3
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		// Skip
		// PhyAddr column was processed before
		// If only one single sort key, it was processed before
		vecs = vecs[:0]
		for i := range task.compacted {
			vec := views[i].Columns[def.Idx].Orphan().TryConvertConst()
			defer vec.Close()
			if vec.Length() == 0 {
				continue
			}
			vecs = append(vecs, vec)
		}
		vecs, _ := mergeColumns(vecs, &sortedIdx, false, rows, to, schema.HasSortKey(), task.rt.VectorPool.Transient)
		for i := range vecs {
			defer vecs[i].Close()
		}
		for i, vec := range vecs {
			batchs[i].AddVector(def.Name, vec)
		}
	}

	phaseNumber = 4
	name := objectio.BuildObjectNameWithObjectID(&task.toObjEntry.ID)
	writer, err := blockio.NewBlockWriterNew(task.mergedBlks[0].GetBlockData().GetFs().Service, name, schema.Version, seqnums)
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
	blocks, _, err := writer.Sync(ctx)
	if err != nil {
		return err
	}
	phaseNumber = 5
	var metaLoc objectio.Location
	for i, block := range blocks {
		metaLoc = blockio.EncodeLocation(name, block.GetExtent(), uint32(batchs[i].Length()), block.GetID())
		if err = blockHandles[i].UpdateMetaLoc(metaLoc); err != nil {
			return err
		}
	}
	if err = toObjEntry.UpdateStats(writer.Stats()); err != nil {
		return err
	}
	for _, blk := range task.createdBlks {
		if err = blk.GetBlockData().Init(); err != nil {
			return err
		}
	}

	phaseNumber = 6
	for _, compacted := range task.compacted {
		obj := compacted.GetObject()
		if err = obj.SoftDeleteBlock(compacted.ID()); err != nil {
			return err
		}
	}
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
		task.mergedBlks,
		task.createdBlks,
		task.transMappings,
		mapping,
		fromAddr,
		toAddr,
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

func (task *mergeBlocksTask) GetCreatedBlocks() []*catalog.BlockEntry {
	return task.createdBlks
}
