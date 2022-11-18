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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap/zapcore"
)

var CompactSegmentTaskFactory = func(mergedBlks []*catalog.BlockEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		mergedSegs := make([]*catalog.SegmentEntry, 1)
		mergedSegs[0] = mergedBlks[0].GetSegment()
		return NewMergeBlocksTask(ctx, txn, mergedBlks, mergedSegs, nil, scheduler)
	}
}

var MergeBlocksIntoSegmentTaskFctory = func(mergedBlks []*catalog.BlockEntry, toSegEntry *catalog.SegmentEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	if toSegEntry == nil {
		panic(tasks.ErrBadTaskRequestPara)
	}
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewMergeBlocksTask(ctx, txn, mergedBlks, nil, toSegEntry, scheduler)
	}
}

type mergeBlocksTask struct {
	*tasks.BaseTask
	txn         txnif.AsyncTxn
	toSegEntry  *catalog.SegmentEntry
	createdSegs []*catalog.SegmentEntry
	mergedSegs  []*catalog.SegmentEntry
	mergedBlks  []*catalog.BlockEntry
	createdBlks []*catalog.BlockEntry
	compacted   []handle.Block
	rel         handle.Relation
	scheduler   tasks.TaskScheduler
	scopes      []common.ID
	deletes     []*roaring.Bitmap
}

func NewMergeBlocksTask(ctx *tasks.Context, txn txnif.AsyncTxn, mergedBlks []*catalog.BlockEntry, mergedSegs []*catalog.SegmentEntry, toSegEntry *catalog.SegmentEntry, scheduler tasks.TaskScheduler) (task *mergeBlocksTask, err error) {
	task = &mergeBlocksTask{
		txn:         txn,
		mergedBlks:  mergedBlks,
		mergedSegs:  mergedSegs,
		createdBlks: make([]*catalog.BlockEntry, 0),
		compacted:   make([]handle.Block, 0),
		scheduler:   scheduler,
		toSegEntry:  toSegEntry,
	}
	dbName := mergedBlks[0].GetSegment().GetTable().GetDB().GetName()
	database, err := txn.GetDatabase(dbName)
	if err != nil {
		return
	}
	relName := mergedBlks[0].GetSchema().Name
	task.rel, err = database.GetRelationByName(relName)
	if err != nil {
		return
	}
	for _, meta := range mergedBlks {
		seg, err := task.rel.GetSegment(meta.GetSegment().GetID())
		if err != nil {
			return nil, err
		}
		blk, err := seg.GetBlock(meta.GetID())
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

func (task *mergeBlocksTask) mergeColumn(
	vecs []containers.Vector,
	sortedIdx *[]uint32,
	isPrimary bool,
	fromLayout,
	toLayout []uint32,
	sort bool) (column []containers.Vector, mapping []uint32) {
	if len(vecs) == 0 {
		return
	}
	if sort {
		if isPrimary {
			column, mapping = mergesort.MergeSortedColumn(vecs, sortedIdx, fromLayout, toLayout)
		} else {
			column = mergesort.ShuffleColumn(vecs, *sortedIdx, fromLayout, toLayout)
		}
	} else {
		column, mapping = task.mergeColumnWithOutSort(vecs, fromLayout, toLayout)
	}
	return
}

func (task *mergeBlocksTask) mergeColumnWithOutSort(column []containers.Vector, fromLayout, toLayout []uint32) (ret []containers.Vector, mapping []uint32) {
	totalLength := uint32(0)
	for _, i := range toLayout {
		totalLength += i
	}
	mapping = make([]uint32, totalLength)
	for i := range mapping {
		mapping[i] = uint32(i)
	}
	ret = mergesort.Reshape(column, fromLayout, toLayout)
	return
}

func (task *mergeBlocksTask) MarshalLogObject(enc zapcore.ObjectEncoder) (err error) {
	blks := ""
	for _, blk := range task.mergedBlks {
		blks = fmt.Sprintf("%s%d,", blks, blk.GetID())
	}
	enc.AddString("blks", blks)
	segs := ""
	for _, seg := range task.mergedSegs {
		segs = fmt.Sprintf("%s%d,", segs, seg.GetID())
	}
	enc.AddString("segs", segs)
	return
}

func (task *mergeBlocksTask) Execute() (err error) {
	logutil.Info("[Start]", common.OperationField(fmt.Sprintf("[%d]mergeblocks", task.ID())),
		common.OperandField(task))
	var toSegEntry handle.Segment
	if task.toSegEntry == nil {
		if toSegEntry, err = task.rel.CreateNonAppendableSegment(); err != nil {
			return err
		}
		task.toSegEntry = toSegEntry.GetMeta().(*catalog.SegmentEntry)
		task.createdSegs = append(task.createdSegs, task.toSegEntry)
	} else {
		if toSegEntry, err = task.rel.GetSegment(task.toSegEntry.GetID()); err != nil {
			return
		}
	}

	schema := task.mergedBlks[0].GetSchema()
	var view *model.ColumnView
	vecs := make([]containers.Vector, 0)
	rows := make([]uint32, 0)
	skipBlks := make([]int, 0)
	length := 0
	fromAddr := make([]uint32, 0, len(task.compacted))
	ids := make([]*common.ID, 0, len(task.compacted))
	task.deletes = make([]*roaring.Bitmap, len(task.compacted))

	// Prepare sort key resources
	// If there's no sort key, use physical address key
	var sortColDef *catalog.ColDef
	if schema.HasSortKey() {
		sortColDef = schema.GetSingleSortKey()
	} else {
		sortColDef = schema.PhyAddrKey
	}

	for i, block := range task.compacted {
		if view, err = block.GetColumnDataById(sortColDef.Idx, nil); err != nil {
			return
		}
		defer view.Close()
		task.deletes[i] = view.DeleteMask
		view.ApplyDeletes()
		vec := view.Orphan()
		defer vec.Close()
		if vec.Length() == 0 {
			skipBlks = append(skipBlks, i)
			continue
		}
		vecs = append(vecs, vec)
		rows = append(rows, uint32(vec.Length()))
		fromAddr = append(fromAddr, uint32(length))
		length += vec.Length()
		ids = append(ids, block.Fingerprint())
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
	node, err := common.DefaultAllocator.Alloc(length * 4)
	if err != nil {
		panic(err)
	}
	buf := node[:length]
	defer common.DefaultAllocator.Free(node)
	sortedIdx := *(*[]uint32)(unsafe.Pointer(&buf))
	vecs, mapping := task.mergeColumn(vecs, &sortedIdx, true, rows, to, schema.HasSortKey())
	for _, vec := range vecs {
		defer vec.Close()
	}
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
	for _, vec := range vecs {
		toAddr = append(toAddr, uint32(length))
		length += vec.Length()
		blk, err = toSegEntry.CreateNonAppendableBlock()
		if err != nil {
			return err
		}
		task.createdBlks = append(task.createdBlks, blk.GetMeta().(*catalog.BlockEntry))
		blockHandles = append(blockHandles, blk)
		batch := containers.NewBatch()
		batchs = append(batchs, batch)
	}
	phyAddr := schema.PhyAddrKey

	// Build and flush block index if sort key is defined
	// Flush sort key it correlates to only one column

	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		// Skip
		// PhyAddr column was processed before
		// If only one single sort key, it was processed before
		vecs = vecs[:0]
		for _, block := range task.compacted {
			if view, err = block.GetColumnDataById(def.Idx, nil); err != nil {
				return
			}
			defer view.Close()
			view.ApplyDeletes()
			vec := view.Orphan()
			if vec.Length() == 0 {
				continue
			}
			defer vec.Close()
			vecs = append(vecs, vec)
		}
		vecs, _ := task.mergeColumn(vecs, &sortedIdx, false, rows, to, schema.HasSortKey())
		for i := range vecs {
			defer vecs[i].Close()
		}
		for i, vec := range vecs {
			batchs[i].AddVector(def.Name, vec)
		}
	}

	name := blockio.EncodeObjectName()
	writer := blockio.NewWriter(context.Background(), task.mergedBlks[0].GetBlockData().GetFs(), name)
	for _, bat := range batchs {
		block, err := writer.WriteBlock(bat)
		if err != nil {
			return err
		}
		for idx, vec := range bat.Vecs {
			if phyAddr.Idx == idx {
				continue
			}
			isPk := idx == sortColDef.Idx
			_, err = BuildColumnIndex(writer.GetWriter(), block, schema.ColDefs[idx], vec, isPk, isPk)
			if err != nil {
				return err
			}
		}
	}
	blocks, err := writer.Sync()
	if err != nil {
		return err
	}
	var metaLoc string
	for i, block := range blocks {
		metaLoc, err = blockio.EncodeMetaLocWithObject(block.GetExtent(), uint32(batchs[i].Length()), blocks)
		if err != nil {
			return
		}
		err = blockHandles[i].UpdateMetaLoc(metaLoc)
	}
	for _, blk := range task.createdBlks {
		if err = blk.GetBlockData().Init(); err != nil {
			return err
		}
	}

	for _, compacted := range task.compacted {
		seg := compacted.GetSegment()
		if err = seg.SoftDeleteBlock(compacted.Fingerprint().BlockID); err != nil {
			return err
		}
	}
	for _, entry := range task.mergedSegs {
		if err = task.rel.SoftDeleteSegment(entry.GetID()); err != nil {
			return err
		}
	}

	table := task.toSegEntry.GetTable()
	txnEntry := txnentries.NewMergeBlocksEntry(
		task.txn,
		task.rel,
		task.mergedSegs,
		task.createdSegs,
		task.mergedBlks,
		task.createdBlks,
		mapping,
		fromAddr,
		toAddr,
		task.deletes,
		skipBlks,
		task.scheduler)
	if err = task.txn.LogTxnEntry(table.GetDB().ID, table.ID, txnEntry, ids); err != nil {
		return err
	}
	return err
}
