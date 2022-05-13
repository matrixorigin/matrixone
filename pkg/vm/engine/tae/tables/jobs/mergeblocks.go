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
	"fmt"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/container/vector"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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
	newSeg      handle.Segment
	scheduler   tasks.TaskScheduler
	scopes      []common.ID
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

func (task *mergeBlocksTask) mergeColumn(vecs []*vector.Vector, sortedIdx *[]uint32, isPrimary bool, fromLayout, toLayout []uint32) (column []*vector.Vector, mapping []uint32) {
	if isPrimary {
		column, mapping = mergesort.MergeSortedColumn(vecs, sortedIdx, fromLayout, toLayout)
	} else {
		column = mergesort.ShuffleColumn(vecs, *sortedIdx, fromLayout, toLayout)
	}
	return
}

func (task *mergeBlocksTask) Execute() (err error) {
	segStr := ""
	for _, seg := range task.mergedSegs {
		segStr = fmt.Sprintf("%d,", seg.GetID())
	}
	message := fmt.Sprintf("[MergeBlocks] | Segments[%s] | Blocks[", segStr)
	for _, blk := range task.mergedBlks {
		message = fmt.Sprintf("%s%d,", message, blk.GetID())
	}
	message = fmt.Sprintf("%s] | Started", message)
	logutil.Info(message)
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
	vecs := make([]*vector.Vector, 0)
	rows := make([]uint32, len(task.compacted))
	length := 0
	fromAddr := make([]uint32, 0, len(task.compacted))
	ids := make([]*common.ID, 0, len(task.compacted))
	for i, block := range task.compacted {
		if view, err = block.GetColumnDataById(int(schema.PrimaryKey), nil, nil); err != nil {
			return
		}
		vec := view.ApplyDeletes()
		vecs = append(vecs, vec)
		rows[i] = uint32(gvec.Length(vec))
		fromAddr = append(fromAddr, uint32(length))
		length += vector.Length(vec)
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

	node := common.GPool.Alloc(uint64(length * 4))
	buf := node.Buf[:length]
	defer common.GPool.Free(node)
	sortedIdx := *(*[]uint32)(unsafe.Pointer(&buf))
	vecs, mapping := task.mergeColumn(vecs, &sortedIdx, true, rows, to)
	// logutil.Infof("mapping is %v", mapping)
	// logutil.Infof("sortedIdx is %v", sortedIdx)
	ts := task.txn.GetStartTS()
	var flushTask tasks.Task
	length = 0
	var blk handle.Block
	toAddr := make([]uint32, 0, len(vecs))
	for _, vec := range vecs {
		toAddr = append(toAddr, uint32(length))
		length += gvec.Length(vec)
		blk, err = toSegEntry.CreateNonAppendableBlock()
		if err != nil {
			return err
		}
		task.createdBlks = append(task.createdBlks, blk.GetMeta().(*catalog.BlockEntry))
		meta := blk.GetMeta().(*catalog.BlockEntry)
		closure := meta.GetBlockData().FlushColumnDataClosure(ts, int(schema.PrimaryKey), vec, false)
		flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, meta.AsCommonID(), closure)
		if err != nil {
			return
		}
		if err = flushTask.WaitDone(); err != nil {
			return
		}
		if err = BuildAndFlushBlockIndex(meta.GetBlockData().GetBlockFile(), meta, vec); err != nil {
			return
		}
		if err = meta.GetBlockData().ReplayData(); err != nil {
			return
		}
		// bf := blk.GetMeta().(*catalog.BlockEntry).GetBlockData().GetBlockFile()
		// if bf.WriteColumnVec(task.txn.GetStartTS(), int(schema.PrimaryKey), vec); err != nil {
		// 	return
		// }
	}

	for i := 0; i < len(schema.ColDefs); i++ {
		if i == int(schema.PrimaryKey) {
			continue
		}
		vecs = vecs[:0]
		for _, block := range task.compacted {
			if view, err = block.GetColumnDataById(i, nil, nil); err != nil {
				return
			}
			vec := view.ApplyDeletes()
			vecs = append(vecs, vec)
		}
		vecs, _ = task.mergeColumn(vecs, &sortedIdx, false, rows, to)
		for pos, vec := range vecs {
			blk := task.createdBlks[pos]
			closure := blk.GetBlockData().FlushColumnDataClosure(ts, i, vec, false)
			flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, blk.AsCommonID(), closure)
			if err != nil {
				return
			}
			if err = flushTask.WaitDone(); err != nil {
				return
			}
		}
	}
	for i, blk := range task.createdBlks {
		closure := blk.GetBlockData().SyncBlockDataClosure(ts, rows[i])
		flushTask, err = task.scheduler.ScheduleScopedFn(tasks.WaitableCtx, tasks.IOTask, blk.AsCommonID(), closure)
		if err != nil {
			return
		}
		if err = flushTask.WaitDone(); err != nil {
			return
		}
	}
	for _, compacted := range task.compacted {
		seg := compacted.GetSegment()
		if err = seg.SoftDeleteBlock(compacted.Fingerprint().BlockID); err != nil {
			return
		}
	}
	for _, entry := range task.mergedSegs {
		if err = task.rel.SoftDeleteSegment(entry.GetID()); err != nil {
			return
		}
	}

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
		task.scheduler)
	if err = task.txn.LogTxnEntry(task.toSegEntry.GetTable().GetID(), txnEntry, ids); err != nil {
		return
	}

	return
}
