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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var CompactBlockTaskFactory = func(meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewCompactBlockTask(ctx, txn, meta, scheduler)
	}
}

type compactBlockTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	compacted handle.Block
	created   handle.Block
	meta      *catalog.BlockEntry
	scheduler tasks.TaskScheduler
	scopes    []common.ID
	mapping   []uint32
	deletes   *roaring.Bitmap
}

func NewCompactBlockTask(ctx *tasks.Context, txn txnif.AsyncTxn, meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) (task *compactBlockTask, err error) {
	task = &compactBlockTask{
		txn:       txn,
		meta:      meta,
		scheduler: scheduler,
	}
	dbId := meta.GetSegment().GetTable().GetDB().GetID()
	database, err := txn.UnsafeGetDatabase(dbId)
	if err != nil {
		return
	}
	tableId := meta.GetSegment().GetTable().GetID()
	rel, err := database.UnsafeGetRelation(tableId)
	if err != nil {
		return
	}
	seg, err := rel.GetSegment(meta.GetSegment().GetID())
	if err != nil {
		return
	}
	task.compacted, err = seg.GetBlock(meta.GetID())
	if err != nil {
		return
	}
	task.scopes = append(task.scopes, *task.compacted.Fingerprint())
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *compactBlockTask) Scopes() []common.ID { return task.scopes }

func (task *compactBlockTask) PrepareData(blkKey []byte) (preparer *model.PreparedCompactedBlockData, err error) {
	preparer = model.NewPreparedCompactedBlockData()
	preparer.Columns = containers.NewBatch()

	schema := task.meta.GetSchema()
	var view *model.ColumnView
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		view, err = task.compacted.GetColumnDataById(def.Idx, nil)
		if err != nil {
			return
		}
		task.deletes = view.DeleteMask
		view.ApplyDeletes()
		vec := view.Orphan()
		preparer.Columns.AddVector(def.Name, vec)
	}
	// Sort only if sort key is defined
	if schema.HasSortKey() {
		idx := schema.GetSingleSortKeyIdx()
		preparer.SortKey = preparer.Columns.Vecs[idx]
		if task.mapping, err = mergesort.SortBlockColumns(preparer.Columns.Vecs, idx); err != nil {
			return preparer, err
		}
	}
	return
}

func (task *compactBlockTask) GetNewBlock() handle.Block { return task.created }
func (task *compactBlockTask) Name() string {
	return fmt.Sprintf("[%d]compact", task.ID())
}

func (task *compactBlockTask) Execute() (err error) {
	logutil.Info("[Start]", common.OperationField(task.Name()),
		common.OperandField(task.meta.Repr()))
	now := time.Now()
	seg := task.compacted.GetSegment()
	// Prepare a block placeholder
	newBlk, err := seg.CreateNonAppendableBlock()
	if err != nil {
		return err
	}
	newMeta := newBlk.GetMeta().(*catalog.BlockEntry)
	oldBMeta := task.compacted.GetMeta().(*catalog.BlockEntry)
	// data, sortCol, closer, err := task.PrepareData(newMeta.MakeKey())
	preparer, err := task.PrepareData(newMeta.MakeKey())
	if err != nil {
		return
	}
	defer preparer.Close()
	if err = seg.SoftDeleteBlock(task.compacted.Fingerprint().BlockID); err != nil {
		return err
	}
	newBlkData := newMeta.GetBlockData()
	oldBlkData := oldBMeta.GetBlockData()
	var deletes *containers.Batch
	var deltaLoc string
	if !oldBMeta.IsAppendable() {
		deletes, err = oldBlkData.CollectDeleteInRange(types.TS{}, task.txn.GetStartTS(), true)
		if err != nil {
			return
		}
		if deletes != nil {
			defer deletes.Close()
		}
	}
	ioTask := NewFlushBlkTask(
		tasks.WaitableCtx,
		newBlkData.GetFs(),
		task.txn.GetStartTS(),
		newMeta,
		preparer.Columns,
		deletes)
	if err = task.scheduler.Schedule(ioTask); err != nil {
		return
	}
	if err = ioTask.WaitDone(); err != nil {
		return
	}
	metaLoc, err := blockio.EncodeBlkMetaLocWithObject(ioTask.meta.AsCommonID(),
		ioTask.blocks[0].GetExtent(),
		uint32(preparer.Columns.Length()),
		ioTask.blocks)
	if err != nil {
		return err
	}
	if err = newBlk.UpdateMetaLoc(metaLoc); err != nil {
		return err
	}
	if deletes != nil {
		deltaLoc, err = blockio.EncodeBlkMetaLocWithObject(ioTask.meta.AsCommonID(),
			ioTask.blocks[1].GetExtent(),
			0,
			ioTask.blocks)
		if err != nil {
			return
		}
		if err = task.compacted.UpdateDeltaLoc(deltaLoc); err != nil {
			return err
		}
	}

	if err = newBlkData.ReplayIndex(); err != nil {
		return err
	}
	task.created = newBlk
	table := task.meta.GetSegment().GetTable()
	// write ablock
	if oldBMeta.IsAppendable() {
		var data *containers.Batch
		data, err = oldBlkData.CollectAppendInRange(types.TS{}, task.txn.GetStartTS(), true)
		if err != nil {
			return
		}
		defer data.Close()
		deletes, err = oldBlkData.CollectDeleteInRange(types.TS{}, task.txn.GetStartTS(), true)
		if err != nil {
			return
		}
		if deletes != nil {
			defer deletes.Close()
		}
		ablockTask := NewFlushBlkTask(
			tasks.WaitableCtx,
			oldBlkData.GetFs(),
			task.txn.GetStartTS(),
			oldBMeta,
			data,
			deletes,
		)
		if err = task.scheduler.Schedule(ablockTask); err != nil {
			return
		}
		if err = ablockTask.WaitDone(); err != nil {
			return
		}
		var metaLocABlk string
		metaLocABlk, err = blockio.EncodeBlkMetaLocWithObject(ablockTask.meta.AsCommonID(),
			ablockTask.blocks[0].GetExtent(),
			uint32(data.Length()),
			ablockTask.blocks)
		if err != nil {
			return
		}
		if err = task.compacted.UpdateMetaLoc(metaLocABlk); err != nil {
			return err
		}
		if deletes != nil {
			deltaLoc, err = blockio.EncodeBlkMetaLocWithObject(ablockTask.meta.AsCommonID(),
				ablockTask.blocks[1].GetExtent(),
				0,
				ablockTask.blocks)
			if err != nil {
				return
			}
			if err = task.compacted.UpdateDeltaLoc(deltaLoc); err != nil {
				return err
			}
		}
		if err = oldBlkData.ReplayIndex(); err != nil {
			return err
		}
	}
	txnEntry := txnentries.NewCompactBlockEntry(task.txn, task.compacted, task.created, task.scheduler, task.mapping, task.deletes)
	if err = task.txn.LogTxnEntry(table.GetDB().ID, table.ID, txnEntry, []*common.ID{task.compacted.Fingerprint()}); err != nil {
		return
	}
	logutil.Info("[Done]",
		common.OperationField(task.Name()),
		common.AnyField("created", task.created.Fingerprint().BlockString()),
		common.DurationField(time.Since(now)))
	return
}
