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

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/txnentries"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var CompactBlockTaskFactory = func(
	meta *catalog.BlockEntry, rt *dbutils.Runtime,
) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewCompactBlockTask(ctx, txn, meta, rt)
	}
}

type compactBlockTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	rt        *dbutils.Runtime
	compacted handle.Block
	created   handle.Block
	schema    *catalog.Schema
	meta      *catalog.BlockEntry
	scopes    []common.ID
	mapping   []int32
	deletes   *nulls.Bitmap
}

func NewCompactBlockTask(
	ctx *tasks.Context,
	txn txnif.AsyncTxn,
	meta *catalog.BlockEntry,
	rt *dbutils.Runtime,
) (task *compactBlockTask, err error) {
	task = &compactBlockTask{
		txn:  txn,
		meta: meta,
		rt:   rt,
	}
	dbId := meta.GetSegment().GetTable().GetDB().ID
	database, err := txn.UnsafeGetDatabase(dbId)
	if err != nil {
		return
	}
	tableId := meta.GetSegment().GetTable().ID
	rel, err := database.UnsafeGetRelation(tableId)
	if err != nil {
		return
	}
	task.schema = rel.Schema().(*catalog.Schema)
	seg, err := rel.GetSegment(&meta.GetSegment().ID)
	if err != nil {
		return
	}
	defer seg.Close()
	task.compacted, err = seg.GetBlock(meta.ID)
	if err != nil {
		return
	}
	task.scopes = append(task.scopes, *task.compacted.Fingerprint())
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *compactBlockTask) Scopes() []common.ID { return task.scopes }

func (task *compactBlockTask) PrepareData(ctx context.Context) (
	preparer *model.PreparedCompactedBlockData, empty bool, err error,
) {
	preparer = model.NewPreparedCompactedBlockData()
	preparer.Columns = containers.NewBatch()

	schema := task.schema
	colLen := len(schema.ColDefs)
	seqnums := make([]uint16, 0, colLen)
	idxs := make([]int, 0, colLen)
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		idxs = append(idxs, def.Idx)
		seqnums = append(seqnums, def.SeqNum)
	}
	if len(idxs) > 0 {
		var views *containers.BlockView
		views, err = task.compacted.GetColumnDataByIds(ctx, idxs)
		if err != nil {
			return
		}
		task.deletes = views.DeleteMask
		views.ApplyDeletes()
		for i := 0; i < colLen; i++ {
			if schema.ColDefs[i].IsPhyAddr() {
				continue
			}
			if views.Columns[i] == nil {
				preparer.Close()
				return nil, true, nil
			}
			vec := views.Columns[i].Orphan()
			if vec.Length() == 0 {
				empty = true
				vec.Close()
				return
			}
			preparer.Columns.AddVector(schema.ColDefs[i].Name, vec)
		}
	}
	preparer.SchemaVersion = schema.Version
	preparer.Seqnums = seqnums
	// Sort only if sort key is defined
	if schema.HasSortKey() {
		idx := schema.GetSingleSortKeyIdx()
		preparer.SortKey = preparer.Columns.Vecs[idx]
		if task.mapping, err = mergesort.SortBlockColumns(
			preparer.Columns.Vecs, idx, task.rt.VectorPool.Transient,
		); err != nil {
			return preparer, false, err
		}
	}
	return
}

func (task *compactBlockTask) GetNewBlock() handle.Block { return task.created }
func (task *compactBlockTask) Name() string {
	return fmt.Sprintf("[%d]compact", task.ID())
}

func (task *compactBlockTask) Execute(ctx context.Context) (err error) {
	logutil.Info("[Start]", common.OperationField(task.Name()),
		common.OperandField(task.meta.Repr()))
	phaseNumber := 0
	defer func() {
		if err != nil {
			logutil.Error("[DoneWithErr]", common.OperationField(task.Name()),
				common.AnyField("error", err),
				common.AnyField("phase", phaseNumber),
			)
		}
	}()
	now := time.Now()
	seg := task.compacted.GetSegment()
	defer seg.Close()
	// Prepare a block placeholder
	oldBMeta := task.compacted.GetMeta().(*catalog.BlockEntry)
	phaseNumber = 1
	preparer, empty, err := task.PrepareData(ctx)
	if err != nil {
		return
	}
	if preparer == nil {
		return
	}
	defer preparer.Close()
	phaseNumber = 2
	if err = seg.SoftDeleteBlock(task.compacted.Fingerprint().BlockID); err != nil {
		return err
	}
	oldBlkData := oldBMeta.GetBlockData()
	var deletes *containers.Batch
	phaseNumber = 3
	if !oldBMeta.IsAppendable() {
		deletes, err = oldBlkData.CollectDeleteInRange(ctx, types.TS{}, task.txn.GetStartTS(), true)
		if err != nil {
			return
		}
		if deletes != nil {
			defer deletes.Close()
		}
	}
	phaseNumber = 4
	if !empty {
		createOnSeg := seg
		curSeg := seg.GetMeta().(*catalog.SegmentEntry)
		// double the threshold to make more room for creating new appendable segment during appending, just a piece of defensive code
		// check GetAppender function in tableHandle
		if curSeg.GetNextObjectIndex() > options.DefaultObejctPerSegment*2 {
			nextSeg := curSeg.GetTable().LastAppendableSegmemt()
			if nextSeg.ID == curSeg.ID {
				// we can't create appendable seg here because compaction can be rollbacked.
				// so just wait until the new appendable seg is available.
				// actually this log can barely be printed.
				logutil.Infof("do not compact on seg %s %d, wait", curSeg.ID.ToString(), curSeg.GetNextObjectIndex())
				return moerr.GetOkExpectedEOB()
			}
			if createOnSeg, err = task.compacted.GetSegment().GetRelation().GetSegment(&nextSeg.ID); err != nil {
				return err
			} else {
				defer createOnSeg.Close()
			}
		}

		if _, err = task.createAndFlushNewBlock(
			createOnSeg, preparer, deletes,
		); err != nil {
			return
		}
	}

	table := task.meta.GetSegment().GetTable()
	// write ablock
	phaseNumber = 5
	if oldBMeta.IsAppendable() {
		var data *containers.Batch
		dataVer, errr := oldBlkData.CollectAppendInRange(types.TS{}, task.txn.GetStartTS(), true)
		if errr != nil {
			return errr
		}
		data = dataVer.Batch
		defer data.Close()
		deletes, err = oldBlkData.CollectDeleteInRange(ctx, types.TS{}, task.txn.GetStartTS(), true)
		if err != nil {
			return
		}
		if deletes != nil {
			defer deletes.Close()
		}
		ablockTask := NewFlushBlkTask(
			tasks.WaitableCtx,
			dataVer.Version,
			dataVer.Seqnums,
			oldBlkData.GetFs(),
			oldBMeta,
			data,
			deletes,
		)
		if err = task.rt.Scheduler.Schedule(ablockTask); err != nil {
			return
		}
		if err = ablockTask.WaitDone(); err != nil {
			return
		}
		metaLocABlk := blockio.EncodeLocation(
			ablockTask.name,
			ablockTask.blocks[0].GetExtent(),
			uint32(data.Length()),
			ablockTask.blocks[0].GetID())

		if err = task.compacted.UpdateMetaLoc(metaLocABlk); err != nil {
			return err
		}
		if deletes != nil {
			deltaLoc := blockio.EncodeLocation(
				ablockTask.name,
				ablockTask.blocks[1].GetExtent(),
				uint32(deletes.Length()),
				ablockTask.blocks[1].GetID())

			if err = task.compacted.UpdateDeltaLoc(deltaLoc); err != nil {
				return err
			}
		}
	}
	// sortkey does not change, nerver mind the schema version
	if !task.schema.HasSortKey() && task.created != nil {
		n := task.created.Rows()
		task.mapping = make([]int32, n)
		for i := 0; i < n; i++ {
			task.mapping[i] = int32(i)
		}
	}
	phaseNumber = 6
	txnEntry := txnentries.NewCompactBlockEntry(
		task.txn,
		task.compacted,
		task.created,
		task.mapping,
		task.deletes,
		task.rt,
	)

	if err = task.txn.LogTxnEntry(
		table.GetDB().ID,
		table.ID,
		txnEntry,
		[]*common.ID{task.compacted.Fingerprint()},
	); err != nil {
		return
	}
	createdStr := "nil"
	rows := 0
	if task.created != nil {
		createdStr = task.created.Fingerprint().BlockString()
		rows = task.created.Rows()
	}
	logutil.Info("[Done]",
		common.AnyField("txn-start-ts", task.txn.GetStartTS().ToString()),
		common.OperationField(task.Name()),
		common.AnyField("compacted", task.meta.Repr()),
		common.AnyField("created", createdStr),
		common.AnyField("compactedRows", task.compacted.Rows()),
		common.AnyField("TotalChanges", task.compacted.GetTotalChanges()),
		common.AnyField("createdRows", rows),
		common.DurationField(time.Since(now)))
	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Segment.CompactBlock.Add(1)
	})
	return
}

func (task *compactBlockTask) createAndFlushNewBlock(
	seg handle.Segment,
	preparer *model.PreparedCompactedBlockData,
	deletes *containers.Batch,
) (newBlk handle.Block, err error) {
	newBlk, err = seg.CreateNonAppendableBlock(nil)
	if err != nil {
		return
	}
	task.created = newBlk
	newMeta := newBlk.GetMeta().(*catalog.BlockEntry)
	id := newMeta.ID
	newBlkData := newMeta.GetBlockData()
	ioTask := NewFlushBlkTask(
		tasks.WaitableCtx,
		preparer.SchemaVersion,
		preparer.Seqnums,
		newBlkData.GetFs(),
		newMeta,
		preparer.Columns,
		deletes)
	if err = task.rt.Scheduler.Schedule(ioTask); err != nil {
		return
	}
	if err = ioTask.WaitDone(); err != nil {
		logutil.Warnf("flush error for %s %v", id.String(), err)
		return
	}
	metaLoc := blockio.EncodeLocation(
		ioTask.name,
		ioTask.blocks[0].GetExtent(),
		uint32(preparer.Columns.Length()),
		ioTask.blocks[0].GetID())

	logutil.Debugf("update metaloc for %s", id.String())
	if err = newBlk.UpdateMetaLoc(metaLoc); err != nil {
		return
	}
	if deletes != nil {
		deltaLoc := blockio.EncodeLocation(
			ioTask.name,
			ioTask.blocks[1].GetExtent(),
			uint32(deletes.Length()),
			ioTask.blocks[1].GetID())

		if err = task.compacted.UpdateDeltaLoc(deltaLoc); err != nil {
			return
		}
	}

	err = newBlkData.Init()
	return
}
