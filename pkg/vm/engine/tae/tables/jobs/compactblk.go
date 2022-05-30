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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
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
}

func NewCompactBlockTask(ctx *tasks.Context, txn txnif.AsyncTxn, meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) (task *compactBlockTask, err error) {
	task = &compactBlockTask{
		txn:       txn,
		meta:      meta,
		scheduler: scheduler,
	}
	dbName := meta.GetSegment().GetTable().GetDB().GetName()
	database, err := txn.GetDatabase(dbName)
	if err != nil {
		return
	}
	relName := meta.GetSchema().Name
	rel, err := database.GetRelationByName(relName)
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

func (task *compactBlockTask) PrepareData(blkKey []byte) (bat *batch.Batch, closer func(), err error) {
	attrs := make([]string, 0, 4)
	bat = batch.New(true, attrs)

	var view *model.ColumnView
	hiddenDef := task.meta.GetSchema().HiddenKeyDef()
	for _, def := range task.meta.GetSchema().ColDefs {
		if hiddenDef.Idx == def.Idx {
			continue
		}
		view, err = task.compacted.GetColumnDataById(def.Idx, nil, nil)
		if err != nil {
			return
		}
		vec := view.ApplyDeletes()
		bat.Vecs = append(bat.Vecs, vec)
		bat.Attrs = append(bat.Attrs, def.Name)
	}
	// Merge sort only if hidden column is not primary key
	if !hiddenDef.IsPrimary() {
		if err = mergesort.SortBlockColumns(bat.Vecs, task.meta.GetSchema().GetPrimaryKeyIdx()); err != nil {
			return
		}
	}
	// Prepare hidden column data
	hidden, closer, err := model.PrepareHiddenData(catalog.HiddenColumnType, blkKey, 0, uint32(compute.LengthOfBatch(bat)))
	if err != nil {
		return
	}
	bat.Vecs = append(bat.Vecs, hidden)
	bat.Attrs = append(bat.Attrs, catalog.HiddenColumnName)
	return
}

func (task *compactBlockTask) GetNewBlock() handle.Block { return task.created }

func (task *compactBlockTask) Execute() (err error) {
	now := time.Now()
	seg := task.compacted.GetSegment()
	// Prepare a block placeholder
	newBlk, err := seg.CreateNonAppendableBlock()
	if err != nil {
		return err
	}
	newMeta := newBlk.GetMeta().(*catalog.BlockEntry)
	data, closer, err := task.PrepareData(newMeta.MakeKey())
	if err != nil {
		return
	}
	defer closer()
	if err = seg.SoftDeleteBlock(task.compacted.Fingerprint().BlockID); err != nil {
		return err
	}
	newBlkData := newMeta.GetBlockData()
	blockFile := newBlkData.GetBlockFile()

	ioTask := NewFlushBlkTask(tasks.WaitableCtx, blockFile, task.txn.GetStartTS(), newMeta, data)
	if err = task.scheduler.Schedule(ioTask); err != nil {
		return
	}
	if err = ioTask.WaitDone(); err != nil {
		return
	}

	if err = newBlkData.ReplayData(); err != nil {
		return err
	}
	task.created = newBlk
	table := task.meta.GetSegment().GetTable()
	txnEntry := txnentries.NewCompactBlockEntry(task.txn, task.compacted, task.created, task.scheduler)
	if err = task.txn.LogTxnEntry(table.GetDB().ID, table.ID, txnEntry, []*common.ID{task.compacted.Fingerprint()}); err != nil {
		return
	}
	logutil.Infof("(%s) [Compacted] | (%s) [Created] | %s", task.compacted.Fingerprint().BlockString(), task.created.Fingerprint().BlockString(), time.Since(now))
	return
}
