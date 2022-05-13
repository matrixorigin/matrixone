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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/mergesort"
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

func (task *compactBlockTask) PrepareData() (bat *batch.Batch, err error) {
	attrs := task.meta.GetSchema().Attrs()
	bat = batch.New(true, attrs)

	for i := range task.meta.GetSchema().ColDefs {
		view, err := task.compacted.GetColumnDataById(i, nil, nil)
		if err != nil {
			return bat, err
		}
		vec := view.ApplyDeletes()
		bat.Vecs[i] = vec
	}
	if err = mergesort.SortBlockColumns(bat.Vecs, int(task.meta.GetSchema().PrimaryKey)); err != nil {
		return
	}
	return
}

func (task *compactBlockTask) GetNewBlock() handle.Block { return task.created }

func (task *compactBlockTask) Execute() (err error) {
	now := time.Now()
	data, err := task.PrepareData()
	if err != nil {
		return
	}
	seg := task.compacted.GetSegment()
	// rel := seg.GetRelation()
	newBlk, err := seg.CreateNonAppendableBlock()
	if err != nil {
		return err
	}
	if err = seg.SoftDeleteBlock(task.compacted.Fingerprint().BlockID); err != nil {
		return err
	}
	newMeta := newBlk.GetMeta().(*catalog.BlockEntry)
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
	txnEntry := txnentries.NewCompactBlockEntry(task.txn, task.compacted, task.created, task.scheduler)
	if err = task.txn.LogTxnEntry(task.meta.GetSegment().GetTable().GetID(), txnEntry, []*common.ID{task.compacted.Fingerprint()}); err != nil {
		return
	}
	logutil.Infof("(%s) [Compacted] | (%s) [Created] | %s", task.compacted.Fingerprint().BlockString(), task.created.Fingerprint().BlockString(), time.Since(now))
	return
}
