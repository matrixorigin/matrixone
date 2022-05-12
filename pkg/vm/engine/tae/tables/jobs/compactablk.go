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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var CompactABlockTaskFactory = func(meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) tasks.TxnTaskFactory {
	return func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return NewCompactABlockTask(ctx, txn, meta, scheduler)
	}
}

type compactABlockTask struct {
	*tasks.BaseTask
	txn       txnif.AsyncTxn
	meta      *catalog.BlockEntry
	scopes    []common.ID
	scheduler tasks.TaskScheduler
}

func NewCompactABlockTask(ctx *tasks.Context, txn txnif.AsyncTxn, meta *catalog.BlockEntry, scheduler tasks.TaskScheduler) (task *compactABlockTask, err error) {
	task = &compactABlockTask{
		txn:       txn,
		meta:      meta,
		scheduler: scheduler,
	}
	task.scopes = append(task.scopes, *meta.AsCommonID())
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return
}

func (task *compactABlockTask) Scopes() []common.ID { return task.scopes }

func (task *compactABlockTask) Execute() (err error) {
	dataBlock := task.meta.GetBlockData()
	return dataBlock.ForceCompact()
}
