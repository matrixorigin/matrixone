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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushABlkTask struct {
	*tasks.BaseTask
	data  *containers.Batch
	delta *containers.Batch
	meta  *catalog.BlockEntry
	file  file.Block
	ts    types.TS
}

func NewFlushABlkTask(
	ctx *tasks.Context,
	bf file.Block,
	ts types.TS,
	meta *catalog.BlockEntry,
	data *containers.Batch,
	delta *containers.Batch,
) *flushABlkTask {
	task := &flushABlkTask{
		ts:    ts,
		data:  data,
		meta:  meta,
		file:  bf,
		delta: delta,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushABlkTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushABlkTask) Execute() error {
	block, err := task.file.WriteBatch(task.data, task.ts)
	if err != nil {
		return err
	}
	if err = BuildBlockIndex(task.file.GetWriter(), block, task.meta, task.data, false); err != nil {
		return err
	}

	if task.delta != nil {
		_, err := task.file.WriteBatch(task.delta, task.ts)
		if err != nil {
			return err
		}
	}
	return task.file.Sync()
}
