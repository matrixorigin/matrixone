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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushBlkTask struct {
	*tasks.BaseTask
	data   *containers.Batch
	delta  *containers.Batch
	meta   *catalog.BlockEntry
	fs     *objectio.ObjectFS
	ts     types.TS
	blocks []objectio.BlockObject
}

func NewFlushBlkTask(
	ctx *tasks.Context,
	fs *objectio.ObjectFS,
	ts types.TS,
	meta *catalog.BlockEntry,
	data *containers.Batch,
	delta *containers.Batch,
) *flushBlkTask {
	task := &flushBlkTask{
		ts:    ts,
		data:  data,
		meta:  meta,
		fs:    fs,
		delta: delta,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushBlkTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushBlkTask) Execute() error {
	name := blockio.EncodeObjectName()
	writer := blockio.NewWriter(context.Background(), task.fs, name)
	block, err := writer.WriteBlock(task.data)
	if err != nil {
		return err
	}
	if err = BuildBlockIndex(writer.GetWriter(), block, task.meta.GetSchema(), task.data, true); err != nil {
		return err
	}
	if task.delta != nil {
		_, err := writer.WriteBlock(task.delta)
		if err != nil {
			return err
		}
	}
	task.blocks, err = writer.Sync()
	return err
}
