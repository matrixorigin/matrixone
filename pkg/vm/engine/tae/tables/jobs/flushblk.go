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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushBlkTask struct {
	*tasks.BaseTask
	data   *containers.Batch
	delta  *containers.Batch
	meta   *catalog.BlockEntry
	fs     *objectio.ObjectFS
	ts     types.TS
	name   objectio.ObjectName
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
	seg := task.meta.ID.Segment()
	num, _ := task.meta.ID.Offsets()
	name := objectio.BuildObjectName(seg, num)
	task.name = name
	writer, err := blockio.NewBlockWriterNew(task.fs.Service, name)
	if err != nil {
		return err
	}
	if task.meta.GetSchema().HasPK() {
		writer.SetPrimaryKey(uint16(task.meta.GetSchema().GetSingleSortKeyIdx()))
	}
	_, err = writer.WriteBatch(containers.ToCNBatch(task.data))
	if err != nil {
		return err
	}
	if task.delta != nil {
		_, err := writer.WriteBatchWithOutIndex(containers.ToCNBatch(task.delta))
		if err != nil {
			return err
		}
	}
	task.blocks, _, err = writer.Sync(context.Background())
	return err
}
