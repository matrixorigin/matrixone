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
	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushDeletesTask struct {
	*tasks.BaseTask
	delta     *containers.Batch
	meta      *catalog.BlockEntry
	fs        *objectio.ObjectFS
	name      objectio.ObjectName
	blocks    []objectio.BlockObject
	schemaVer uint32
	seqnums   []uint16
}

func NewFlushDeletesTask(
	ctx *tasks.Context,
	schemaVer uint32,
	seqnums []uint16,
	fs *objectio.ObjectFS,
	meta *catalog.BlockEntry,
	delta *containers.Batch,
) *flushDeletesTask {
	task := &flushDeletesTask{
		schemaVer: schemaVer,
		seqnums:   seqnums,
		meta:      meta,
		fs:        fs,
		delta:     delta,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushDeletesTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushDeletesTask) Execute(ctx context.Context) error {
	name := task.meta.BuildDeleteObjectName()
	task.name = name
	writer, err := blockio.NewBlockWriterNew(task.fs.Service, name, task.schemaVer, task.seqnums)
	if err != nil {
		return err
	}
	_, err = writer.WriteBatchWithOutIndex(containers.ToCNBatch(task.delta))
	if err != nil {
		return err
	}
	task.blocks, _, err = writer.Sync(ctx)

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.Flush.Add(1)
	})
	return err
}
