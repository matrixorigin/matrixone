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
	"math/rand"
	"time"

	"github.com/matrixorigin/matrixone/pkg/perfcounter"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushObjTask struct {
	*tasks.BaseTask
	data      *containers.Batch
	delta     *containers.Batch
	meta      *catalog.ObjectEntry
	fs        *objectio.ObjectFS
	name      objectio.ObjectName
	blocks    []objectio.BlockObject
	schemaVer uint32
	seqnums   []uint16
	stat      objectio.ObjectStats
	isAObj    bool

	Stats objectio.ObjectStats
}

func NewFlushObjTask(
	ctx *tasks.Context,
	schemaVer uint32,
	seqnums []uint16,
	fs *objectio.ObjectFS,
	meta *catalog.ObjectEntry,
	data *containers.Batch,
	delta *containers.Batch,
	isAObj bool,
) *flushObjTask {
	task := &flushObjTask{
		schemaVer: schemaVer,
		seqnums:   seqnums,
		data:      data,
		meta:      meta,
		fs:        fs,
		delta:     delta,
		isAObj:    isAObj,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushObjTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushObjTask) Execute(ctx context.Context) (err error) {
	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	}
	seg := task.meta.ID.Segment()
	name := objectio.BuildObjectName(seg, 0)
	task.name = name
	writer, err := blockio.NewBlockWriterNew(task.fs.Service, name, task.schemaVer, task.seqnums)
	if err != nil {
		return err
	}
	if task.isAObj {
		writer.SetAppendable()
	}
	if task.meta.GetSchema().HasPK() {
		writer.SetPrimaryKey(uint16(task.meta.GetSchema().GetSingleSortKeyIdx()))
	} else if task.meta.GetSchema().HasSortKey() {
		writer.SetSortKey(uint16(task.meta.GetSchema().GetSingleSortKeyIdx()))
	}

	cnBatch := containers.ToCNBatch(task.data)
	for _, vec := range cnBatch.Vecs {
		if vec == nil {
			// this task has been canceled
			return nil
		}
	}
	_, err = writer.WriteBatch(cnBatch)
	if err != nil {
		return err
	}
	if task.delta != nil {
		_, err := writer.WriteTombstoneBatch(containers.ToCNBatch(task.delta))
		if err != nil {
			return err
		}
	}
	task.blocks, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	task.Stats = writer.GetObjectStats()[objectio.SchemaData]

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.Flush.Add(1)
	})
	task.stat = writer.Stats()
	return err
}
