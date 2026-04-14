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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	ftnative "github.com/matrixorigin/matrixone/pkg/fulltext/native"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/perfcounter"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type flushObjTask struct {
	*tasks.BaseTask
	data      *containers.Batch
	meta      *catalog.ObjectEntry
	fs        fileservice.FileService
	name      objectio.ObjectName
	blocks    []objectio.BlockObject
	schemaVer uint32
	seqnums   []uint16
	stat      objectio.ObjectStats

	createAt    time.Time
	partentTask string

	stats objectio.ObjectStats
	done  bool
}

func NewFlushObjTask(
	ctx *tasks.Context,
	schemaVer uint32,
	seqnums []uint16,
	fs fileservice.FileService,
	meta *catalog.ObjectEntry,
	data *containers.Batch,
	parentTask string,
) *flushObjTask {

	if meta.IsTombstone {
		// [data rowId, pk, tombstone rowId, commitTS]
		// remove the `tombstone rowId`
		seqnums = append(seqnums[:2], seqnums[3:]...)
		delete(data.Nameidx, data.Attrs[2])
		data.Attrs = append(data.Attrs[:2], data.Attrs[3:]...)

		data.Vecs[2].Close()
		data.Vecs = append(data.Vecs[:2], data.Vecs[3:]...)
	}

	task := &flushObjTask{
		schemaVer:   schemaVer,
		seqnums:     seqnums,
		data:        data,
		meta:        meta,
		fs:          fs,
		createAt:    time.Now(),
		partentTask: parentTask,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushObjTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushObjTask) Execute(ctx context.Context) (err error) {
	if v := ctx.Value(TestFlushBailoutPos1{}); v != nil {
		time.Sleep(time.Duration(rand.Intn(200)) * time.Millisecond)
	}
	waitT := time.Since(task.createAt)
	seg := task.meta.ID().Segment()
	name := objectio.BuildObjectName(seg, 0)
	task.name = name
	writer, err := ioutil.NewBlockWriterNew(
		task.fs,
		name,
		task.schemaVer,
		task.seqnums,
		task.meta.IsTombstone,
	)
	if err != nil {
		return err
	}
	writer.SetAppendable()

	if task.meta.IsTombstone {
		writer.SetPrimaryKeyWithType(
			uint16(objectio.TombstonePrimaryKeyIdx),
			index.HBF,
			index.ObjectPrefixFn,
			index.BlockPrefixFn,
		)
	} else {
		if task.meta.GetSchema().HasPK() {
			writer.SetPrimaryKey(uint16(task.meta.GetSchema().GetSingleSortKeyIdx()))
		} else if task.meta.GetSchema().HasSortKey() {
			writer.SetSortKey(uint16(task.meta.GetSchema().GetSingleSortKeyIdx()))
		}
	}

	cnBatch := containers.ToCNBatch(task.data)
	for _, vec := range cnBatch.Vecs {
		if vec == nil {
			// this task has been canceled
			return nil
		}
	}
	dataRows := cnBatch.RowCount()
	inst := time.Now()
	_, err = writer.WriteBatch(cnBatch)
	if err != nil {
		return err
	}
	copyT := time.Since(inst)
	inst = time.Now()
	task.blocks, _, err = writer.Sync(ctx)
	if err != nil {
		return err
	}
	if !task.meta.IsTombstone {
		schema := task.meta.GetSchema()
		indexer, idxErr := ftnative.NewObjectIndexer(schema)
		if idxErr != nil {
			task.logNativeSidecarError("flush-init", idxErr)
		} else {
			logutil.Infof(
				"[FTS-DEBUG] flush sidecar init: table=%s schema_version=%d constraint_len=%d index_defs=%d active_builders=%d object=%s",
				schema.Name,
				schema.Version,
				len(schema.Constraint),
				indexer.IndexCount(),
				indexer.ActiveIndexCount(),
				task.name.String(),
			)
			if !indexer.Empty() {
				blockRows := make([]uint32, len(task.blocks))
				for i, block := range task.blocks {
					blockRows[i] = block.GetRows()
				}
				if idxErr = indexer.AddBatch(cnBatch, blockRows); idxErr != nil {
					task.logNativeSidecarError("flush-add", idxErr)
				} else if idxErr = indexer.Write(ctx, task.fs, task.name); idxErr != nil {
					task.logNativeSidecarError("flush-write", idxErr)
				} else {
					logutil.Infof(
						"[FTS-DEBUG] flush sidecar written: table=%s schema_version=%d object=%s active_builders=%d",
						schema.Name,
						schema.Version,
						task.name.String(),
						indexer.ActiveIndexCount(),
					)
				}
			} else {
				logutil.Infof(
					"[FTS-DEBUG] flush sidecar skipped: table=%s schema_version=%d constraint_len=%d index_defs=%d object=%s",
					schema.Name,
					schema.Version,
					len(schema.Constraint),
					indexer.IndexCount(),
					task.name.String(),
				)
			}
		}
	}
	ioT := time.Since(inst)
	if time.Since(task.createAt) > SlowFlushIOTask {
		logutil.Info(
			"[FLUSH-SLOW-OBJ]",
			zap.String("task", task.partentTask),
			common.AnyField("obj", task.meta.ID().ShortStringEx()),
			common.AnyField("wait", waitT),
			common.AnyField("copy", copyT),
			common.AnyField("io", ioT),
			common.AnyField("data-rows", dataRows),
		)
	}
	task.stats = writer.GetObjectStats()

	perfcounter.Update(ctx, func(counter *perfcounter.CounterSet) {
		counter.TAE.Block.Flush.Add(1)
	})
	task.stat = writer.Stats()
	return err
}

func (task *flushObjTask) release() {
	if task == nil {
		return
	}
	if !task.done {
		ctx, cancel := context.WithTimeoutCause(
			context.Background(),
			10*time.Second,
			moerr.CauseReleaseFlushObjTasks,
		)
		defer cancel()
		task.WaitDone(ctx)
	}

	if task.data != nil {
		task.data.Close()
	}
}

func (task *flushObjTask) logNativeSidecarError(phase string, err error) {
	logutil.Error(
		"[NATIVE-FTS-SIDECAR-SKIP]",
		zap.String("phase", phase),
		zap.String("table", task.meta.GetSchema().Name),
		common.AnyField("obj", task.meta.ID().ShortStringEx()),
		zap.Error(err),
	)
}
