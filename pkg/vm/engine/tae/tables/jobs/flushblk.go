package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushBlkTask struct {
	*tasks.BaseTask
	data *batch.Batch
	meta *catalog.BlockEntry
	file file.Block
	ts   uint64
}

func NewFlushBlkTask(ctx *tasks.Context, bf file.Block, ts uint64, meta *catalog.BlockEntry, data *batch.Batch) *flushBlkTask {
	task := &flushBlkTask{
		ts:   ts,
		data: data,
		meta: meta,
		file: bf,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushBlkTask) Scope() *common.ID { return task.meta.AsCommonID() }

func (task *flushBlkTask) Execute() (err error) {
	pkColumnData := task.data.Vecs[task.meta.GetSchema().PrimaryKey]
	if err = BuildAndFlushBlockIndex(task.file, task.meta, pkColumnData); err != nil {
		return
	}
	if err = task.file.WriteBatch(task.data, task.ts); err != nil {
		return
	}
	return task.file.Sync()
}
