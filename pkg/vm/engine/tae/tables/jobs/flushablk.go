package jobs

import (
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type flushABlkTask struct {
	*tasks.BaseTask
	scope   *common.ID
	data    batch.IBatch
	ts      uint64
	masks   map[uint16]*roaring.Bitmap
	vals    map[uint16]map[uint32]interface{}
	deletes *roaring.Bitmap
	file    file.Block
}

func NewFlushABlkTask(ctx *tasks.Context, bf file.Block, scope *common.ID, data batch.IBatch, ts uint64, masks map[uint16]*roaring.Bitmap, vals map[uint16]map[uint32]interface{}, deletes *roaring.Bitmap) *flushABlkTask {
	task := &flushABlkTask{
		scope:   scope,
		data:    data,
		ts:      ts,
		masks:   masks,
		vals:    vals,
		deletes: deletes,
		file:    bf,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *flushABlkTask) Scope() *common.ID { return task.scope }

func (task *flushABlkTask) Execute() error {
	if err := task.file.WriteIBatch(task.data, task.ts, task.masks, task.vals, task.deletes); err != nil {
		return err
	}
	return task.file.Sync()
}
