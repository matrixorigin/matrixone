package jobs

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type checkpointABlkTask struct {
	*tasks.BaseTask
	scope     *common.ID
	ablk      data.Block
	ts        uint64
	scheduler tasks.TaskScheduler
}

func NewCheckpointABlkTask(ctx *tasks.Context, scheduler tasks.TaskScheduler, scope *common.ID, ablk data.Block, ts uint64) *checkpointABlkTask {
	task := &checkpointABlkTask{
		scope:     scope,
		scheduler: scheduler,
		ablk:      ablk,
		ts:        ts,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.IOTask, ctx)
	return task
}

func (task *checkpointABlkTask) Scope() *common.ID { return task.scope }

func (task *checkpointABlkTask) Execute() (err error) {
	ckpTs := task.ablk.GetMaxCheckpointTS()
	if task.ts <= ckpTs {
		return nil
	}
	indexes := task.ablk.CollectAppendLogIndexes(ckpTs+1, task.ts)
	task.scheduler.Checkpoint(indexes)
	// for i, index := range indexes {
	// 	logutil.Infof("Checkpoint %d: %s", i, index.String())
	// }
	task.ablk.SetMaxCheckpointTS(task.ts)
	return
}
