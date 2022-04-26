package tasks

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/ops"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/ops/base"
)

type Context struct {
	DoneCB   ops.OpDoneCB
	Waitable bool
}

type BaseTask struct {
	ops.Op
	id       uint64
	taskType TaskType
	exec     func(Task) error
}

func NewBaseTask(impl base.IOpInternal, taskType TaskType, ctx *Context) *BaseTask {
	task := &BaseTask{
		id:       NextTaskId(),
		taskType: taskType,
	}
	var doneCB ops.OpDoneCB
	if ctx != nil {
		if ctx.DoneCB == nil && !ctx.Waitable {
			doneCB = task.onDone
		}
	} else {
		doneCB = task.onDone
	}
	if impl == nil {
		impl = task
	}
	task.Op = ops.Op{
		Impl:   impl,
		DoneCB: doneCB,
	}
	if doneCB == nil {
		task.Op.ErrorC = make(chan error)
	}
	return task
}

func (task *BaseTask) onDone(_ base.IOp) {
	/* Noop */
}
func (task *BaseTask) Type() TaskType      { return task.taskType }
func (task *BaseTask) Cancel() (err error) { panic("todo") }
func (task *BaseTask) ID() uint64          { return task.id }
func (task *BaseTask) Execute() (err error) {
	if task.exec != nil {
		return task.exec(task)
	}
	logutil.Debugf("Execute Task Type=%d, ID=%d", task.taskType, task.id)
	return nil
}
