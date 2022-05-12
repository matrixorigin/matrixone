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

package tasks

import (
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
)

var WaitableCtx = &Context{Waitable: true}

type Context struct {
	DoneCB   ops.OpDoneCB
	Waitable bool
}

// func NewWaitableCtx() *Context {
// 	return &Context{Waitable: true}
// }

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
