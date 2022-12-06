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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
)

var (
	ErrBadTaskRequestPara    = moerr.NewInternalErrorNoCtx("tae scheduler: bad task request parameters")
	ErrScheduleScopeConflict = moerr.NewInternalErrorNoCtx("tae scheduler: scope conflict")
)

type FuncT = func() error

type TaskType uint16

var taskIdAlloctor *common.IdAlloctor

const (
	NoopTask TaskType = iota
	MockTask
	CustomizedTask

	DataCompactionTask
	CheckpointTask
	GCTask
	IOTask
)

var taskNames = map[TaskType]string{
	NoopTask:           "Noop",
	MockTask:           "Mock",
	DataCompactionTask: "Compaction",
	CheckpointTask:     "Checkpoint",
	GCTask:             "GC",
	IOTask:             "IO",
}

func RegisterType(t TaskType, name string) {
	_, ok := taskNames[t]
	if ok {
		panic(moerr.NewInternalErrorNoCtx("duplicate task type: %d, %s", t, name))
	}
	taskNames[t] = name
}

func TaskName(t TaskType) string {
	return taskNames[t]
}

func init() {
	taskIdAlloctor = common.NewIdAlloctor(1)
}

type TxnTaskFactory = func(ctx *Context, txn txnif.AsyncTxn) (Task, error)

func NextTaskId() uint64 {
	return taskIdAlloctor.Alloc()
}

type Task interface {
	base.IOp
	ID() uint64
	Type() TaskType
	Cancel() error
	Name() string
}

type ScopedTask interface {
	Task
	Scope() *common.ID
}

type MScopedTask interface {
	Task
	Scopes() []common.ID
}

var DefaultScopeSharder = func(scope *common.ID) int {
	if scope == nil {
		return 0
	}
	return int(scope.TableID + scope.SegmentID)
}

func IsSameScope(left, right *common.ID) bool {
	return left.TableID == right.TableID && left.SegmentID == right.SegmentID
}

type FnTask struct {
	*BaseTask
	Fn FuncT
}

func NewFnTask(ctx *Context, taskType TaskType, fn FuncT) *FnTask {
	task := &FnTask{
		Fn: fn,
	}
	task.BaseTask = NewBaseTask(task, taskType, ctx)
	return task
}

func (task *FnTask) Execute() error {
	return task.Fn()
}

type ScopedFnTask struct {
	*FnTask
	scope *common.ID
}

func NewScopedFnTask(ctx *Context, taskType TaskType, scope *common.ID, fn FuncT) *ScopedFnTask {
	task := &ScopedFnTask{
		FnTask: new(FnTask),
		scope:  scope,
	}
	task.Fn = fn
	task.BaseTask = NewBaseTask(task, taskType, ctx)
	return task
}

func (task *ScopedFnTask) Scope() *common.ID { return task.scope }

type MultiScopedFnTask struct {
	*FnTask
	scopes []common.ID
}

func NewMultiScopedFnTask(ctx *Context, taskType TaskType, scopes []common.ID, fn FuncT) *MultiScopedFnTask {
	task := &MultiScopedFnTask{
		FnTask: new(FnTask),
		scopes: scopes,
	}
	task.Fn = fn
	task.BaseTask = NewBaseTask(task, taskType, ctx)
	return task
}

func (task *MultiScopedFnTask) Scopes() []common.ID {
	return task.scopes
}
