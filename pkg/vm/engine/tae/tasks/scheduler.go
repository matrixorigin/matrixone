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
	"github.com/matrixorigin/matrixone/pkg/container/types"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	iops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/ops/base"
	ops "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrDispatcherNotFound = moerr.NewInternalErrorNoCtx("tae sched: dispatcher not found")
	ErrSchedule           = moerr.NewInternalErrorNoCtx("tae sched: cannot schedule")
)

type Scheduler interface {
	Start()
	Stop()
	Schedule(Task) error
}

type TaskScheduler interface {
	Scheduler
	ScheduleTxnTask(ctx *Context, taskType TaskType, factory TxnTaskFactory) (Task, error)
	ScheduleMultiScopedTxnTask(ctx *Context, taskType TaskType, scopes []common.ID, factory TxnTaskFactory) (Task, error)
	ScheduleMultiScopedFn(ctx *Context, taskType TaskType, scopes []common.ID, fn FuncT) (Task, error)
	ScheduleFn(ctx *Context, taskType TaskType, fn func() error) (Task, error)
	ScheduleScopedFn(ctx *Context, taskType TaskType, scope *common.ID, fn func() error) (Task, error)
	Checkpoint(indexes []*wal.Index) error

	AddTransferPage(*model.TransferHashPage) error
	DeleteTransferPage(id *common.ID) error

	GetCheckpointedLSN() uint64
	GetPenddingLSNCnt() uint64
	GetGCTS() types.TS
	GetCheckpointTS() types.TS
}

type BaseScheduler struct {
	ops.OpWorker
	idAlloc     *common.IdAlloctor
	Dispatchers map[TaskType]Dispatcher
}

func NewBaseScheduler(name string) *BaseScheduler {
	scheduler := &BaseScheduler{
		OpWorker:    *ops.NewOpWorker(name),
		idAlloc:     common.NewIdAlloctor(1),
		Dispatchers: make(map[TaskType]Dispatcher),
	}
	scheduler.ExecFunc = scheduler.doDispatch
	return scheduler
}

func (s *BaseScheduler) RegisterDispatcher(t TaskType, dispatcher Dispatcher) {
	s.Dispatchers[t] = dispatcher
}

func (s *BaseScheduler) Schedule(task Task) error {
	// task.AttachID(s.idAlloc())
	if !s.SendOp(task) {
		return ErrSchedule
	}
	return nil
}

func (s *BaseScheduler) doDispatch(op iops.IOp) {
	task := op.(Task)
	dispatcher := s.Dispatchers[task.Type()]
	if dispatcher == nil {
		logutil.Errorf("No dispatcher found for %d[T] Task", task.Type())
		panic(ErrDispatcherNotFound)
	}
	dispatcher.Dispatch(task)
}

func (s *BaseScheduler) Stop() {
	s.OpWorker.Stop()
	for _, d := range s.Dispatchers {
		d.Close()
	}
}
