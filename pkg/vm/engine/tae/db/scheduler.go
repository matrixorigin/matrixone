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

package db

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

var (
	ErrTaskDuplicated = moerr.NewInternalErrorNoCtx("tae task: duplicated task found")
	ErrTaskNotFound   = moerr.NewInternalErrorNoCtx("tae task: task not found")
)

type taskScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newTaskScheduler(db *DB, asyncWorkers int, ioWorkers int) *taskScheduler {
	if asyncWorkers < 0 {
		panic(fmt.Sprintf("bad param: %d txn workers", asyncWorkers))
	}
	if ioWorkers < 0 {
		panic(fmt.Sprintf("bad param: %d io workers", ioWorkers))
	}
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler(db.Opts.Ctx, "taskScheduler"),
		db:            db,
	}
	jobDispatcher := newAsyncJobDispatcher()
	jobHandler := tasks.NewPoolHandler(db.Opts.Ctx, asyncWorkers)
	jobHandler.Start()
	jobDispatcher.RegisterHandler(tasks.DataCompactionTask, jobHandler)
	// jobDispatcher.RegisterHandler(tasks.GCTask, jobHandler)
	gcHandler := tasks.NewSingleWorkerHandler(db.Opts.Ctx, "gc")
	gcHandler.Start()
	jobDispatcher.RegisterHandler(tasks.GCTask, gcHandler)

	ckpDispatcher := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < 4; i++ {
		handler := tasks.NewSingleWorkerHandler(db.Opts.Ctx, fmt.Sprintf("[ckpworker-%d]", i))
		ckpDispatcher.AddHandle(handler)
		handler.Start()
	}

	ioDispatcher := tasks.NewBaseScopedDispatcher(nil)
	for i := 0; i < ioWorkers; i++ {
		handler := tasks.NewSingleWorkerHandler(db.Opts.Ctx, fmt.Sprintf("[ioworker-%d]", i))
		ioDispatcher.AddHandle(handler)
		handler.Start()
	}

	s.RegisterDispatcher(tasks.GCTask, jobDispatcher)
	s.RegisterDispatcher(tasks.DataCompactionTask, jobDispatcher)
	s.RegisterDispatcher(tasks.IOTask, ioDispatcher)
	s.RegisterDispatcher(tasks.CheckpointTask, ckpDispatcher)
	s.Start()
	return s
}

func (s *taskScheduler) Stop() {
	s.BaseScheduler.Stop()
	logutil.Info("TaskScheduler Stopped")
}

func (s *taskScheduler) ScheduleTxnTask(
	ctx *tasks.Context,
	taskType tasks.TaskType,
	factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, taskType, nil, factory)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) ScheduleMultiScopedTxnTask(
	ctx *tasks.Context,
	taskType tasks.TaskType,
	scopes []common.ID,
	factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, taskType, scopes, factory)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) ScheduleMultiScopedFn(
	ctx *tasks.Context,
	taskType tasks.TaskType,
	scopes []common.ID,
	fn tasks.FuncT) (task tasks.Task, err error) {
	task = tasks.NewMultiScopedFnTask(ctx, taskType, scopes, fn)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) GetPenddingLSNCnt() uint64 {
	return s.db.Wal.GetPenddingCnt()
}

func (s *taskScheduler) GetCheckpointedLSN() uint64 {
	return s.db.Wal.GetCheckpointed()
}

func (s *taskScheduler) ScheduleFn(ctx *tasks.Context, taskType tasks.TaskType, fn func() error) (task tasks.Task, err error) {
	task = tasks.NewFnTask(ctx, taskType, fn)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) ScheduleScopedFn(ctx *tasks.Context, taskType tasks.TaskType, scope *common.ID, fn func() error) (task tasks.Task, err error) {
	task = tasks.NewScopedFnTask(ctx, taskType, scope, fn)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) Schedule(task tasks.Task) (err error) {
	taskType := task.Type()
	// if taskType == tasks.DataCompactionTask || taskType == tasks.GCTask {
	if taskType == tasks.DataCompactionTask {
		dispatcher := s.Dispatchers[task.Type()].(*asyncJobDispatcher)
		return dispatcher.TryDispatch(task)
	}
	return s.BaseScheduler.Schedule(task)
}
