package db

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var (
	ErrTaskDuplicated = errors.New("tae task: duplicated task found")
	ErrTaskNotFound   = errors.New("tae task: task not found")
)

type taskTable struct {
	sync.RWMutex
	sharded map[tasks.TaskType]map[uint64]tasks.Task
	count   int64
}

func NewTaskTable() *taskTable {
	table := &taskTable{
		sharded: make(map[tasks.TaskType]map[uint64]tasks.Task),
	}
	table.sharded[tasks.DataCompactionTask] = make(map[uint64]tasks.Task)
	table.sharded[tasks.DataCompactionTask] = make(map[uint64]tasks.Task)
	table.sharded[tasks.CheckpointTask] = make(map[uint64]tasks.Task)
	return table
}

func (table *taskTable) TotalTaskCount() int {
	return int(atomic.LoadInt64(&table.count))
}

func (table *taskTable) TaskCount(taskType tasks.TaskType) int {
	table.RLock()
	defer table.RUnlock()
	shard := table.sharded[taskType]
	return len(shard)
}

func (table *taskTable) ChangeTaskCount(delta int64) {
	v := atomic.AddInt64(&table.count, delta)
	if v < 0 {
		panic("logic error")
	}
}

func (table *taskTable) RegisterTask(task tasks.Task) error {
	table.Lock()
	defer table.Unlock()
	shard := table.sharded[task.Type()]
	existed := shard[task.ID()]
	if existed != nil {
		return ErrTaskDuplicated
	}
	shard[task.ID()] = task
	task.AddObserver(table)
	return nil
}

func (table *taskTable) UnregisterTask(task tasks.Task) error {
	table.Lock()
	defer table.Unlock()
	shard := table.sharded[task.Type()]
	existed := shard[task.ID()]
	if existed == nil {
		return ErrTaskDuplicated
	}
	delete(shard, task.ID())
	return nil
}

func (table *taskTable) OnExecDone(v interface{}) {
	task := v.(tasks.Task)
	err := table.UnregisterTask(task)
	if err != nil {
		logutil.Warnf("UnregisterTask %d:%d: %v", task.Type(), task.ID(), err)
	} else {
		logutil.Warnf("Task %d:%d Done [%s]", task.Type(), task.ID(), time.Duration(task.GetExecutTime()))
	}
}

type taskScheduler struct {
	*tasks.BaseScheduler
	db        *DB
	taskTable *taskTable
}

func newTaskScheduler(db *DB, asyncWorkers int, ioWorkers int) *taskScheduler {
	if asyncWorkers < 0 || asyncWorkers > 100 {
		panic(fmt.Sprintf("bad param: %d txn workers", asyncWorkers))
	}
	if ioWorkers < 0 || ioWorkers > 100 {
		panic(fmt.Sprintf("bad param: %d io workers", ioWorkers))
	}
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler("taskScheduler"),
		db:            db,
	}
	jobDispatcher := newAsyncJobDispatcher()
	jobHandler := tasks.NewPoolHandler(asyncWorkers)
	jobHandler.Start()
	jobDispatcher.RegisterHandler(tasks.DataCompactionTask, jobHandler)

	ckpDispatcher := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < 4; i++ {
		handler := tasks.NewSingleWorkerHandler(fmt.Sprintf("[ckpworker-%d]", i))
		ckpDispatcher.AddHandle(handler)
		handler.Start()
	}

	ioDispatcher := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < ioWorkers; i++ {
		handler := tasks.NewSingleWorkerHandler(fmt.Sprintf("[ioworker-%d]", i))
		ioDispatcher.AddHandle(handler)
		handler.Start()
	}

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

func (s *taskScheduler) ScheduleTxnTask(ctx *tasks.Context, taskType tasks.TaskType, factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, taskType, nil, factory)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) ScheduleMultiScopedTxnTask(ctx *tasks.Context, taskType tasks.TaskType, scopes []common.ID, factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, taskType, scopes, factory)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) Checkpoint(indexes []*wal.Index) (err error) {
	entry, err := s.db.Wal.Checkpoint(indexes)
	if err != nil {
		return err
	}
	s.db.CKPDriver.EnqueueCheckpointEntry(entry)
	return
}

func (s *taskScheduler) GetPenddingCnt() uint64 {
	return s.db.Wal.GetPenddingCnt()
}

func (s *taskScheduler) GetCheckpointed() uint64 {
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
	if taskType == tasks.DataCompactionTask {
		dispatcher := s.Dispatchers[task.Type()].(*asyncJobDispatcher)
		return dispatcher.TryDispatch(task)
	}
	return s.BaseScheduler.Schedule(task)
}
