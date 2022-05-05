package db

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type taskScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newTaskScheduler(db *DB, txnWorkers int, ioWorkers int) *taskScheduler {
	if txnWorkers < 0 || txnWorkers > 100 {
		panic(fmt.Sprintf("bad param: %d txn workers", txnWorkers))
	}
	if ioWorkers < 0 || ioWorkers > 100 {
		panic(fmt.Sprintf("bad param: %d io workers", ioWorkers))
	}
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler("taskScheduler"),
		db:            db,
	}
	dispatcher := tasks.NewBaseDispatcher()
	txnHandler := tasks.NewPoolHandler(txnWorkers)
	txnHandler.Start()
	genericHandler := tasks.NewPoolHandler(4)
	genericHandler.Start()

	dispatcher.RegisterHandler(tasks.TxnTask, txnHandler)
	dispatcher.RegisterHandler(tasks.CompactBlockTask, txnHandler)
	dispatcher.RegisterHandler(tasks.CheckpointWalTask, genericHandler)

	dispatcher2 := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < 4; i++ {
		handler := tasks.NewSingleWorkerHandler(fmt.Sprintf("[ckpworker-%d]", i))
		dispatcher2.AddHandle(handler)
		handler.Start()
	}

	ioDispatcher := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < ioWorkers; i++ {
		handler := tasks.NewSingleWorkerHandler(fmt.Sprintf("[ioworker-%d]", i))
		ioDispatcher.AddHandle(handler)
		handler.Start()
	}

	s.RegisterDispatcher(tasks.TxnTask, dispatcher)
	s.RegisterDispatcher(tasks.CompactBlockTask, dispatcher)
	s.RegisterDispatcher(tasks.CheckpointWalTask, dispatcher)
	s.RegisterDispatcher(tasks.IOTask, ioDispatcher)
	s.RegisterDispatcher(tasks.CheckpointDataTask, dispatcher2)
	s.RegisterDispatcher(tasks.CheckpointCatalogTask, dispatcher2)
	s.Start()
	return s
}

func (s *taskScheduler) ScheduleTxnTask(ctx *tasks.Context, factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, factory)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) Checkpoint(indexes []*wal.Index) (err error) {
	task := NewCheckpointWalTask(nil, s.db, indexes)
	err = s.Schedule(task)
	return
}

func (s *taskScheduler) GetCheckpointed() uint64 {
	return s.db.Wal.GetCheckpointed()
}

func (s *taskScheduler) ScheduleFn(ctx *tasks.Context, taskType tasks.TaskType, fn func() error) (task tasks.Task, err error) {
	task = tasks.NewFnTask(ctx, taskType, fn)
	err = s.db.Scheduler.Schedule(task)
	return
}

func (s *taskScheduler) ScheduleScopedFn(ctx *tasks.Context, taskType tasks.TaskType, scope *common.ID, fn func() error) (task tasks.Task, err error) {
	task = tasks.NewScopedFnTask(ctx, taskType, scope, fn)
	err = s.db.Scheduler.Schedule(task)
	return
}
