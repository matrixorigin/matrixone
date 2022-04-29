package db

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ioScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newIOScheduler(db *DB, workers int) *ioScheduler {
	if workers < 0 || workers > 100 {
		panic(fmt.Sprintf("bad param: %d workers", workers))
	}
	s := &ioScheduler{
		BaseScheduler: tasks.NewBaseScheduler("ioScheduler"),
		db:            db,
	}
	dispatcher := tasks.NewBaseScopedDispatcher(tasks.DefaultScopeSharder)
	for i := 0; i < workers; i++ {
		handler := tasks.NewSingleWorkerHandler(fmt.Sprintf("[ioworker-%d]", i))
		dispatcher.AddHandle(handler)
		handler.Start()
	}

	s.RegisterDispatcher(tasks.IOTask, dispatcher)
	s.Start()
	return s
}

type taskScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newTaskScheduler(db *DB, txnWorkers int) *taskScheduler {
	if txnWorkers < 0 || txnWorkers > 100 {
		panic(fmt.Sprintf("bad param: %d txn workers", txnWorkers))
	}
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler("taskScheduler"),
		db:            db,
	}
	dispatcher := tasks.NewBaseDispatcher()
	txnHandler := tasks.NewPoolHandler(txnWorkers)
	txnHandler.Start()

	dispatcher.RegisterHandler(tasks.TxnTask, txnHandler)
	dispatcher.RegisterHandler(tasks.CompactBlockTask, txnHandler)

	s.RegisterDispatcher(tasks.TxnTask, dispatcher)
	s.RegisterDispatcher(tasks.CompactBlockTask, dispatcher)
	s.Start()
	return s
}

func (s *taskScheduler) ScheduleTxnTask(ctx *tasks.Context, factory tasks.TxnTaskFactory) (task tasks.Task, err error) {
	task = NewScheduledTxnTask(ctx, s.db, factory)
	err = s.Schedule(task)
	return
}
