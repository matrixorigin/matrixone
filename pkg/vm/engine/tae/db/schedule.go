package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ioScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newIOScheduler(db *DB) *ioScheduler {
	s := &ioScheduler{
		BaseScheduler: tasks.NewBaseScheduler("ioScheduler"),
		db:            db,
	}
	dispatcher := tasks.NewBaseDispatcher()
	handler := tasks.NewPoolHandler(8)
	handler.Start()

	dispatcher.RegisterHandler(tasks.TxnTask, handler)
	s.RegisterDispatcher(tasks.TxnTask, dispatcher)
	s.Start()
	return s
}

type taskScheduler struct {
	*tasks.BaseScheduler
	db *DB
}

func newTaskScheduler(db *DB) *taskScheduler {
	s := &taskScheduler{
		BaseScheduler: tasks.NewBaseScheduler("taskScheduler"),
		db:            db,
	}
	dispatcher := tasks.NewBaseDispatcher()
	txnHandler := tasks.NewPoolHandler(1)
	txnHandler.Start()
	// ioHandlers := tasks.NewPoolHandler(1)
	// ioHandlers.Start()

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
