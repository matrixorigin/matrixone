package db

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

type ScheduledTxnTask struct {
	*tasks.BaseTask
	db      *DB
	factory tasks.TxnTaskFactory
}

func NewScheduledTxnTask(ctx *tasks.Context, db *DB, factory tasks.TxnTaskFactory) (task *ScheduledTxnTask) {
	task = &ScheduledTxnTask{
		db:      db,
		factory: factory,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.TxnTask, ctx)
	return
}

func (task *ScheduledTxnTask) Execute() (err error) {
	txn, err := task.db.StartTxn(nil)
	if err != nil {
		return
	}
	ctx := &tasks.Context{Waitable: false}
	txnTask, err := task.factory(ctx, txn)
	if err != nil {
		txn.Rollback()
		return
	}
	err = txnTask.OnExec()
	if err != nil {
		err = txn.Rollback()
	} else {
		err = txn.Commit()
	}
	return
}
