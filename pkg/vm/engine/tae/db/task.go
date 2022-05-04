package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

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
	txn := task.db.StartTxn(nil)
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

type CheckpointWalTask struct {
	*tasks.BaseTask
	db      *DB
	indexes []*wal.Index
}

func NewCheckpointWalTask(ctx *tasks.Context, db *DB, indexes []*wal.Index) (task *CheckpointWalTask) {
	task = &CheckpointWalTask{
		db:      db,
		indexes: indexes,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.CheckpointWalTask, ctx)
	return
}

func (task *CheckpointWalTask) Execute() (err error) {
	entry, err := task.db.Wal.Checkpoint(task.indexes)
	if err != nil {
		return err
	}
	task.db.CKPDriver.EnqueueCheckpointEntry(entry)
	return
}
