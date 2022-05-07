package db

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ScheduledTxnTask struct {
	*tasks.BaseTask
	db      *DB
	factory tasks.TxnTaskFactory
	scopes  []common.ID
}

func NewScheduledTxnTask(ctx *tasks.Context, db *DB, taskType tasks.TaskType, scopes []common.ID, factory tasks.TxnTaskFactory) (task *ScheduledTxnTask) {
	task = &ScheduledTxnTask{
		db:      db,
		factory: factory,
		scopes:  scopes,
	}
	task.BaseTask = tasks.NewBaseTask(task, taskType, ctx)
	return
}

func (task *ScheduledTxnTask) Scopes() []common.ID { return task.scopes }
func (task *ScheduledTxnTask) Scope() *common.ID {
	if task.scopes == nil || len(task.scopes) == 0 {
		return nil
	}
	return &task.scopes[0]
}

func (task *ScheduledTxnTask) Execute() (err error) {
	txn := task.db.StartTxn(nil)
	txnTask, err := task.factory(nil, txn)
	if err != nil {
		txn.Rollback()
		return
	}
	err = txnTask.OnExec()
	if err != nil {
		txn.Rollback()
	} else {
		txn.Commit()
		err = txn.GetError()
	}
	return
}
