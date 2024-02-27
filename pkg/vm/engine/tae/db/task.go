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
	"context"

	"github.com/matrixorigin/matrixone/pkg/logutil"
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

func (task *ScheduledTxnTask) Execute(ctx context.Context) (err error) {
	txn, err := task.db.TxnMgr.StartTxn(nil)
	if err != nil {
		return
	}
	txnTask, err := task.factory(nil, txn)
	if err != nil {
		err2 := txn.Rollback(ctx)
		if err2 != nil {
			panic(err2)
		}
		logutil.Warnf("Execute ScheduleTxnTask: %v. Rollbacked", err)
		return
	}
	err = txnTask.OnExec(task.db.Opts.Ctx)
	if err != nil {
		logutil.Warnf("Task[%d] exec error: %v", task.ID(), err)
		err2 := txn.Rollback(ctx)
		if err2 != nil {
			panic(err)
		}
	} else {
		err = txn.Commit(ctx)
		if err != nil {
			return
		}
		err = txn.GetError()
	}
	return
}
