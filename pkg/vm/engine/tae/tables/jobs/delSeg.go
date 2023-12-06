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

package jobs

import (
	"context"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type delObjTask struct {
	*tasks.BaseTask
	delObjs []*catalog.ObjectEntry
	txn     txnif.AsyncTxn
}

func NewDelObjTask(ctx *tasks.Context, txn txnif.AsyncTxn, delObjs []*catalog.ObjectEntry) *delObjTask {
	task := &delObjTask{
		delObjs: delObjs,
		txn:     txn,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return task
}

func (t *delObjTask) String() string {
	objs := "DelObj:"
	for _, obj := range t.delObjs {
		objs = fmt.Sprintf("%s%s,", objs, obj.ID.String())
	}
	return objs
}

func (t *delObjTask) Execute(ctx context.Context) (err error) {
	tdesc := t.String()
	logutil.Info("Mergeblocks delete merged Objects [Start]", zap.String("task", tdesc))
	dbId := t.delObjs[0].GetTable().GetDB().ID
	database, err := t.txn.GetDatabaseByID(dbId)
	if err != nil {
		return
	}
	relId := t.delObjs[0].GetTable().ID
	rel, err := database.GetRelationByID(relId)
	if err != nil {
		return
	}
	for _, entry := range t.delObjs {
		if err = rel.SoftDeleteObject(&entry.ID); err != nil {
			return
		}
	}
	logutil.Info("Mergeblocks delete merged Objects [Done]", zap.String("task", tdesc))
	return
}
