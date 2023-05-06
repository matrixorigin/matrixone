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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type delSegTask struct {
	*tasks.BaseTask
	delSegs []*catalog.SegmentEntry
	txn     txnif.AsyncTxn
}

func NewDelSegTask(ctx *tasks.Context, txn txnif.AsyncTxn, delSegs []*catalog.SegmentEntry) *delSegTask {
	task := &delSegTask{
		delSegs: delSegs,
		txn:     txn,
	}
	task.BaseTask = tasks.NewBaseTask(task, tasks.DataCompactionTask, ctx)
	return task
}

func (t *delSegTask) String() string {
	segs := "DelSeg:"
	for _, seg := range t.delSegs {
		segs = fmt.Sprintf("%s%s,", segs, seg.ID.ToString())
	}
	return segs
}

func (t *delSegTask) Execute() (err error) {
	tdesc := t.String()
	logutil.Info("Mergeblocks delete merged segments [Start]", zap.String("task", tdesc))
	dbId := t.delSegs[0].GetTable().GetDB().ID
	database, err := t.txn.GetDatabaseByID(dbId)
	if err != nil {
		return
	}
	relId := t.delSegs[0].GetTable().ID
	rel, err := database.GetRelationByID(relId)
	if err != nil {
		return
	}
	for _, entry := range t.delSegs {
		if err = rel.SoftDeleteSegment(&entry.ID); err != nil {
			return
		}
	}
	logutil.Info("Mergeblocks delete merged segments [Done]", zap.String("task", tdesc))
	return
}
