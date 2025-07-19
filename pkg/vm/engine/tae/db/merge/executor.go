// Copyright 2023 Matrix Origin
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

package merge

import (
	"errors"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type MergeTaskExecutor interface {
	ExecuteFor(target catalog.MergeTable, task mergeTask) (success bool)
}

// executor consider resources to decide to merge or not.
type executor struct {
	rt *dbutils.Runtime
	// cnSched *CNMergeScheduler
}

func NewTNMergeExecutor(rt *dbutils.Runtime) *executor {
	return &executor{
		rt: rt,
	}
}

func (e *executor) ExecuteFor(target catalog.MergeTable, task mergeTask) (success bool) {
	entry := target.(catalog.TNMergeTable).TableEntry
	return e.executeFor(entry, task)
}

func (e *executor) executeFor(entry *catalog.TableEntry, task mergeTask) (success bool) {
	kind := task.kind
	level := task.level
	note := task.note
	doneCB := task.doneCB
	if len(task.objs) == 0 {
		return
	}

	objs := make([]*catalog.ObjectEntry, 0, len(task.objs))

	for _, obj := range task.objs {
		objEntry, _ := entry.GetObjectByID(obj.ObjectName().ObjectId(), task.isTombstone)
		if objEntry != nil {
			objs = append(objs, objEntry)
		}
	}

	for _, o := range objs {
		if o.IsTombstone != task.isTombstone {
			panic("merging tombstone and data objects in one merge")
		}
	}

	if kind != taskHostDN {
		logutil.Error("MergeExecutorError",
			zap.String("error", "not supported task host"),
			zap.String("task", task.String()),
			zap.String("table", entry.GetNameDesc()),
		)
		return
	}

	return e.scheduleMergeObjects(slices.Clone(objs), entry, task.isTombstone, level, note, doneCB)
}

func (e *executor) scheduleMergeObjects(
	mObjs []*catalog.ObjectEntry,
	entry *catalog.TableEntry,
	isTombstone bool,
	level int8,
	note string,
	doneCB *taskObserver,
) (success bool) {
	scopes := make([]common.ID, 0, len(mObjs))
	for _, obj := range mObjs {
		scopes = append(scopes, *obj.AsCommonID())
	}
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		task, err := jobs.NewMergeObjectsTask(ctx,
			txn,
			mObjs,
			e.rt,
			common.DefaultMaxOsizeObjBytes,
			isTombstone,
		)
		if err != nil {
			return nil, err
		}
		task.SetLevel(level)
		task.SetTaskSourceNote(note)
		return task, nil
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTaskWithObserver(
		nil,
		tasks.DataCompactionTask,
		scopes,
		factory,
		doneCB,
	)
	if err != nil {
		if !errors.Is(err, tasks.ErrScheduleScopeConflict) {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("schedule-merge-task"),
				common.AnyField("error", err),
				common.AnyField("task", task.Name()),
			)
		}
		return
	}
	entry.Stats.SetLastMergeTime()
	return true
}
