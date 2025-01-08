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
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

// executor consider resources to decide to merge or not.
type executor struct {
	rt      *dbutils.Runtime
	cnSched *CNMergeScheduler
}

func newMergeExecutor(rt *dbutils.Runtime, sched *CNMergeScheduler) *executor {
	return &executor{
		rt:      rt,
		cnSched: sched,
	}
}

func (e *executor) executeFor(entry *catalog.TableEntry, objs []*catalog.ObjectEntry, kind taskHostKind) {
	if len(objs) == 0 {
		return
	}
	// check objects are merging by CNs.
	if e.cnSched.checkOverlapOnCNActive(objs) {
		return
	}

	isTombstone := objs[0].IsTombstone
	for _, o := range objs {
		if o.IsTombstone != isTombstone {
			panic("merging tombstone and data objects in one merge")
		}
	}

	if kind == taskHostDN {
		e.scheduleMergeObjects(slices.Clone(objs), entry, isTombstone)
		return
	}

	// prevent CN OOM
	if len(objs) > 30 {
		objs = objs[:30]
	}

	stats := make([][]byte, 0, len(objs))
	cids := make([]common.ID, 0, len(objs))
	for _, obj := range objs {
		stat := *obj.GetObjectStats()
		stats = append(stats, stat[:])
		cids = append(cids, *obj.AsCommonID())
	}
	// check objects are merging by TN.
	if e.rt.Scheduler != nil && e.rt.Scheduler.CheckAsyncScopes(cids) != nil {
		return
	}
	schema := entry.GetLastestSchema(false)
	cntask := &api.MergeTaskEntry{
		AccountId:         schema.AcInfo.TenantID,
		UserId:            schema.AcInfo.UserID,
		RoleId:            schema.AcInfo.RoleID,
		TblId:             entry.ID,
		DbId:              entry.GetDB().GetID(),
		TableName:         entry.GetLastestSchema(isTombstone).Name,
		DbName:            entry.GetDB().GetName(),
		ToMergeObjs:       stats,
		EstimatedMemUsage: uint64(estimateMergeSize(objs)),
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), 10*time.Second, moerr.CauseCreateCNMerge)
	defer cancel()
	err := e.cnSched.sendMergeTask(ctx, cntask)
	if err != nil {
		logutil.Info("MergeExecutorError",
			common.OperationField("send-cn-task"),
			common.AnyField("task", fmt.Sprintf("table-%d-%s", cntask.TblId, cntask.TableName)),
			common.AnyField("error", err),
		)
		return
	}

	e.cnSched.addActiveObjects(objs)
	entry.Stats.SetLastMergeTime()
}

func (e *executor) scheduleMergeObjects(mObjs []*catalog.ObjectEntry, entry *catalog.TableEntry, isTombstone bool) {
	scopes := make([]common.ID, 0, len(mObjs))
	for _, obj := range mObjs {
		scopes = append(scopes, *obj.AsCommonID())
	}
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return jobs.NewMergeObjectsTask(ctx, txn, mObjs, e.rt, common.DefaultMaxOsizeObjMB*common.Const1MBytes, isTombstone)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
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
}
