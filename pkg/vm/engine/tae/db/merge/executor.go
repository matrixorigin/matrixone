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
	tableName string
	rt        *dbutils.Runtime
	cnSched   CNMergeScheduler
}

func newMergeExecutor(rt *dbutils.Runtime, sched CNMergeScheduler) *executor {
	return &executor{
		rt:      rt,
		cnSched: sched,
	}
}

func (e *executor) executeFor(entry *catalog.TableEntry, mobjs []*catalog.ObjectEntry, kind TaskHostKind) {
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema(false).Name)

	if ActiveCNObj.CheckOverlapOnCNActive(mobjs) {
		return
	}

	if kind == TaskHostCN {
		_, esize := estimateMergeConsume(mobjs)
		stats := make([][]byte, 0, len(mobjs))
		cids := make([]common.ID, 0, len(mobjs))
		for _, obj := range mobjs {
			stat := *obj.GetObjectStats()
			stats = append(stats, stat[:])
			cids = append(cids, *obj.AsCommonID())
		}
		if e.rt.Scheduler.CheckAsyncScopes(cids) != nil {
			return
		}
		schema := entry.GetLastestSchema(false)
		cntask := &api.MergeTaskEntry{
			AccountId:         schema.AcInfo.TenantID,
			UserId:            schema.AcInfo.UserID,
			RoleId:            schema.AcInfo.RoleID,
			TblId:             entry.ID,
			DbId:              entry.GetDB().GetID(),
			TableName:         entry.GetLastestSchema(false).Name,
			DbName:            entry.GetDB().GetName(),
			ToMergeObjs:       stats,
			EstimatedMemUsage: uint64(esize),
		}
		if err := e.cnSched.SendMergeTask(context.TODO(), cntask); err == nil {
			ActiveCNObj.AddActiveCNObj(mobjs)
		} else {
			logutil.Info(
				"MergeExecutorError",
				common.OperationField("send-cn-task"),
				common.AnyField("task", fmt.Sprintf("table-%d-%s", cntask.TblId, cntask.TableName)),
				common.AnyField("error", err),
			)
			return
		}
		entry.Stats.SetLastMergeTime()
	} else {
		nTombstone, nData := 0, 0
		for _, obj := range mobjs {
			if obj.IsTombstone {
				nTombstone += 1
			} else {
				nData += 1
			}
		}

		objs := make([]*catalog.ObjectEntry, 0, nData)
		objScopes := make([]common.ID, 0, nData)
		tombstones := make([]*catalog.ObjectEntry, 0, nTombstone)
		tombstoneScopes := make([]common.ID, 0, nTombstone)
		for _, obj := range mobjs {
			if obj.IsTombstone {
				tombstones = append(tombstones, obj)
				tombstoneScopes = append(tombstoneScopes, *obj.AsCommonID())
			} else {
				objs = append(objs, obj)
				objScopes = append(objScopes, *obj.AsCommonID())
			}
		}

		if len(objs) > 0 {
			e.scheduleMergeObjects(objScopes, objs, entry, false)
		}
		if len(tombstones) > 1 {
			e.scheduleMergeObjects(tombstoneScopes, tombstones, entry, true)
		}
	}
}

func (e *executor) scheduleMergeObjects(scopes []common.ID, mobjs []*catalog.ObjectEntry, entry *catalog.TableEntry, isTombstone bool) {
	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		txn.GetMemo().IsFlushOrMerge = true
		return jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt, common.DefaultMaxOsizeObjMB*common.Const1MBytes, isTombstone)
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
