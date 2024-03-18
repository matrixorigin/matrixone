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
	"bytes"
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

type activeTaskStats map[uint64]struct {
	blk      int
	estBytes int
}

// MergeExecutor consider resources to decide to merge or not.
type MergeExecutor struct {
	tableName           string
	rt                  *dbutils.Runtime
	memAvail            int
	memSpare            int // 15% of total memory
	cpuPercent          float64
	activeMergeBlkCount int32
	activeEstimateBytes int64
	taskConsume         struct {
		sync.Mutex
		m activeTaskStats
	}
}

func NewMergeExecutor(rt *dbutils.Runtime) *MergeExecutor {
	return &MergeExecutor{
		rt: rt,
	}
}

func (e *MergeExecutor) RefreshMemInfo() {
	if stats, err := mem.VirtualMemory(); err == nil {
		e.memAvail = int(stats.Available)
		if e.memSpare == 0 {
			e.memSpare = int(float32(stats.Total) * 0.15)
		}
	}
	if percents, err := cpu.Percent(0, false); err == nil {
		e.cpuPercent = percents[0]
	}
}

func (e *MergeExecutor) PrintStats() {
	cnt := atomic.LoadInt32(&e.activeMergeBlkCount)
	if cnt == 0 && e.MemAvailBytes() > 512*common.Const1MBytes {
		return
	}

	logutil.Infof(
		"Mergeblocks avail mem: %v(%v reserved), active mergeing size: %v, active merging blk cnt: %d",
		common.HumanReadableBytes(e.memAvail),
		common.HumanReadableBytes(e.memSpare),
		common.HumanReadableBytes(int(atomic.LoadInt64(&e.activeEstimateBytes))), cnt,
	)
}

func (e *MergeExecutor) AddActiveTask(taskId uint64, blkn, esize int) {
	atomic.AddInt64(&e.activeEstimateBytes, int64(esize))
	atomic.AddInt32(&e.activeMergeBlkCount, int32(blkn))
	e.taskConsume.Lock()
	if e.taskConsume.m == nil {
		e.taskConsume.m = make(activeTaskStats)
	}
	e.taskConsume.m[taskId] = struct {
		blk      int
		estBytes int
	}{blkn, esize}
	e.taskConsume.Unlock()
}

func (e *MergeExecutor) OnExecDone(v any) {
	task := v.(tasks.MScopedTask)

	e.taskConsume.Lock()
	stat := e.taskConsume.m[task.ID()]
	delete(e.taskConsume.m, task.ID())
	e.taskConsume.Unlock()

	atomic.AddInt32(&e.activeMergeBlkCount, -int32(stat.blk))
	atomic.AddInt64(&e.activeEstimateBytes, -int64(stat.estBytes))
}

func (e *MergeExecutor) ManuallyExecute(entry *catalog.TableEntry, objs []*catalog.ObjectEntry) error {
	mem := e.MemAvailBytes()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	osize, esize, _ := estimateMergeConsume(objs)
	if esize > 2*mem/3 {
		return moerr.NewInternalErrorNoCtx("no enough mem to merge. osize %d, mem %d", osize, mem)
	}

	mergedBlks, mobjs := expandObjectList(objs)
	blkCnt := len(mergedBlks)

	scopes := make([]common.ID, blkCnt)
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, tasks.DataCompactionTask, scopes, factory)
	if err == tasks.ErrScheduleScopeConflict {
		return moerr.NewInternalErrorNoCtx("conflict with running merging jobs, try later")
	} else if err != nil {
		return moerr.NewInternalErrorNoCtx("schedule error: %v", err)
	}
	logMergeTask(entry.GetLastestSchemaLocked().Name, task.ID(), mobjs, len(mergedBlks), osize, esize)
	if err = task.WaitDone(context.Background()); err != nil {
		return moerr.NewInternalErrorNoCtx("merge error: %v", err)
	}
	return nil
}

func (e *MergeExecutor) ExecuteFor(entry *catalog.TableEntry, policy Policy) {
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema().Name)

	objectList := policy.Revise(int64(e.cpuPercent), int64(e.MemAvailBytes()))
	mergedBlks, mobjs := expandObjectList(objectList)
	blkCnt := len(mergedBlks)

	if blkCnt == 0 {
		return
	}

	scopes := make([]common.ID, blkCnt)
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeObjectsTask(ctx, txn, mobjs, e.rt)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Infof("[Mergeblocks] Schedule error info=%v", err)
		}
		return
	}

	osize, esize, _ := estimateMergeConsume(mobjs)
	e.AddActiveTask(task.ID(), blkCnt, esize)
	task.AddObserver(e)
	entry.Stats.AddMerge(osize, len(mobjs), blkCnt)
	logMergeTask(e.tableName, task.ID(), mobjs, blkCnt, osize, esize)
}

func (e *MergeExecutor) MemAvailBytes() int {
	merging := int(atomic.LoadInt64(&e.activeEstimateBytes))
	avail := e.memAvail - e.memSpare - merging
	if avail < 0 {
		avail = 0
	}
	return avail
}

func expandObjectList(objs []*catalog.ObjectEntry) (
	mblks []*catalog.BlockEntry, mobjs []*catalog.ObjectEntry,
) {
	if len(objs) < 2 {
		return
	}
	mobjs = objs
	mblks = make([]*catalog.BlockEntry, 0, len(objs)*constMergeMinBlks)
	for _, obj := range objs {
		blkit := obj.MakeBlockIt(true)
		for ; blkit.Valid(); blkit.Next() {
			entry := blkit.Get().GetPayload()
			if !entry.IsActive() {
				continue
			}
			entry.RLock()
			if entry.IsCommitted() &&
				catalog.ActiveWithNoTxnFilter(&entry.BaseEntryImpl) {
				mblks = append(mblks, entry)
			}
			entry.RUnlock()
		}
	}
	return
}

func logMergeTask(name string, taskId uint64, merges []*catalog.ObjectEntry, blkn, osize, esize int) {
	v2.TaskMergeScheduledByCounter.Inc()
	v2.TaskMergedBlocksCounter.Add(float64(blkn))
	v2.TasKMergedSizeCounter.Add(float64(osize))

	rows := 0
	infoBuf := &bytes.Buffer{}
	for _, obj := range merges {
		r := obj.GetRemainingRows()
		rows += r
		infoBuf.WriteString(fmt.Sprintf(" %d(%s)", r, common.ShortObjId(obj.ID)))
	}
	logutil.Infof(
		"[Mergeblocks] Scheduled %v [t%d|on%d,bn%d|%s,%s], merged(%v): %s", name,
		taskId, len(merges), blkn,
		common.HumanReadableBytes(osize), common.HumanReadableBytes(esize),
		rows,
		infoBuf.String(),
	)
}
