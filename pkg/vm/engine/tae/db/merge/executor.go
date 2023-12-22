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

func (e *MergeExecutor) ManuallyExecute(entry *catalog.TableEntry, segs []*catalog.SegmentEntry) error {
	mem := e.MemAvailBytes()
	if mem > constMaxMemCap {
		mem = constMaxMemCap
	}
	osize, esize := estimateMergeConsume(segs)
	if esize > 2*mem/3 {
		return moerr.NewInternalErrorNoCtx("no enough mem to merge. osize %d, mem %d", osize, mem)
	}

	mergedBlks, msegs := expandObjectList(segs)
	blkCnt := len(mergedBlks)

	scopes := make([]common.ID, blkCnt)
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeBlocksTask(ctx, txn, mergedBlks, msegs, nil, e.rt)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(tasks.WaitableCtx, tasks.DataCompactionTask, scopes, factory)
	if err == tasks.ErrScheduleScopeConflict {
		return moerr.NewInternalErrorNoCtx("conflict with running merging jobs, try later")
	} else if err != nil {
		return moerr.NewInternalErrorNoCtx("schedule error: %v", err)
	}
	logMergeTask(entry.GetLastestSchema().Name, task.ID(), nil, msegs, len(mergedBlks), osize, esize)
	if err = task.WaitDone(context.Background()); err != nil {
		return moerr.NewInternalErrorNoCtx("merge error: %v", err)
	}
	return nil
}

func (e *MergeExecutor) ExecuteFor(entry *catalog.TableEntry, delSegs []*catalog.SegmentEntry, policy Policy) {
	e.tableName = fmt.Sprintf("%v-%v", entry.ID, entry.GetLastestSchema().Name)
	hasDelSeg := len(delSegs) > 0

	originalDelCnt := len(delSegs)

	hasMergeObjects := false

	objectList := policy.Revise(0, int64(e.MemAvailBytes()))
	mergedBlks, msegs := expandObjectList(objectList)
	blkCnt := len(mergedBlks)
	if blkCnt > 0 {
		delSegs = append(delSegs, msegs...)
		hasMergeObjects = true
	}

	if !hasDelSeg && !hasMergeObjects {
		return
	}

	segScopes := make([]common.ID, len(delSegs))
	for i, s := range delSegs {
		segScopes[i] = *s.AsCommonID()
	}

	// remove stale segments only
	if hasDelSeg && !hasMergeObjects {
		factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
			return jobs.NewDelSegTask(ctx, txn, delSegs), nil
		}
		if _, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, segScopes, factory); err != nil {
			logutil.Infof("[Mergeblocks] Schedule del seg errinfo=%v", err)
		} else {
			logutil.Infof("[Mergeblocks] Scheduled Object Del| %d-%s del %d segs", entry.ID, e.tableName, len(delSegs))
		}
		return
	}

	// remove stale segments and mrege objects
	scopes := make([]common.ID, blkCnt)
	for i, blk := range mergedBlks {
		scopes[i] = *blk.AsCommonID()
	}
	scopes = append(scopes, segScopes...)

	factory := func(ctx *tasks.Context, txn txnif.AsyncTxn) (tasks.Task, error) {
		return jobs.NewMergeBlocksTask(ctx, txn, mergedBlks, delSegs, nil, e.rt)
	}
	task, err := e.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.DataCompactionTask, scopes, factory)
	if err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Infof("[Mergeblocks] Schedule error info=%v", err)
		}
		return
	}

	osize, esize := estimateMergeConsume(msegs)
	e.AddActiveTask(task.ID(), blkCnt, esize)
	task.AddObserver(e)
	entry.Stats.AddMerge(osize, len(msegs), blkCnt)
	var delPrint []*catalog.SegmentEntry
	if delSegs != nil {
		delPrint = delSegs[:originalDelCnt]
	}
	logMergeTask(e.tableName, task.ID(), delPrint, msegs, blkCnt, osize, esize)
}

func (e *MergeExecutor) MemAvailBytes() int {
	merging := int(atomic.LoadInt64(&e.activeEstimateBytes))
	avail := e.memAvail - e.memSpare - merging
	if avail < 0 {
		avail = 0
	}
	return avail
}

func expandObjectList(segs []*catalog.SegmentEntry) (
	mblks []*catalog.BlockEntry, msegs []*catalog.SegmentEntry,
) {
	if len(segs) < 2 {
		return
	}
	msegs = segs
	mblks = make([]*catalog.BlockEntry, 0, len(segs)*constMergeMinBlks)
	for _, seg := range segs {
		blkit := seg.MakeBlockIt(true)
		for ; blkit.Valid(); blkit.Next() {
			entry := blkit.Get().GetPayload()
			if !entry.IsActive() {
				continue
			}
			entry.RLock()
			if entry.IsCommitted() &&
				catalog.ActiveWithNoTxnFilter(entry.BaseEntryImpl) {
				mblks = append(mblks, entry)
			}
			entry.RUnlock()
		}
	}
	return
}

func logMergeTask(name string, taskId uint64, dels, merges []*catalog.SegmentEntry, blkn, osize, esize int) {
	v2.TaskMergeScheduledByCounter.Inc()
	v2.TaskMergedBlocksCounter.Add(float64(blkn))
	v2.TasKMergedSizeCounter.Add(float64(osize))

	rows := 0
	infoBuf := &bytes.Buffer{}
	for _, seg := range merges {
		r := seg.Stat.GetRemainingRows()
		rows += r
		infoBuf.WriteString(fmt.Sprintf(" %d(%s)", r, common.ShortSegId(seg.ID)))
	}
	if len(dels) > 0 {
		infoBuf.WriteString(" | del:")
		for _, seg := range dels {
			infoBuf.WriteString(fmt.Sprintf(" %s", common.ShortSegId(seg.ID)))
		}
	}
	logutil.Infof(
		"[Mergeblocks] Scheduled %v [t%d|on%d,bn%d|%s,%s], merged(%v): %s", name,
		taskId, len(merges), blkn,
		common.HumanReadableBytes(osize), common.HumanReadableBytes(esize),
		rows,
		infoBuf.String(),
	)
}
