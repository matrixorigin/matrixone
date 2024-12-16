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

package checkpoint

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"go.uber.org/zap"
)

type FlushMutableCfg struct {
	ForceFlushTimeout       time.Duration
	ForceFlushCheckInterval time.Duration
}

type Flusher interface {
	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
	ForceFlush(ts types.TS, ctx context.Context, duration time.Duration) error
	ForceFlushWithInterval(ts types.TS, ctx context.Context, forceDuration, flushInterval time.Duration) (err error)
	ChangeForceFlushTimeout(timeout time.Duration)
	ChangeForceCheckInterval(interval time.Duration)
	Start()
	Stop()
}

type FlushRequest struct {
	force bool
	tree  *logtail.DirtyTreeEntry
}

type FlusherOption func(*flusher)

func WithFlusherCronPeriod(period time.Duration) FlusherOption {
	return func(flusher *flusher) {
		flusher.cronPeriod = period
	}
}

func WithFlusherInterval(interval time.Duration) FlusherOption {
	return func(flusher *flusher) {
		flusher.flushInterval = interval
	}
}

func WithFlusherQueueSize(size int) FlusherOption {
	return func(flusher *flusher) {
		flusher.flushQueueSize = size
	}
}

func WithFlusherForceTimeout(timeout time.Duration) FlusherOption {
	return func(flusher *flusher) {
		for {
			var newCfg FlushMutableCfg
			oldCfg := flusher.mutableCfg.Load()
			if oldCfg == nil {
				newCfg.ForceFlushTimeout = timeout
				if flusher.mutableCfg.CompareAndSwap(oldCfg, &newCfg) {
					break
				}
			} else {
				newCfg = *oldCfg
				newCfg.ForceFlushTimeout = timeout
				if flusher.mutableCfg.CompareAndSwap(oldCfg, &newCfg) {
					break
				}
			}
		}
	}
}

func WithFlusherForceCheckInterval(interval time.Duration) FlusherOption {
	return func(flusher *flusher) {
		for {
			var newCfg FlushMutableCfg
			oldCfg := flusher.mutableCfg.Load()
			if oldCfg == nil {
				newCfg.ForceFlushCheckInterval = interval
				if flusher.mutableCfg.CompareAndSwap(oldCfg, &newCfg) {
					break
				}
			} else {
				newCfg = *oldCfg
				newCfg.ForceFlushCheckInterval = interval
				if flusher.mutableCfg.CompareAndSwap(oldCfg, &newCfg) {
					break
				}
			}
		}
	}
}

type flusher struct {
	mutableCfg     atomic.Pointer[FlushMutableCfg]
	flushInterval  time.Duration
	flushLag       time.Duration
	cronPeriod     time.Duration
	flushQueueSize int

	sourcer            logtail.Collector
	catalogCache       *catalog.Catalog
	checkpointSchduler CheckpointScheduler
	rt                 *dbutils.Runtime

	cronTrigger   *tasks.CancelableJob
	flushRequestQ sm.Queue

	objMemSizeList []tableAndSize

	onceStart sync.Once
	onceStop  sync.Once
}

func NewFlusher(
	rt *dbutils.Runtime,
	checkpointSchduler CheckpointScheduler,
	catalogCache *catalog.Catalog,
	sourcer logtail.Collector,
	opts ...FlusherOption,
) *flusher {
	flusher := &flusher{
		rt:                 rt,
		checkpointSchduler: checkpointSchduler,
		catalogCache:       catalogCache,
		sourcer:            sourcer,
	}
	for _, opt := range opts {
		opt(flusher)
	}

	flusher.fillDefaults()

	flusher.flushRequestQ = sm.NewSafeQueue(
		flusher.flushQueueSize,
		100,
		flusher.onFlushRequest,
	)

	flusher.cronTrigger = tasks.NewCancelableCronJob(
		"flusher",
		flusher.cronPeriod,
		flusher.triggerJob,
		true,
		1,
	)
	return flusher
}

func (flusher *flusher) fillDefaults() {
	if flusher.cronPeriod <= 0 {
		flusher.cronPeriod = time.Second * 5
	}
	cfg := flusher.mutableCfg.Load()
	if cfg == nil {
		cfg = new(FlushMutableCfg)
		flusher.mutableCfg.Store(cfg)
	}
	if cfg.ForceFlushTimeout <= 0 {
		cfg.ForceFlushTimeout = time.Second * 90
	}
	if cfg.ForceFlushCheckInterval <= 0 {
		cfg.ForceFlushCheckInterval = time.Millisecond * 500
	}
	if flusher.flushInterval <= 0 {
		flusher.flushInterval = time.Minute
	}
	// TODO: what is flushLag? Here just refactoring the original code.
	if flusher.flushLag <= 0 {
		if flusher.flushInterval < time.Second {
			flusher.flushLag = 0
		} else {
			flusher.flushLag = time.Second * 3
		}
	}

	if flusher.flushQueueSize <= 0 {
		flusher.flushQueueSize = 1000
	}
}

func (flusher *flusher) triggerJob(ctx context.Context) {
	flusher.sourcer.Run(flusher.flushLag)
	entry := flusher.sourcer.GetAndRefreshMerged()
	if !entry.IsEmpty() {
		request := new(FlushRequest)
		request.tree = entry
		flusher.flushRequestQ.Enqueue(request)
	}
	_, endTS := entry.GetTimeRange()
	flusher.checkpointSchduler.TryScheduleCheckpoint(endTS)
}

func (flusher *flusher) onFlushRequest(items ...any) {
	fromCrons := logtail.NewEmptyDirtyTreeEntry()
	fromForce := logtail.NewEmptyDirtyTreeEntry()
	for _, item := range items {
		e := item.(*FlushRequest)
		if e.force {
			fromForce.Merge(e.tree)
		} else {
			fromCrons.Merge(e.tree)
		}
	}
	flusher.scheduleFlush(fromForce, true)
	flusher.scheduleFlush(fromCrons, false)
}

func (flusher *flusher) scheduleFlush(
	entry *logtail.DirtyTreeEntry,
	force bool,
) {
	if entry.IsEmpty() {
		return
	}
	pressure := flusher.collectTableMemUsage(entry)
	flusher.checkFlushConditionAndFire(entry, force, pressure)
}

func (flusher *flusher) EstimateTableMemSize(
	table *catalog.TableEntry,
	tree *model.TableTree,
) (asize int, dsize int) {
	for _, obj := range tree.Objs {
		object, err := table.GetObjectByID(obj.ID, false)
		if err != nil {
			panic(err)
		}
		a, _ := object.GetObjectData().EstimateMemSize()
		asize += a
	}
	for _, obj := range tree.Tombstones {
		object, err := table.GetObjectByID(obj.ID, true)
		if err != nil {
			panic(err)
		}
		a, _ := object.GetObjectData().EstimateMemSize()
		dsize += a
	}
	return
}

func (flusher *flusher) collectTableMemUsage(
	entry *logtail.DirtyTreeEntry,
) (memPressureRate float64) {
	// reuse the list
	flusher.objMemSizeList = flusher.objMemSizeList[:0]
	sizevisitor := new(model.BaseTreeVisitor)
	var totalSize int
	sizevisitor.TableFn = func(did, tid uint64) error {
		db, err := flusher.catalogCache.GetDatabaseByID(did)
		if err != nil {
			panic(err)
		}
		table, err := db.GetTableEntryByID(tid)
		if err != nil {
			panic(err)
		}
		table.Stats.Init(flusher.flushInterval)
		dirtyTree := entry.GetTree().GetTable(tid)
		asize, dsize := flusher.EstimateTableMemSize(table, dirtyTree)
		totalSize += asize + dsize
		flusher.objMemSizeList = append(flusher.objMemSizeList, tableAndSize{table, asize, dsize})
		return moerr.GetOkStopCurrRecur()
	}
	if err := entry.GetTree().Visit(sizevisitor); err != nil {
		panic(err)
	}

	slices.SortFunc(flusher.objMemSizeList, func(a, b tableAndSize) int {
		return b.asize - a.asize // sort by asize desc
	})

	pressure := float64(totalSize) / float64(common.RuntimeOverallFlushMemCap.Load())
	if pressure > 1.0 {
		pressure = 1.0
	}
	logutil.Info(
		"Flush-CollectMemUsage",
		zap.Float64("pressure", pressure),
		zap.String("size", common.HumanReadableBytes(totalSize)),
	)

	return pressure
}

func (flusher *flusher) fireFlushTabletail(
	table *catalog.TableEntry,
	tree *model.TableTree,
) error {
	tableDesc := fmt.Sprintf("%d-%s", table.ID, table.GetLastestSchemaLocked(false).Name)
	metas := make([]*catalog.ObjectEntry, 0, 10)
	for _, obj := range tree.Objs {
		object, err := table.GetObjectByID(obj.ID, false)
		if err != nil {
			panic(err)
		}
		metas = append(metas, object)
	}
	tombstoneMetas := make([]*catalog.ObjectEntry, 0, 10)
	for _, obj := range tree.Tombstones {
		object, err := table.GetObjectByID(obj.ID, true)
		if err != nil {
			panic(err)
		}
		tombstoneMetas = append(tombstoneMetas, object)
	}

	// freeze all append
	scopes := make([]common.ID, 0, len(metas))
	for _, meta := range metas {
		if !meta.GetObjectData().PrepareCompact() {
			logutil.Info("[FlushTabletail] data prepareCompact false", zap.String("table", tableDesc), zap.String("obj", meta.ID().String()))
			return moerr.GetOkExpectedEOB()
		}
		scopes = append(scopes, *meta.AsCommonID())
	}
	for _, meta := range tombstoneMetas {
		if !meta.GetObjectData().PrepareCompact() {
			logutil.Info("[FlushTabletail] tomb prepareCompact false", zap.String("table", tableDesc), zap.String("obj", meta.ID().String()))
			return moerr.GetOkExpectedEOB()
		}
		scopes = append(scopes, *meta.AsCommonID())
	}

	factory := jobs.FlushTableTailTaskFactory(metas, tombstoneMetas, flusher.rt)
	if _, err := flusher.rt.Scheduler.ScheduleMultiScopedTxnTask(nil, tasks.FlushTableTailTask, scopes, factory); err != nil {
		if err != tasks.ErrScheduleScopeConflict {
			logutil.Error("[FlushTabletail] Sched Failure", zap.String("table", tableDesc), zap.Error(err))
		}
		return moerr.GetOkExpectedEOB()
	}
	return nil
}

func (flusher *flusher) checkFlushConditionAndFire(
	entry *logtail.DirtyTreeEntry, force bool, pressure float64,
) {
	count := 0
	for _, ticket := range flusher.objMemSizeList {
		table, asize, dsize := ticket.tbl, ticket.asize, ticket.dsize
		dirtyTree := entry.GetTree().GetTable(table.ID)

		if force {
			logutil.Info(
				"Flush-Force",
				zap.Uint64("id", table.ID),
				zap.String("name", table.GetLastestSchemaLocked(false).Name),
			)
			if err := flusher.fireFlushTabletail(table, dirtyTree); err == nil {
				table.Stats.ResetDeadline(flusher.flushInterval)
			}
			continue
		}

		flushReady := func() bool {
			if !table.IsActive() {
				count++
				if pressure < 0.5 || count < 200 {
					// if the table has been dropped, flush it immediately if
					// resources are available.
					// count is used to avoid too many flushes in one round
					return true
				}
				return false
			}
			// time to flush
			if table.Stats.GetFlushDeadline().Before(time.Now()) {
				return true
			}
			// this table is too large, flush it
			if asize+dsize > int(common.FlushMemCapacity.Load()) {
				return true
			}
			// unflushed data is too large, flush it
			if asize > common.Const1MBytes && rand.Float64() < pressure {
				return true
			}
			return false
		}

		ready := flushReady()

		if asize+dsize > 2*1000*1024 {
			logutil.Info(
				"Flush-Tabletail",
				zap.String("name", table.GetLastestSchemaLocked(false).Name),
				zap.String("size", common.HumanReadableBytes(asize+dsize)),
				zap.String("dsize", common.HumanReadableBytes(dsize)),
				zap.Duration("count-down", time.Until(table.Stats.GetFlushDeadline())),
				zap.Bool("ready", ready),
			)
		}

		if ready {
			if err := flusher.fireFlushTabletail(table, dirtyTree); err == nil {
				table.Stats.ResetDeadline(flusher.flushInterval)
			}
		}
	}
}

func (flusher *flusher) ChangeForceFlushTimeout(timeout time.Duration) {
	WithFlusherForceTimeout(timeout)(flusher)
}

func (flusher *flusher) ChangeForceCheckInterval(interval time.Duration) {
	WithFlusherForceCheckInterval(interval)(flusher)
}

func (flusher *flusher) ForceFlush(
	ts types.TS, ctx context.Context, forceDuration time.Duration,
) (err error) {
	return flusher.ForceFlushWithInterval(
		ts, ctx, forceDuration, 0,
	)
}

func (flusher *flusher) ForceFlushWithInterval(
	ts types.TS, ctx context.Context, forceDuration, flushInterval time.Duration,
) (err error) {
	makeRequest := func() *FlushRequest {
		tree := flusher.sourcer.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		if tree.IsEmpty() {
			return nil
		}
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, tree.GetTree())
		request := new(FlushRequest)
		request.tree = entry
		request.force = true
		// logutil.Infof("try flush %v",tree.String())
		return request
	}
	op := func() (ok bool, err error) {
		request := makeRequest()
		if request == nil {
			return true, nil
		}
		if _, err = flusher.flushRequestQ.Enqueue(request); err != nil {
			return true, nil
		}
		return false, nil
	}

	cfg := flusher.mutableCfg.Load()

	if forceDuration <= 0 {
		forceDuration = cfg.ForceFlushTimeout
	}
	if flushInterval <= 0 {
		flushInterval = cfg.ForceFlushCheckInterval
	}
	if err = common.RetryWithIntervalAndTimeout(
		op,
		forceDuration,
		flushInterval,
		false,
	); err != nil {
		return moerr.NewInternalErrorf(ctx, "force flush failed: %v", err)
	}
	_, sarg, _ := fault.TriggerFault(objectio.FJ_FlushTimeout)
	if sarg != "" {
		err = moerr.NewInternalError(ctx, sarg)
	}
	return

}

func (flusher *flusher) FlushTable(
	ctx context.Context, dbID, tableID uint64, ts types.TS,
) (err error) {
	iarg, sarg, flush := fault.TriggerFault("flush_table_error")
	if flush && (iarg == 0 || rand.Int63n(iarg) == 0) {
		return moerr.NewInternalError(ctx, sarg)
	}
	makeRequest := func() *FlushRequest {
		tree := flusher.sourcer.ScanInRangePruned(types.TS{}, ts)
		tree.GetTree().Compact()
		tableTree := tree.GetTree().GetTable(tableID)
		if tableTree == nil {
			return nil
		}
		nTree := model.NewTree()
		nTree.Tables[tableID] = tableTree
		entry := logtail.NewDirtyTreeEntry(types.TS{}, ts, nTree)
		request := new(FlushRequest)
		request.tree = entry
		request.force = true
		return request
	}

	op := func() (ok bool, err error) {
		request := makeRequest()
		if request == nil {
			return true, nil
		}
		if _, err = flusher.flushRequestQ.Enqueue(request); err != nil {
			// TODO: why (true,nil)???
			return true, nil
		}
		return false, nil
	}

	cfg := flusher.mutableCfg.Load()

	err = common.RetryWithIntervalAndTimeout(
		op,
		cfg.ForceFlushTimeout,
		cfg.ForceFlushCheckInterval,
		true,
	)
	if moerr.IsMoErrCode(err, moerr.ErrInternal) || moerr.IsMoErrCode(err, moerr.OkExpectedEOB) {
		logutil.Warnf("Flush %d-%d :%v", dbID, tableID, err)
		return nil
	}
	return
}

func (flusher *flusher) Start() {
	flusher.onceStart.Do(func() {
		flusher.flushRequestQ.Start()
		flusher.cronTrigger.Start()
		cfg := flusher.mutableCfg.Load()
		logutil.Info(
			"flusher-Started",
			zap.Duration("cron-period", flusher.cronPeriod),
			zap.Duration("flush-interval", flusher.flushInterval),
			zap.Duration("flush-lag", flusher.flushLag),
			zap.Duration("force-flush-timeout", cfg.ForceFlushTimeout),
			zap.Duration("force-flush-check-interval", cfg.ForceFlushCheckInterval),
		)
	})
}

func (flusher *flusher) Stop() {
	flusher.onceStop.Do(func() {
		flusher.cronTrigger.Stop()
		flusher.flushRequestQ.Stop()
		logutil.Info("flusher-Stopped")
	})
}
