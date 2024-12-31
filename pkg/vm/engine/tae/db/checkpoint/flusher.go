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

var ErrFlusherStopped = moerr.NewInternalErrorNoCtx("flusher stopped")

type FlushCfg struct {
	ForceFlushTimeout       time.Duration
	ForceFlushCheckInterval time.Duration
	FlushInterval           time.Duration
	CronPeriod              time.Duration
}

type FlushMutableCfg struct {
	ForceFlushTimeout       time.Duration
	ForceFlushCheckInterval time.Duration
}

type Flusher interface {
	IsAllChangesFlushed(start, end types.TS, doPrint bool) bool
	FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error
	ForceFlush(ctx context.Context, ts types.TS, duration time.Duration) error
	ForceFlushWithInterval(ctx context.Context, ts types.TS, forceDuration, flushInterval time.Duration) (err error)
	ChangeForceFlushTimeout(timeout time.Duration)
	ChangeForceCheckInterval(interval time.Duration)
	GetCfg() FlushCfg
	Restart(opts ...FlusherOption)
	IsStopped() bool
	Start()
	Stop()
}

var _ Flusher = (*flusher)(nil)

type FlushRequest struct {
	force bool
	tree  *logtail.DirtyTreeEntry
}

type FlusherOption func(*flushImpl)

func WithFlusherCronPeriod(period time.Duration) FlusherOption {
	return func(flusher *flushImpl) {
		flusher.cronPeriod = period
	}
}

func WithFlusherInterval(interval time.Duration) FlusherOption {
	return func(flusher *flushImpl) {
		flusher.flushInterval = interval
	}
}

func WithFlusherQueueSize(size int) FlusherOption {
	return func(flusher *flushImpl) {
		flusher.flushQueueSize = size
	}
}

func WithFlusherCfg(cfg FlushCfg) FlusherOption {
	return func(flusher *flushImpl) {
		WithFlusherInterval(cfg.FlushInterval)(flusher)
		WithFlusherCronPeriod(cfg.CronPeriod)(flusher)
		WithFlusherForceTimeout(cfg.ForceFlushTimeout)(flusher)
		WithFlusherForceCheckInterval(cfg.ForceFlushCheckInterval)(flusher)
	}
}

func WithFlusherForceTimeout(timeout time.Duration) FlusherOption {
	return func(flusher *flushImpl) {
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
	return func(flusher *flushImpl) {
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
	rt                 *dbutils.Runtime
	catalogCache       *catalog.Catalog
	sourcer            logtail.Collector
	checkpointSchduler CheckpointScheduler

	impl atomic.Pointer[flushImpl]
}

func NewFlusher(
	rt *dbutils.Runtime,
	checkpointSchduler CheckpointScheduler,
	catalogCache *catalog.Catalog,
	sourcer logtail.Collector,
	opts ...FlusherOption,
) Flusher {
	flusher := &flusher{
		rt:                 rt,
		checkpointSchduler: checkpointSchduler,
		catalogCache:       catalogCache,
		sourcer:            sourcer,
	}
	flusher.impl.Store(newFlusherImpl(rt, checkpointSchduler, catalogCache, sourcer, opts...))
	return flusher
}

func (f *flusher) IsStopped() bool {
	return f.impl.Load() == nil
}

func (f *flusher) IsAllChangesFlushed(start, end types.TS, doPrint bool) bool {
	impl := f.impl.Load()
	if impl == nil {
		return false
	}
	return impl.IsAllChangesFlushed(start, end, doPrint)
}

func (f *flusher) Restart(opts ...FlusherOption) {
	newImpl := newFlusherImpl(
		f.rt,
		f.checkpointSchduler,
		f.catalogCache,
		f.sourcer,
		opts...,
	)

	for {
		oldImpl := f.impl.Load()
		if f.impl.CompareAndSwap(oldImpl, newImpl) {
			if oldImpl != nil {
				oldImpl.Stop()
			}
			break
		}
	}
	newImpl.Start()
}

func (f *flusher) FlushTable(ctx context.Context, dbID, tableID uint64, ts types.TS) error {
	impl := f.impl.Load()
	if impl == nil {
		return ErrFlusherStopped
	}
	return impl.FlushTable(ctx, dbID, tableID, ts)
}

func (f *flusher) ForceFlush(ctx context.Context, ts types.TS, duration time.Duration) error {
	impl := f.impl.Load()
	if impl == nil {
		return ErrFlusherStopped
	}
	return impl.ForceFlush(ctx, ts, duration)
}

func (f *flusher) ForceFlushWithInterval(
	ctx context.Context, ts types.TS, forceDuration, flushInterval time.Duration,
) (err error) {
	impl := f.impl.Load()
	if impl == nil {
		return ErrFlusherStopped
	}
	return impl.ForceFlushWithInterval(ctx, ts, forceDuration, flushInterval)
}

func (f *flusher) ChangeForceFlushTimeout(timeout time.Duration) {
	impl := f.impl.Load()
	if impl == nil {
		logutil.Warn("flusher stopped")
		return
	}
	impl.ChangeForceFlushTimeout(timeout)
}

func (f *flusher) ChangeForceCheckInterval(interval time.Duration) {
	impl := f.impl.Load()
	if impl == nil {
		logutil.Warn("flusher stopped")
		return
	}
	impl.ChangeForceCheckInterval(interval)
}

func (f *flusher) GetCfg() FlushCfg {
	impl := f.impl.Load()
	if impl == nil {
		return FlushCfg{}
	}
	return impl.GetCfg()
}

func (f *flusher) Start() {
	impl := f.impl.Load()
	if impl == nil {
		logutil.Warn("need restart")
		return
	}
	impl.Start()
}

func (f *flusher) Stop() {
	impl := f.impl.Load()
	if impl == nil {
		return
	}
	if f.impl.CompareAndSwap(impl, nil) {
		impl.Stop()
	}
}

type flushImpl struct {
	mutableCfg     atomic.Pointer[FlushMutableCfg]
	flushInterval  time.Duration
	cronPeriod     time.Duration
	flushLag       time.Duration
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

func newFlusherImpl(
	rt *dbutils.Runtime,
	checkpointSchduler CheckpointScheduler,
	catalogCache *catalog.Catalog,
	sourcer logtail.Collector,
	opts ...FlusherOption,
) *flushImpl {
	flusher := &flushImpl{
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

func (flusher *flushImpl) fillDefaults() {
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

func (flusher *flushImpl) triggerJob(ctx context.Context) {
	flusher.sourcer.Run(flusher.flushLag)
	entry := flusher.sourcer.GetAndRefreshMerged()
	if !entry.IsEmpty() {
		request := new(FlushRequest)
		request.tree = entry
		flusher.flushRequestQ.Enqueue(request)
	}
	_, ts := entry.GetTimeRange()
	flusher.checkpointSchduler.TryScheduleCheckpoint(ts, false)
}

func (flusher *flushImpl) onFlushRequest(items ...any) {
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

func (flusher *flushImpl) scheduleFlush(
	entry *logtail.DirtyTreeEntry,
	force bool,
) {
	if entry.IsEmpty() {
		return
	}
	pressure := flusher.collectTableMemUsage(entry)
	flusher.checkFlushConditionAndFire(entry, force, pressure)
}

func (flusher *flushImpl) EstimateTableMemSize(
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

func (flusher *flushImpl) collectTableMemUsage(
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

func (flusher *flushImpl) fireFlushTabletail(
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

func (flusher *flushImpl) checkFlushConditionAndFire(
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

func (flusher *flushImpl) ChangeForceFlushTimeout(timeout time.Duration) {
	WithFlusherForceTimeout(timeout)(flusher)
}

func (flusher *flushImpl) ChangeForceCheckInterval(interval time.Duration) {
	WithFlusherForceCheckInterval(interval)(flusher)
}

func (flusher *flushImpl) ForceFlush(
	ctx context.Context, ts types.TS, forceDuration time.Duration,
) (err error) {
	return flusher.ForceFlushWithInterval(
		ctx, ts, forceDuration, 0,
	)
}

func (flusher *flushImpl) ForceFlushWithInterval(
	ctx context.Context, ts types.TS, forceDuration, flushInterval time.Duration,
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

func (flusher *flushImpl) GetCfg() FlushCfg {
	var cfg FlushCfg
	mCfg := flusher.mutableCfg.Load()
	cfg.ForceFlushTimeout = mCfg.ForceFlushTimeout
	cfg.ForceFlushCheckInterval = mCfg.ForceFlushCheckInterval
	cfg.FlushInterval = flusher.flushInterval
	cfg.CronPeriod = flusher.cronPeriod
	return cfg
}

func (flusher *flushImpl) FlushTable(
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

func (flusher *flushImpl) IsAllChangesFlushed(
	start, end types.TS, doPrint bool,
) bool {
	tree := flusher.sourcer.ScanInRangePruned(start, end)
	tree.GetTree().Compact()
	if doPrint && !tree.IsEmpty() {
		logutil.Info(
			"IsAllChangesFlushed",
			zap.String("dirty-tree", tree.String()),
		)
	}
	return tree.IsEmpty()
}

func (flusher *flushImpl) Start() {
	flusher.onceStart.Do(func() {
		flusher.flushRequestQ.Start()
		flusher.cronTrigger.Start()
		cfg := flusher.mutableCfg.Load()
		logutil.Info(
			"flushImpl-Started",
			zap.Duration("cron-period", flusher.cronPeriod),
			zap.Duration("flush-interval", flusher.flushInterval),
			zap.Duration("flush-lag", flusher.flushLag),
			zap.Duration("force-flush-timeout", cfg.ForceFlushTimeout),
			zap.Duration("force-flush-check-interval", cfg.ForceFlushCheckInterval),
		)
	})
}

func (flusher *flushImpl) Stop() {
	flusher.onceStop.Do(func() {
		flusher.cronTrigger.Stop()
		flusher.flushRequestQ.Stop()
		logutil.Info("flushImpl-Stopped")
	})
}
