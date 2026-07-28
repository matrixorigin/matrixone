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
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
)

const (
	WALDir = "wal"

	Phase_Open = "open-tae"
)

func fillRuntimeOptions(opts *options.Options) {
	common.RuntimeCNMergeMemControl.Store(opts.MergeCfg.CNMergeMemControlHint)
	common.RuntimeMinCNMergeSize.Store(opts.MergeCfg.CNTakeOverExceed)
	common.RuntimeCNTakeOverAll.Store(opts.MergeCfg.CNTakeOverAll)
	common.RuntimeOverallFlushMemCap.Store(opts.CheckpointCfg.OverallFlushMemControl)
	if opts.IsStandalone {
		common.IsStandaloneBoost.Store(true)
	}
	if opts.MergeCfg.CNStandaloneTake {
		common.ShouldStandaloneCNTakeOver.Store(true)
	}
	if opts.MergeCfg.DisableZMBasedMerge {
		common.RuntimeDisableZMBasedMerge.Store(true)
	}
}

func Open(
	ctx context.Context,
	dirname string,
	opts *options.Options,
	dbOpts ...DBOption,
) (db *DB, err error) {
	opts = opts.FillDefaults(dirname)
	fillRuntimeOptions(opts)

	var (
		dbLocker      io.Closer
		startTime     = time.Now()
		rollbackSteps stepFuncs
		logger        = logutil.Info
	)

	logutil.Info(Phase_Open+"-start", zap.String("dirname", dirname))

	defer func() {
		if err == nil && dbLocker != nil {
			db.DBLocker, dbLocker = dbLocker, nil
		}
		if dbLocker != nil {
			dbLocker.Close()
		}
		if err != nil {
			if err2 := rollbackSteps.Apply("open-tae", true, 1); err2 != nil {
				panic(fmt.Sprintf("open-tae: rollback failed, %s", err2))
			}
			logger = logutil.Error
		}
		logger(
			Phase_Open+"-end",
			zap.Duration("cost", time.Since(startTime)),
			zap.String("dirname", dirname),
			zap.Error(err),
		)
	}()

	db = &DB{
		Dir:       dirname,
		Opts:      opts,
		Closed:    new(atomic.Value),
		usageMemo: logtail.NewTNUsageMemo(nil),
	}
	for _, opt := range dbOpts {
		opt(db)
	}

	if db.IsWriteMode() {
		if dbLocker, err = createDBLock(dirname); err != nil {
			return
		}
	}

	transferTable, err := model.NewTransferTable[*model.TransferHashPage](ctx, opts.LocalFs)
	if err != nil {
		return
	}

	logutil.Info(Phase_Open + "-open-wal-start")
	if opts.WalClientFactory != nil {
		db.Wal = wal.NewLogserviceHandle(opts.WalClientFactory)
	} else {
		db.Wal = wal.NewLocalHandle(dirname, WALDir, nil)
	}
	rollbackSteps.Add("rollback open wal", func() error {
		return db.Wal.Close()
	})
	logutil.Info(Phase_Open + "-open-wal-end")

	scheduler := newTaskScheduler(
		db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers,
	)
	rollbackSteps.Add("rollback open scheduler", func() error {
		scheduler.Stop()
		return nil
	})

	db.Runtime = dbutils.NewRuntime(
		dbutils.WithRuntimeTransferTable(transferTable),
		dbutils.WithRuntimeObjectFS(opts.Fs),
		dbutils.WithRuntimeLocalFS(opts.LocalFs),
		dbutils.WithRuntimeTmpFS(opts.TmpFs),
		dbutils.WithRuntimeSmallPool(dbutils.MakeDefaultSmallPool("small-vector-pool")),
		dbutils.WithRuntimeTransientPool(dbutils.MakeDefaultTransientPool("trasient-vector-pool")),
		dbutils.WithRuntimeScheduler(scheduler),
		dbutils.WithRuntimeOptions(db.Opts),
	)

	logutil.Info(Phase_Open + "-open-catalog-start")
	dataFactory := tables.NewDataFactory(
		db.Runtime, db.Dir,
	)
	if db.Catalog, err = catalog.OpenCatalog(db.usageMemo, dataFactory); err != nil {
		logutil.Error(Phase_Open+"-open-catalog-error", zap.Error(err))
		return
	}
	db.usageMemo.C = db.Catalog
	rollbackSteps.Add("rollback open catalog", func() error {
		db.Catalog.Close()
		return nil
	})
	logutil.Info(Phase_Open + "-open-catalog-end")

	db.Controller = NewController(db)
	if err = db.Controller.AssembleDB(ctx); err != nil {
		return
	}
	db.Controller.Start()

	return
}

// TODO: remove it
// func checkObjectState(db *DB) {
// 	p := &catalog.LoopProcessor{}
// 	p.ObjectFn = func(oe *catalog.ObjectEntry) error {
// 		if oe.IsAppendable() == oe.IsSorted() {
// 			panic(fmt.Sprintf("logic err %v", oe.ID.String()))
// 		}
// 		return nil
// 	}
// 	db.Catalog.RecurLoop(p)
// }

func mpoolAllocatorSubTask() {
	v2.MemTAEDefaultAllocatorGauge.Set(float64(common.DefaultAllocator.CurrNB()))
	v2.MemTAEDefaultHighWaterMarkGauge.Set(float64(common.DefaultAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMutableAllocatorGauge.Set(float64(common.MutMemAllocator.CurrNB()))
	v2.MemTAEMutableHighWaterMarkGauge.Set(float64(common.MutMemAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAESmallAllocatorGauge.Set(float64(common.SmallAllocator.CurrNB()))
	v2.MemTAESmallHighWaterMarkGauge.Set(float64(common.SmallAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEVectorPoolDefaultAllocatorGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().CurrNB()))
	v2.MemTAEVectorPoolDefaultHighWaterMarkGauge.Set(float64(containers.GetDefaultVectorPoolALLocator().Stats().HighWaterMark.Load()))

	v2.MemTAELogtailAllocatorGauge.Set(float64(common.LogtailAllocator.CurrNB()))
	v2.MemTAELogtailHighWaterMarkGauge.Set(float64(common.LogtailAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAECheckpointAllocatorGauge.Set(float64(common.CheckpointAllocator.CurrNB()))
	v2.MemTAECheckpointHighWaterMarkGauge.Set(float64(common.CheckpointAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEMergeAllocatorGauge.Set(float64(common.MergeAllocator.CurrNB()))
	v2.MemTAEMergeHighWaterMarkGauge.Set(float64(common.MergeAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEWorkSpaceAllocatorGauge.Set(float64(common.WorkspaceAllocator.CurrNB()))
	v2.MemTAEWorkSpaceHighWaterMarkGauge.Set(float64(common.WorkspaceAllocator.Stats().HighWaterMark.Load()))

	v2.MemTAEDebugAllocatorGauge.Set(float64(common.DebugAllocator.CurrNB()))
	v2.MemTAEDebugHighWaterMarkGauge.Set(float64(common.DebugAllocator.Stats().HighWaterMark.Load()))

}
