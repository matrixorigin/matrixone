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
	"bytes"
	"context"
	"fmt"
	"path"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"

	"github.com/BurntSushi/toml"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc/v3"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/merge"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	WALDir = "wal"
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

func Open(ctx context.Context, dirname string, opts *options.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)

	logutil.Info(
		"open-tae",
		common.OperationField("Start"),
		common.OperandField("open"),
	)
	totalTime := time.Now()

	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
		logutil.Info(
			"open-tae", common.OperationField("End"),
			common.OperandField("open"),
			common.AnyField("cost", time.Since(totalTime)),
			common.AnyField("err", err),
		)
	}()

	opts = opts.FillDefaults(dirname)
	fillRuntimeOptions(opts)

	wbuf := &bytes.Buffer{}
	werr := toml.NewEncoder(wbuf).Encode(opts)
	logutil.Info(
		"open-tae",
		common.OperationField("Config"),
		common.AnyField("toml", wbuf.String()),
		common.ErrorField(werr),
	)
	serviceDir := path.Join(dirname, "data")
	if opts.Fs == nil {
		// TODO:fileservice needs to be passed in as a parameter
		opts.Fs = objectio.TmpNewFileservice(ctx, path.Join(dirname, "data"))
	}
	if opts.LocalFs == nil {
		opts.LocalFs = objectio.TmpNewFileservice(ctx, path.Join(dirname, "data"))
	}

	db = &DB{
		Dir:          dirname,
		Opts:         opts,
		Closed:       new(atomic.Value),
		usageMemo:    logtail.NewTNUsageMemo(nil),
		CNMergeSched: merge.NewTaskServiceGetter(opts.TaskServiceGetter),
	}
	fs := objectio.NewObjectFS(opts.Fs, serviceDir)
	localFs := objectio.NewObjectFS(opts.LocalFs, serviceDir)
	transferTable, err := model.NewTransferTable[*model.TransferHashPage](ctx, opts.LocalFs)
	if err != nil {
		panic(fmt.Sprintf("open-tae: model.NewTransferTable failed, %s", err))
	}

	switch opts.LogStoreT {
	case options.LogstoreBatchStore:
		db.Wal = wal.NewDriverWithBatchStore(opts.Ctx, dirname, WALDir, nil)
	case options.LogstoreLogservice:
		db.Wal = wal.NewDriverWithLogservice(opts.Ctx, opts.Lc)
	}
	scheduler := newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	db.Runtime = dbutils.NewRuntime(
		dbutils.WithRuntimeTransferTable(transferTable),
		dbutils.WithRuntimeObjectFS(fs),
		dbutils.WithRuntimeLocalFS(localFs),
		dbutils.WithRuntimeSmallPool(dbutils.MakeDefaultSmallPool("small-vector-pool")),
		dbutils.WithRuntimeTransientPool(dbutils.MakeDefaultTransientPool("trasient-vector-pool")),
		dbutils.WithRuntimeScheduler(scheduler),
		dbutils.WithRuntimeOptions(db.Opts),
	)

	dataFactory := tables.NewDataFactory(
		db.Runtime, db.Dir,
	)
	catalog.DefaultTableDataFactory = dataFactory.MakeTableFactory()
	if db.Catalog, err = catalog.OpenCatalog(db.usageMemo); err != nil {
		return
	}
	db.usageMemo.C = db.Catalog

	// Init and start txn manager
	txnStoreFactory := txnimpl.TxnStoreFactory(
		opts.Ctx,
		db.Catalog,
		db.Wal,
		db.Runtime,
		dataFactory,
		opts.MaxMessageSize,
	)
	txnFactory := txnimpl.TxnFactory(db.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory, db.Opts.Clock)
	db.LogtailMgr = logtail.NewManager(
		db.Runtime,
		int(db.Opts.LogtailCfg.PageSize),
		db.TxnMgr.Now,
	)
	db.Runtime.Now = db.TxnMgr.Now
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)
	db.TxnMgr.Start(opts.Ctx)
	db.LogtailMgr.Start()
	db.BGCheckpointRunner = checkpoint.NewRunner(
		opts.Ctx,
		db.Runtime,
		db.Catalog,
		logtail.NewDirtyCollector(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor)),
		db.Wal,
		checkpoint.WithFlushInterval(opts.CheckpointCfg.FlushInterval),
		checkpoint.WithCollectInterval(opts.CheckpointCfg.ScanInterval),
		checkpoint.WithMinCount(int(opts.CheckpointCfg.MinCount)),
		checkpoint.WithCheckpointBlockRows(opts.CheckpointCfg.BlockRows),
		checkpoint.WithCheckpointSize(opts.CheckpointCfg.Size),
		checkpoint.WithMinIncrementalInterval(opts.CheckpointCfg.IncrementalInterval),
		checkpoint.WithGlobalMinCount(int(opts.CheckpointCfg.GlobalMinCount)),
		checkpoint.WithGlobalVersionInterval(opts.CheckpointCfg.GlobalVersionInterval),
		checkpoint.WithReserveWALEntryCount(opts.CheckpointCfg.ReservedWALEntryCount))

	now := time.Now()
	ckpReplayer := db.BGCheckpointRunner.Replay(dataFactory)
	if err = ckpReplayer.ReadCkpFiles(); err != nil {
		panic(err)
	}

	// 1. replay three tables objectlist
	checkpointed, ckpLSN, valid, err := ckpReplayer.ReplayThreeTablesObjectlist()
	if err != nil {
		panic(err)
	}

	var txn txnif.AsyncTxn
	{
		// create a txn manually
		txnIdAlloc := common.NewTxnIDAllocator()
		store := txnStoreFactory()
		txn = txnFactory(db.TxnMgr, store, txnIdAlloc.Alloc(), checkpointed, types.TS{})
		store.BindTxn(txn)
	}
	// 2. replay all table Entries
	ckpReplayer.ReplayCatalog(txn)

	// 3. replay other tables' objectlist
	if err = ckpReplayer.ReplayObjectlist(); err != nil {
		panic(err)
	}
	logutil.Info(
		"open-tae",
		common.OperationField("replay"),
		common.OperandField("checkpoints"),
		common.AnyField("cost", time.Since(now)),
		common.AnyField("checkpointed", checkpointed.ToString()),
	)

	now = time.Now()
	db.Replay(dataFactory, checkpointed, ckpLSN, valid)
	db.Catalog.ReplayTableRows()

	// checkObjectState(db)
	logutil.Info(
		"open-tae",
		common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("cost", time.Since(now)),
	)

	db.DBLocker, dbLocker = dbLocker, nil

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	db.MergeScheduler = merge.NewScheduler(db.Runtime, db.CNMergeSched)
	scanner.RegisterOp(db.MergeScheduler)
	db.Wal.Start()
	db.BGCheckpointRunner.Start()

	db.BGScanner = w.NewHeartBeater(
		opts.CheckpointCfg.ScanInterval,
		scanner)
	db.BGScanner.Start()
	// TODO: WithGCInterval requires configuration parameters
	gc2.SetDeleteTimeout(opts.GCCfg.GCDeleteTimeout)
	gc2.SetDeleteBatchSize(opts.GCCfg.GCDeleteBatchSize)
	cleaner := gc2.NewCheckpointCleaner(opts.Ctx,
		opts.SID, fs, db.BGCheckpointRunner,
		gc2.WithCanGCCacheSize(opts.GCCfg.CacheSize),
		gc2.WithMaxMergeCheckpointCount(opts.GCCfg.GCMergeCount),
		gc2.WithEstimateRows(opts.GCCfg.GCestimateRows),
		gc2.WithGCProbility(opts.GCCfg.GCProbility),
		gc2.WithCheckOption(opts.GCCfg.CheckGC),
		gc2.WithGCCheckpointOption(!opts.CheckpointCfg.DisableGCCheckpoint))
	cleaner.AddChecker(
		func(item any) bool {
			checkpoint := item.(*checkpoint.CheckpointEntry)
			ts := types.BuildTS(time.Now().UTC().UnixNano()-int64(opts.GCCfg.GCTTL), 0)
			endTS := checkpoint.GetEnd()
			return !endTS.GE(&ts)
		}, cmd_util.CheckerKeyTTL)
	db.DiskCleaner = gc2.NewDiskCleaner(cleaner)
	db.DiskCleaner.Start()
	// Init gc manager at last
	// TODO: clean-try-gc requires configuration parameters
	cronJobs := []func(*gc.Manager){
		gc.WithCronJob(
			"clean-transfer-table",
			opts.CheckpointCfg.TransferInterval,
			func(_ context.Context) (err error) {
				db.Runtime.PoolUsageReport()
				db.Runtime.TransferDelsMap.Prune(opts.TransferTableTTL)
				transferTable.RunTTL()
				return
			}),

		gc.WithCronJob(
			"disk-gc",
			opts.GCCfg.ScanGCInterval,
			func(ctx context.Context) (err error) {
				db.DiskCleaner.GC(ctx)
				return
			}),
		gc.WithCronJob(
			"checkpoint-gc",
			opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) error {
				if opts.CheckpointCfg.DisableGCCheckpoint {
					return nil
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetCheckpointGCWaterMark()
				if gcWaterMark == nil {
					return nil
				}
				return db.BGCheckpointRunner.GCByTS(ctx, *gcWaterMark)
			}),
		gc.WithCronJob(
			"catalog-gc",
			opts.CatalogCfg.GCInterval,
			func(ctx context.Context) error {
				if opts.CatalogCfg.DisableGC {
					return nil
				}
				gcWaterMark := db.DiskCleaner.GetCleaner().GetScanWaterMark()
				if gcWaterMark == nil {
					return nil
				}
				db.Catalog.GCByTS(ctx, gcWaterMark.GetEnd())
				return nil
			}),
		gc.WithCronJob(
			"logtail-gc",
			opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) error {
				logutil.Info(db.Runtime.ExportLogtailStats())
				ckp := db.BGCheckpointRunner.MaxIncrementalCheckpoint()
				if ckp != nil {
					// use previous end to gc logtail
					ts := types.BuildTS(ckp.GetStart().Physical(), 0) // GetStart is previous + 1, reset it here
					db.LogtailMgr.GCByTS(ctx, ts)
				}
				return nil
			},
		),
		gc.WithCronJob(
			"prune-lockmerge",
			options.DefaultLockMergePruneInterval,
			func(ctx context.Context) error {
				db.Runtime.LockMergeService.Prune()
				return nil
			},
		),
	}
	if opts.CheckpointCfg.MetadataCheckInterval != 0 {
		cronJobs = append(cronJobs,
			gc.WithCronJob(
				"metadata-check",
				opts.CheckpointCfg.MetadataCheckInterval,
				func(ctx context.Context) error {
					db.Catalog.CheckMetadata()
					return nil
				}))
	}
	db.GCManager = gc.NewManager(cronJobs...)

	db.GCManager.Start()

	go TaeMetricsTask(ctx)

	// For debug or test
	//fmt.Println(db.Catalog.SimplePPString(common.PPL3))
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

func TaeMetricsTask(ctx context.Context) {
	logutil.Info("tae metrics task started")
	defer logutil.Info("tae metrics task exit")

	timer := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			mpoolAllocatorSubTask()
		}
	}

}

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
