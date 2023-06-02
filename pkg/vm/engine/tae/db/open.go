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
	"path"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	gc2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/gc"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/gc"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
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

func Open(dirname string, opts *options.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)

	logutil.Info("open-tae", common.OperationField("Start"),
		common.OperandField("open"))
	totalTime := time.Now()

	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
		logutil.Info("open-tae", common.OperationField("End"),
			common.OperandField("open"),
			common.AnyField("cost", time.Since(totalTime)),
			common.AnyField("err", err))
	}()

	opts = opts.FillDefaults(dirname)

	indexCache := model.NewSimpleLRU(int64(opts.CacheCfg.IndexCapacity))

	serviceDir := path.Join(dirname, "data")
	if opts.Fs == nil {
		// TODO:fileservice needs to be passed in as a parameter
		opts.Fs = objectio.TmpNewFileservice(path.Join(dirname, "data"))
	}
	fs := objectio.NewObjectFS(opts.Fs, serviceDir)

	db = &DB{
		Dir:        dirname,
		Opts:       opts,
		IndexCache: indexCache,
		Fs:         fs,
		Closed:     new(atomic.Value),
	}

	switch opts.LogStoreT {
	case options.LogstoreBatchStore:
		db.Wal = wal.NewDriverWithBatchStore(opts.Ctx, dirname, WALDir, nil)
	case options.LogstoreLogservice:
		db.Wal = wal.NewDriverWithLogservice(opts.Ctx, opts.Lc)
	}
	db.Scheduler = newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	dataFactory := tables.NewDataFactory(
		db.Fs, indexCache, db.Scheduler, db.Dir)
	if db.Opts.Catalog, err = catalog.OpenCatalog(db.Scheduler, dataFactory); err != nil {
		return
	}
	db.Catalog = db.Opts.Catalog
	// Init and start txn manager
	db.TransferTable = model.NewTransferTable[*model.TransferHashPage](db.Opts.TransferTableTTL)
	txnStoreFactory := txnimpl.TxnStoreFactory(
		opts.Ctx,
		db.Opts.Catalog,
		db.Wal,
		db.TransferTable,
		indexCache,
		dataFactory)
	txnFactory := txnimpl.TxnFactory(db.Opts.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory, db.Opts.Clock)
	db.LogtailMgr = logtail.NewManager(
		int(db.Opts.LogtailCfg.PageSize),
		db.TxnMgr.Now,
	)
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)
	db.TxnMgr.Start(opts.Ctx)
	db.LogtailMgr.Start()
	db.BGCheckpointRunner = checkpoint.NewRunner(
		opts.Ctx,
		db.Fs,
		db.Catalog,
		db.Scheduler,
		logtail.NewDirtyCollector(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor)),
		db.Wal,
		checkpoint.WithFlushInterval(opts.CheckpointCfg.FlushInterval),
		checkpoint.WithCollectInterval(opts.CheckpointCfg.ScanInterval),
		checkpoint.WithMinCount(int(opts.CheckpointCfg.MinCount)),
		checkpoint.WithMinIncrementalInterval(opts.CheckpointCfg.IncrementalInterval),
		checkpoint.WithGlobalMinCount(int(opts.CheckpointCfg.GlobalMinCount)),
		checkpoint.WithGlobalVersionInterval(opts.CheckpointCfg.GlobalVersionInterval))

	now := time.Now()
	checkpointed, err := db.BGCheckpointRunner.Replay(dataFactory)
	if err != nil {
		panic(err)
	}
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("checkpoints"),
		common.AnyField("cost", time.Since(now)),
		common.AnyField("checkpointed", checkpointed.ToString()))

	now = time.Now()
	db.Replay(dataFactory, checkpointed)
	db.Catalog.ReplayTableRows()
	logutil.Info("open-tae", common.OperationField("replay"),
		common.OperandField("wal"),
		common.AnyField("cost", time.Since(now)))

	db.DBLocker, dbLocker = dbLocker, nil

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	mergeOp := newMergeTaskBuiler(db)
	scanner.RegisterOp(mergeOp)
	db.Wal.Start()
	db.BGCheckpointRunner.Start()

	db.BGScanner = w.NewHeartBeater(
		opts.CheckpointCfg.ScanInterval,
		scanner)
	db.BGScanner.Start()
	// TODO: WithGCInterval requires configuration parameters
	db.DiskCleaner = gc2.NewDiskCleaner(opts.Ctx, db.Fs, db.BGCheckpointRunner, db.Catalog)
	db.DiskCleaner.Start()
	db.DiskCleaner.AddChecker(
		func(item any) bool {
			checkpoint := item.(*checkpoint.CheckpointEntry)
			ts := types.BuildTS(time.Now().UTC().UnixNano()-int64(opts.GCCfg.GCTTL), 0)
			return !checkpoint.GetEnd().GreaterEq(ts)
		})
	// Init gc manager at last
	// TODO: clean-try-gc requires configuration parameters
	db.GCManager = gc.NewManager(
		gc.WithCronJob(
			"clean-transfer-table",
			opts.CheckpointCfg.FlushInterval,
			func(_ context.Context) (err error) {
				db.TransferTable.RunTTL(time.Now())
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
				consumed := db.DiskCleaner.GetMaxConsumed()
				if consumed == nil {
					return nil
				}
				return db.BGCheckpointRunner.GCByTS(ctx, consumed.GetEnd())
			}),
		gc.WithCronJob(
			"catalog-gc",
			opts.CatalogCfg.GCInterval,
			func(ctx context.Context) error {
				if opts.CatalogCfg.DisableGC {
					return nil
				}
				consumed := db.DiskCleaner.GetMaxConsumed()
				if consumed == nil {
					return nil
				}
				db.Catalog.GCByTS(ctx, consumed.GetEnd())
				return nil
			}),
		gc.WithCronJob(
			"logtail-gc",
			opts.CheckpointCfg.GCCheckpointInterval,
			func(ctx context.Context) error {
				ckp := db.BGCheckpointRunner.MaxCheckpoint()
				if ckp != nil && ckp.IsCommitted() {
					db.LogtailMgr.GCByTS(ctx, ckp.GetEnd())
				}
				return nil
			},
		),
	)

	db.GCManager.Start()

	// For debug or test
	// logutil.Info(db.Catalog.SimplePPString(common.PPL2))
	return
}
