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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
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
	WALDir     = "wal"
	CATALOGDir = "catalog"
)

func Open(dirname string, opts *options.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
	}()

	opts = opts.FillDefaults(dirname)

	indexBufMgr := buffer.NewNodeManager(opts.CacheCfg.IndexCapacity, nil)
	mutBufMgr := buffer.NewNodeManager(opts.CacheCfg.InsertCapacity, nil)
	txnBufMgr := buffer.NewNodeManager(opts.CacheCfg.TxnCapacity, nil)

	SegmentFactory := blockio.NewObjectFactory(dirname)

	db = &DB{
		Dir:         dirname,
		Opts:        opts,
		IndexBufMgr: indexBufMgr,
		MTBufMgr:    mutBufMgr,
		TxnBufMgr:   txnBufMgr,
		FileFactory: SegmentFactory,
		Closed:      new(atomic.Value),
	}

	switch opts.LogStoreT {
	case options.LogstoreBatchStore:
		db.Wal = wal.NewDriverWithBatchStore(dirname, WALDir, nil)
	case options.LogstoreLogservice:
		db.Wal = wal.NewDriverWithLogservice(opts.Lc)
	}
	db.Scheduler = newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	dataFactory := tables.NewDataFactory(db.FileFactory, mutBufMgr, db.Scheduler, db.Dir)
	if db.Opts.Catalog, err = catalog.OpenCatalog(dirname, CATALOGDir, nil, db.Scheduler, dataFactory); err != nil {
		return
	}
	db.Catalog = db.Opts.Catalog

	// Init and start txn manager
	txnStoreFactory := txnimpl.TxnStoreFactory(db.Opts.Catalog, db.Wal, txnBufMgr, dataFactory)
	txnFactory := txnimpl.TxnFactory(db.Opts.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory, db.Opts.Clock)
	db.LogtailMgr = logtail.NewLogtailMgr(db.Opts.LogtailCfg.PageSize, db.Opts.Clock)
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)

	db.Replay(dataFactory)
	db.Catalog.ReplayTableRows()

	db.TxnMgr.Start()

	db.DBLocker, dbLocker = dbLocker, nil

	// Init checkpoint driver
	policyCfg := new(checkpoint.PolicyCfg)
	policyCfg.Levels = int(opts.CheckpointCfg.ExecutionLevels)
	policyCfg.Interval = opts.CheckpointCfg.ExecutionInterval
	policyCfg.FlushInterval = opts.CheckpointCfg.FlushInterval
	db.CKPDriver = checkpoint.NewDriver(db.Scheduler, policyCfg)

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	calibrationOp := newCalibrationOp(db)
	catalogMonotor := newCatalogStatsMonitor(db, opts.CheckpointCfg.CatalogUnCkpLimit, time.Duration(opts.CheckpointCfg.CatalogCkpInterval))
	scanner.RegisterOp(calibrationOp)
	scanner.RegisterOp(catalogMonotor)

	// Start workers
	db.CKPDriver.Start()

	db.HeartBeatJobs = stopper.NewStopper("HeartbeatJobs")
	db.HeartBeatJobs.RunNamedTask("DirtyBlockWatcher", func(ctx context.Context) {
		forest := newDirtyForest(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor))
		hb := w.NewHeartBeaterWithFunc(time.Duration(opts.CheckpointCfg.ScannerInterval)*time.Millisecond, func() {
			forest.Run()
			dirtyTree := forest.MergeForest()
			if dirtyTree.IsEmpty() {
				return
			}
			logutil.Infof(dirtyTree.String())
		}, nil)
		hb.Start()
		<-ctx.Done()
		hb.Stop()
	})
	db.HeartBeatJobs.RunNamedTask("BackgroundScanner", func(ctx context.Context) {
		hb := w.NewHeartBeater(time.Duration(opts.CheckpointCfg.ScannerInterval)*time.Millisecond, scanner)
		hb.Start()
		<-ctx.Done()
		hb.Stop()
	})

	return
}
