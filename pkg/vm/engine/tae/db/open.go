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
	"path"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"

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

	mutBufMgr := buffer.NewNodeManager(opts.CacheCfg.InsertCapacity, nil)
	txnBufMgr := buffer.NewNodeManager(opts.CacheCfg.TxnCapacity, nil)

	serviceDir := path.Join(dirname, "data")
	if opts.Fs == nil {
		// TODO:fileservice needs to be passed in as a parameter
		opts.Fs = objectio.TmpNewFileservice(path.Join(dirname, "data"))
	}
	fs := objectio.NewObjectFS(opts.Fs, serviceDir)

	db = &DB{
		Dir:       dirname,
		Opts:      opts,
		MTBufMgr:  mutBufMgr,
		TxnBufMgr: txnBufMgr,
		Fs:        fs,
		Closed:    new(atomic.Value),
	}

	switch opts.LogStoreT {
	case options.LogstoreBatchStore:
		db.Wal = wal.NewDriverWithBatchStore(dirname, WALDir, nil)
	case options.LogstoreLogservice:
		db.Wal = wal.NewDriverWithLogservice(opts.Lc)
	}
	db.Scheduler = newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	dataFactory := tables.NewDataFactory(
		db.Fs, mutBufMgr, db.Scheduler, db.Dir)
	if db.Opts.Catalog, err = catalog.OpenCatalog(db.Scheduler, dataFactory); err != nil {
		return
	}
	db.Catalog = db.Opts.Catalog

	// Init and start txn manager
	txnStoreFactory := txnimpl.TxnStoreFactory(db.Opts.Catalog, db.Wal, txnBufMgr, dataFactory)
	txnFactory := txnimpl.TxnFactory(db.Opts.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory, db.Opts.Clock)
	db.LogtailMgr = logtail.NewLogtailMgr(db.Opts.LogtailCfg.PageSize, db.Opts.Clock)
	db.TxnMgr.CommitListener.AddTxnCommitListener(db.LogtailMgr)
	db.TxnMgr.Start()
	db.BGCheckpointRunner = checkpoint.NewRunner(
		db.Fs,
		db.Catalog,
		db.Scheduler,
		logtail.NewDirtyCollector(db.LogtailMgr, db.Opts.Clock, db.Catalog, new(catalog.LoopProcessor)),
		db.Wal,
		checkpoint.WithFlushInterval(opts.CheckpointCfg.FlushInterval),
		checkpoint.WithCollectInterval(opts.CheckpointCfg.ScanInterval),
		checkpoint.WithMinCount(int(opts.CheckpointCfg.MinCount)),
		checkpoint.WithMinIncrementalInterval(opts.CheckpointCfg.IncrementalInterval),
		checkpoint.WithMinGlobalInterval(opts.CheckpointCfg.GlobalInterval))

	now := time.Now()
	ts, err := db.BGCheckpointRunner.Replay(dataFactory)
	if err != nil {
		panic(err)
	}
	logutil.Infof("replay checkpoint takes %s", time.Since(now))

	now = time.Now()
	db.Replay(dataFactory, ts)
	db.Catalog.ReplayTableRows()
	logutil.Infof("replay wal takes %s", time.Since(now))

	db.DBLocker, dbLocker = dbLocker, nil

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	calibrationOp := newCalibrationOp(db)
	gcCollector := newGarbageCollector(
		db,
		opts.CheckpointCfg.FlushInterval)
	scanner.RegisterOp(calibrationOp)
	scanner.RegisterOp(gcCollector)
	db.BGCheckpointRunner.Start()

	db.BGScanner = w.NewHeartBeater(
		opts.CheckpointCfg.ScanInterval,
		scanner)
	db.BGScanner.Start()

	// For debug or test
	// logutil.Info(db.Catalog.SimplePPString(common.PPL2))
	return
}
