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
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/segmentio"

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

	db = &DB{
		Dir:         dirname,
		Opts:        opts,
		IndexBufMgr: indexBufMgr,
		MTBufMgr:    mutBufMgr,
		TxnBufMgr:   txnBufMgr,
		FileFactory: segmentio.SegmentFactory,
		Closed:      new(atomic.Value),
	}

	db.Wal = wal.NewDriver(dirname, WALDir, nil)
	db.Scheduler = newTaskScheduler(db, db.Opts.SchedulerCfg.AsyncWorkers, db.Opts.SchedulerCfg.IOWorkers)
	dataFactory := tables.NewDataFactory(db.FileFactory, mutBufMgr, db.Scheduler, db.Dir)
	if db.Opts.Catalog, err = catalog.OpenCatalog(dirname, CATALOGDir, nil, db.Scheduler, dataFactory); err != nil {
		return
	}
	db.Catalog = db.Opts.Catalog

	// Init and start txn manager
	txnStoreFactory := txnimpl.TxnStoreFactory(db.Opts.Catalog, db.Wal, txnBufMgr, dataFactory)
	txnFactory := txnimpl.TxnFactory(db.Opts.Catalog)
	db.TxnMgr = txnbase.NewTxnManager(txnStoreFactory, txnFactory)

	db.Replay(dataFactory)
	db.Catalog.ReplayTableRows()

	db.TxnMgr.Start()

	db.DBLocker, dbLocker = dbLocker, nil

	// Init checkpoint driver
	policyCfg := new(checkpoint.PolicyCfg)
	policyCfg.Levels = int(opts.CheckpointCfg.ExecutionLevels)
	policyCfg.Interval = opts.CheckpointCfg.ExecutionInterval
	db.CKPDriver = checkpoint.NewDriver(db.Scheduler, policyCfg)

	// Init timed scanner
	scanner := NewDBScanner(db, nil)
	calibrationOp := newCalibrationOp(db)
	catalogMonotor := newCatalogStatsMonitor(db, opts.CheckpointCfg.CatalogUnCkpLimit, time.Duration(opts.CheckpointCfg.CatalogCkpInterval))
	scanner.RegisterOp(calibrationOp)
	scanner.RegisterOp(catalogMonotor)
	db.TimedScanner = w.NewHeartBeater(time.Duration(opts.CheckpointCfg.ScannerInterval)*time.Millisecond, scanner)

	// Start workers
	db.CKPDriver.Start()
	db.TimedScanner.Start()

	return
}
