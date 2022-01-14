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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage"
	bm "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/factories"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/gcreqs"
	sched "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/flusher"
	ldio "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/logstore"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal/shard"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/worker"
)

func OpenWithWalBroker(dirname string, opts *storage.Options) (db *DB, err error) {
	if opts.Wal != nil && opts.Wal.GetRole() != wal.BrokerRole {
		return nil, ErrUnexpectedWalRole
	}
	opts.WalRole = wal.BrokerRole
	return Open(dirname, opts)
}

func Open(dirname string, opts *storage.Options) (db *DB, err error) {
	dbLocker, err := createDBLock(dirname)
	if err != nil {
		return nil, err
	}
	defer func() {
		if dbLocker != nil {
			dbLocker.Close()
		}
	}()
	opts.FillDefaults(dirname)

	flushDriver := flusher.NewDriver()

	fsMgr := ldio.NewManager(dirname, false)
	indexBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.IndexCapacity)
	sstBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.DataCapacity)

	mutNodeMgr := mb.NewNodeManager(opts.CacheCfg.InsertCapacity, nil)
	mtBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.InsertCapacity)

	db = &DB{
		Dir:            dirname,
		Opts:           opts,
		FsMgr:          fsMgr,
		IndexBufMgr:    indexBufMgr,
		MTBufMgr:       mtBufMgr,
		SSTBufMgr:      sstBufMgr,
		MutationBufMgr: mutNodeMgr,
		ClosedC:        make(chan struct{}),
		Closed:         new(atomic.Value),
	}

	db.Store.Mu = &opts.Mu
	db.Store.DataTables = table.NewTables(opts, &opts.Mu, db.FsMgr, db.MTBufMgr, db.SSTBufMgr, db.IndexBufMgr, flushDriver)
	factory := factories.NewMutFactory(mutNodeMgr, nil)
	db.Store.DataTables.MutFactory = factory

	flushDriver.InitFactory(createFlusherFactory(db.Store.DataTables))
	db.FlushDriver = flushDriver

	store, err := logstore.NewBatchStore(common.MakeMetaDir(dirname), "store", nil)
	if err != nil {
		return
	}
	if db.Opts.Wal == nil {
		db.Opts.Wal = shard.NewManagerWithDriver(store, false, db.Opts.WalRole)
	}
	db.Wal = db.Opts.Wal

	db.TimedFlusher = w.NewHeartBeater(DefaultFlushInterval, &timedFlusherHandle{
		driver:   flushDriver,
		producer: db.Wal,
	})

	catalogCfg := metadata.CatalogCfg{
		Dir:              dirname,
		BlockMaxRows:     opts.Meta.Conf.BlockMaxRows,
		SegmentMaxBlocks: opts.Meta.Conf.SegmentMaxBlocks,
	}
	if opts.Meta.Catalog, err = metadata.OpenCatalogWithDriver(new(sync.RWMutex), &catalogCfg, store, db.Wal); err != nil {
		return
	}
	db.Store.Catalog = opts.Meta.Catalog
	db.Store.Catalog.Start()

	db.Opts.Scheduler = sched.NewScheduler(opts, db.Store.DataTables)
	db.Scheduler = db.Opts.Scheduler

	db.startWorkers()
	replayHandle := NewReplayHandle(dirname, opts.Meta.Catalog, db.Store.DataTables, nil)
	if err = replayHandle.Replay(); err != nil {
		opts.Meta.Catalog.Close()
		db.stopWorkers()
		return nil, err
	}

	db.DBLocker, dbLocker = dbLocker, nil
	replayHandle.ScheduleEvents(db.Opts, db.Store.DataTables)

	db.Opts.GC.Acceptor.Accept(gcreqs.NewCatalogCompactionRequest(db.Store.Catalog, db.Opts.MetaCleanerCfg.Interval))
	os.RemoveAll(db.GetTempDir())
	os.MkdirAll(db.GetTempDir(), os.FileMode(0755))

	os.MkdirAll(common.MakeIndexDir(filepath.Join(db.Dir, "data")), os.FileMode(0755))
	return db, err
}
