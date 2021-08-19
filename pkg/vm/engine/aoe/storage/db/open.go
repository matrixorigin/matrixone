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
	e "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mt "matrixone/pkg/vm/engine/aoe/storage/memtable/v1"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"sync/atomic"
)

func Open(dirname string, opts *e.Options) (db *DB, err error) {
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
	replayHandle := NewReplayHandle(dirname)

	opts.Meta.Info = replayHandle.RebuildInfo(&opts.Mu, opts.Meta.Info.Conf)

	fsMgr := ldio.NewManager(dirname, false)
	memtblMgr := mt.NewManager(opts)
	indexBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.IndexCapacity)
	mtBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.InsertCapacity)
	sstBufMgr := bm.NewBufferManager(dirname, opts.CacheCfg.DataCapacity)

	db = &DB{
		Dir:         dirname,
		Opts:        opts,
		FsMgr:       fsMgr,
		MemTableMgr: memtblMgr,
		IndexBufMgr: indexBufMgr,
		MTBufMgr:    mtBufMgr,
		SSTBufMgr:   sstBufMgr,
		ClosedC:     make(chan struct{}),
		Closed:      new(atomic.Value),
	}

	db.Store.Mu = &opts.Mu
	db.Store.DataTables = table.NewTables(&opts.Mu, db.FsMgr, db.MTBufMgr, db.SSTBufMgr, db.IndexBufMgr)
	db.Store.MetaInfo = opts.Meta.Info
	db.Cleaner.MetaFiles = w.NewHeartBeater(db.Opts.MetaCleanerCfg.Interval, NewMetaFileCleaner(db.Opts.Meta.Info))
	db.Opts.Scheduler = dbsched.NewScheduler(opts, db.Store.DataTables)
	db.Scheduler = db.Opts.Scheduler

	replayHandle.Cleanup()
	db.replayData()

	db.startCleaner()
	db.startWorkers()
	db.DBLocker, dbLocker = dbLocker, nil
	replayHandle.ScheduleEvents(db.Opts, db.Store.DataTables)
	return db, err
}
