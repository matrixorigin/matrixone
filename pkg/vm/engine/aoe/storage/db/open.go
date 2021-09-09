package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	mt "matrixone/pkg/vm/engine/aoe/storage/memtable"
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
