package db

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
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
	metaReplayHandle := NewMetaHandle(dirname)

	opts.Meta.Info = metaReplayHandle.RebuildInfo(&opts.Mu, opts.Meta.Conf)

	// TODO: refactor needed
	dio.WRITER_FACTORY.Init(opts, dirname)
	dio.READER_FACTORY.Init(opts, dirname)

	fsMgr := ldio.NewManager(dirname, false)
	memtblMgr := mt.NewManager(opts)
	indexBufMgr := bm.NewBufferManager(opts.Meta.Conf.Dir, opts.CacheCfg.IndexCapacity, opts.MemData.Updater)
	mtBufMgr := bm.NewBufferManager(opts.Meta.Conf.Dir, opts.CacheCfg.InsertCapacity, opts.MemData.Updater)
	sstBufMgr := bm.NewBufferManager(opts.Meta.Conf.Dir, opts.CacheCfg.DataCapacity, opts.MemData.Updater)

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
	db.Store.DataTables = table.NewTables()
	db.Store.MetaInfo = opts.Meta.Info
	db.Cleaner.MetaFiles = w.NewHeartBeater(db.Opts.MetaCleanerCfg.Interval, NewMetaFileCleaner(db.Opts.Meta.Info))

	metaReplayHandle.Cleanup()
	db.replayAndCleanData()

	db.startCleaner()
	db.startWorkers()
	db.DBLocker, dbLocker = dbLocker, nil
	return db, err
}
