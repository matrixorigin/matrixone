package db

import (
	"fmt"
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	mt "matrixone/pkg/vm/engine/aoe/storage/memtable"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path"
	"sync/atomic"
)

// type Reader interface {
// }

func loadMetaInfo(cfg *md.Configuration) *md.MetaInfo {
	empty := false
	var err error
	dir := e.MakeMetaDir(cfg.Dir)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		empty = true
	}
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	if empty {
		return md.NewMetaInfo(cfg)
	}

	files, err := ioutil.ReadDir(dir)
	if err != nil {
		panic(fmt.Sprintf("err: %s", err))
	}
	if len(files) == 0 {
		return md.NewMetaInfo(cfg)
	}

	maxVersion := -1
	maxIdx := -1

	for idx, file := range files {
		version, ok := e.ParseMetaFileName(file.Name())
		if !ok {
			continue
		}
		if version > maxVersion {
			maxVersion = version
			maxIdx = idx
		}
	}

	if maxIdx == -1 {
		return md.NewMetaInfo(cfg)
	}

	r, err := os.OpenFile(path.Join(dir, files[maxIdx].Name()), os.O_RDONLY, 0666)
	if err != nil {
		panic(err)
	}
	defer r.Close()
	metaInfo, err := md.Deserialize(r)
	if err != nil {
		panic(err)
	}
	metaInfo.Conf = cfg
	return metaInfo
}

func Open(dirname string, opts *e.Options) (db *DB, err error) {
	opts.FillDefaults(dirname)
	opts.Meta.Info = loadMetaInfo(opts.Meta.Conf)

	// TODO: refactor needed
	dio.WRITER_FACTORY.Init(opts, dirname)
	dio.READER_FACTORY.Init(opts, dirname)

	fsMgr := ldio.NewManager(dirname, false)
	memtblMgr := mt.NewManager(opts)
	indexBufMgr := bm.NewBufferManager(opts.CacheCfg.IndexCapacity, opts.MemData.Updater)
	mtBufMgr := bm.NewBufferManager(opts.CacheCfg.InsertCapacity, opts.MemData.Updater)
	sstBufMgr := bm.NewBufferManager(opts.CacheCfg.DataCapacity, opts.MemData.Updater)

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

	db.Store.DataTables = table.NewTables()
	db.Store.MetaInfo = opts.Meta.Info

	cleanStaleMeta(opts.Meta.Conf.Dir)
	db.replayAndCleanData()

	db.startWorkers()
	return db, err
}
