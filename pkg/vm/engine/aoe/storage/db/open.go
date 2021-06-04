package db

import (
	"fmt"
	"io/ioutil"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mt "matrixone/pkg/vm/engine/aoe/storage/memtable"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
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

	memtblMgr := mt.NewManager(opts)
	mutBufMgr := bm.NewBufferManager(opts.CacheCfg.InsertCapacity, opts.MemData.Updater)
	dataBufMgr := bm.NewBufferManager(opts.CacheCfg.DataCapacity, opts.MemData.Updater)

	db = &DB{
		Dir:             dirname,
		Opts:            opts,
		MemTableMgr:     memtblMgr,
		MutableBufMgr:   mutBufMgr,
		TableDataBufMgr: dataBufMgr,
		ClosedC:         make(chan struct{}),
		Closed:          new(atomic.Value),
	}

	db.store.MetaInfo = opts.Meta.Info

	cleanStaleMeta(opts.Meta.Conf.Dir)
	db.validateAndCleanStaleData()

	db.startWorkers()
	return db, err
}
