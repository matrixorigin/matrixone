package db

import (
	"errors"
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrClosed = errors.New("aoe: closed")
)

// type Reader interface {
// }
// type Writer interface {
// }

type DB struct {
	Dir  string
	Opts *e.Options

	MemTableMgr     mtif.IManager
	MutableBufMgr   bmgrif.IBufferManager
	TableDataBufMgr bmgrif.IBufferManager

	MetaInfo *md.MetaInfo

	DataDir  *os.File
	FileLock io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}

	sync.RWMutex
}

// var (
// 	_ Reader = (*DB)(nil)
// 	_ Writer = (*DB)(nil)
// )

func (d *DB) restoreMeta() {
	// db.MetaInfo
}

func (d *DB) startWorkers() {
	d.Opts.MemData.Updater.Start()
	d.Opts.Data.Flusher.Start()
	d.Opts.Data.Sorter.Start()
	d.Opts.Meta.Flusher.Start()
	d.Opts.Meta.Updater.Start()
}

func (d *DB) stopWorkers() {
	d.Opts.MemData.Updater.Stop()
	d.Opts.Data.Flusher.Stop()
	d.Opts.Data.Sorter.Stop()
	d.Opts.Meta.Flusher.Stop()
	d.Opts.Meta.Updater.Stop()
}

func (d *DB) Close() error {
	if err := d.Closed.Load(); err != nil {
		panic(err)
	}

	d.Closed.Store(ErrClosed)
	close(d.ClosedC)
	d.stopWorkers()
	return nil
}
