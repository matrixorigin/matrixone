package db

import (
	"io"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	mtif "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"os"
	"sync/atomic"
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

	DataDir  *os.File
	FileLock io.Closer

	Closed  *atomic.Value
	ClosedC chan struct{}
}

// var (
// 	_ Reader = (*DB)(nil)
// 	_ Writer = (*DB)(nil)
// )
