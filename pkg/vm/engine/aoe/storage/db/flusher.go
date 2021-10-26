package db

import (
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/flusher"
	imem "github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/memtable/v1/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/wal"
)

var (
	DefaultFlushInterval     = time.Duration(20) * time.Second
	DefaultNodeFlushInterval = time.Duration(120) * time.Second
)

type flusherDriver struct {
	mgr     imem.IManager
	id      uint64
	checker func(int64) bool
}

func (driver *flusherDriver) GetId() uint64 {
	return driver.id
}

func (driver *flusherDriver) FlushNode(id uint64) error {
	c := driver.mgr.StrongRefCollection(id)
	if c == nil {
		return nil
	}
	defer c.Unref()
	meta := c.GetMeta()
	if !driver.checker(meta.GetFlushTS()) {
		return nil
	}
	logutil.Infof("TimedFlushing | Shard-%d | Node-%d", driver.id, id)
	return c.Flush()
}

func createFlusherFactory(mgr imem.IManager) flusher.DriverFactory {
	return func(id uint64) flusher.FlushDriver {
		driver := &flusherDriver{
			mgr: mgr,
			id:  id,
			checker: func(ts int64) bool {
				return time.Now().UnixMicro()-ts > DefaultNodeFlushInterval.Microseconds()
			},
		}
		return driver
	}
}

type timedFlusherHandle struct {
	driver   flusher.Flusher
	producer wal.ShardWal
}

func (h *timedFlusherHandle) OnStopped() {
	logutil.Infof(h.driver.String())
}

func (h *timedFlusherHandle) OnExec() {
	entries := h.producer.GetAllPendingEntries()
	if entries == nil {
		return
	}
	h.driver.OnStats(entries)
}
