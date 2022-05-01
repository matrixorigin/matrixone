package wal

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

type walDriver struct {
	sync.RWMutex
	impl store.Store
	own  bool
}

func NewDriver(dir, name string, cfg *store.StoreCfg) Driver {
	impl, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	driver := NewDriverWithStore(impl, true)
	return driver
}

func NewDriverWithStore(impl store.Store, own bool) Driver {
	driver := new(walDriver)
	driver.impl = impl
	driver.own = own
	return driver
}

func (driver *walDriver) Checkpoint(indexes []*Index) (err error) {
	// TODO
	return
}

func (driver *walDriver) LoadEntry(groupId uint32, lsn uint64) (LogEntry, error) {
	return driver.impl.Load(groupId, lsn)
}

func (driver *walDriver) AppendEntry(group uint32, e LogEntry) (uint64, error) {
	id, err := driver.impl.AppendEntry(group, e)
	return id, err
}

func (driver *walDriver) Close() error {
	if driver.own {
		return driver.impl.Close()
	}
	return nil
}
