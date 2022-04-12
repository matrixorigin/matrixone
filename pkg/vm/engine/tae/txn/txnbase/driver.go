package txnbase

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
)

const (
	GroupC uint32 = iota + 10
	GroupUC
)

type NodeDriver interface {
	AppendEntry(uint32, NodeEntry) (uint64, error)
	LoadEntry(groupId uint32, lsn uint64) (NodeEntry, error)
	Close() error
}

type nodeDriver struct {
	sync.RWMutex
	impl store.Store
	own  bool
}

func NewNodeDriver(dir, name string, cfg *store.StoreCfg) NodeDriver {
	impl, err := store.NewBaseStore(dir, name, cfg)
	if err != nil {
		panic(err)
	}
	driver := NewNodeDriverWithStore(impl, true)
	return driver
}

func NewNodeDriverWithStore(impl store.Store, own bool) NodeDriver {
	driver := new(nodeDriver)
	driver.impl = impl
	driver.own = own
	return driver
}

func (nd *nodeDriver) LoadEntry(groupId uint32, lsn uint64) (NodeEntry, error) {
	return nd.impl.Load(groupId, lsn)
}

func (nd *nodeDriver) AppendEntry(group uint32, e NodeEntry) (uint64, error) {
	id, err := nd.impl.AppendEntry(group, e)
	return id, err
}

func (nd *nodeDriver) Close() error {
	if nd.own {
		return nd.impl.Close()
	}
	return nil
}
