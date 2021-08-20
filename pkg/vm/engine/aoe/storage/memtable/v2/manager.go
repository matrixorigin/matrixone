package memtable

import (
	"errors"
	"fmt"
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/memtable/v2/base"
	mbase "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"sync"
)

type manager struct {
	sync.RWMutex
	opts        *engine.Options
	collections map[uint64]*collection
	data        iface.ITableData
	nodemgr     mbase.INodeManager
}

func NewManager(opts *engine.Options, nodemgr mbase.INodeManager) *manager {
	m := &manager{
		opts:        opts,
		nodemgr:     nodemgr,
		collections: make(map[uint64]*collection),
	}
	return m
}

func (m *manager) WeakRefCollection(id uint64) base.ICollection {
	m.RLock()
	c, ok := m.collections[id]
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) StrongRefCollection(id uint64) base.ICollection {
	m.RLock()
	c, ok := m.collections[id]
	if ok {
		c.Ref()
	}
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) RegisterCollection(td interface{}) (base.ICollection, error) {
	m.Lock()
	defer m.Unlock()
	tableData := td.(iface.ITableData)
	_, ok := m.collections[tableData.GetID()]
	if ok {
		return nil, errors.New(fmt.Sprintf("duplicate collection %d", tableData.GetID()))
	}
	c := newCollection(m, tableData)
	m.collections[tableData.GetID()] = c
	c.Ref()
	return c, nil
}

func (m *manager) UnregisterCollection(id uint64) (c base.ICollection, err error) {
	m.Lock()
	defer m.Unlock()
	c, ok := m.collections[id]
	if ok {
		delete(m.collections, id)
	} else {
		return nil, errors.New(fmt.Sprintf("cannot unregister collection %d", id))
	}
	return c, err
}
