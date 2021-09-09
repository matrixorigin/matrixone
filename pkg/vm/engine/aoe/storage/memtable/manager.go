package memtable

import (
	"errors"
	"fmt"
	engine "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"sync"
)

type manager struct {
	sync.RWMutex
	opts        *engine.Options
	collections map[uint64]imem.ICollection
	tableData   iface.ITableData
}

var (
	_ imem.IManager = (*manager)(nil)
)

func NewManager(opts *engine.Options) imem.IManager {
	m := &manager{
		opts:        opts,
		collections: make(map[uint64]imem.ICollection),
	}
	return m
}

func (m *manager) CollectionIDs() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	m.RLock()
	for k, _ := range m.collections {
		ids[k] = k
	}
	m.RUnlock()
	return ids
}

func (m *manager) WeakRefCollection(id uint64) imem.ICollection {
	m.RLock()
	c, ok := m.collections[id]
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *manager) StrongRefCollection(id uint64) imem.ICollection {
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

func (m *manager) String() string {
	m.RLock()
	defer m.RUnlock()
	s := fmt.Sprintf("<MTManager>(TableCnt=%d)", len(m.collections))
	for _, c := range m.collections {
		s = fmt.Sprintf("%s\n\t%s", s, c.String())
	}
	return s
}

func (m *manager) RegisterCollection(td interface{}) (c imem.ICollection, err error) {
	m.Lock()
	tableData := td.(iface.ITableData)
	_, ok := m.collections[tableData.GetID()]
	if ok {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	c = NewCollection(tableData, m.opts)
	m.collections[tableData.GetID()] = c
	m.Unlock()
	c.Ref()
	return c, err
}

func (m *manager) UnregisterCollection(id uint64) (c imem.ICollection, err error) {
	m.Lock()
	c, ok := m.collections[id]
	if ok {
		delete(m.collections, id)
	} else {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	m.Unlock()
	return c, err
}
