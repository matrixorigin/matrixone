package memtable

import (
	"errors"
	"matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	imem "matrixone/pkg/vm/engine/aoe/storage/memtable/base"
	"sync"
	// log "github.com/sirupsen/logrus"
)

type Manager struct {
	sync.RWMutex
	Opts        *engine.Options
	Collections map[uint64]imem.ICollection
	TableData   iface.ITableData
}

var (
	_ imem.IManager = (*Manager)(nil)
)

func NewManager(opts *engine.Options) imem.IManager {
	m := &Manager{
		Opts:        opts,
		Collections: make(map[uint64]imem.ICollection),
	}
	return m
}

func (m *Manager) CollectionIDs() map[uint64]uint64 {
	ids := make(map[uint64]uint64)
	m.RLock()
	for k, _ := range m.Collections {
		ids[k] = k
	}
	m.RUnlock()
	return ids
}

func (m *Manager) GetCollection(id uint64) imem.ICollection {
	m.RLock()
	c, ok := m.Collections[id]
	m.RUnlock()
	if !ok {
		return nil
	}
	return c
}

func (m *Manager) RegisterCollection(td interface{}) (c imem.ICollection, err error) {
	m.Lock()
	tableData := td.(iface.ITableData)
	_, ok := m.Collections[tableData.GetID()]
	if ok {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	c = NewCollection(tableData, m.Opts)
	m.Collections[tableData.GetID()] = c
	m.Unlock()
	return c, err
}

func (m *Manager) UnregisterCollection(id uint64) (c imem.ICollection, err error) {
	m.Lock()
	c, ok := m.Collections[id]
	if ok {
		delete(m.Collections, id)
	} else {
		m.Unlock()
		return nil, errors.New("logic error")
	}
	m.Unlock()
	return c, err
}
