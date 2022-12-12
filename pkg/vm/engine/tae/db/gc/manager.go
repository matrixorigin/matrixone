package gc

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"sync"
)

type Manager struct {
	sync.Mutex
	table      []GcTable
	mergeCache []GcTable
	gc         []string
	fs         *objectio.ObjectFS
}

func NewManager(fs *objectio.ObjectFS) *Manager {
	return &Manager{
		table: make([]GcTable, 0),
		gc:    make([]string, 0),
		fs:    fs,
	}
}

func (m *Manager) MergeTable() error {
	m.Lock()
	m.mergeCache = m.table
	m.table = make([]GcTable, 0)
	m.Unlock()
	mergeTable := NewGcTable()
	for _, table := range m.mergeCache {
		for name, ids := range table.table {
			mergeTable.table[name] = append(mergeTable.table[name], ids...)
		}
		for name, ids := range table.delete {
			mergeTable.delete[name] = append(mergeTable.delete[name], ids...)
		}
	}
	gc := make([]string, 0)
	for name, ids := range mergeTable.delete {
		blocks := mergeTable.table[name]
		if blocks == nil {
			panic(any("error"))
		}
		if len(blocks) == len(ids) {
			gc = append(gc, name)
			delete(mergeTable.table, name)
			delete(mergeTable.delete, name)
		}
	}
	if len(gc) > 0 {
		m.Lock()
		defer m.Unlock()
		m.gc = append(m.gc, gc...)
		m.table = append(m.table, mergeTable)
		m.mergeCache = nil
		return nil
	}
	m.AddTable(mergeTable)
	m.mergeCache = nil
	return nil
}

func (m *Manager) AddTable(table GcTable) {
	m.Lock()
	defer m.Unlock()
	m.table = append(m.table, table)
}
