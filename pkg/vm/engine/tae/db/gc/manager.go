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
		mergeTable.Merge(table)
	}
	gc := mergeTable.GetGcObject()
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
