package gc

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"sync"
	"sync/atomic"
)

type CleanerState int8

const (
	Idle CleanerState = iota
	Running
)

type DiskCleaner struct {
	sync.RWMutex
	state       CleanerState
	table       []GCTable
	mergeCache  []GCTable
	gc          []string
	fs          *objectio.ObjectFS
	ckpRunner   checkpoint.Runner
	ckp         *checkpoint.CheckpointEntry
	catalog     *catalog.Catalog
	waitConsume atomic.Int32
}

func NewDiskCleaner(fs *objectio.ObjectFS, runner checkpoint.Runner, catalog *catalog.Catalog) *DiskCleaner {
	m := &DiskCleaner{
		table:     make([]GCTable, 0),
		gc:        make([]string, 0),
		fs:        fs,
		ckpRunner: runner,
		catalog:   catalog,
		state:     Idle,
	}
	return m
}

func (m *DiskCleaner) TryGC() {
	m.waitConsume.Add(1)
	m.tryConsume()
}

func (m *DiskCleaner) SetIdle() {
	m.Lock()
	defer m.Unlock()
	m.state = Idle
}

func (m *DiskCleaner) GetState() CleanerState {
	m.RLock()
	defer m.RUnlock()
	return m.state
}

func (m *DiskCleaner) consume(num int32) {
	m.waitConsume.Add(0 - num)
}

func (m *DiskCleaner) tryConsume() {
	if m.GetState() == Running {
		return
	}
	go func() {
		defer m.SetIdle()
		num := m.waitConsume.Load()
		for i := int32(0); i < num; i++ {
			err := m.CronTask()
			if err != nil {
				m.consume(i + 1)
				break
			}
		}
		m.MergeTable()
	}()
}

func (m *DiskCleaner) CronTask() error {
	prvCkp := m.GetCkp()
	ckp := m.GetCheckpoint(prvCkp)
	if ckp == nil {
		return nil
	}
	err := m.consumeCheckpoint(ckp)
	if err != nil {
		return err
	}
	m.SetCkp(ckp)
	return err
}

func (m *DiskCleaner) GetCkp() *checkpoint.CheckpointEntry {
	m.RLock()
	defer m.RUnlock()
	return m.ckp
}

func (m *DiskCleaner) SetCkp(ckp *checkpoint.CheckpointEntry) {
	m.Lock()
	defer m.Unlock()
	m.ckp = ckp
}

func (m *DiskCleaner) MergeTable() error {
	m.Lock()
	if len(m.table) < 2 {
		m.Unlock()
		return nil
	}
	m.mergeCache = m.table
	m.table = make([]GCTable, 0)
	m.Unlock()
	mergeTable := NewGCTable()
	for _, table := range m.mergeCache {
		mergeTable.Merge(table)
	}
	gc := mergeTable.GetGCObject()
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

func (m *DiskCleaner) AddTable(table GCTable) {
	m.Lock()
	defer m.Unlock()
	m.table = append(m.table, table)
}

func (m *DiskCleaner) GetGC() []string {
	return m.gc
}

func (m *DiskCleaner) GetCheckpoint(entry *checkpoint.CheckpointEntry) *checkpoint.CheckpointEntry {
	ckps := m.ckpRunner.GetAllCheckpoints()
	if entry == nil {
		return ckps[0]
	}
	for _, ckp := range ckps {
		if ckp.GetStart().Prev().Equal(entry.GetEnd()) {
			return ckp
		}
	}
	return nil
}

func (m *DiskCleaner) consumeCheckpoint(entry *checkpoint.CheckpointEntry) (err error) {
	factory := logtail.IncrementalCheckpointDataFactory(entry.GetStart(), entry.GetEnd())
	data, err := factory(m.catalog)
	if err != nil {
		return
	}
	defer data.Close()
	table := NewGCTable()
	table.UpdateTable(data)
	_, err = table.SaveTable(entry.GetStart(), entry.GetEnd(), m.fs)
	m.AddTable(table)
	return err
}
