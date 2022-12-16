package gc

import (
	"context"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	w "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks/worker"
	"sync"
	"sync/atomic"
	"time"
)

type Manager struct {
	sync.RWMutex
	options struct {
		consumeInterval time.Duration
	}
	table       []GcTable
	mergeCache  []GcTable
	gc          []string
	fs          *objectio.ObjectFS
	ckpRunner   checkpoint.Runner
	ckp         *checkpoint.CheckpointEntry
	catalog     *catalog.Catalog
	waitConsume atomic.Int32
	stopper     *stopper.Stopper
	onceStart   sync.Once
	onceStop    sync.Once
}

func NewManager(fs *objectio.ObjectFS, runner checkpoint.Runner, catalog *catalog.Catalog) *Manager {
	m := &Manager{
		table:     make([]GcTable, 0),
		gc:        make([]string, 0),
		fs:        fs,
		ckpRunner: runner,
		catalog:   catalog,
	}
	m.options.consumeInterval = time.Second * 1
	return m
}

func (m *Manager) TryGC() {
	m.waitConsume.Add(1)
}

func (m *Manager) consume(num int32) {
	m.waitConsume.Add(0 - num)
}

func (m *Manager) cronjob(ctx context.Context) {
	hb := w.NewHeartBeaterWithFunc(m.options.consumeInterval, m.tryConsume, nil)
	hb.Start()
	<-ctx.Done()
	hb.Stop()
}

func (m *Manager) tryConsume() {
	num := m.waitConsume.Load()
	for i := int32(0); i < num; i++ {
		err := m.CronTask()
		if err != nil {
			m.consume(i + 1)
			break
		}
	}
}

func (m *Manager) CronTask() error {
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

func (m *Manager) GetCkp() *checkpoint.CheckpointEntry {
	m.RLock()
	defer m.RUnlock()
	return m.ckp
}

func (m *Manager) SetCkp(ckp *checkpoint.CheckpointEntry) {
	m.Lock()
	defer m.Unlock()
	m.ckp = ckp
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

func (m *Manager) GetGc() []string {
	return m.gc
}

func (m *Manager) GetCheckpoint(entry *checkpoint.CheckpointEntry) *checkpoint.CheckpointEntry {
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

func (m *Manager) consumeCheckpoint(entry *checkpoint.CheckpointEntry) (err error) {
	factory := logtail.IncrementalCheckpointDataFactory(entry.GetStart(), entry.GetEnd())
	data, err := factory(m.catalog)
	if err != nil {
		return
	}
	defer data.Close()
	table := NewGcTable()
	table.UpdateTable(data)
	_, err = table.SaveTable(entry.GetStart(), entry.GetEnd(), m.fs)
	m.AddTable(table)
	return err
}

func (m *Manager) Start() {
	m.onceStart.Do(func() {
		if err := m.stopper.RunNamedTask("gc-job", m.cronjob); err != nil {
			panic(err)
		}
	})
}

func (m *Manager) Stop() {
	m.onceStop.Do(func() {
		m.stopper.Stop()
	})
}
