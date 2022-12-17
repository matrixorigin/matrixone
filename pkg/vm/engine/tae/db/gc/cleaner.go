package gc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type CleanerState int8

const (
	Idle CleanerState = iota
	Running
)

type DiskCleanerTmp struct {
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
	gcTask      *GCTask
}

func NewDiskCleanerTmp(fs *objectio.ObjectFS, runner checkpoint.Runner, catalog *catalog.Catalog) *DiskCleanerTmp {
	m := &DiskCleanerTmp{
		table:     make([]GCTable, 0),
		gc:        make([]string, 0),
		fs:        fs,
		ckpRunner: runner,
		catalog:   catalog,
		state:     Idle,
	}
	m.gcTask = NewGCTask(fs)
	return m
}

func (m *DiskCleanerTmp) TryGC() {
	m.waitConsume.Add(1)
	m.tryConsume()
}

func (m *DiskCleanerTmp) SetIdle() {
	m.Lock()
	defer m.Unlock()
	m.state = Idle
}

func (m *DiskCleanerTmp) GetState() CleanerState {
	m.RLock()
	defer m.RUnlock()
	return m.state
}

func (m *DiskCleanerTmp) consume(num int32) {
	m.waitConsume.Add(0 - num)
}

func (m *DiskCleanerTmp) tryConsume() {
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

func (m *DiskCleanerTmp) tryDelete() {
	if m.gcTask.GetState() == Running {
		return
	}
	go func() {
		m.Lock()
		gc := m.gc
		m.gc = make([]string, 0)
		m.Unlock()
		m.gcTask.ExecDelete(gc)
	}()
}

func (m *DiskCleanerTmp) CronTask() error {
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

func (m *DiskCleanerTmp) GetCkp() *checkpoint.CheckpointEntry {
	m.RLock()
	defer m.RUnlock()
	return m.ckp
}

func (m *DiskCleanerTmp) SetCkp(ckp *checkpoint.CheckpointEntry) {
	m.Lock()
	defer m.Unlock()
	m.ckp = ckp
}

func (m *DiskCleanerTmp) MergeTable() error {
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
	gc := mergeTable.SoftGC()
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

func (m *DiskCleanerTmp) delObject() {

}

func (m *DiskCleanerTmp) AddTable(table GCTable) {
	m.Lock()
	defer m.Unlock()
	m.table = append(m.table, table)
}

func (m *DiskCleanerTmp) GetGC() []string {
	return m.gc
}

func (m *DiskCleanerTmp) GetCheckpoint(entry *checkpoint.CheckpointEntry) *checkpoint.CheckpointEntry {
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

func (m *DiskCleanerTmp) consumeCheckpoint(entry *checkpoint.CheckpointEntry) (err error) {
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

func (m *DiskCleanerTmp) Replay() error {
	dirs, err := m.fs.ListDir(GCMetaDir)
	if err != nil {
		return err
	}
	if len(dirs) == 0 {
		return nil
	}
	jobs := make([]*tasks.Job, len(dirs))
	jobScheduler := tasks.NewParallelJobScheduler(100)
	defer jobScheduler.Stop()
	makeJob := func(i int) (job *tasks.Job) {
		dir := dirs[i]
		exec := func(ctx context.Context) (result *tasks.JobResult) {
			result = &tasks.JobResult{}
			table := NewGCTable()
			err := table.ReadTable(ctx, GCMetaDir+dir.Name, dir.Size, m.fs)
			if err != nil {
				result.Err = err
				return
			}
			m.AddTable(table)
			return
		}
		job = tasks.NewJob(
			fmt.Sprintf("load-%s", dir.Name),
			context.Background(),
			exec)
		return
	}

	for i := range dirs {
		jobs[i] = makeJob(i)
		if err = jobScheduler.Schedule(jobs[i]); err != nil {
			return err
		}
	}

	for _, job := range jobs {
		result := job.WaitDone()
		if err = result.Err; err != nil {
			return err
		}
	}
	m.MergeTable()
	return nil
}
