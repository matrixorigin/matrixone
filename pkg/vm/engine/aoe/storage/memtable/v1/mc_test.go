package memtable

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/db/factories"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMutCollection(t *testing.T) {
	dir := "/tmp/memtable/mc"
	os.RemoveAll(dir)
	colcnt := 2
	blockRows, blockCnt := uint64(1024), uint64(4)

	opts := config.NewCustomizedMetaOptions(dir, config.CST_Customize, blockRows, blockCnt)

	capacity := blockRows * 4 * uint64(colcnt) * 2 * 2 * 4
	manager := NewManager(opts)
	fsMgr := ldio.NewManager(dir, false)
	indexBufMgr := bmgr.NewBufferManager(dir, capacity)
	mtBufMgr := bmgr.NewBufferManager(dir, capacity)
	sstBufMgr := bmgr.NewBufferManager(dir, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	opts.Scheduler = sched.NewScheduler(opts, tables)

	tabletInfo := metadata.MockTableInfo(colcnt)
	ctx := &sched.Context{Opts: opts, Waitable: true}
	event := meta.NewCreateTableEvent(ctx, dbi.TableOpCtx{TableName: tabletInfo.Name}, tabletInfo)
	assert.NotNil(t, event)
	opts.Scheduler.Schedule(event)
	err := event.WaitDone()
	assert.Nil(t, err)
	tbl := event.GetTable()

	maxsize := uint64(capacity)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	factory := factories.NewMutFactory(mgr, nil)
	tables.MutFactory = factory

	t0, err := tables.RegisterTable(tbl)
	assert.Nil(t, err)

	c0 := newMutableCollection(manager, t0)
	blks := uint64(20)
	expectBlks := blks
	batchSize := uint64(8)
	step := expectBlks / batchSize
	var wg sync.WaitGroup
	seq := uint64(0)
	for expectBlks > 0 {
		thisStep := step
		if expectBlks < step {
			thisStep = expectBlks
			expectBlks = 0
		} else {
			expectBlks -= step
		}
		wg.Add(1)
		logId := seq
		seq++
		go func(id uint64, wgp *sync.WaitGroup) {
			defer wgp.Done()
			insert := chunk.MockBatch(tbl.Schema.Types(), thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &metadata.LogIndex{
				ID:       id,
				Capacity: uint64(insert.Vecs[0].Length()),
			}
			err := c0.Append(insert, index)
			assert.Nil(t, err)
		}(logId, &wg)
	}
	wg.Wait()
	time.Sleep(time.Duration(10) * time.Millisecond)
	t.Log(mgr.String())
	c0.Unref()
	assert.Equal(t, int64(0), t0.RefCount())
	assert.Equal(t, 0, mgr.Count())
}
