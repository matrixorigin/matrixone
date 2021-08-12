package memtable

import (
	"matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	dbsched "matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/memtable/mt_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
	os.RemoveAll(WORK_DIR)
}

func TestManager(t *testing.T) {
	opts := &engine.Options{}
	manager := NewManager(opts)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	capacity := uint64(4096)
	flusher := w.NewOpWorker("Mock Flusher")
	fsMgr := ldio.DefaultFsMgr
	indexBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity, flusher)
	mtBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity, flusher)
	sstBufMgr := bmgr.NewBufferManager(WORK_DIR, capacity, flusher)
	tableMeta := md.MockTable(nil, nil, 10)
	t0_data := table.NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tableMeta)

	c0, err := manager.RegisterCollection(t0_data)
	assert.Nil(t, err)
	assert.NotNil(t, c0)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	assert.Equal(t, int64(2), c0.RefCount())
	c0.Unref()
	assert.Equal(t, int64(1), c0.RefCount())
	c00, err := manager.RegisterCollection(t0_data)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(tableMeta.ID + 1)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	t.Log(manager.String())
	c00, err = manager.UnregisterCollection(tableMeta.ID)
	assert.Nil(t, err)
	assert.NotNil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	assert.Equal(t, int64(1), c0.RefCount())
	assert.Equal(t, int64(1), c00.RefCount())
	c00.Unref()
	assert.Equal(t, int64(0), c0.RefCount())
}

func TestCollection(t *testing.T) {
	maxRows := uint64(1024)
	cols := 2
	capacity := maxRows * 4 * uint64(cols) * 2 * 2 * 4
	opts := new(engine.Options)
	opts.FillDefaults(WORK_DIR)
	opts.Meta.Conf.BlockMaxRows = maxRows

	var mu sync.RWMutex
	tables := table.NewTables(&mu)
	opts.Scheduler = dbsched.NewScheduler(opts, tables)

	tabletInfo := md.MockTableInfo(2)
	eCtx := &meta.Context{Opts: opts, Waitable: true}
	event := meta.NewCreateTableEvent(eCtx, dbi.TableOpCtx{TableName: tabletInfo.Name}, tabletInfo)
	assert.NotNil(t, event)
	opts.Scheduler.Schedule(event)
	err := event.WaitDone()
	assert.Nil(t, err)
	tbl := event.GetTable()

	manager := NewManager(opts)
	fsMgr := ldio.NewManager(WORK_DIR, false)
	flusher := w.NewOpWorker("Mock Flusher")
	indexBufMgr := bmgr.NewBufferManager(opts.Meta.Conf.Dir, capacity, flusher)
	mtBufMgr := bmgr.NewBufferManager(opts.Meta.Conf.Dir, capacity, flusher)
	sstBufMgr := bmgr.NewBufferManager(opts.Meta.Conf.Dir, capacity, flusher)
	// tableMeta := md.MockTable(opts.Meta.Info, tbl.Schema, 10)
	// tableMeta := md.MockTable(nil, tbl.Schema, 10)
	tableMeta := tbl
	t0_data := table.NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tableMeta)
	err = tables.CreateTable(t0_data)
	assert.Nil(t, err)
	c0, _ := manager.RegisterCollection(t0_data)
	blks := uint64(20)
	expect_blks := blks
	batch_size := uint64(8)
	step := expect_blks / batch_size
	var waitgroup sync.WaitGroup
	seq := uint64(0)
	for expect_blks > 0 {
		thisStep := step
		if expect_blks < step {
			thisStep = expect_blks
			expect_blks = 0
		} else {
			expect_blks -= step
		}
		waitgroup.Add(1)
		logid := seq
		seq++
		go func(id uint64, wg *sync.WaitGroup) {
			defer wg.Done()
			insert := chunk.MockBatch(tbl.Schema.Types(), thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &md.LogIndex{
				ID:       id,
				Capacity: uint64(insert.Vecs[0].Length()),
			}
			err := c0.Append(insert, index)
			assert.Nil(t, err)
			// t.Log(mtBufMgr.String())
		}(logid, &waitgroup)
	}
	waitgroup.Wait()
	assert.Equal(t, len(tbl.SegmentIDs()), int(blks/(opts.Meta.Info.Conf.SegmentMaxBlocks)))
	time.Sleep(time.Duration(40) * time.Millisecond)

	// for _, column := range t0_data.GetCollumns() {
	// 	loopSeg := column.GetSegmentRoot()
	// 	for loopSeg != nil {
	// 		cursor := col.ScanCursor{}
	// 		for {
	// 			loopSeg.InitScanCursor(&cursor)
	// 			err := cursor.Init()
	// 			assert.Nil(t, err)
	// 			cursor.Next()
	// 			if cursor.Current == nil {
	// 				break
	// 			}
	// 		}
	// 		cursor.Close()
	// 		loopSeg.UnRef()
	// 		loopSeg = loopSeg.GetNext()
	// 	}
	// }
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
	t.Log(indexBufMgr.String())
	t.Log(fsMgr.String())
	t.Log(manager)
	// t.Log(common.GPool.String())
	t.Log(t0_data.String())

	opts.Scheduler.Stop()
}
