package memtable

import (
	"github.com/stretchr/testify/assert"
	"matrixone/pkg/container/types"
	"matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	mops "matrixone/pkg/vm/engine/aoe/storage/ops/meta"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"runtime"
	"sync"
	"testing"
	"time"
)

var WORK_DIR = "/tmp/memtable/mt_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestManager(t *testing.T) {
	opts := &engine.Options{}
	manager := NewManager(opts)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
	capacity := uint64(4096)
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	t0 := uint64(0)
	colDefs := make([]types.Type, 2)
	t0_data := table.NewTableData(bufMgr, t0, colDefs)

	c0, err := manager.RegisterCollection(t0_data)
	assert.Nil(t, err)
	assert.NotNil(t, c0)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err := manager.RegisterCollection(t0_data)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(t0 + 1)
	assert.NotNil(t, err)
	assert.Nil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 1)
	c00, err = manager.UnregisterCollection(t0)
	assert.Nil(t, err)
	assert.NotNil(t, c00)
	assert.Equal(t, len(manager.CollectionIDs()), 0)
}

func TestCollection(t *testing.T) {
	maxRows := uint64(1024)
	cols := 2
	capacity := maxRows * 4 * uint64(cols) * 2
	opts := new(engine.Options)
	// opts.EventListener = e.NewLoggingEventListener()
	dirname := "/tmp"
	opts.FillDefaults(dirname)
	opts.Meta.Conf.BlockMaxRows = maxRows

	opts.Meta.Updater.Start()
	opts.Meta.Flusher.Start()
	opts.Data.Flusher.Start()
	opts.Data.Sorter.Start()
	opts.MemData.Updater.Start()

	schema := md.MockSchema(2)
	opCtx := mops.OpCtx{Opts: opts, Schema: schema}
	op := mops.NewCreateTblOp(&opCtx)
	op.Push()
	err := op.WaitDone()
	assert.Nil(t, err)
	tbl := op.GetTable()

	manager := NewManager(opts)
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	colDefs := make([]types.Type, cols)
	for i := 0; i < cols; i++ {
		colDefs[i] = types.Type{types.T_int32, 4, 4, 0}
	}
	// colDefs[0] = types.Type{types.T_int32, 4, 4, 0}
	// colDefs[1] = types.Type{types.T_int32, 4, 4, 0}
	t0_data := table.NewTableData(bufMgr, tbl.ID, colDefs)
	c0, _ := manager.RegisterCollection(t0_data)
	blks := uint64(20)
	expect_blks := blks
	batch_size := uint64(4)
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
			insert := chunk.MockChunk(colDefs, thisStep*opts.Meta.Conf.BlockMaxRows)
			index := &md.LogIndex{
				ID:       id,
				Capacity: insert.GetCount(),
			}
			err = c0.Append(insert, index)
			assert.Nil(t, err)
			// t.Log(bufMgr.String())
		}(logid, &waitgroup)
	}
	waitgroup.Wait()
	assert.Equal(t, len(tbl.SegmentIDs()), int(blks/opts.Meta.Info.Conf.SegmentMaxBlocks))
	for i := 0; i < 50; i++ {
		runtime.GC()
		time.Sleep(time.Duration(1) * time.Millisecond)
	}

	for _, column := range t0_data.GetCollumns() {
		loopSeg := column.GetSegmentRoot()
		for loopSeg != nil {
			cursor := col.ScanCursor{}
			for {
				loopSeg.InitScanCursor(&cursor)
				err := cursor.Init()
				assert.Nil(t, err)
				cursor.Next()
				if cursor.Current == nil {
					break
				}
			}
			cursor.Close()
			loopSeg = loopSeg.GetNext()
		}
	}
	t.Log(bufMgr.String())

	opts.MemData.Updater.Stop()
	opts.Data.Flusher.Stop()
	opts.Meta.Flusher.Stop()
	opts.Meta.Updater.Stop()
	opts.Data.Sorter.Stop()
}

func TestContainer(t *testing.T) {
	capacity := uint64(4096)
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)

	baseid := common.ID{}
	step := capacity / 2
	// step := capacity
	con := NewDynamicContainer(bufMgr, baseid, step)
	assert.Equal(t, uint64(0), con.GetCapacity())

	err := con.Allocate()
	assert.Nil(t, err)
	assert.Equal(t, step, con.GetCapacity())
	assert.True(t, con.IsPined())

	id2 := baseid
	id2.BlockID += 1
	con2 := NewDynamicContainer(bufMgr, id2, step)
	assert.NotNil(t, con2)
	err = con2.Allocate()
	assert.Nil(t, err)

	err = con2.Allocate()
	assert.NotNil(t, err)
	assert.Equal(t, step, con2.GetCapacity())

	con.Unpin()
	err = con2.Allocate()
	assert.Nil(t, err)
	assert.Equal(t, step*2, con2.GetCapacity())
	assert.Equal(t, capacity, bufMgr.GetUsage())

	con.Close()
	con2.Close()
	assert.Equal(t, uint64(0), con.GetCapacity())
	assert.Equal(t, uint64(0), con2.GetCapacity())
	assert.Equal(t, capacity, bufMgr.GetCapacity())
}
