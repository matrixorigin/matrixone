package memtable

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/events/meta"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	mbase "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createNodeMgr(maxsize uint64) mbase.INodeManager {
	evicter := bm.NewSimpleEvictHolder()
	nmgr := buffer.NewNodeManager(maxsize, evicter)
	return nmgr
}

func TestAll(t *testing.T) {
	dir := "/tmp/testmemtable"
	os.RemoveAll(dir)
	maxsize := uint64(100)
	nmgr := createNodeMgr(maxsize)
	opts := new(engine.Options)
	opts.FillDefaults(dir)
	mgr := NewManager(opts, nmgr)

	tables := table.NewTables(&sync.RWMutex{})
	opts.Scheduler = sched.NewScheduler(opts, tables)

	info := metadata.MockTableInfo(2)
	ctx := &sched.Context{
		Opts:     opts,
		Waitable: true,
	}
	event := meta.NewCreateTableEvent(ctx, dbi.TableOpCtx{TableName: info.Name}, info)
	assert.NotNil(t, event)
	opts.Scheduler.Schedule(event)
	err := event.WaitDone()
	assert.Nil(t, err)

	tablemeta := event.GetTable()

	fsmgr := ldio.DefaultFsMgr
	indexbufmgr := bm.NewBufferManager(dir, 100000)
	mtbufmgr := bm.NewBufferManager(dir, 100000)
	sstbufmgr := bm.NewBufferManager(dir, 100000)
	tabledata := table.NewTableData(fsmgr, indexbufmgr, mtbufmgr, sstbufmgr, tablemeta)

	err = tables.CreateTable(tabledata)
	assert.Nil(t, err)

	c, err := mgr.RegisterCollection(tabledata)
	assert.Nil(t, err)
	assert.NotNil(t, c)
	bat := chunk.MockBatch(tablemeta.Schema.Types(), tablemeta.Conf.BlockMaxRows/2)
	assert.NotNil(t, bat)
	// err = c.Append(bat, &metadata.LogIndex{
	// 	ID:       uint64(1),
	// 	Capacity: uint64(bat.Vecs[0].Length()),
	// })
	// assert.Nil(t, err)
}
