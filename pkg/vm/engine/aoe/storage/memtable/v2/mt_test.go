package memtable

import (
	engine "matrixone/pkg/vm/engine/aoe/storage"
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	// "matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"testing"

	"github.com/stretchr/testify/assert"
)

func createNodeMgr(maxsize uint64) *nodeManager {
	evicter := bm.NewSimpleEvictHolder()
	nmgr := newNodeManager(maxsize, evicter)
	return nmgr
}

func TestAll(t *testing.T) {
	dir := "/tmp/testmemtable"
	maxsize := uint64(100)
	nmgr := createNodeMgr(maxsize)
	opts := new(engine.Options)
	opts.FillDefaults(dir)
	mgr := NewManager(opts, nmgr)

	schema := metadata.MockSchema(2)
	tablemeta := metadata.MockTable(opts.Meta.Info, schema, 1)
	// blkmeta, err := tablemeta.ReferenceBlock(uint64(1), uint64(1))
	// assert.Nil(t, err)

	fsmgr := ldio.DefaultFsMgr
	indexbufmgr := bm.NewBufferManager(dir, 100000)
	mtbufmgr := bm.NewBufferManager(dir, 100000)
	sstbufmgr := bm.NewBufferManager(dir, 100000)
	tabledata := table.NewTableData(fsmgr, indexbufmgr, mtbufmgr, sstbufmgr, tablemeta)

	_, err := mgr.RegisterCollection(tabledata)
	assert.Nil(t, err)

	// bat := chunk.MockBatch(schema.Types(), tablemeta.Conf.BlockMaxRows/2)
}
