package mutation

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/db/sched"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"matrixone/pkg/vm/engine/aoe/storage/testutils/config"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMutableBlockNode(t *testing.T) {
	dir := "/tmp/mutableblk"
	opts := config.NewOptions(dir, config.CST_None, config.BST_S, config.SST_S)
	rowCount, blkCount := uint64(30), uint64(4)
	info := metadata.MockInfo(&sync.RWMutex{}, rowCount, blkCount)
	info.Conf.Dir = dir
	opts.Meta.Info = info
	opts.Scheduler = sched.NewScheduler(opts, nil)
	os.RemoveAll(dir)
	schema := metadata.MockSchema(2)
	tablemeta := metadata.MockTable(info, schema, 2)

	meta1, err := tablemeta.ReferenceBlock(uint64(1), uint64(1))
	assert.Nil(t, err)
	meta2, err := tablemeta.ReferenceBlock(uint64(1), uint64(2))
	assert.Nil(t, err)

	segfile := dataio.NewUnsortedSegmentFile(dir, *meta1.Segment.AsCommonID())
	tblkfile := dataio.NewTBlockFile(segfile, *meta1.AsCommonID())
	assert.NotNil(t, tblkfile)
	capacity := uint64(4096)
	fsMgr := ldio.DefaultFsMgr
	indexBufMgr := bm.NewBufferManager(dir, capacity)
	mtBufMgr := bm.NewBufferManager(dir, capacity)
	sstBufMgr := bm.NewBufferManager(dir, capacity)
	tables := table.NewTables(new(sync.RWMutex), fsMgr, mtBufMgr, sstBufMgr, indexBufMgr)
	tabledata, err := tables.RegisterTable(tablemeta)

	maxsize := uint64(140)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	node1 := NewMutableBlockNode(mgr, tblkfile, tabledata, meta1, nil)
	mgr.RegisterNode(node1)
	h1 := mgr.Pin(node1)
	assert.NotNil(t, h1)
	rows := uint64(10)
	factor := uint64(4)

	bat := chunk.MockBatch(schema.Types(), rows)
	insert := func(n *MutableBlockNode) func() error {
		return func() error {
			for idx, attr := range n.Data.GetAttrs() {
				if _, err = n.Data.GetVectorByAttr(attr).AppendVector(bat.Vecs[idx], 0); err != nil {
					return err
				}
				// assert.Nil(t, err)
			}
			return nil
		}
	}
	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	t.Logf("length=%d", node1.Data.Length())
	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	t.Logf("length=%d", node1.Data.Length())
	assert.Equal(t, rows*factor*2, mgr.Total())

	blkmeta2, err := tablemeta.ReferenceBlock(uint64(1), uint64(2))
	assert.Nil(t, err)
	tblkfile2 := dataio.NewTBlockFile(segfile, *meta2.AsCommonID())
	node2 := NewMutableBlockNode(mgr, tblkfile2, tabledata, blkmeta2, nil)
	mgr.RegisterNode(node2)
	h2 := mgr.Pin(node2)
	assert.NotNil(t, h2)

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)
	err = node2.Expand(rows*factor, insert(node2))
	assert.NotNil(t, err)

	h1.Close()

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)

	h2.Close()
	h1 = mgr.Pin(node1)
	assert.Equal(t, int(rows*2), node1.Data.Length())

	err = node1.Expand(rows*factor, insert(node1))
	assert.Nil(t, err)
	h1.Close()
	t.Log(mgr.String())
	h2 = mgr.Pin(node2)
	assert.NotNil(t, h2)

	err = node2.Expand(rows*factor, insert(node2))
	assert.Nil(t, err)
	t.Log(mgr.String())
	err = node2.Flush()
	assert.Nil(t, err)
	t.Log(mgr.String())

	h2.Close()
	h1 = mgr.Pin(node1)

	t.Log(mgr.String())
	t.Log(common.GPool.String())
}
