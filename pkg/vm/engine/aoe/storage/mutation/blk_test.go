package mutation

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"matrixone/pkg/vm/engine/aoe/storage/mutation/buffer"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMutableBlockNode(t *testing.T) {
	dir := "/tmp/mutableblk"
	rowCount, blkCount := uint64(10), uint64(4)
	info := metadata.MockInfo(&sync.RWMutex{}, rowCount, blkCount)
	info.Conf.Dir = dir
	os.RemoveAll(dir)
	schema := metadata.MockSchema(2)
	tablemeta := metadata.MockTable(info, schema, 2)

	blkmeta, err := tablemeta.ReferenceBlock(uint64(1), uint64(1))
	assert.Nil(t, err)

	segfile := dataio.NewUnsortedSegmentFile(dir, *blkmeta.Segment.AsCommonID())
	tblkfile := dataio.NewTBlockFile(segfile, *blkmeta.AsCommonID())
	assert.NotNil(t, tblkfile)
	capacity := uint64(4096)
	fsMgr := ldio.DefaultFsMgr
	indexBufMgr := bm.NewBufferManager(dir, capacity)
	mtBufMgr := bm.NewBufferManager(dir, capacity)
	sstBufMgr := bm.NewBufferManager(dir, capacity)
	tabledata := table.NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tablemeta)

	maxsize := uint64(100)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	blknode := NewMutableBlockNode(mgr, tblkfile, tabledata, blkmeta)
	mgr.RegisterNode(blknode)
	h := mgr.Pin(blknode)
	assert.NotNil(t, h)
	delta := uint64(10)

	bat := chunk.MockBatch(schema.Types(), delta)
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
	err = blknode.Expand(delta*6, insert(blknode))
	assert.Nil(t, err)
	t.Logf("length=%d", blknode.Data.Length())
	err = blknode.Expand(delta, insert(blknode))
	assert.NotNil(t, err)
	t.Logf("length=%d", blknode.Data.Length())
	assert.Equal(t, delta*6, mgr.Total())

	blkmeta2, err := tablemeta.ReferenceBlock(uint64(1), uint64(2))
	assert.Nil(t, err)
	tblkfile2 := dataio.NewTBlockFile(segfile, *blkmeta.AsCommonID())
	node2 := NewMutableBlockNode(mgr, tblkfile2, tabledata, blkmeta2)
	mgr.RegisterNode(node2)
	h2 := mgr.Pin(node2)
	assert.NotNil(t, h2)

	err = node2.Expand(delta*6, insert(node2))
	assert.NotNil(t, err)

	h.Close()

	err = node2.Expand(delta*6, insert(node2))
	assert.Nil(t, err)

	t.Log(mgr.String())
}
