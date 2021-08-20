package mutation

import (
	bm "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
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
	tablemeta := metadata.MockTable(info, schema, 1)

	blkmeta, err := tablemeta.ReferenceBlock(uint64(1), uint64(1))
	assert.Nil(t, err)

	segfile := dataio.NewUnsortedSegmentFile(dir, *blkmeta.Segment.AsCommonID())
	tblkfile := dataio.NewTBlockFile(segfile, *blkmeta.AsCommonID())
	capacity := uint64(4096)
	fsMgr := ldio.DefaultFsMgr
	indexBufMgr := bm.NewBufferManager(dir, capacity)
	mtBufMgr := bm.NewBufferManager(dir, capacity)
	sstBufMgr := bm.NewBufferManager(dir, capacity)
	tabledata := table.NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tablemeta)

	maxsize := uint64(1000)
	evicter := bm.NewSimpleEvictHolder()
	mgr := buffer.NewNodeManager(maxsize, evicter)
	blknode := NewMutableBlockNode(mgr, tblkfile, tabledata, blkmeta)
	mgr.RegisterNode(blknode)
	h := mgr.Pin(blknode)
	assert.NotNil(t, h)

	t.Log(mgr.String())
}
