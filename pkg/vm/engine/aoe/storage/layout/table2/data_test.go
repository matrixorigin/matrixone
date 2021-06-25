package table

import (
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"testing"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/layout/data_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestBase1(t *testing.T) {
	segCnt := uint64(4)
	blkCnt := uint64(4)
	rowCount := uint64(10)
	capacity := uint64(10000)
	info := md.MockInfo(rowCount, blkCnt)
	schema := md.MockSchema(2)
	tableMeta := md.MockTable(info, schema, segCnt*blkCnt)

	fsMgr := ldio.NewManager(WORK_DIR, false)
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(capacity)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	tblData := NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tableMeta)

	segIds := tableMeta.SegmentIDs()
	for segId, _ := range segIds {
		segMeta, err := tableMeta.ReferenceSegment(segId)
		assert.Nil(t, err)
		seg, err := tblData.RegisterSegment(segMeta)
		assert.Nil(t, err)
		seg.Unref()
		assert.Equal(t, int64(1), seg.RefCount())

		refSeg := tblData.StrongRefSegment(segId)
		refSeg.Unref()
		assert.Equal(t, int64(1), seg.RefCount())
		refSeg = tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1), seg.RefCount())

		blkIds := segMeta.BlockIDs()
		for blkId, _ := range blkIds {
			blkMeta, err := segMeta.ReferenceBlock(blkId)
			assert.Nil(t, err)
			blk, err := tblData.RegisterBlock(blkMeta)
			assert.Nil(t, err)
			blk.Unref()
			assert.Equal(t, int64(1), blk.RefCount())
		}
	}

	assert.Equal(t, segCnt, uint64(tblData.GetSegmentCount()))

	t.Log(tblData.String())
	t.Log(fsMgr.String())
}
