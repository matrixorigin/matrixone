package table

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	dio "matrixone/pkg/vm/engine/aoe/storage/dataio"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"testing"

	"github.com/stretchr/testify/assert"
)

var WORK_DIR = "/tmp/layout/data_test"

func init() {
	dio.WRITER_FACTORY.Init(nil, WORK_DIR)
	dio.READER_FACTORY.Init(nil, WORK_DIR)
}

func TestBase1(t *testing.T) {
	opts := new(e.Options)
	opts.FillDefaults(WORK_DIR)
	segCnt := uint64(4)
	blkCnt := uint64(4)
	rowCount := uint64(10)
	capacity := uint64(200)
	info := md.MockInfo(&opts.Mu, rowCount, blkCnt)
	schema := md.MockSchema(2)
	tableMeta := md.MockTable(info, schema, segCnt*blkCnt)

	fsMgr := ldio.NewManager(WORK_DIR, true)
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(2000)
	sstBufMgr := bmgr.MockBufMgr(capacity)
	tblData := NewTableData(fsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tableMeta)

	idx := 0
	segIds := tableMeta.SegmentIDs()
	ids := make([]uint64, 0)
	for segId, _ := range segIds {
		delta := 0
		if idx > 0 {
			delta = 1
		}
		idx++
		ids = append(ids, segId)
		segMeta, err := tableMeta.ReferenceSegment(segId)
		assert.Nil(t, err)
		seg, err := tblData.RegisterSegment(segMeta)
		assert.Nil(t, err)
		seg.Unref()
		assert.Equal(t, int64(1+delta), seg.RefCount())

		refSeg := tblData.StrongRefSegment(segId)
		refSeg.Unref()
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		refSeg = tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())

		blkIds := segMeta.BlockIDs()
		id := 0
		for blkId, _ := range blkIds {
			idelta := 0
			if id > 0 {
				idelta = 1
			}
			id++
			blkMeta, err := segMeta.ReferenceBlock(blkId)
			assert.Nil(t, err)
			blk, err := tblData.RegisterBlock(blkMeta)
			assert.Nil(t, err)
			blk.Unref()
			assert.Equal(t, int64(1+idelta), blk.RefCount())
		}
		refSeg = tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
	}

	assert.Equal(t, segCnt, uint64(tblData.GetSegmentCount()))

	t.Log(tblData.String())
	t.Log(fsMgr.String())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
	}

	for id, segId := range ids {
		delta := 0
		if id > 0 {
			delta = 1
		}
		segMeta, err := tableMeta.ReferenceSegment(segId)
		assert.Nil(t, err)
		blkIds := segMeta.BlockIDs()
		refSeg := tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		for blkId, _ := range blkIds {
			blkMeta, err := segMeta.ReferenceBlock(blkId)
			assert.Nil(t, err)
			upgraded, err := tblData.UpgradeBlock(blkMeta)
			assert.Nil(t, err)
			upgraded.Unref()
			// assert.Equal(t, int64(1), upgraded.RefCount())
		}
	}

	t.Log(tblData.String())
	t.Log(fsMgr.String())
	for id, segId := range ids {
		delta := 0
		if id > 0 {
			delta = 1
		}
		refSeg := tblData.WeakRefSegment(segId)
		assert.Equal(t, int64(1+delta), refSeg.RefCount())
		upgraded, err := tblData.UpgradeSegment(segId)
		assert.Nil(t, err)
		upgraded.Unref()
		assert.Equal(t, int64(1+delta), upgraded.RefCount())
	}

	for segId, _ := range segIds {
		segMeta, err := tableMeta.ReferenceSegment(segId)
		assert.Nil(t, err)
		blkIds := segMeta.BlockIDs()
		for blkId, _ := range blkIds {
			refBlk := tblData.WeakRefBlock(segId, blkId)
			assert.NotNil(t, refBlk)
			// handle := refBlk.GetBlockHandle()
			// handle.Close()
		}

	}
	t.Log(tblData.String())
	t.Log(mtBufMgr.String())
	t.Log(sstBufMgr.String())
}
