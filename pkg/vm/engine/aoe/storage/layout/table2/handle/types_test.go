package handle

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSnapshot(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	seg_cnt := 100
	blk_cnt := 64
	capacity := typeSize * row_count * uint64(seg_cnt) * uint64(blk_cnt) * 2
	indexBufMgr := bmgr.MockBufMgr(capacity)
	mtBufMgr := bmgr.MockBufMgr(capacity)
	sstBufMgr := bmgr.MockBufMgr(capacity)

	info := md.MockInfo(row_count, uint64(blk_cnt))
	tableMeta := md.MockTable(info, schema, uint64(blk_cnt*seg_cnt))

	tableData := table.NewTableData(ldio.DefaultFsMgr, indexBufMgr, mtBufMgr, sstBufMgr, tableMeta)
	segIDs := table.MockSegments(tableMeta, tableData)
	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())
	now := time.Now()

	cols := []int{0, 1}
	ss := NewSnapshot(segIDs, cols, tableData)
	segIt := ss.NewSegmentIt()
	actualSegCnt := 0
	actualBlkCnt := 0
	for segIt.Valid() {
		actualSegCnt++
		segment := segIt.GetHandle()
		blkIt := segment.NewIt()
		for blkIt.Valid() {
			actualBlkCnt++
			blk := blkIt.GetHandle()
			h := blk.Prefetch()
			h.Close()
			blk.Close()
			blkIt.Next()
		}
		blkIt.Close()
		segment.Close()
		segIt.Next()
	}
	segIt.Close()
	assert.Equal(t, seg_cnt, actualSegCnt)
	assert.Equal(t, seg_cnt*blk_cnt, actualBlkCnt)
	du := time.Since(now)
	t.Log(du)
	t.Log(sstBufMgr.String())
	ss.Close()
}
