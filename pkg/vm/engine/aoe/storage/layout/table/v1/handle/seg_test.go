package handle

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegmentHandle(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	seg_cnt := 100
	blk_cnt := 64
	capacity := typeSize * row_count * uint64(seg_cnt) * uint64(blk_cnt) * 2
	bufMgr := bmgr.MockBufMgr(capacity)

	info := md.MockInfo(row_count, uint64(blk_cnt))
	tableMeta := md.MockTable(info, schema, uint64(blk_cnt*seg_cnt))

	tableData := table.NewTableData(ldio.DefaultFsMgr, bufMgr, bufMgr, bufMgr, tableMeta)
	segIDs := table.MockSegments(ldio.DefaultFsMgr, bufMgr, bufMgr, tableMeta, tableData)
	assert.Equal(t, uint64(seg_cnt), tableData.GetSegmentCount())
	// t.Log(bufMgr.String())

	cols := []int{0, 1}
	handle1 := NewSegmentsHandle(segIDs, cols, tableData)
	handle2 := NewAllSegmentsHandle(cols, tableData)

	now := time.Now()
	handles := []*SegmentsHandle{handle2, handle1}
	handles = []*SegmentsHandle{handle1}

	for _, handle := range handles {
		sit := handle.NewSegIt()
		assert.True(t, sit.Valid())
		totalSegCnt := 0
		for sit.Valid() {
			totalSegCnt++
			segHandle := sit.GetSegmentHandle()
			assert.NotNil(t, segHandle)
			blkit := segHandle.NewIterator()
			assert.True(t, blkit.Valid())
			segBlks := 0
			for blkit.Valid() {
				segBlks++
				blkit.Next()
			}
			blkit.Close()
			assert.Equal(t, blk_cnt, segBlks)
			segHandle.Close()
			sit.Next()
		}
		assert.Equal(t, seg_cnt, totalSegCnt)

		lit := handle.NewBlkIt()
		assert.True(t, lit.Valid())
		totalBlks := 0
		for lit.Valid() {
			totalBlks++
			blkHandle := lit.GetBlockHandle()
			assert.NotNil(t, blkHandle)
			blkHandle.Close()
			lit.Next()
		}
		assert.Equal(t, seg_cnt*blk_cnt, totalBlks)
		lit.Close()
	}
	du := time.Since(now)
	t.Log(du)
}
