package handle

import (
	"matrixone/pkg/container/types"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	tutil "matrixone/pkg/vm/engine/aoe/storage/testutils/data"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSegmentHandle(t *testing.T) {
	colDefs := make([]types.Type, 2)
	colDefs[0] = types.Type{types.T_int32, 4, 4, 0}
	colDefs[1] = types.Type{types.T_int32, 4, 4, 0}
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(colDefs[0].Size)
	row_count := uint64(64)
	seg_cnt := 100
	blk_cnt := 64
	capacity := typeSize * row_count * uint64(seg_cnt) * uint64(blk_cnt) * 2
	bufMgr := tutil.MakeBufMagr(capacity)
	t0 := uint64(0)
	tableData := table.NewTableData(bufMgr, t0, colDefs)
	segIDs := tutil.MakeSegments(bufMgr, seg_cnt, blk_cnt, row_count, typeSize, tableData, t)
	assert.Equal(t, uint64(seg_cnt), tableData.GetSegmentCount())

	cols := []int{0, 1}
	handle1 := NewSegmentsHandle(segIDs, cols, tableData)
	handle2 := NewAllSegmentsHandle(cols, tableData)

	now := time.Now()
	handles := []*SegmentsHandle{handle1, handle2}

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
