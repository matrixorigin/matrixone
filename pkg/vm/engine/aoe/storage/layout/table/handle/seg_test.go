package handle

import (
	"matrixone/pkg/container/types"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func makeSegment(bufMgr mgrif.IBufferManager, colIdx int, id common.ID, blkCnt int, rowCount, typeSize uint64, t *testing.T) col.IColumnSegment {
	colType := types.Type{types.T_int32, 4, 4, 0}
	seg := col.NewColumnSegment(id, colIdx, colType, col.UNSORTED_SEG)
	blk_id := id
	for i := 0; i < blkCnt; i++ {
		_, err := seg.RegisterBlock(bufMgr, blk_id.NextBlock(), rowCount)
		assert.Nil(t, err)
	}
	return seg
}

func makeBufMagr(capacity uint64) mgrif.IBufferManager {
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	return bufMgr
}

func makeSegments(bufMgr mgrif.IBufferManager, segCnt, blkCnt int, rowCount, typeSize uint64, tableData table.ITableData, t *testing.T) []common.ID {
	baseid := common.ID{}
	var segIDs []common.ID
	for i := 0; i < segCnt; i++ {
		var colSegs []col.IColumnSegment
		seg_id := baseid.NextSegment()
		for colIdx, _ := range tableData.GetColTypes() {
			colSeg := makeSegment(bufMgr, colIdx, seg_id, blkCnt, rowCount, typeSize, t)
			colSegs = append(colSegs, colSeg)
		}
		tableData.AppendColSegments(colSegs)
		segIDs = append(segIDs, seg_id)
	}
	return segIDs
}

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
	bufMgr := makeBufMagr(capacity)
	t0 := uint64(0)
	tableData := table.NewTableData(bufMgr, t0, colDefs)
	segIDs := makeSegments(bufMgr, seg_cnt, blk_cnt, row_count, typeSize, tableData, t)
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
			assert.Equal(t, blk_cnt, segBlks)
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
			lit.Next()
		}
		assert.Equal(t, seg_cnt*blk_cnt, totalBlks)
	}
	du := time.Since(now)
	t.Log(du)
}
