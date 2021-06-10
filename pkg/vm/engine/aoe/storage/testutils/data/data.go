package data

import (
	"matrixone/pkg/container/types"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"testing"

	"github.com/stretchr/testify/assert"
)

func MakeSegment(bufMgr mgrif.IBufferManager, colIdx int, id common.ID, blkCnt int, rowCount, typeSize uint64, t *testing.T) col.IColumnSegment {
	colType := types.Type{types.T_int32, 4, 4, 0}
	seg := col.NewColumnSegment(bufMgr, bufMgr, id, colIdx, colType, col.UNSORTED_SEG)
	blk_id := id
	for i := 0; i < blkCnt; i++ {
		_, err := seg.RegisterBlock(blk_id.NextBlock(), rowCount)
		assert.Nil(t, err)
	}
	return seg
}

func MakeBufMagr(capacity uint64) mgrif.IBufferManager {
	flusher := w.NewOpWorker("Mock Flusher")
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	return bufMgr
}

func MakeSegments(bufMgr mgrif.IBufferManager, segCnt, blkCnt int, rowCount, typeSize uint64, tableData table.ITableData, t *testing.T) []common.ID {
	baseid := common.ID{}
	var segIDs []common.ID
	for i := 0; i < segCnt; i++ {
		var colSegs []col.IColumnSegment
		seg_id := baseid.NextSegment()
		for colIdx, _ := range tableData.GetColTypes() {
			colSeg := MakeSegment(bufMgr, colIdx, seg_id, blkCnt, rowCount, typeSize, t)
			colSegs = append(colSegs, colSeg)
		}
		tableData.AppendColSegments(colSegs)
		segIDs = append(segIDs, seg_id)
	}
	return segIDs
}
