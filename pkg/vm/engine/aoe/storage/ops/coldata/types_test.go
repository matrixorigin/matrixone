package coldata

import (
	"github.com/stretchr/testify/assert"
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	mgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	mock "matrixone/pkg/vm/engine/aoe/storage/mock/type"
	w "matrixone/pkg/vm/engine/aoe/storage/worker"
	"runtime"
	"testing"
	"time"
	// log "github.com/sirupsen/logrus"
)

func makeBufMagr(capacity uint64) mgrif.IBufferManager {
	flusher := w.NewOpWorker()
	bufMgr := bmgr.NewBufferManager(capacity, flusher)
	return bufMgr
}

func makeSegment(bufMgr mgrif.IBufferManager, colIdx int, id layout.ID, blkCnt int, rowCount, typeSize uint64, t *testing.T) col.IColumnSegment {
	colType := mock.INTEGER
	seg := col.NewColumnSegment(id, colIdx, colType, col.UNSORTED_SEG)
	blk_id := id
	for i := 0; i < blkCnt; i++ {
		_, err := seg.RegisterBlock(bufMgr, blk_id.NextBlock(), rowCount)
		assert.Nil(t, err)
	}
	return seg
}

func makeSegments(bufMgr mgrif.IBufferManager, segCnt, blkCnt int, rowCount, typeSize uint64, tableData table.ITableData, t *testing.T) []layout.ID {
	baseid := layout.ID{}
	var segIDs []layout.ID
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

func TestUpgradeSegOp(t *testing.T) {
	colDefs := make([]mock.ColType, 2)
	colDefs[0] = mock.INTEGER
	colDefs[1] = mock.INTEGER
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := colDefs[0].Size()
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := makeBufMagr(capacity)
	t0 := uint64(0)
	tableData := table.NewTableData(bufMgr, t0, colDefs)
	seg_cnt := 4
	blk_cnt := 4
	segIDs := makeSegments(bufMgr, seg_cnt, blk_cnt, row_count, typeSize, tableData, t)
	assert.Equal(t, uint64(seg_cnt), tableData.GetSegmentCount())

	for idx, segID := range segIDs {
		ctx := new(OpCtx)
		ctx.Opts = opts
		op := NewUpgradeSegOp(ctx, segID, tableData)
		op.Push()
		op.WaitDone()
		for _, seg := range op.Segments {
			assert.Equal(t, col.SORTED_SEG, seg.GetSegmentType())
			if idx < seg_cnt-1 {
				assert.NotNil(t, seg.GetNext())
			} else {
				assert.Nil(t, seg.GetNext())
			}
		}
	}

	opts.MemData.Updater.Stop()
}

func TestUpgradeBlkOp(t *testing.T) {
	colDefs := make([]mock.ColType, 2)
	colDefs[0] = mock.INTEGER
	colDefs[1] = mock.INTEGER
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := colDefs[0].Size()
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := makeBufMagr(capacity)
	t0 := uint64(0)
	tableData := table.NewTableData(bufMgr, t0, colDefs)
	seg_cnt := 2
	blk_cnt := 2
	segIDs := makeSegments(bufMgr, seg_cnt, blk_cnt, row_count, typeSize, tableData, t)
	assert.Equal(t, uint64(seg_cnt), tableData.GetSegmentCount())
	t.Log(bufMgr.String())
	assert.Equal(t, seg_cnt*blk_cnt, bufMgr.NodeCount())
	for _, segID := range segIDs {
		var ops []*UpgradeBlkOp
		blkID := segID
		ctx := new(OpCtx)
		ctx.Opts = opts
		op := NewUpgradeBlkOp(ctx, blkID, tableData)
		op.Push()
		ops = append(ops, op)

		op = NewUpgradeBlkOp(ctx, blkID, tableData)
		op.Push()
		ops = append(ops, op)

		for idx, op := range ops {
			op.WaitDone()
			assert.Equal(t, len(colDefs), len(op.Blocks))
			for _, blk := range op.Blocks {
				if idx == 0 {
					assert.Equal(t, col.PERSISTENT_BLK, blk.GetBlockType())
				} else if idx == 1 {
					assert.Equal(t, col.PERSISTENT_SORTED_BLK, blk.GetBlockType())
				}
			}
		}
	}
	for i := 0; i < 4; i++ {
		runtime.GC()
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	t.Log(bufMgr.String())
	assert.Equal(t, seg_cnt*blk_cnt, bufMgr.NodeCount())
	opts.MemData.Updater.Stop()
}
