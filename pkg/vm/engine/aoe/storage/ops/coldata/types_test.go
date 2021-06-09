package coldata

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpgradeSegOp(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	info := md.MockInfo(row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData := table.NewTableData(bufMgr, bufMgr, tableMeta)

	segIDs := table.MockSegments(bufMgr, bufMgr, tableMeta, tableData)
	assert.Equal(t, uint64(seg_cnt), tableData.GetSegmentCount())

	segs := make([]col.IColumnSegment, 0)

	for idx, segID := range segIDs {
		ctx := new(OpCtx)
		ctx.Opts = opts
		op := NewUpgradeSegOp(ctx, segID, tableData)
		op.Push()
		op.WaitDone()
		for _, seg := range op.Segments {
			assert.Equal(t, col.SORTED_SEG, seg.GetSegmentType())
			nextSeg := seg.GetNext()
			if idx < int(seg_cnt)-1 {
				assert.NotNil(t, nextSeg)
				nextSeg.UnRef()
			} else {
				assert.Nil(t, nextSeg)
			}
			seg.UnRef()
			segs = append(segs, seg)
		}
	}

	opts.MemData.Updater.Stop()
}

func TestUpgradeBlkOp(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	info := md.NewMetaInfo(&md.Configuration{
		BlockMaxRows:     uint64(10),
		SegmentMaxBlocks: uint64(2),
	})
	row_count := info.Conf.BlockMaxRows
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	segCnt := uint64(2)
	blkCnt := segCnt * info.Conf.SegmentMaxBlocks

	tableMeta := md.MockTable(info, schema, blkCnt)
	tableData := table.NewTableData(bufMgr, bufMgr, tableMeta)
	segIDs := table.MockSegments(bufMgr, bufMgr, tableMeta, tableData)
	assert.Equal(t, uint64(segCnt), tableData.GetSegmentCount())
	t.Log(bufMgr.String())
	assert.Equal(t, int(blkCnt)*len(schema.ColDefs), bufMgr.NodeCount())
	for _, segID := range segIDs {
		t.Logf("seg %s", segID.SegmentString())
	}
	for _, segID := range segIDs {
		var ops []*UpgradeBlkOp
		segMeta, _ := tableMeta.ReferenceSegment(segID.SegmentID)
		blkID := segMeta.Blocks[0].AsCommonID()
		ctx := new(OpCtx)
		ctx.Opts = opts
		// ctx.BlkMeta =
		t.Logf("upgrade blk %s", blkID.BlockString())
		op := NewUpgradeBlkOp(ctx, *blkID, tableData)
		op.Push()
		ops = append(ops, op)

		op = NewUpgradeBlkOp(ctx, *blkID, tableData)
		op.Push()
		ops = append(ops, op)

		for idx, op := range ops {
			op.WaitDone()
			assert.Equal(t, len(schema.ColDefs), len(op.Blocks))
			for _, blk := range op.Blocks {
				if idx == 0 {
					assert.Equal(t, col.PERSISTENT_BLK, blk.GetBlockType())
				} else if idx == 1 {
					assert.Equal(t, col.PERSISTENT_SORTED_BLK, blk.GetBlockType())
				}
				blk.UnRef()
			}
		}
	}
	for i := 0; i < 0; i++ {
		runtime.GC()
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	t.Log(bufMgr.String())
	assert.Equal(t, int(blkCnt)*len(schema.ColDefs), bufMgr.NodeCount())

	opts.MemData.Updater.Stop()
}
