package coldata

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	tbl "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpgradeBlkOp(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	info := md.MockInfo(row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData := tbl.NewTableData(fsMgr, bufMgr, bufMgr, bufMgr, tableMeta)
	segIds := tbl.MockSegments(tableMeta, tableData)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
	}

	for _, segID := range segIds {
		segMeta, err := tableMeta.ReferenceSegment(segID)
		assert.Nil(t, err)
		cpCtx := md.CopyCtx{}
		blkMeta, err := segMeta.CloneBlock(segMeta.Blocks[0].ID, cpCtx)
		assert.Nil(t, err)
		ctx := new(OpCtx)
		ctx.Opts = opts
		ctx.BlkMeta = blkMeta
		op := NewUpgradeBlkOp(ctx, tableData)
		op.Push()
		err = op.WaitDone()
		assert.Nil(t, err)
		blk := op.Block
		assert.Equal(t, base.PERSISTENT_BLK, blk.GetType())
		blk.Unref()

		op2 := NewUpgradeSegOp(ctx, segID, tableData)
		op2.Push()
		err = op2.WaitDone()
		assert.Nil(t, err)
		seg := op2.Segment
		assert.Equal(t, base.SORTED_SEG, seg.GetType())
		seg.Unref()
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	t.Log(bufMgr.String())
	opts.MemData.Updater.Stop()
}

func TestUpgradeSegOp(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	opts.MemData.Updater.Start()
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	info := md.MockInfo(row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData := tbl.NewTableData(fsMgr, bufMgr, bufMgr, bufMgr, tableMeta)
	segIds := tbl.MockSegments(tableMeta, tableData)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
	}

	for _, segID := range segIds {
		ctx := new(OpCtx)
		ctx.Opts = opts
		op := NewUpgradeSegOp(ctx, segID, tableData)
		op.Push()
		err := op.WaitDone()
		assert.Nil(t, err)
		seg := op.Segment
		assert.Equal(t, base.SORTED_SEG, seg.GetType())
		seg.Unref()
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	// t.Log(bufMgr.String())
	opts.MemData.Updater.Stop()
}
