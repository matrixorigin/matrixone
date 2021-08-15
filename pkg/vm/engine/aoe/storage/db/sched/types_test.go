package sched

import (
	e "matrixone/pkg/vm/engine/aoe/storage"
	bmgr "matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	ldio "matrixone/pkg/vm/engine/aoe/storage/layout/dataio"
	tbl "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUpgradeBlk(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	info := md.MockInfo(&opts.Mu, row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData := tbl.NewTableData(fsMgr, bufMgr, bufMgr, bufMgr, tableMeta)
	segIds := tbl.MockSegments(tableMeta, tableData)
	tables := tbl.NewTables(new(sync.RWMutex))
	opts.Scheduler = NewScheduler(opts, tables)

	err := tables.CreateTable(tableData)
	assert.Nil(t, err)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
	}

	for _, segID := range segIds[:1] {
		segMeta, err := tableMeta.ReferenceSegment(segID)
		assert.Nil(t, err)
		cpCtx := md.CopyCtx{}
		blkMeta, err := segMeta.CloneBlock(segMeta.Blocks[0].ID, cpCtx)
		assert.Nil(t, err)
		ctx := &Context{
			Opts:     opts,
			Waitable: true,
		}
		ctx.RemoveDataScope()
		tableData.Ref()
		e := NewUpgradeBlkEvent(ctx, blkMeta, tableData)
		opts.Scheduler.Schedule(e)
		err = e.WaitDone()
		assert.Nil(t, err)
		blk := e.Data
		assert.Equal(t, base.PERSISTENT_BLK, blk.GetType())
		blk.Unref()

		segMeta.TryClose()
		seg := tableData.StrongRefSegment(segID)
		assert.NotNil(t, seg)
		tableData.Ref()
		upseg := NewUpgradeSegEvent(ctx, seg, tableData)
		opts.Scheduler.Schedule(upseg)
		err = upseg.WaitDone()
		assert.Nil(t, err)
		assert.Equal(t, base.SORTED_SEG, upseg.Segment.GetType())
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	t.Log(bufMgr.String())
	opts.Scheduler.Stop()
}

func TestUpgradeSeg(t *testing.T) {
	schema := md.MockSchema(2)
	opts := new(e.Options)
	opts.FillDefaults("/tmp")
	typeSize := uint64(schema.ColDefs[0].Type.Size)
	row_count := uint64(64)
	capacity := typeSize * row_count * 10000
	bufMgr := bmgr.MockBufMgr(capacity)
	fsMgr := ldio.NewManager("/tmp", true)
	seg_cnt := uint64(4)
	blk_cnt := uint64(4)

	info := md.MockInfo(&opts.Mu, row_count, blk_cnt)
	tableMeta := md.MockTable(info, schema, seg_cnt*blk_cnt)
	tableData := tbl.NewTableData(fsMgr, bufMgr, bufMgr, bufMgr, tableMeta)
	segIds := tbl.MockSegments(tableMeta, tableData)

	tables := tbl.NewTables(new(sync.RWMutex))
	opts.Scheduler = NewScheduler(opts, tables)

	err := tables.CreateTable(tableData)
	assert.Nil(t, err)

	assert.Equal(t, uint32(seg_cnt), tableData.GetSegmentCount())

	for _, segMeta := range tableMeta.Segments {
		for _, blkMeta := range segMeta.Blocks {
			blkMeta.DataState = md.FULL
			blkMeta.Count = blkMeta.MaxRowCount
		}
		segMeta.TryClose()
	}

	for _, segID := range segIds {
		ctx := &Context{
			Waitable: true,
			Opts:     opts,
		}
		old := tableData.StrongRefSegment(segID)
		tableData.Ref()
		e := NewUpgradeSegEvent(ctx, old, tableData)
		opts.Scheduler.Schedule(e)
		err = e.WaitDone()
		assert.Nil(t, err)
		seg := e.Segment
		assert.Equal(t, base.SORTED_SEG, seg.GetType())
	}
	t.Log(fsMgr.String())
	t.Log(tableData.String())
	// t.Log(bufMgr.String())
	opts.Scheduler.Stop()
}
