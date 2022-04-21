package tables

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
)

type tableHandle struct {
	table    *dataTable
	block    *dataBlock
	appender data.BlockAppender
}

func newHandle(table *dataTable, block *dataBlock) *tableHandle {
	h := &tableHandle{
		table: table,
		block: block,
	}
	if block != nil {
		h.appender, _ = block.MakeAppender()
	}
	return h
}

func (h *tableHandle) SetAppender(id *common.ID) (appender data.BlockAppender) {
	tableMeta := h.table.meta
	segMeta, _ := tableMeta.GetSegmentByID(id.SegmentID)
	blkMeta, _ := segMeta.GetBlockEntryByID(id.BlockID)
	h.block = blkMeta.GetBlockData().(*dataBlock)
	h.appender, _ = h.block.MakeAppender()

	return h.appender
}

func (h *tableHandle) GetAppender() (appender data.BlockAppender, err error) {
	var segEntry *catalog.SegmentEntry
	if h.appender == nil {
		segEntry = h.table.meta.LastAppendableSegmemt()
		if segEntry == nil {
			err = data.ErrAppendableSegmentNotFound
			return
		}
		blkEntry := segEntry.LastAppendableBlock()
		h.block = blkEntry.GetBlockData().(*dataBlock)
		h.appender, err = h.block.MakeAppender()
		if err != nil {
			panic(err)
		}
	}
	if !h.appender.IsAppendable() {
		id := h.appender.GetID()
		segEntry, _ = h.table.meta.GetSegmentByID(id.SegmentID)
		if segEntry.GetAppendableBlockCnt() >= int(segEntry.GetTable().GetSchema().SegmentMaxBlocks) {
			err = data.ErrAppendableSegmentNotFound
		} else {
			err = data.ErrAppendableBlockNotFound

			appender = h.appender
		}
		h.block = nil
		h.appender = nil
		return
	}
	appender = h.appender
	return
}
