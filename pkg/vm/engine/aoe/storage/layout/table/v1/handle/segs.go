package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	table "matrixone/pkg/vm/engine/aoe/storage/layout/table/v1"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle/base"
	// log "github.com/sirupsen/logrus"
)

type SegmentsHandle struct {
	IDS       []common.ID
	ColIdxes  []int
	LinkAll   bool
	TableData table.ITableData
}

func NewSegmentsHandle(segIds []common.ID, colIdxes []int, tableData table.ITableData) *SegmentsHandle {
	h := &SegmentsHandle{
		IDS:       segIds,
		ColIdxes:  colIdxes,
		TableData: tableData,
		LinkAll:   false,
	}
	return h
}

func NewAllSegmentsHandle(colIdxes []int, tableData table.ITableData) *SegmentsHandle {
	h := &SegmentsHandle{
		ColIdxes:  colIdxes,
		LinkAll:   true,
		TableData: tableData,
	}
	return h
}

func (sh *SegmentsHandle) NewBlkIt() base.IBlockIterator {
	segIt := sh.NewSegIt()
	if !segIt.Valid() {
		return nil
	}

	h := segIt.GetSegmentHandle()

	it := &BlockLinkIterator{
		SegIt: segIt,
		BlkIt: h.NewIterator(),
	}
	h.Close()
	return it
}

func (sh *SegmentsHandle) NewSegIt() base.ISegmentIterator {
	var it base.ISegmentIterator
	if sh.LinkAll {
		it = sh.newSegmentLinkIterator()
	} else {
		it = &SegmentIt{
			Pos:    0,
			Handle: sh,
		}
	}
	return it
}

func (sh *SegmentsHandle) newSegmentLinkIterator() base.ISegmentIterator {
	it := &SegmentLinkIt{}
	for _, colIdx := range sh.ColIdxes {
		colData := sh.TableData.GetCollumn(colIdx)
		colSeg := colData.GetSegmentRoot()
		if colSeg == nil {
			return nil
		}
		it.Cols = append(it.Cols, colSeg)
	}
	return it
}
