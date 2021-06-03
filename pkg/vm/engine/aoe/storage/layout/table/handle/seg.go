package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
)

type SegmentHandle struct {
	ID   common.ID
	Cols []col.IColumnSegment
}

func (sh *SegmentHandle) NewIterator() base.IBlockIterator {
	it := &BlockIt{
		Cols: make([]col.IColumnBlock, 0),
	}
	for _, colSeg := range sh.Cols {
		it.Cols = append(it.Cols, colSeg.GetBlockRoot())
	}
	return it
}
