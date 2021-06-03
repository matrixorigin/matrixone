package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
)

type SegmentHandle struct {
	ID   common.ID
	Pos  int
	Cols []col.IColumnSegment
}

func (sh *SegmentHandle) NewIterator() *BlockIt {
	it := &BlockIt{
		Cols:   make([]col.IColumnBlock, 0),
		SegPos: sh.Pos,
	}
	for _, colSeg := range sh.Cols {
		it.Cols = append(it.Cols, colSeg.GetBlockRoot())
	}
	return it
}
