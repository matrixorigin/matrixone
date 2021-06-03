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
	buf := itBlkAllocPool.Get().(*itBlkAlloc)
	buf.It.Alloc = buf
	for _, colSeg := range sh.Cols {
		buf.It.Cols = append(buf.It.Cols, colSeg.GetBlockRoot())
	}
	return &buf.It
}
