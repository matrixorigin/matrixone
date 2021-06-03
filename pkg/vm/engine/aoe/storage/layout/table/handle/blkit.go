package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
)

var (
	_ base.IBlockIterator = (*BlockIt)(nil)
)

type BlockIt struct {
	Cols []col.IColumnBlock
}

func (sit *BlockIt) Next() {
	newCols := make([]col.IColumnBlock, 0)
	for _, colBlk := range sit.Cols {
		newBlk := colBlk.GetNext()
		if newBlk == nil {
			sit.Cols = nil
			return
		}
		newCols = append(newCols, newBlk)
	}
	sit.Cols = newCols
}

func (sit *BlockIt) Valid() bool {
	if sit == nil {
		return false
	}
	return sit.Cols != nil
}

func (sit *BlockIt) GetBlockHandle() base.IBlockHandle {
	h := &BlockHandle{
		ID:   sit.Cols[0].GetID(),
		Cols: sit.Cols,
	}
	return h
}
