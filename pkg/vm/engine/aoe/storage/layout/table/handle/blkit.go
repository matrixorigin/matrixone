package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
)

type BlockIt struct {
	Cols   []col.IColumnBlock
	SegPos int
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

func (sit *BlockIt) GetBlockHandle() *BlockHandle {
	h := &BlockHandle{
		ID:   sit.Cols[0].GetID(),
		Cols: sit.Cols,
	}
	return h
}
