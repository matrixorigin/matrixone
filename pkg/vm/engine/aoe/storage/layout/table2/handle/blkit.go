package handle

import (
	hif "matrixone/pkg/vm/engine/aoe/storage/layout/table2/handle/iface"
	// "matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
)

type BlockIt struct {
	Segment *Segment
	Ids     []uint64
	Pos     int
}

func NewBlockIt(segment *Segment, blkIds []uint64) hif.IBlockIt {
	it := &BlockIt{
		Ids:     blkIds,
		Segment: segment,
	}
	return it
}

func (it *BlockIt) GetHandle() hif.IBlock {
	h := &Block{
		Data: it.Segment.Data.WeakRefBlock(it.Ids[it.Pos]),
		Host: it.Segment,
	}
	return h
}

func (it *BlockIt) Valid() bool {
	if it.Segment == nil {
		return false
	}
	if it.Pos >= len(it.Ids) {
		return false
	}
	return true
}

func (it *BlockIt) Next() {
	it.Pos++
}

func (it *BlockIt) Close() error {
	return nil
}
