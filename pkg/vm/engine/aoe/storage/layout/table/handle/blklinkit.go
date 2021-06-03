package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
)

var (
	_ base.IBlockIterator = (*BlockLinkIterator)(nil)
)

type BlockLinkIterator struct {
	SegIt base.ISegmentIterator
	BlkIt base.IBlockIterator
}

func (it *BlockLinkIterator) Next() {
	it.BlkIt.Next()
	if it.BlkIt.Valid() {
		return
	}

	it.SegIt.Next()
	if !it.SegIt.Valid() {
		return
	}

	h := it.SegIt.GetSegmentHandle()
	it.BlkIt = h.NewIterator()
}

func (it *BlockLinkIterator) Valid() bool {
	if it == nil {
		return false
	}
	return it.BlkIt.Valid()
}

func (it *BlockLinkIterator) GetBlockHandle() base.IBlockHandle {
	return it.BlkIt.GetBlockHandle()
}
