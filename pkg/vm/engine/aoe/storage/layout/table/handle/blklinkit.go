package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
	// log "github.com/sirupsen/logrus"
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
	it.BlkIt.Close()
	it.BlkIt = h.NewIterator()
	h.Close()
}

func (it *BlockLinkIterator) Close() error {
	var err error
	if it.BlkIt != nil {
		err = it.BlkIt.Close()
	}
	return err
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
