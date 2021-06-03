package handle

type BlockLinkIterator struct {
	SegIt *SegmentIt
	BlkIt *BlockIt
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

func (it *BlockLinkIterator) GetBlockHandle() *BlockHandle {
	return it.BlkIt.GetBlockHandle()
}
