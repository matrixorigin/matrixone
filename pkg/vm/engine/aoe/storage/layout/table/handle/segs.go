package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table"
)

type SegmentsHandle struct {
	IDS       []common.ID
	ColIdxes  []int
	TableData table.ITableData
}

func (ssh *SegmentsHandle) NewLinkIterator() *BlockLinkIterator {
	segIt := ssh.NewIterator()
	if !segIt.Valid() {
		return nil
	}

	h := segIt.GetSegmentHandle()

	it := &BlockLinkIterator{
		SegIt: segIt,
		BlkIt: h.NewIterator(),
	}
	return it
}

func (ssh *SegmentsHandle) NewIterator() *SegmentIt {
	it := &SegmentIt{
		Pos:    0,
		Handle: ssh,
	}
	return it
}
