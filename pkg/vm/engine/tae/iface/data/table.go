package data

import "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"

func IsSegmentID(id *common.ID) bool {
	return id.SegmentID != 0 && id.BlockID == 0
}

func IsBlockID(id *common.ID) bool {
	return id.BlockID != 0
}

type Table interface {
	GetAppender() (*common.ID, BlockAppender, error)
	SetAppender(id *common.ID) (BlockAppender, error)
	HasAppendableSegment() bool
}

// func append() {
// 	segment, err := table.GetAppendableSegment()
// 	if err == ErrNotAppendable {
// 		segMeta := tableMeta.CreateSegment()
// 		blkMeta := segMeta.CreateBlock()
// 		segment, _ := table.SetAppendableSegment(segMeta.ToID())
// 	}
// 	blk, err := segment.GetAppendableBlock()
// 	if err == ErrNotAppendable() {
// 		blkMeta := segMeta.CreateBlock()
// 		blk, _ := segment.SetAppendableBlock(blkMeta.ToID())
// 	}
// 	n := blk.PrepareAppend()
// 	err := blk.ApplyAppend(bat, offset, n)
// }
