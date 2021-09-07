package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
)

var (
	_ dbi.IBlock = (*Block)(nil)
)

type Block struct {
	Host *Segment
	Id   uint64
}

func (blk *Block) Prefetch() dbi.IBatchReader {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetBatch(blk.Host.Attr)
}

func (blk *Block) GetID() uint64 {
	return blk.Id
}

func (blk *Block) GetSegmentID() uint64 {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetMeta().Segment.ID
}

func (blk *Block) GetTableID() uint64 {
	realBlk := blk.Host.Data.StrongRefBlock(blk.Id)
	defer realBlk.Unref()
	return realBlk.GetMeta().Segment.Table.ID
}
