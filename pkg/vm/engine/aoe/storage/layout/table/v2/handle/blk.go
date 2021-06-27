package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

var (
	_ dbi.IBlock = (*Block)(nil)
)

type Block struct {
	Host *Segment
	Data iface.IBlock
}

func (blk *Block) Prefetch() dbi.IBlockHandle {
	return blk.Data.StrongWrappedBlock(blk.Host.Attr)
}

func (blk *Block) GetID() uint64 {
	return blk.Data.GetMeta().ID
}

func (blk *Block) GetSegmentID() uint64 {
	return blk.Data.GetMeta().Segment.ID
}

func (blk *Block) GetTableID() uint64 {
	return blk.Data.GetMeta().Segment.TableID
}

// func (blk *Block) Close() error {
// 	return nil
// }
