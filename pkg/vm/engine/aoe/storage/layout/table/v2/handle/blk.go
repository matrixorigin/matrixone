package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

type Block struct {
	Host *Segment
	Data iface.IBlock
}

func (blk *Block) Prefetch() iface.IBlockHandle {
	return blk.Data.WeakGetWrappedBlock(blk.Host.Attr)
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

func (blk *Block) Close() error {
	return nil
}
